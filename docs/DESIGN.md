# 设计与语义说明

本控制平面由相互独立、职责分离的包组成，而非集中在单一入口：

```
cmd/rccp                  本地 HTTP 服务入口（只做装配与参数解析）
internal/source           本地来源（JSON 文件）、内容版本化、热更新轮询
internal/config           规范化模型、全量校验、编解码、快照类型化访问
internal/merge            多来源分层合并 + 来源可追溯（origin）
internal/store            原子快照指针、线性版本历史、CAS、回滚、发布前持久化钩子
internal/flag             稳定主体哈希分桶与功能开关判定
internal/auth             令牌认证与读写分离的 RBAC
internal/audit            只追加审计日志（含拒绝事件）+ 可选持久化下沉
internal/persistence      崩溃安全的本地状态文件与校验帧审计日志、恢复/回退/裁剪
internal/controlplane     装配以上模块：启动恢复、热更新重合并、API 覆盖层、HTTP API
internal/errs             稳定的错误分类
```

## 1. 来源优先级（稳定且可解释）

- 每个来源在启动参数中带**显式优先级数字**（`name=priority:path`），
  数字越小优先级越高；优先级相同按名称字典序兜底，排序用稳定排序。
- 合并只按给定的有序层进行，**绝不依赖 map 迭代顺序或 goroutine 调度**，
  因此同一组来源的优先级结果在任何机器、任何并发时序下都相同。
- 一个键同时出现在多个来源时，**最高优先级来源整条目胜出**（不做条目内部
  半合并），语义清晰。
- 每个生效条目记录 `origin`（来源名），每次合并记录各来源的内容版本
  `source_versions`，通过 `GET /v1/config` 可直接解释“这个值是谁给的”。
- 通过 API 写入的值进入内存最高优先级层 `api-override`，只包含被显式改动
  的键；未被改动的键继续由文件来源驱动；删除用墓碑（tombstone）表达，
  所以来源优先级在热更新后依然成立且可解释。

## 2. 热更新一致性语义

- **校验前置、全量拒绝**：文件改动先经 `config.Parse` 全量校验。非法 JSON、
  缺失值、类型不兼容、`percentage` 越界等任何错误都会导致整份候选被拒绝；
  来源内容版本不递增，控制平面不发布任何东西，**最后一次有效配置保留**。
  被拒绝的事件进入审计（`source:reload / error`）与 watcher 错误通道。
- **原子快照发布**：生效配置是不可变 `Snapshot`，通过
  `atomic.Pointer[Snapshot]` 单次发布。读路径无锁，任何进行中的读取要么
  完整看到旧快照、要么完整看到新快照，**不存在半更新状态**。
- **变更不静默覆盖**：热更新重合并后用 CAS 提交；若期间发生了别的提交，
  会基于最新版本重合并重试，而不是盲目覆盖。
- 轮询检测同时看 mtime 与内容签名，避免高分辨率写入下漏检。

## 3. 灰度规则

判定是 `(subject, feature, snapshot)` 的纯函数：

1. 未知特性 → `enabled=false, reason=unknown_feature`
2. 特性关闭 → `reason=disabled`（关闭优先于白名单）
3. 主体在 `allow_list` → `enabled=true, reason=allow_list`
4. 否则按比例：`bucket = FNV-1a64(feature + \\0 + subject) mod 100`，
   `bucket < percentage` 命中。

- 配置版本不变时，同一主体结果**必然稳定**（纯函数，无随机数、无时间因子）。
- 边界精确：`percentage=0` 永不命中；`=100` 全部命中；bucket 域为
  `[0,100)`，命中概率恰为 `percentage/100`。
- 64 位哈希对模 100 的边界偏置可忽略；不同特性混入特性名独立打散。
  测试对 20000 个主体做了分布均匀性检查，并对 0%/100% 各 2000 主体验证边界。
- 判定响应与测试日志包含 `version / bucket / percentage / reason`，判定依据可解释。

## 4. 写入并发、过期冲突与回滚

- 所有写入走**乐观并发控制（CAS）**：请求必须携带 `expected_version`。
  与当前版本不符时明确返回 `409 STALE_VERSION`，**不做任何修改**并写审计；
  并发更新因此需要重新读取后重试，不会互相静默覆盖。
- 单个补丁内部**全有或全无**：补丁中任一条目非法（类型错误/越界/空键），
  整个补丁拒绝，不会出现“前半个补丁生效”。
- 回滚把目标版本的**完整生效配置**（含 properties、features 与 origin）
  恢复出来，并发布一个**全新线性版本号**（时间线不回退、可继续审计）；
  目标版本已不在保留窗口时返回 `404 ROLLBACK_FAILED`。随版本保存的
  `api-override` 状态一并恢复，保证回滚后生效配置与目标版本完全一致。
- 历史窗口默认保留最近 100 个版本（store 与 override 状态一致裁剪，
  且始终保留初始版本）。

## 5. 权限边界与脱敏

- 认证（无/错令牌 → `401 UNAUTHORIZED`）与授权（角色不足 →
  `403 FORBIDDEN`）严格区分，调用方可以区分“你是谁”与“你不能做”。
- reader 只读；writer 可读可写但**不能回滚、不能看审计**；
  只有 admin 能回滚和读取审计。未授权请求一律不改变运行状态，
  且拒绝事件同样写入审计（`result=denied`）。
- `sensitive` 类型：内部保留原值（供授权路径使用），但任何面向外部的
  序列化/视图都输出 `********` 且标记 `masked:true`；`Encode` 默认
  不导出密钥，仅 `EncodeWithSecrets` 可用于本地受控的往返。

## 6. 审计

只追加、序列号由审计器单调分配（调用方无法伪造顺序），记录 actor/角色/
动作/目标/结果（allowed|denied|error）/from/to 版本/错误分类/原因。
`List()` 返回防御性副本。配置了持久化目录时，每条记录在追加内存后额外写入
校验帧日志并 fsync；启动时恢复的条目会把序号计数器接续到历史最大值，
**跨重启绝不复用序列号**。

## 7. 持久化、崩溃安全与重启恢复

持久化由 `internal/persistence` 承担，控制平面通过 `-data-dir`（Options.DataDir）
装配；目录为空字符串时整套机制关闭，行为与纯内存版完全一致。

### 7.1 磁盘布局与写入协议

- `state-NNNNNNNNN.json`：**全量状态**（当前版本 + 保留窗口内的历史快照 +
  每个版本的 API override 层与墓碑）。代次号单调递增，只保留**最新 2 代**。
  文件末尾带 `checksum = SHA-256(去掉 checksum 字段的规范化 JSON)`。
- 写入协议是 `write(tmp) → fsync(tmp) → close → rename(tmp, final) →
  fsync(dir)`，`tmp` 与 `final` 同目录（rename 在本地文件系统上原子）。
  文件/目录权限分别是 `0600/0700`，因为状态中包含敏感值原文（恢复所必需）。
- `audit.log`：追加式校验帧，帧格式 `magic(4) | len(4,BE) | JSON | SHA-256(32)`，
  每次追加一次 write + fsync。

### 7.2 “先持久化、后生效”的提交顺序

store 在持有更新锁时先做历史裁剪，再调用**发布前钩子**，最后才把新快照写入
`atomic.Pointer`。控制平面的钩子负责同步把全量状态写到磁盘：

1. 钩子失败（I/O / 超容量）→ 回滚内存中的追加，快照指针不动，本次提交返回
   `PERSIST_FAILED`（HTTP 500）。客户端重试时基于的仍然是旧版本。
2. 钩子成功但进程在指针发布前被杀 → 磁盘多了一份完整状态。由于状态内容只包含
   已通过全部校验、已经走完 CAS 的候选，且恢复后时间线严格按文件版本为准，
   这种极端情况下重启得到的正是该次提交（等价于“已落盘即已生效”），绝不会是
   半成品：状态文件要么不存在/是 `.tmp`（忽略），要么是带正确 checksum 的全量。
3. 校验失败（非法补丁）、`STALE_VERSION`、`UNAUTHORIZED/FORBIDDEN` 都发生在
   钩子之前，**根本不会产生任何状态文件**；因此重启不会把被拒绝/被拦下的请求
   “变成生效”，只在审计日志中保留其拒绝记录。

写路径（API 写入 / 回滚 / 热更新重合并提交）由 `commitMu` 串行化，避免多个
提交交错修改 override 层；读路径依旧无锁（原子指针）。

### 7.3 启动恢复（在对外服务前一次性完成）

1. 读取状态目录：`.tmp` 与非状态文件一律忽略；按代次从新到旧尝试，JSON 解析失败
   或 checksum 不符记为 `STATE_CORRUPT` 并继续尝试上一代。
2. 最新代损坏、上一代可用 → **回退到最后一次完整提交**，并在日志中明确写出
   “fell back from X to Y”及每个损坏原因；审计日志尾部的半成品帧（恰好落在 EOF）
   被物理截断并记录。
3. 所有代次都不可用，或审计日志在**非尾部**出现损坏 → 返回
   `STATE_CORRUPT/STATE_INCONSISTENT`，控制平面 **fail-closed 拒绝启动**，
   不会短暂对外提供任何错误状态。
4. 解析成功后再做时间线一致性校验：版本必须从 1 起严格递增、无重复、
   `current_version` 等于历史最后一个版本、每个保留快照都有对应的 override 层。
   不满足则按 `STATE_INCONSISTENT` 拒绝启动。
5. 用恢复出的历史整体替换 store 的历史窗口，恢复当前 override 层，并
   `Restore` 审计条目（接续 seq）。恢复完成后才启动 HTTP 监听与文件 watcher。

因为灰度判定是 `(subject, feature, snapshot)` 的纯函数（见 §3），恢复出同一快照后，
同一主体的判定结果必然与重启前一致。恢复后下一次提交版本 = 当前版本 + 1。

### 7.4 容量上限与裁剪自洽性

- 历史快照与 override 时间线都只保留**最近 100 个版本**，且在同一次提交中一起
  裁剪（store 先裁剪、再把裁剪后的窗口交给钩子落盘），所以磁盘窗口与内存窗口
  **版本集合严格一致**，恢复后 `History()/AtVersion()` 与重启前相同。
- 单个状态文件硬上限 `16 MiB`（`persistence.MaxStateBytes`），超限拒绝提交；
  按窗口 100、单条目受控，正常长期运行不会触及。
- 审计日志条目超过 `10000` 时自动原子压缩为最新 `5000` 条。
- 被裁掉的版本不再可回滚，返回 `404 ROLLBACK_FAILED`（与重启前一致）；窗口内
  最老版本仍可回滚，回滚生成全新线性版本且生效内容与目标逐键一致。
- 状态文件始终保留 2 代，额外占用有界；旧代与遗留 `.tmp` 在每次成功保存后清扫。

## 8. 测试矩阵

| 文件 | 覆盖点 |
| --- | --- |
| `internal/config/config_test.go` | 缺失值/null/类型不兼容/非法类型/越界百分比，分类可区分；脱敏编解码 |
| `internal/merge/merge_test.go` | 优先级稳定（50 次合并）、origin、来源版本、非法层拒绝 |
| `internal/store/store_test.go` | CAS 过期拒绝、50 并发写无丢失、回滚完整恢复、并发读者无撕裂读、持久化钩子失败则不发布 |
| `internal/source/file_test.go` | 非法热更新不升版本、合法更新升一次、文件缺失、watcher 只放行合法更新 |
| `internal/flag/flag_test.go` | 判定稳定、0/100 边界、白名单与关闭优先级、分布均匀、特性独立打散 |
| `internal/auth/auth_test.go` | 三角色权限矩阵、401 与 403 分类分离 |
| `internal/audit/audit_test.go` | 单调序号、只追加、副本隔离、恢复后续号不回退 |
| `internal/persistence/persistence_test.go` | 全量状态往返（含敏感值/feature/origin）、最新代损坏回退、全损 fail-closed、checksum 篡改识别、`.tmp` 忽略、双代保留、超容量拒绝 |
| `internal/persistence/auditlog_test.go` | 校验帧追加/读取、尾帧撕裂截断且可继续追加、中部损坏 `STATE_CORRUPT`、压缩保留最新 N 条 |
| `internal/controlplane/controlplane_test.go` | 热更新回退、过期冲突、非法补丁全拒、未授权不改状态、端到端回滚、8 读者×100 写无撕裂、脱敏与灰度边界（纯内存路径无回归） |
| `internal/controlplane/recovery_test.go` | 单次与连续多次重启恢复（生效配置/历史/灰度结果/回滚窗口一致、版本不回退不重复）、被拒与未授权请求重启后不复活、审计拒绝记录与序号连续性、最新代损坏回退、全损拒绝启动、超过 100 版本裁剪后窗口与回滚自洽、4 写者×25 提交并发下无撕裂且重启后状态严格等于最后一次生效版本 |

所有测试在 `go test -race` 下通过。
