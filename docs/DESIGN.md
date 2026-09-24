# 设计与语义说明

本控制平面由相互独立、职责分离的包组成，而非集中在单一入口：

```
cmd/rccp                  本地 HTTP 服务入口（只做装配与参数解析）
internal/source           本地来源（JSON 文件）、内容版本化、热更新轮询
internal/config           规范化模型、全量校验、编解码、快照类型化访问
internal/merge            多来源分层合并 + 来源可追溯（origin）
internal/store            原子快照指针、线性版本历史、CAS、回滚
internal/flag             稳定主体哈希分桶与功能开关判定
internal/auth             令牌认证与读写分离的 RBAC
internal/audit            只追加审计日志（含拒绝事件）
internal/controlplane     装配以上模块：热更新重合并、API 覆盖层、HTTP API
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
`List()` 返回防御性副本。

## 7. 测试矩阵

| 文件 | 覆盖点 |
| --- | --- |
| `internal/config/config_test.go` | 缺失值/null/类型不兼容/非法类型/越界百分比，分类可区分；脱敏编解码 |
| `internal/merge/merge_test.go` | 优先级稳定（50 次合并）、origin、来源版本、非法层拒绝 |
| `internal/store/store_test.go` | CAS 过期拒绝、50 并发写无丢失、回滚完整恢复、并发读者无撕裂读 |
| `internal/source/file_test.go` | 非法热更新不升版本、合法更新升一次、文件缺失、watcher 只放行合法更新 |
| `internal/flag/flag_test.go` | 判定稳定、0/100 边界、白名单与关闭优先级、分布均匀、特性独立打散 |
| `internal/auth/auth_test.go` | 三角色权限矩阵、401 与 403 分类分离 |
| `internal/audit/audit_test.go` | 单调序号、只追加、副本隔离 |
| `internal/controlplane/controlplane_test.go` | 热更新回退、过期冲突、非法补丁全拒、未授权不改状态、端到端回滚、8 读者×100 写无撕裂、脱敏与灰度边界 |

所有测试在 `go test -race` 下通过。
