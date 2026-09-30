# 设计与语义说明

本控制平面由相互独立、职责分离的包组成，而非集中在单一入口：

```
cmd/rccp                  本地 HTTP 服务入口（只做装配与参数解析）
internal/source           本地来源（JSON 文件）、内容版本化、热更新轮询
internal/config           规范化模型、全量校验、编解码、快照类型化访问
internal/merge            多来源分层合并 + 来源可追溯（origin）
internal/store            原子快照指针、线性版本历史、CAS、回滚、提交前持久化钩子
internal/flag             稳定主体哈希分桶与功能开关判定
internal/auth             令牌认证与读写分离的 RBAC
internal/audit            只追加审计日志（含拒绝事件）、重启回放、可挂持久化 sink
internal/persist          崩溃安全的磁盘持久化：CRC 校验、原子写、备份与审计日志
internal/controlplane     装配以上模块：热更新重合并、API 覆盖层、HTTP API、恢复
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
- **与 API 写入共用同一条串行通道**：一次热加载是一个完整的状态迁移
  （在临界区内重读来源 → 安装 layer → 重合并 → 落盘 → 发布），它和
  API 写入、回滚由**同一把平面级互斥锁串行化**，两者绝不互相穿插。
  源文件的 `(内容版本, 文档)` 也在该临界区内成对读取，不会出现“版本号来自
  新文件、文档来自旧文件”的错配。热加载在落盘/合并阶段失败时，其候选 layer
  会回滚到上一次已接受的内容，**从没生效过的文件值不会泄漏进后续合并**。
  因为迁移已被串行化，提交时的 CAS expected 校验不会失败，热加载无需
  “读当前版本—重试”循环；丢失的文件事件与非法文件等价——下一次文件改动会
  再次产生事件。
- 轮询检测同时看 mtime 与内容签名，避免高分辨率写入下漏检。

## 2.1 并发写入与热加载同时发生时的行为

所有会改变生效配置的路径（API 写入、回滚、热加载）都收敛为**由
`controlplane.Plane` 的单把互斥锁保护的一个读-改-写临界区**，锁序恒为
`plane.mu → store.mu`（store 的读者两把锁都不需要，因为 live 快照是
不可变原子指针）。一次迁移在临界区内完成：读当前版本 → 构造**仅存在于
局部变量**的候选 override 与候选文档 → 校验 → 在快照可见之前同步落盘 →
原子发布 → 提交 override 历史。由此保证：

- **同版本并发写只有一个生效**：两个请求带着相同 `expected_version` 进入时，
  先拿到锁的提交成功；后到者在临界区里读到的已是新版本，直接以
  `409 STALE_VERSION` 失败，且失败发生在它触碰任何共享状态**之前**，
  所以输方的值不可能混进赢方的生效配置、历史快照、落盘文件或重启结果。
- **写与热加载不穿插**：任一时刻生效配置都是完整一致的一份；热加载不会用
  文件层覆盖掉尚未提交的 API 改动，API 写也不会基于半更新的 layer 合并。
  冲突统一按版本冲突失败，不做静默覆盖。
- **失败请求不留痕迹**：校验失败、版本冲突或落盘失败都只操作局部候选；
  失败路径会把 live override 恢复到“仍为当前版本所对应的那一份”，
  已成功的提交不会被随后失败的请求带回旧值。
- **版本号线性递增**：每个成功迁移恰好产生 `当前版本 + 1`，不回退、不重复、
  不复用；回滚同样发布全新线性版本。store 的快照历史窗口与 override 历史
  窗口**同步裁剪为同一版本集合**（默认各保留最新 100 个，含当前），因此
  “能回滚到哪些版本”和“回滚出来的配置是否等于当时生效配置”始终自洽。
- **三份一致**：一次成功提交对外可见的生效配置、`state.json` 落盘内容、
  以及重启恢复结果是同一份配置（持久化先于可见性，落盘内容就是支撑新快照的
  override 与完整快照；恢复只重放完整已提交状态）。

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

- 所有写入走**乐观并发控制**：请求必须携带 `expected_version`。版本检查与提交
  在同一个平面级串行临界区内完成，所以并发同版本请求被**确定性地排序**：
  恰好一个成功，其余在修改任何状态之前得到 `409 STALE_VERSION`，**不做任何
  修改**并写审计。客户端收到冲突后重新读取当前版本再重试即可，绝不会互相
  静默覆盖，也不会出现输方的值污染赢方结果。
- 单个补丁内部**全有或全无**：补丁中任一条目非法（类型错误/越界/空键），
  整个补丁拒绝，不会出现“前半个补丁生效”。
- 回滚把目标版本的**完整生效配置**（含 properties、features 与 origin）
  恢复出来，并发布一个**全新线性版本号**（时间线不回退、可继续审计）；
  目标版本已不在保留窗口时返回 `404 ROLLBACK_FAILED`。随版本保存的
  `api-override` 状态一并恢复，保证回滚后生效配置与目标版本完全一致。
  回滚也是同一临界区内的原子迁移，失败时当前快照与 override 层都不变。
- 历史窗口默认保留最近 100 个版本；store 快照历史与 override 历史裁剪为
  **完全相同的版本集合**（不再永久保留初始 v1：连续提交超过窗口后最老版本
  含 v1 一并被挤出），因此历史中可回滚的快照与当时真正生效的配置一一对应。

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

## 7. 持久化与重启恢复（`-persist-dir`）

不传 `-persist-dir` 时行为与原来**完全一致**（纯内存）。传入一个数据目录后，
生效配置时间线与审计流水落到磁盘，进程重启后自动恢复。持久化是可选叠加层，
对外的读写权限、脱敏、错误分类、灰度规则、CAS 冲突语义都不改变。

### 7.1 磁盘布局

数据目录权限 `0700`，其中文件权限 `0600`（持久化内容包含敏感值原值，仅属主可读写）：

| 文件 | 内容 | 写方式 |
| --- | --- | --- |
| `state.json` | 最后一次完整状态：当前生效快照、保留窗口内全部历史快照、每个版本的 `api-override` 状态（含墓碑） | 临时文件 + fsync + 原子 rename |
| `state.json.bak` | 上一次完整状态，作为主文件损坏时的回退 | 主文件安装前由旧主文件 rename 而来 |
| `audit.log` | 只追加的分帧审计日志（长度 + JSON 载荷 + CRC32） | O_APPEND 逐帧追加并 fsync |

`state.json` 外层是 `{format, crc32, data}` 信封，`crc32`（IEEE，十六进制）
覆盖 `data` 的**确切字节**，恢复时先校验 CRC 再解析。`data` 内为带 `schema`
版本号的状态结构。**整状态原子快照**（不是增量日志）：任意时刻磁盘上的状态
要么是上一份完整状态、要么是新一份完整状态，不存在“半个新版本”。

### 7.2 恢复规则（恢复完成前不对外服务）

启动时在 `controlplane.New` 内、任何 HTTP/goroutine 服务开始之前完成恢复：

1. `state.json` 存在且 CRC/结构合法 → 使用它；
2. 主文件损坏（坏 JSON / CRC 不符 / 结构不一致）但 `state.json.bak` 完整
   → **回退到最后一次完整可用的提交**，并在日志中按分类记录主文件问题，
   随后立即重写一份干净的主文件（自愈）；
3. 主文件缺失但备份在（rename 安装前被杀）→ 同样用备份恢复；
4. 两者都损坏/无法解析 → **启动失败、fail-closed**，绝不以损坏或中间状态对外服务；
5. 空目录 → 像以前一样从文件来源合并出 v1，并在对外服务前先把 v1 落盘；落盘失败同样中止启动。

- 审计日志按帧回放：遇到第一个损坏/截断帧（被 kill 时写了一半的尾部）即停，
  保留其前面的**完整前缀**，把损坏尾物理截断，并在恢复报告/日志中分类说明。
- 恢复出的 store 直接以磁盘时间线重建，后续版本号从“当前版本 + 1”继续：
  **不回退、不重复、也不复用已发布过的版本号**；回滚也照常产生全新线性版本。
- 每个持久化版本都带当时的 `api-override`（含墓碑）与属性版本号，因此恢复后的
  生效配置、origin、灰度判定、可回滚窗口与重启前**逐字一致**。

### 7.3 “被拒绝/未授权的请求不会在重启后复活”

只有**已提交生效**的状态进入 `state.json`。持久化在 store 的串行提交区内、
快照对读者可见**之前**同步完成（durability precedes visibility）：

- 持久化成功 → 才发布原子快照；客户端一旦观察到某版本，该版本必已落盘；
- 持久化失败（磁盘错误/超容量）→ 提交整体中止，内存与磁盘都不变，
  对客户端返回分类错误（`500 PERSIST_IO/PERSIST_CORRUPT/...` 或
  `507 PERSIST_TOO_LARGE`），客户端按 STALE 之外的错误处理，可安全重试；
- 因此写到一半被 kill，只会让“那次未完成的提交”整体缺席，恢复到它之前最后
  一次完整状态；被 401/403 挡下或 409 冲突拒绝的请求从来没有进入状态文件，
  重启不可能把它们“变成生效”。拒绝事件本身仍在 `audit.log` 中保留（可审计），
  但不会改变任何配置。

### 7.4 容量上限与裁剪（磁盘占用可控）

`persist.Limits`（默认值，可用 `Options.PersistLimits` 覆盖）：

| 限制 | 默认 | 触顶行为 |
| --- | --- | --- |
| `RetainVersions` | 100 | 状态只保留最新 100 个版本（含当前），override 数据同步裁剪 |
| `MaxStateBytes` | 16 MiB | 超过则拒绝本次提交（`PERSIST_TOO_LARGE`），不替换磁盘状态 |
| `MaxAuditEntries` | 10000 | 追加前压缩审计日志，保留最新 10000 条 |
| `MaxAuditBytes` | 16 MiB | 超过则进一步按字节收缩到放得下的最新窗口 |
| 单条审计帧 | 1 MiB | 超过则拒绝该帧（仅审计写入失败，不影响配置提交） |

状态裁剪窗口与内存 `store` 的历史窗口（默认也是 100）一致：磁盘与内存保留的
版本集合相同，裁剪后“能回滚到哪些版本”在重启前后保持自洽。内存中的 override
历史也按**同一窗口、同一边界**裁剪，不留孤立条目，所以回滚目标与当时生效配置
严格一一对应。目标版本已不在窗口内时，回滚仍返回 `404 ROLLBACK_FAILED`，语义
不变。审计日志是只增的合规流水，因此独立保留更大的窗口（默认一万条）；其裁剪
只影响可回看的审计范围，不影响配置与回滚。

> 历史窗口保留“当前 + 最近 99 个版本”。连续提交超过窗口后，最老版本（含最初
> 的 v1）会被挤出；store 快照、override 历史、`state.json` 三者裁剪边界一致。

### 7.5 稳定的新增错误分类

| Kind | 含义 |
| --- | --- |
| `PERSIST_CORRUPT` | 持久化内容无法解析或结构不一致（历史不递增、缺 override 等） |
| `PERSIST_CHECKSUM` | CRC32 不符：撕裂写或内容被改动 |
| `PERSIST_IO` | 文件系统读写/fsync/rename 失败 |
| `PERSIST_TOO_LARGE` | 状态或审计帧超过容量上限 |
| `PERSIST_SCHEMA` | 遇到不支持的磁盘格式/schema 版本 |

这些分类只用于持久化失败路径，既有 `VALIDATION/.../STALE_VERSION/ROLLBACK_FAILED`
分类与 HTTP 状态码映射保持不变。

## 8. 测试矩阵

| 文件 | 覆盖点 |
| --- | --- |
| `internal/config/config_test.go` | 缺失值/null/类型不兼容/非法类型/越界百分比，分类可区分；脱敏编解码 |
| `internal/merge/merge_test.go` | 优先级稳定（50 次合并）、origin、来源版本、非法层拒绝 |
| `internal/store/store_test.go` | CAS 过期拒绝、50 并发写无丢失、回滚完整恢复、并发读者无撕裂读 |
| `internal/store/hook_test.go` | 提交前持久化钩子失败时提交整体中止（不发布、不占版本）；钩子看到裁剪后的完整时间线；恢复式构造后续线性递增 |
| `internal/source/file_test.go` | 非法热更新不升版本、合法更新升一次、文件缺失、watcher 只放行合法更新 |
| `internal/flag/flag_test.go` | 判定稳定、0/100 边界、白名单与关闭优先级、分布均匀、特性独立打散 |
| `internal/auth/auth_test.go` | 三角色权限矩阵、401 与 403 分类分离 |
| `internal/audit/audit_test.go` | 单调序号、只追加、副本隔离 |
| `internal/audit/sink_test.go` | 重启回放后续号继续、sink 收到新条目、sink 失败不影响被审计操作 |
| `internal/persist/persist_test.go` | 往返恢复（含密钥/origin/override）、连续多次重启版本单调、坏主文件回退备份、CRC 篡改双损坏 fail-closed、撕裂临时文件保留最后完整态、超容量拒绝不落盘、裁剪窗口自洽、审计撕裂尾截断与压缩 |
| `internal/controlplane/controlplane_test.go` | 热更新回退、过期冲突、非法补丁全拒、未授权不改状态、端到端回滚、8 读者×100 写无撕裂、脱敏与灰度边界 |
| `internal/controlplane/persist_e2e_test.go` | 重启恢复生效配置/灰度/版本时间线、连续多轮重启、裁剪窗口下回滚一致、拒绝/未授权不复活且审计留存、损坏回退后自愈、裁剪后恢复一致、8×15 并发写持久化不变量与重启核对 |
| `internal/controlplane/concurrency_bugfix_test.go` | 同版本多写恰好一个生效且输方在 live/历史/落盘/重启四处无痕；API 写与热加载高并发下版本严格线性、override 历史逐版本可重合并、重启三份一致；失败请求不回退已提交状态、回滚快照与当时生效一致；真实 watcher 轮询与 API 写并发（race-clean）；超过 100 版本裁剪边界下快照历史与 override 历史窗口严格对齐（内存与重启后） |

所有测试在 `go test -race` 下通过。

## 9. 本地验证并发一致性

这些测试面向并发场景，建议始终在竞态检测下、并多跑几轮（并发缺陷可能与时序
有关）：

```bash
# 全量测试 + 竞态检测
go test -race ./...

# 只跑并发一致性用例，重复 10 轮压时序
go test -race -count=10 ./internal/controlplane/ \
  -run 'TestSameVersionConcurrentWritesExactlyOneWins|TestConcurrentWritesAndHotReloadNeverInterleave|TestFailedWriteNeverRollsBackCommittedState|TestRealWatcherConcurrentWithWrites'

# 既有用例不回归
go test -race ./internal/controlplane/
```

这些用例对每一种场景都断言同一条不变量：**每次成功提交后，对外读到的生效配置、
`state.json` 落盘内容、以及重新打开同一数据目录（连同同一批配置文件）重启恢复
出来的配置是同一份**；版本号线性递增、历史快照与其 override 逐版本可重合并；
失败/冲突请求在 live 配置、保留历史、磁盘、重启结果里都不留痕迹。

手工快速验证“同版本并发写只有一个生效”（两个请求都带 `expected_version:1`，
默认演示令牌见 `cmd/rccp`）：

```bash
go build -o /tmp/rccp ./cmd/rccp
/tmp/rccp -source base=1:testdata/base.json -persist-dir /tmp/rccp-data &
# 两个写同时打同一个版本；预期一个 200、一个 409，且 409 的键不出现在结果里
curl -s -XPOST 127.0.0.1:8080/v1/config -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":1,"properties":[{"key":"a","type":"int","value":1}]}' &
curl -s -XPOST 127.0.0.1:8080/v1/config -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":1,"properties":[{"key":"b","type":"int","value":2}]}' &
wait
curl -s 127.0.0.1:8080/v1/properties/a -H 'Authorization: Bearer reader-token'
curl -s 127.0.0.1:8080/v1/properties/b -H 'Authorization: Bearer reader-token'
# 用同样的参数重启进程后，结果与重启前完全一致
```
