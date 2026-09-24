# runtime-config-control-plane-go

本地可运行的运行时配置与功能开关控制平面。支持多配置来源合并与热更新、
基于稳定主体标识的灰度判定、读写权限分离、敏感值脱敏与变更审计；所有
变更在并发访问下保持一致且可审计。

## 模块划分

| 包 | 职责 |
|---|---|
| `internal/config` | 配置模型、深拷贝、语义校验、错误分类（`Category`） |
| `internal/source` | 文件配置源、优先级合并、轮询热更新 watcher |
| `internal/store` | 版本化快照存储：CAS 提交、回滚、历史 |
| `internal/flag` | 功能开关判定（FNV-1a 分桶，0..9999） |
| `internal/security` | RBAC 权限矩阵与敏感值脱敏 |
| `internal/audit` | 审计环形日志 + JSONL 文件 sink |
| `internal/controlplane` | 编排：加载→合并→校验→CAS 提交→审计 |
| `internal/httpapi` | REST API，错误分类到 HTTP 状态码映射 |
| `cmd/rccp` | 本地运行入口 |

## 来源优先级与合并规则

- `-sources a.json,b.json` 中**排在前面的来源优先级更高**。
- 合并按 flag 名 / setting key 进行：高优先级来源一旦定义某个条目，
  低优先级来源的同名条目被整体忽略（不存在字段级拼接）。
- 每个条目的胜出来源记录在 `MergeResult.Provenance`，并写入快照的
  `Source` 字段与审计日志 `detail`，可随时解释"这个值来自哪里"。
- 任何来源加载失败（文件缺失、JSON 非法）或合并结果校验失败
  （百分比越界、类型不兼容、缺字段等），**整个合并被拒绝**，
  最后一次有效配置保持生效，绝不产生半更新状态。

## 热更新一致性语义

- 文件 watcher 按 `-poll` 间隔轮询 mtime+size 指纹，变化时触发全量
  重新加载与合并。
- 存储层只发布**完整、不可变的快照**；读取方拿到的是深拷贝，进行中的
  读取永远不会观察到半更新状态。
- 提交采用 CAS：`expected_version` 必须等于当前版本，否则返回
  `conflict` 并拒绝——并发更新不会静默覆盖。
- 合并内容与当前配置完全一致时不产生新版本（幂等跳过，审计记录
  `no-op`）。
- 热更新路径（watcher）在冲突时自动重试；非法文件内容只产生
  `rejected` 审计记录，不影响运行状态。

## 灰度与回滚规则

判定优先级：`flag 未启用` > `deny list` > `allow list` > `百分比分桶`。

- 分桶：`bucket = FNV1a(flagName + "\0" + subject) % 10000`，
  阈值 `threshold = percentage * 100`，`bucket < threshold` 即命中。
- 分桶只依赖 flag 名与主体标识，不含时间与版本号：**同一主体在配置
  未变化时结果必然稳定**。
- 边界：`percentage=0` 全关（`rollout_0_percent`），`percentage=100`
  全开（`rollout_100_percent`），bucket 恒在 0..9999 内，不会越界；
  分布测试（2 万主体）验证 10% 灰度观测比例在 8%..12% 内，无明显偏置。
- 回滚：`Rollback(target_version, expected_version)` 把历史版本的完整
  配置深拷贝为**新版本**发布（`Source=rollback`，原因可解释），历史
  本身不可变，可再次回滚。

## 权限边界

| 角色 | 权限 |
|---|---|
| `admin` | 全部：读、读敏感值、写、回滚、判定、读审计 |
| `operator` | 读、写、回滚、判定、读审计（**不能读敏感值明文**） |
| `reader` | 读配置（脱敏后）、判定开关 |
| 匿名 | 无（`unauthorized`） |

- 已认证但权限不足返回 `forbidden`；未认证返回 `unauthorized`。
- 未授权操作**不改变任何运行状态**，且以 `denied` 记入审计。
- `secret: true` 的 setting 对无 `config:read_secret` 权限的调用方
  返回 `********`；脱敏在深拷贝上进行，不污染共享快照。

## 错误分类

所有失败都带机器可读分类（`config.Category`）：
`validation_error` / `type_mismatch` / `conflict` / `unauthorized` /
`forbidden` / `not_found` / `internal_error`。HTTP 层分别映射到
400 / 400 / 409 / 401 / 403 / 404 / 500，测试据此区分失败原因。

## 本地验证

```bash
# 全部测试（含竞态检测）
go test ./... -race -v

# 启动服务（前者优先级高）
go run ./cmd/rccp -addr 127.0.0.1:8080 \
  -sources testdata/override.json,testdata/base.json \
  -poll 1s -audit-log /tmp/audit.jsonl

# 读取（reader 视角，敏感值脱敏）
curl -s -H 'X-Actor-Roles: reader' -H 'X-Actor-Id: r1' 127.0.0.1:8080/v1/config

# 判定开关
curl -s -H 'X-Actor-Roles: reader' \
  '127.0.0.1:8080/v1/flags/checkout_v2/evaluate?subject=user-7'

# 写入（operator，CAS）
curl -s -X POST -H 'X-Actor-Roles: operator' 127.0.0.1:8080/v1/config/write \
  -d '{"config":{"flags":{},"settings":{"greeting":{"value":"hola","type":"string"}}},
       "expected_version":1,"reason":"change greeting"}'

# 回滚（admin）
curl -s -X POST -H 'X-Actor-Roles: admin' 127.0.0.1:8080/v1/config/rollback \
  -d '{"target_version":1,"expected_version":2,"reason":"revert"}'

# 历史与审计
curl -s -H 'X-Actor-Roles: reader' 127.0.0.1:8080/v1/history
curl -s -H 'X-Actor-Roles: admin'   127.0.0.1:8080/v1/audit
```

热更新验证：修改 `-sources` 指向的文件，约一个 `-poll` 周期后
`/healthz` 中的 `version` 递增；写入非法 JSON 或越界百分比时版本
保持不变，审计日志出现 `rejected` 记录。

## 测试覆盖

- 并发读取与更新（`-race`，读写交错、多写者 CAS 单胜者）
- 非法配置回退（越界百分比、类型不兼容、非法 JSON、缺失文件）
- 过期版本冲突（stale `expected_version` → `conflict`）
- 未授权访问（匿名/越权 → `unauthorized`/`forbidden`，状态不变）
- 灰度边界（0%/100%/阈值等式、分布偏置、跨版本稳定性）
- 测试日志均打印配置版本、判定依据（reason/bucket/threshold）与
  错误分类（category）。
