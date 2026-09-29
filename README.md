# Runtime Config Control Plane (本地版)

一套可在本地直接运行的**运行时配置与功能开关控制平面**，保证配置变更、灰度判定和
权限操作在并发访问下的**一致性与可审计性**，并把已生效配置、版本历史与审计流水
**持久化到本地磁盘、重启后自动恢复**。仅依赖 Go 标准库。

## 快速开始

```bash
# 启动：两个本地 JSON 来源，名称=优先级:路径（数字越小优先级越高）
# -data-dir 指向持久化目录（默认 .rccp-data；置空则纯内存，进程退出即丢）
go run ./cmd/rccp \
  -source base=1:testdata/base.json \
  -source override=0:testdata/override.json \
  -addr 127.0.0.1:8080 -poll 500ms -data-dir .rccp-data

# 读取生效配置（敏感值自动脱敏）
curl -s -H 'Authorization: Bearer reader-token' localhost:8080/v1/config | jq .

# 灰度判定（结果随响应给出 version / bucket / reason）
curl -s -H 'Authorization: Bearer reader-token' \
  'localhost:8080/v1/features/new_checkout/evaluate?subject=alice' | jq .

# 乐观并发写入（必须携带当前版本号）
curl -s -X POST -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":1,"properties":[{"key":"service.timeout_ms","type":"int","value":4200}]}' \
  localhost:8080/v1/config

# 回滚到历史版本（仅 admin，回滚生成全新线性版本号）
curl -s -X POST -H 'Authorization: Bearer admin-token' \
  -d '{"version":1}' localhost:8080/v1/rollback

# 审计流水（仅 admin，包含被拒绝的未授权尝试）
curl -s -H 'Authorization: Bearer admin-token' localhost:8080/v1/audit | jq .
```

内置演示令牌（生产请用 `-tokens-file` 覆盖）：

| 令牌 | 角色 | 能力 |
| --- | --- | --- |
| `reader-token` | reader | 只读配置/判定 |
| `writer-token` | writer | 读 + 写入 |
| `admin-token`  | admin  | 读 + 写入 + 回滚 + 审计读取 |

## HTTP API

| 方法 & 路径 | 权限 | 说明 |
| --- | --- | --- |
| `GET  /healthz` | 无 | 健康检查 |
| `GET  /v1/config` | reader | 生效配置（敏感值 `********`，含来源 origin） |
| `GET  /v1/properties/{key}` | reader | 单个属性 |
| `GET  /v1/features/{name}/evaluate?subject=` | reader | 功能开关判定 |
| `POST /v1/config` | writer | 乐观并发写入（CAS） |
| `POST /v1/rollback` | admin | 回滚到历史版本 |
| `GET  /v1/audit` | admin | 审计流水 |

错误响应统一为 `{"error","kind","version"}`，`kind` 是稳定的错误分类：
`VALIDATION / TYPE_MISMATCH / MISSING_VALUE / NOT_FOUND / UNAUTHORIZED /
FORBIDDEN / STALE_VERSION / ROLLBACK_FAILED / PERSIST_FAILED /
STATE_CORRUPT / STATE_INCONSISTENT`。HTTP 状态码与分类一一对应
（401 未认证、403 已认证无权限、409 版本冲突、404 目标版本不存在；
持久化 I/O / 容量错误为 500，损坏或时间线不一致在启动期被拒绝）。

## 持久化与重启恢复

设置 `-data-dir`（默认 `.rccp-data`，传空字符串关闭）后，目录内保存：

| 文件 | 内容 |
| --- | --- |
| `state-NNNNNNNNN.json` | 全量状态：当前版本、保留窗口内的历史快照（含 origin/source_versions）、每个版本的 API override 层（含墓碑）。SHA-256 校验。 |
| `audit.log` | 校验帧追加式审计日志（每帧 `magic + 长度 + JSON + SHA-256`），逐条 fsync。 |

恢复与崩溃安全规则：

- **先落盘后生效**：每次提交在快照对外可见**之前**把全量状态原子写入磁盘
  （临时文件写全 → fsync → `rename` → fsync 目录）。写盘失败则本次提交整体失败，
  生效版本不变，因此磁盘与内存永远不会发散，被拒绝/未授权的请求也绝不会落盘。
- **版本时间线单调**：恢复后从最后生效版本继续 +1，不回退、不重复、不复用已发版本号；
  回滚仍然生成全新线性版本。生效配置、灰度判定（纯函数）、可回滚窗口与重启前一致。
- **异常中断可识别**：写到一半被 kill / 断电只会留下 `.tmp`（被忽略）或写坏的新一代
  文件（checksum/JSON 校验失败）。重启时自动回退到**上一份完整代次**（保留最近 2 代），
  并在日志中明确记录；若所有代次都不可用则**拒绝启动**（fail-closed），绝不暴露中间态。
- **审计连续性**：撕裂的审计尾帧会被安全截断（仅末尾），中部损坏按 `STATE_CORRUPT` 拒绝；
  恢复后审计序号从历史最大值继续，永不复用。
- **容量与裁剪**：单个状态文件上限 16 MiB（超出拒绝提交）；历史与 override 层均只保留
  最近 100 个版本（两者严格同窗口）；审计日志超过 10000 条自动压缩为最新 5000 条。
  被裁掉的版本不再可回滚（与裁剪前行为一致），保留窗口内的回滚结果始终自洽。
- **敏感值**：状态文件以 `0600` 权限保存并包含敏感原值（恢复所必需）；对外脱敏语义不变。
- **兼容**：不传 `-data-dir` / 传空目录时行为与纯内存版完全一致。

## 配置文件格式

```json
{
  "version": 1,
  "properties": {
    "service.timeout_ms": {"type": "int", "value": 1500},
    "service.name":       {"type": "string", "value": "rccp"},
    "service.sample_rate":{"type": "float", "value": 0.25},
    "service.enabled":    {"type": "bool", "value": true},
    "db.password":        {"type": "sensitive", "value": "s3cr3t"}
  },
  "features": {
    "new_checkout": {"enabled": true, "percentage": 50, "allow_list": ["qa"]}
  }
}
```

## 本地验证

```bash
go vet ./...
gofmt -l .
go test -race -count=1 ./...        # 并发/回退/冲突/越权/边界/重启恢复全覆盖
go test -race -count=5 ./internal/source/ ./internal/controlplane/ ./internal/store/ ./internal/persistence/
```

测试日志会打印**配置版本、判定依据（bucket/percentage/reason）与错误分类**，
断言结果与失败原因可区分。详细语义见 [docs/DESIGN.md](docs/DESIGN.md)。

### 手动验证重启恢复

```bash
go build -o /tmp/rccp ./cmd/rccp
mkdir -p /tmp/rccp-demo && cd /tmp/rccp-demo
cp /path/to/testdata/base.json /path/to/testdata/override.json .
./rccp -source base=1:base.json -source override=0:override.json \
       -addr 127.0.0.1:8080 -data-dir ./data &

# 写入两个版本，再发一个会被拒绝的过期请求与一个未授权请求
curl -s -X POST -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":1,"properties":[{"key":"service.timeout_ms","type":"int","value":4200}]}' \
  localhost:8080/v1/config
curl -s -X POST -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":2,"properties":[{"key":"service.name","type":"string","value":"persisted"}]}' \
  localhost:8080/v1/config

kill -9 %1            # 模拟异常中断（不是优雅退出）
# 可选：把 data/ 中最新 state-*.json 改成半截 JSON，验证自动回退到上一代
./rccp -source base=1:base.json -source override=0:override.json \
       -addr 127.0.0.1:8080 -data-dir ./data
# 启动日志包含 "persistence: recovered N committed version(s); current version M"
curl -s -H 'Authorization: Bearer reader-token' localhost:8080/v1/config   # 仍是 v2 的生效配置
curl -s -H 'Authorization: Bearer admin-token'  localhost:8080/v1/audit    # 审计（含拒绝事件）仍在
```
