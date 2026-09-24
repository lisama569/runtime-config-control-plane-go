# Runtime Config Control Plane (本地版)

一套可在本地直接运行的**运行时配置与功能开关控制平面**，保证配置变更、灰度判定和
权限操作在并发访问下的**一致性与可审计性**。仅依赖 Go 标准库。

## 快速开始

```bash
# 启动：两个本地 JSON 来源，名称=优先级:路径（数字越小优先级越高）
go run ./cmd/rccp \
  -source base=1:testdata/base.json \
  -source override=0:testdata/override.json \
  -addr 127.0.0.1:8080 -poll 500ms

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
FORBIDDEN / STALE_VERSION / ROLLBACK_FAILED`。HTTP 状态码与分类一一对应
（401 未认证、403 已认证无权限、409 版本冲突、404 目标版本不存在）。

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
go test -race -count=1 ./...        # 并发/回退/冲突/越权/边界全覆盖
go test -race -count=5 ./internal/source/ ./internal/controlplane/ ./internal/store/
```

测试日志会打印**配置版本、判定依据（bucket/percentage/reason）与错误分类**，
断言结果与失败原因可区分。详细语义见 [docs/DESIGN.md](docs/DESIGN.md)。
