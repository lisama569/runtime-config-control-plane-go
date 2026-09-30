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

# 同上，但开启持久化：配置时间线与审计落盘，重启自动恢复
mkdir -p .rccp-data
go run ./cmd/rccp \
  -source base=1:testdata/base.json \
  -source override=0:testdata/override.json \
  -addr 127.0.0.1:8080 -poll 500ms \
  -persist-dir .rccp-data

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

开启持久化后还可能出现持久化分类：`PERSIST_CORRUPT / PERSIST_CHECKSUM /
PERSIST_SCHEMA / PERSIST_IO`（500，提交整体中止、不改变状态）与
`PERSIST_TOO_LARGE`（507，超过容量上限）。

## 持久化与重启恢复

传入 `-persist-dir <目录>` 后，生效配置时间线和审计流水落盘，重启自动恢复；
不传则保持纯内存行为。数据目录 `0700`、文件 `0600`（含敏感值，仅属主可读写）。

- **恢复到最后一次已生效配置**：重启后生效配置、灰度判定、可回滚历史与重启前
  一致；版本时间线继续向前，不回退 / 不重复 / 不复用已发布版本号。
- **抗异常中断**：配置先写临时文件 + fsync + 原子 rename（durability 先于可见性），
  并保留上一份完整状态 `state.json.bak`。写到一半被 kill / 断电后，主文件损坏会
  自动回退到最后一次完整提交；主备都坏则启动 fail-closed，绝不暴露中间状态。
  审计日志按 CRC 分帧，撕裂尾被识别、丢弃并截断，完整前缀照常回放。
- **拒绝不会复活**：只有已提交生效的状态落盘；被拒绝、未授权、版本冲突的请求
  不会因重启变成生效（其拒绝事件仍保留在审计里）。
- **容量可控**：默认保留最近 100 个配置版本（与内存回滚窗口一致）、状态 16 MiB、
  审计 10000 条 / 16 MiB，超限即裁剪或拒绝；裁剪后回滚窗口与恢复结果自洽。

详见 [docs/DESIGN.md §7](docs/DESIGN.md)。

## 并发写入与热加载

多人同时改配置、且配置文件在热加载时，控制平面把每次变更（API 写入、
回滚、热加载重合并）放进**单一串行提交区**原子完成，读路径仍走无锁原子
快照。因此：

- **同一版本号的多个写请求恰好一个生效**：先提交者成功，其余稳定返回
  `409 STALE_VERSION` 且什么都不改。失败方的值不会混进最终结果——不会
  出现在 `GET /v1/config`、不会落盘、重启后也不会冒出来；成功那次提交的
  **对外结果、落盘内容、重启恢复结果三者是同一份配置**。
- **写请求与热加载不会互相穿插**：任意时刻读到的都是完整一致的一份配置。
  热加载若与写竞争落败，会基于最新快照重合并并重试，直到文件变更收敛，
  但只作为独立、顺序的新版本出现；竞争失败的写请求统一按 `409` 失败。
- **失败不会带崩已成功的修改**：过期/非法/落盘失败的请求只丢弃自己的
  事务暂存内容，内存生效配置不会退回旧值，可回滚快照与当时真正生效的
  配置一致。版本号始终线性递增，不回退、不重复、不复用。
- **失败边界**：`409 STALE_VERSION`（版本冲突，调用方重新 `GET` 后重试）、
  `400`（补丁非法，全有或全无）、`401/403`（未认证/越权）一律不改任何
  运行状态；持久化类 `500/507` 表示提交整体中止、内存与磁盘都不变，可
  安全重试。所有拒绝事件仍写审计，但绝不改变配置。

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
go test -race -count=1 ./...        # 并发/回退/冲突/越权/边界/持久化恢复全覆盖
go test -race -count=5 ./internal/source/ ./internal/controlplane/ ./internal/store/ ./internal/persist/

# 并发一致性专项（同版本多写、写×热加载、回滚×写、持久化+重启三方一致）
go test -race -count=10 -run Concurrent ./internal/controlplane/
```

并发专项覆盖以下不变量（`internal/controlplane/concurrency_test.go`）：

- 同版本多写：每轮恰一个成功、其余稳定 STALE 且**不留痕**——失败值不在
  live、不在任何可回滚快照；
- 写×热加载：每个线性步骤只引入竞争双方之一的变更，绝不混合；文件变更
  最终收敛；每个中间快照都保留完整 base 配置（无撕裂）；
- 回滚×写：结果连贯、版本严格线性；失败写的键绝不残留；
- 持久化模式下并发写 / 写×热加载后**关闭重开**：恢复版本、完整历史、
  逐键值与关服前对外配置完全一致，失败值不落盘、不复活；重启后版本继续
  线性递增。

这些用例通过一个测试专用栅栏（`testPreCommit`，生产代码不设置）把原本
依赖调度的竞态变成**确定性交错**，因此在 `-race` 下连跑几十轮无告警、
无偶发失败。

手工验证“重启接着用 / 损坏回退”：

```bash
go build -o /tmp/rccp ./cmd/rccp
mkdir -p /tmp/rccp-data
/tmp/rccp -source base=1:testdata/base.json -addr 127.0.0.1:8080 -persist-dir /tmp/rccp-data &
curl -s -X POST -H 'Authorization: Bearer writer-token' \
  -d '{"expected_version":1,"properties":[{"key":"service.timeout_ms","type":"int","value":4242}]}' \
  localhost:8080/v1/config
kill -9 %1                        # 模拟强杀（写一半也安全）
/tmp/rccp -source base=1:testdata/base.json -addr 127.0.0.1:8080 -persist-dir /tmp/rccp-data
# 启动日志：recovered effective version 2 ...；下次写入从版本 3 继续
curl -s -H 'Authorization: Bearer reader-token' localhost:8080/v1/config # version=2

# 损坏回退：破坏主文件后重启，应回退到 state.json.bak 并打印分类日志
echo '{corrupt' > /tmp/rccp-data/state.json
# 主备都破坏时：启动直接失败（fail-closed），不监听端口
```

测试日志会打印**配置版本、判定依据（bucket/percentage/reason）与错误分类**，
断言结果与失败原因可区分。详细语义见 [docs/DESIGN.md](docs/DESIGN.md)。
