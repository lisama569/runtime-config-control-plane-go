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

API 写入、回滚和配置文件热加载都走**同一把平面级串行锁保护的一个读-改-写
临界区**（锁序固定，读路径仍是无锁原子快照）。这带来以下保证：

- **同版本并发写恰好一个生效**：多个请求带着相同 `expected_version` 同时提交时，
  先进入临界区的成功，其余在改动任何状态前就得到 `409 STALE_VERSION`，输方的值
  不会混进赢方结果。
- **写与热加载不互相穿插**：任意时刻读到的都是完整一致的一份配置；文件热加载
  与接口写入冲突时统一按版本冲突处理，不静默覆盖；源文件的“版本号 + 文档”在
  临界区内成对读取，不会错配；从未生效的文件值不会泄漏进后续合并。
- **成功提交不会被后来的失败请求带崩**：失败/冲突只操作局部候选，生效配置不会
  悄悄退回旧值；版本号严格线性递增，不回退、不重复、不复用；可回滚历史快照与
  当时真正生效的配置一一对应（store 快照历史与 override 历史同窗口同步裁剪）。
- **对外 / 落盘 / 重启三份一致**：每次成功提交后，接口看到的生效配置、
  `state.json` 内容、重启恢复结果是同一份（持久化先于可见性）。

完整语义与失败边界见 [docs/DESIGN.md §2.1、§4](docs/DESIGN.md)。

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

# 并发一致性用例多跑几轮压时序（同版本并发写、写与热加载同时发生、真实 watcher）
go test -race -count=10 ./internal/controlplane/ \
  -run 'TestSameVersionConcurrentWritesExactlyOneWins|TestConcurrentWritesAndHotReloadNeverInterleave|TestFailedWriteNeverRollsBackCommittedState|TestRealWatcherConcurrentWithWrites'
```

这些并发用例断言：每次成功提交后**对外结果、持久化内容、重启恢复结果三者一致**，
版本严格线性，失败请求在 live 配置 / 保留历史 / 磁盘 / 重启结果里都不留痕迹，且
在 `-race` 下无告警。

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
