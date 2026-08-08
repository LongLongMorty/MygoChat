# Channel + Kafka Hybrid Router Test Plan

## Purpose

Measure end-to-end delivery for the chat server, distinguish burst absorption from sustained throughput, and verify that hybrid routing does not lose or reorder messages during overflow.

The test client writes real WebSocket messages. Run it only against a dedicated test database and clear `PERF-` messages before every run.

## Environment Record

Record the following beside every result:

- CPU, memory, OS, Go version, MySQL, Redis and Kafka versions.
- `CHANNEL_SIZE`, `messageMode`, Kafka topic/partition count and MySQL connection-pool settings.
- Server commit, test command, three run identifiers and any server errors.

## Metrics

`ws_load` now reports separate values:

| Metric | Meaning |
| --- | --- |
| `send_duration_ms` / `send_throughput_msg_per_sec` | Client offer rate; not delivery capacity. |
| `delivery_duration_ms` / `throughput_msg_per_sec` | Expected receivers only: received messages divided by the interval from first send to the last expected receiver delivery. Sender self-echoes do not affect this value. |
| `completed` / `timed_out` | Whether every requested message arrived before the timeout. A timed-out run is not a throughput result. |
| `success_rate_percent`, P50/P95/P99 | Delivery completeness and end-to-end latency measured at expected receivers only. |
| `delivery_queue_timeouts` / `delivery_client_closed` | Per-run WebSocket delivery handoff failures. A complete run requires both to be zero. |
| `delivery_status_queue_drops` | Dropped "sent" status updates. It must be zero; the current implementation applies writer backpressure rather than silently dropping a status update. |
| `session_queue_timeouts` / `process_failures` / `kafka_commit_failures` | Per-run session processing and Kafka consumer failures. A complete run requires all to be zero. |
| `kafka_consumer_lag` | Sum of Kafka topic high watermarks minus the consumer group's committed offsets. `-1` means the check failed and is not a pass. |
| `persistence_checked` / `metrics_collected` / `kafka_lag_checked` | Whether the corresponding observation succeeded. The client polls persistence and Kafka lag for 15 seconds by default; `false` is not a pass. |

Do not calculate throughput as `received / timeout`. The earlier `1.7 msg/s` and `1.9 msg/s` measurements used that denominator after a timeout and must be treated as failed completeness tests, not capacity figures.

## Preconditions

1. Start MySQL, Redis and Kafka; wait for the Kafka health check. The local Compose file must use one replica for the offsets topic (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`).
2. Set `messageMode` to the scenario value, restart the server, and wait for all test WebSocket users to connect.
   The client delivery queue is fixed at 4096 entries and is deliberately independent from `CHANNEL_SIZE`; record both values.
3. Start the performance server with an isolated Kafka topic and consumer group. Do not reuse `chat_message` / `chat`, which may contain historical test records:

   ```powershell
   $env:KAMA_KAFKA_CHAT_TOPIC = "chat_message_perf"
   $env:KAMA_KAFKA_GROUP_ID = "chat-perf-v6"
   ```

4. Clear test data: `DELETE FROM message WHERE content LIKE 'perf:%';`.
5. Capture server logs for enqueue failures, Kafka write/consume failures, and overflow events.

## Test Matrix

Run each case three times. Use the median for comparison and retain all raw JSON results.

| Scenario | Mode | Users | Messages/user | Rate/user | Timeout | Pass condition |
| --- | --- | ---: | ---: | ---: | --- | --- |
| Baseline | channel | 10 | 20 | 10 msg/s | 30s | `completed=true`, success 100% |
| Sustained ramp | channel | 10 | 100 | 20, 50, 100 msg/s | 45s | Find the highest complete rate |
| Burst absorption | channel | 10 | 50 | burst (`0`) | 45s | Record completion and queue behaviour |
| Kafka baseline | kafka | 10 | 100 | 20, 50 msg/s | 60s | `completed=true`, success 100% |
| Hybrid low load | hybrid | 10 | 100 | 20 msg/s | 60s | No overflow log; complete delivery |
| Hybrid overflow | hybrid | 20 | 100 | 100 msg/s | 90s | Overflow log, Kafka routing log, complete delivery |
| Ordering | hybrid | 2 | 200 | burst (`0`) | 60s | Same-session sequence is strictly increasing |

Example sustained command:

```powershell
go run ./test/performance -users ./test/performance/users_10.json `
  -messages-per-user 100 -rate-per-user 50 -timeout 45s `
  -persistence-timeout 15s `
  -dsn "root:root@tcp(127.0.0.1:3307)/kama_chat_server?parseTime=true" `
  -metrics-url "https://127.0.0.1:8000/metrics" `
  -out ./test/performance/results/channel_10u_50r_run1.json
```

Example burst command:

```powershell
go run ./test/performance -users ./test/performance/users_20.json `
  -messages-per-user 100 -rate-per-user 0 -timeout 90s `
  -persistence-timeout 15s -kafka-lag-timeout 15s `
  -dsn "root:root@tcp(127.0.0.1:3307)/kama_chat_server?parseTime=true" `
  -kafka "127.0.0.1:9092" -kafka-topic "chat_message_perf" -kafka-group "chat-perf-v6" `
  -metrics-url "https://127.0.0.1:8000/metrics" `
  -out ./test/performance/results/hybrid_burst_run1.json
```

## Overflow Verification

For the hybrid overflow test, collect the timestamp and queue depth for:

1. High-water mark detection.
2. Overflow mode activation after the configured duration.
3. Session routing to Kafka.
4. Kafka consumption, `delivery_queue_timeouts`, `session_queue_timeouts`, and every enqueue failure.

The burst test alone does not prove the configured five-second detector works; only sustained high-water traffic does.

## Reporting Template

| Mode | Load | Completed | Success | Offer rate | Delivery throughput | P95 | P99 | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| channel | 10x100 @ 50/s |  |  |  |  |  |  |  |
| kafka | 10x100 @ 50/s |  |  |  |  |  |  |  |
| hybrid | 20x100 @ 100/s |  |  |  |  |  |  |  |

Only use a result in the resume when all of the following are true: `completed=true`, `persistence_checked=true`, `metrics_collected=true`, `messages_received=messages_persisted=messages_requested`, `success_rate_percent=100`, `sequence_gaps=0`, `duplicates=0`, `delivery_queue_timeouts=0`, `delivery_client_closed=0`, `delivery_status_queue_drops=0`, `session_queue_timeouts=0`, `process_failures=0`, `kafka_commit_failures=0`, and, for Kafka or hybrid runs, `kafka_lag_checked=true` and `kafka_consumer_lag=0`. Record the environment and retain three raw JSON results; report their median, never the best single run.

## Current Status

The pre-v6 burst results are retained only as historical debugging evidence. They used a tool that could count a sender's self-echo as delivery and did not query consumer-group committed offsets, so they must not be reported as expected-receiver latency or verified Kafka lag. Re-run the matrix with the current tool before updating the resume.

## 2C4G Container Scenario (2026-08-08)

> 面向简历的核心压测场景：服务器容器限制 **2 核 / 4GB**（`deploy.resources.limits`），
> 详见 [docs/2c4g-performance-test-plan.md](./2c4g-performance-test-plan.md)。

### 环境差异

| 项 | 宿主机模式 | 2C4G 容器模式 |
|---|---|---|
| 服务器 | `go run`（宿主） | `kamachat-server` 容器（2C4G） |
| 依赖 | Docker 容器（host 端口） | 同栈容器（服务名连通） |
| 业务端口 | 127.0.0.1:8000 | 容器映射 8000（Docker NAT） |
| pprof | 无（历史） | **8091 端口，压测期间可采样** |
| 压测延迟 | 直连 | 含 Docker NAT 端到端延迟（略高，预期） |

### 登录适配

`ws_load` 使用 **email 登录**（`login` 接口为 email 驱动）。测试用户文件须含 `email` 字段：

```json
{"uuid":"","nickname":"perf-1","email":"perf1@test.local","password":"Perf123456!","token":""}
```

参考 `test/performance/users_20_email.json`（已注册 perf1-20@test.local）。

### 5000 QPS 积压用例（0 丢包 + 顺序一致）

```bash
go run ./test/performance/ws_load.go \
  -users ./test/performance/users_20_email.json \
  -messages-per-user 250 -rate-per-user 250 -timeout 90s \
  -dsn "root:root@tcp(127.0.0.1:3307)/kama_chat_server?parseTime=true"
```

### pprof 稳定性采样

压测期间采集（佐证"系统稳定不崩溃"）：

```bash
curl http://127.0.0.1:8091/debug/pprof/goroutine?debug=1
curl http://127.0.0.1:8091/debug/pprof/heap?debug=1
```

实测：goroutine 27→27→27（无泄漏），内存 15→23→14MB（压测后回落）。
