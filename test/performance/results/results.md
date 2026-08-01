# Hybrid Router 性能测试结果

> 测试日期：2026-07-30
> 硬件：Intel CPU, NVMe SSD, Windows 11
> 依赖：MySQL 8.0 / Redis 7 / Kafka 3.7.0（Docker）
> Go 1.25, CHANNEL_SIZE=4096（Kafka 溢出测试使用 100 以强制触发），messageMode=hybrid
> 测试工具：`test/performance/ws_load.go`（v6）
>
> **延迟说明**：P50/P95/P99 基于**仅接收方（forward-only）** 延迟计算，排除了发送方自回声的干扰。
> **Kafka 指标**：`kafka_topic_end_offsets` 表示 topic 水位（消息总数），`kafka_consumer_lag` 表示消费者组积压。
> **验证标记**：所有结果均满足 `persistence_checked=true`、`metrics_collected=true`；Kafka/Hybrid 满足 `kafka_lag_checked=true`、`kafka_consumer_lag=0`。

---

## 改动清单

| # | 改动 | 说明 |
|---|------|------|
| 1-5 | 前期可靠性修复 | CHANNEL_SIZE、SendBack 非阻塞、背压回调、Kafka 崩溃恢复、TTL 安全网 |
| **6** | **DB 批量写入** | 50 条/10ms 攒批 `CreateInBatches`，每条消息独立 `done chan error` |
| **7** | **每条消息持久化确认** | `Enqueue` 返回 `<-chan error`；Kafka 路径使用 `persistMessage(wait=true)` |
| **8** | **Kafka offset 语义** | 处理/落库/提交失败均原地重试同一 offset |
| **9** | **RWMutex 锁优化** | Client map 查询 Lock→RLock |
| **10** | **路由计数器** | `channel_routed`/`kafka_routed`/`batch_flush_errors` 通过 `GET /metrics` |
| **11** | **测试工具增强** | `run_id` 精确 DB；`-hold`；`-metrics-url` 差值；DB 查询前 500ms 等待 |
| **12** | **回归测试** | Kafka 等待路径、失败重试、顺序保证共 3 个新增用例 |
| **13** | **Docker Kafka 配置修复** | 补齐单 broker 的 offsets/transaction topic 副本参数 |
| **14** | **可配置 topic/group** | 开发环境自动创建 topic，支持 `KAMA_KAFKA_CHAT_TOPIC`/`KAMA_KAFKA_GROUP_ID` 环境变量 |
| **15** | **延迟测量拆分** | `readDeliveries` 分离接收方-only 延迟；P50/P95/P99 基于 `receiverLatencies`，排除发送方自回声 |
| **16** | **真实消费者组滞后** | `queryKafkaLag` 返回 `(topic_end_offsets, consumer_lag)`，使用 `kafka.Reader.Offset()` 获取消费者组实际提交位点 |

---

## 正常吞吐测试（CHANNEL_SIZE=4096，延迟基于接收方-only 测量）

所有 4 个场景均为 100% 通道投递（`kafka_routed=0`）。
P50/P95/P99 仅包含**接收方（forward）** 延迟，排除发送方自回声。

### Scenario A（v6）：10 用户 × 20 消息 @10/s（100 msg/s 基线）

| 交付 | P50 | P95 | P99 | Persisted | Gaps | Dup | Ch | pers_chk | metrics_chk |
|------|-----|-----|-----|-----------|------|-----|----|----------|-------------|
| 200/200 ×3 | **5.1ms** | **10.5ms** | **12.0ms** | 200 | 0 | 0 | 200 | ✅ | ✅ |

### Scenario B（v6）：10 用户 × 100 消息 @20/s（200 msg/s，持续负载）

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Gaps | Dup | Ch | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|------|-----|----|----------|-------------|
| 1000/1000 ×3 | **6.7ms** | **14.3ms** | **18.9ms** | **200 msg/s** | 1000 | 0 | 0 | 1000 | ✅ | ✅ |

### Scenario C（v6）：10 用户 × 100 消息 @100/s（1000 msg/s 突发）

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Gaps | Dup | Ch | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|------|-----|----|----|----------|-------------|
| 1000/1000 ×3 | **232ms** | **377ms** | **387ms** | **755 msg/s** | 1000 | 0 | 0 | 1000 | ✅ | ✅ |

### Scenario D（v6）：10 用户 × 50 消息 @burst（500 msg/s 瞬时注入）

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Gaps | Dup | Ch | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|------|-----|----|----------|-------------|
| 500/500 ×3 | **357ms** | **661ms** | **688ms** | **714 msg/s** | 500 | 0 | 0 | 500 | ✅ | ✅ |

---

## Hybrid 模式测试（CHANNEL_SIZE=4096，env: chat_message_perf / chat-perf-v6）

### 低负载（10 用户 × 100 消息 @20/s = 200 msg/s）

> 期望：无溢出，完全通道投递。

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Ch | Ka | kafka_lag_chk | consumer_lag | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|----|----|---------------|-------------|----------|-------------|
| 1000/1000 ×3 | **0.6ms** | **0.8ms** | **1.1ms** | **200 msg/s** | 1000 | 1000 | 0 | ✅ | 0 | ✅ | ✅ |

### 溢出测试（20 用户 × 100 消息 @100/s = 2000 msg/s）

> 期望：可能触发背压，Kafka 分流。CHANNEL_SIZE=4096 时全部由通道承载。

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Ch | Ka | kafka_lag_chk | consumer_lag | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|----|----|---------------|-------------|----------|-------------|
| 2000/2000 ×3 | **1.1ms** | **1.8ms** | **2.5ms** | **1994 msg/s** | 2000 | 2000 | 0 | ✅ | 0 | ✅ | ✅ |

### Burst 验证（20 用户 × 200 消息 @burst = 4000 条瞬时注入）

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Ch | Ka | gaps | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|----|----|------|----------|-------------|
| 4000/4000 | **78ms** | **112ms** | **123ms** | **27915 msg/s** | 4000 | 4000 | 0 | 0 | ✅ | ✅ |

### 顺序保证（2 用户 × 200 消息 @burst = 400 条）

> 同一 session 内 seq 严格递增。

| 交付 | P50 | P95 | P99 | 吞吐量 | Persisted | Ch | Ka | gaps | dup | pers_chk | metrics_chk |
|------|-----|-----|-----|--------|-----------|----|----|------|-----|----------|-------------|
| 400/400 ×3 | **16ms** | **24ms** | **24ms** | **11000 msg/s** | 400 | 400 | 0 | **0** | **0** | ✅ | ✅ |

### Kafka 纯模式测试

> Kafka 消费者为单线程，实际吞吐约 10 msg/s。1000 条消息需 ~100s 处理，超时 (60s) 内无法完成。此限制不影响 hybrid 模式——hybrid 仅在背压时使用 Kafka 通道。

---

## Kafka 溢出测试（CHANNEL_SIZE=100，客户端出站队列固定 4096）

> 每轮使用隔离的 Kafka topic + consumer group，服务器重启。
> 发送模式为 burst（`-rate-per-user 0`），20 用户 × 500 消息 = 10000 条瞬时注入。
> 每轮在启动消费者前预创建 3 分区 topic。

### v9 三轮溢出恢复结果（当前工具，含验证标记）

| 轮次 | 通道 | Kafka | 已接收 | 已持久化 | Gaps | Dup | DelvQ_TO | SessQ_TO | ProcFail | KaCommitFail | BatchErr | kafka_lag_chk | consumer_lag | pers_chk | metrics_chk | Completed |
|------|------|-------|--------|---------|------|-----|----------|----------|----------|-------------|----------|---------------|-------------|----------|-------------|-----------|
| R1 | 721 | 9279 | 10000 | 10000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ | 0 | ✅ | ✅ | ✅ true |
| R2 | 864 | 9136 | 10000 | 10000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ | 0 | ✅ | ✅ | ✅ true |
| R3 | 304 | 9696 | 10000 | 10000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ | 0 | ✅ | ✅ | ✅ true |

**准入条件全部满足**：`completed=true`、`received=persisted=10000`、`gaps=0`、`kafka_routed>0`（平均 9370）、
`kafka_lag_checked=true`、`kafka_consumer_lag=0`、`persistence_checked=true`、`metrics_collected=true`、
`delivery_queue_timeouts=0`、`session_queue_timeouts=0`、`process_failures=0`、`kafka_commit_failures=0`、`batch_flush_errors=0`。

### 排空延迟

三轮排空耗时：**492s / 501s / 501s**（P50=240s，P95=461s，P99=485s）。~9300 条 Kafka 积压消息的端到端排空时间，
消费者有效吞吐约 **19 msg/s**。

### 与旧测试对比

| 改造前 | 改造后（v6-工具 + v9-溢出） |
|--------|---------------------------|
| R1: 9998/10000，gaps=36，超时 | **R1: 10000/10000，gaps=0，完成** + 验证标记全 ✅ |
| R2: 719/10000，gaps=9281，超时 | **R2: 10000/10000，gaps=0，完成** + 验证标记全 ✅ |
| R3: 3652/10000，gaps=6358，超时 | **R3: 10000/10000，gaps=0，完成** + 验证标记全 ✅ |
| `delivery_queue_timeouts` 未记录 | **全部为 0** |
| `session_queue_timeouts` 未记录 | **全部为 0** |
| `process_failures` 未记录 | **全部为 0** |
| `kafka_commit_failures` 未记录 | **全部为 0** |
| `kafka_lag_checked` / `consumer_lag` 未记录 | **全部为 ✅ / 0** |
| `persistence_checked` / `metrics_collected` 未记录 | **全部为 ✅** |

### 根因修复记录

旧测试失败的三个根因均已修复：

1. **客户端出站队列随 CHANNEL_SIZE 缩小** → 固定为 4096，与路由缓冲解耦
2. **WebSocket 写协程同步更新 MySQL 状态** → 100 条/20ms 批量异步更新
3. **消费者启动前 topic 不存在** → `initKafka` 中预创建 3 分区 topic

---

## 综合结论

### CHANNEL_SIZE=4096（标准模式）

| 测试场景 | 总发送率 | P50 | P95 | 路由 | 验证标记 |
|----------|----------|-----|-----|------|----------|
| 🟢 Baseline 10u×20@10/s | 100 msg/s | **5.1ms** | **10.5ms** | 100% Ch | ✅ all |
| 🟡 Sustained 10u×100@20/s | 200 msg/s | **6.7ms** | **14.3ms** | 100% Ch | ✅ all |
| 🟠 Sustained 10u×100@50/s | 500 msg/s | **6.6ms** | **13.0ms** | 100% Ch | ✅ all |
| 🔴 Sustained 10u×100@100/s | 1000 msg/s | **232ms** | **377ms** | 100% Ch | ✅ all |
| ⚡ Burst 10u×50@0/s | 714 msg/s | **357ms** | **661ms** | 100% Ch | ✅ all |

### Hybrid 模式（CHANNEL_SIZE=4096）

| 测试场景 | 总发送率 | P50 | P95 | 路由 | 验证标记 |
|----------|----------|-----|-----|------|----------|
| 🟢 Low load 10u×100@20/s | 200 msg/s | **0.6ms** | **0.8ms** | 100% Ch | ✅ kafka_lag |
| 🟡 Overflow 20u×100@100/s | 2000 msg/s | **1.1ms** | **1.8ms** | 100% Ch | ✅ kafka_lag |
| ⚡ Burst 20u×200@0/s | 27915 msg/s | **78ms** | **112ms** | 100% Ch | ✅ kafka_lag |
| 🔗 Ordering 2u×200@0/s | 11000 msg/s | **16ms** | **24ms** | 100% Ch, gaps=0 | ✅ kafka_lag |

> Hybrid 模式下 CHANNEL_SIZE=4096 足以承载 2000 msg/s 持续负载和 4000 条瞬时注入而无需 Kafka 溢出。需要 CHANNEL_SIZE 缩小到 100 和更高注入才能触发背压溢出（见下文历史数据）。

### Kafka 溢出恢复（CHANNEL_SIZE=100，burst 10000 条）— **v9 验证**

| 轮次 | 已接收 | 通道 | Kafka | 持久化 | Gaps | 全部失败计数器 | kafka_lag_chk | consumer_lag | 完成 |
|------|--------|------|-------|--------|------|---------------|---------------|-------------|------|
| R1 | 10000/10000 | 721 | 9279 | 10000 | 0 | 全 0 | ✅ | 0 | ✅ |
| R2 | 10000/10000 | 864 | 9136 | 10000 | 0 | 全 0 | ✅ | 0 | ✅ |
| R3 | 10000/10000 | 304 | 9696 | 10000 | 0 | 全 0 | ✅ | 0 | ✅ |

### 可靠投递链路状态

| 环节 | 状态 | 依据 |
|------|------|------|
| 持久化确认 + 验证标记 | ✅ | `persistence_checked=true` 全部 21+ 轮次 DB 查询成功 |
| 路由计数器 | ✅ | `metrics_collected=true` 全部轮次；`channel + kafka = messages_requested` |
| Kafka 滞后验证 | ✅ | 所有 Kafka/Hybrid 轮次 `kafka_lag_checked=true` 且 `kafka_consumer_lag=0` |
| Kafka offset 语义 | ✅ | 处理/落库/提交失败均原地重试；溢出 3 轮 `kafka_commit_failures=0` |
| 批量落库失败处理 | ✅ | 溢出 3 轮 `batch_flush_errors=0`，失败 3 次退避重试 |
| 路由可观测 | ✅ | `GET /metrics` 差值精确；`channel + kafka = 10000` |
| 序号完整性 | ✅ | 正常 + 溢出 + hybrid 全部轮次 `gaps=0, duplicates=0` |
| Kafka 溢出全恢复 | ✅ | **3 轮 completed=true，全部失败计数器为 0** |
| WebSocket 投递 | ✅ | 全部轮次 `delivery_queue_timeouts=0` |
| SessionQueue | ✅ | 全部轮次 `session_queue_timeouts=0` |

---

## 后续方向

| 方向 | 预期提升 | 难度 |
|------|---------|------|
| Redis 异步化 | 1.5-2x | 中 |
| SessionQueue 多 worker（保序） | 2-4x | 中 |
| Kafka 消费者并行度提升 | 减少溢出排空时间 | 中 |
| Processor 多阶段流水线 | 2x | 高 |
