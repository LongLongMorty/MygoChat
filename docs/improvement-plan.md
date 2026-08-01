# MyGoChat 性能与可靠性改进计划

## 1. 文档信息

| 项目 | 内容 |
| --- | --- |
| 文档版本 | v1.0 |
| 制定日期 | 2026-07-29 |
| 适用范围 | Hybrid Router、Channel、Kafka 消息处理链路 |
| 关联测试 | `test.md`、`test/performance/results/results.md` |
| 当前配置 | `messageMode=hybrid`、`CHANNEL_SIZE=4096` |

## 当前实施状态（2026-07-29）

本轮完成的是可靠性优先的第一批改造，性能目标需要使用新版本重新压测后才可更新。

| 编号 | 状态 | 本轮结果 |
| --- | --- | --- |
| P0-1 | 已完成 | 移除客户端逐条 payload 日志；Kafka 单条消费日志降为 Debug；默认日志级别为 Info，可通过 `KAMA_LOG_LEVEL=debug` 打开调试日志 |
| P0-2 | 已完成 | Hybrid Kafka 消费改为 `FetchMessage`，仅在 SessionRouter 处理成功后调用 `CommitMessages` |
| P0-3 | 部分完成 | SessionRouter 入队支持等待和超时计数；Hybrid 主路径入队失败时分流至 Kafka；传统 Channel 模式的可靠重试仍待补充 |
| P0-4 | 部分完成 | SendBack 使用 500ms 有界等待并记录超时/关闭计数，不再静默 `default` 丢弃；自动离线补发机制仍待实现 |
| P0-5 | 待完成 | 尚未将数据库持久化数、Kafka lag 和序号核对写入压测工具 |
| P1-2 | 已完成 | MySQL 配置支持 `maxOpenConns`、`maxIdleConns` 和连接最大生命周期 |
| P1-3 | 已完成 | Redis 更新改为有界异步 Worker；缓存队列满时记录计数并允许从 MySQL 回源 |
| P1-1、P1-4 | 待完成 | DB 批量写入与 Redis List/ZSet 结构改造必须在新基线确认后实施 |

## 2. 当前基线

基于 2026-07-29 的 v3 测试结果，当前系统表现如下：

| 场景 | 结果 | 结论 |
| --- | --- | --- |
| 10 用户，总发送 50 msg/s | 100% 完成，P95 约 186ms | 当前低延迟稳定基线 |
| 10 用户，总发送 150～300 msg/s | 100% 完成，P95 约 8.5～11s | 可以排空，但已出现明显积压 |
| 20 用户，总发送 100 msg/s | 100% 完成，P95 约 32s | 不能称为低延迟稳定负载 |
| 500 个连接、每个 10 条消息 | 100% 完成，P95 约 8.5s | 连接可扩展，但突发延迟较高 |
| 20 用户，总发送 1000 msg/s | 超时，最高完成率 68.4% | 溢出恢复尚未完成端到端验证 |

当前主要瓶颈为：单条数据库写入、单会话串行处理、同步 Redis JSON 缓存更新，以及客户端回写队列满时的丢弃策略。

## 3. 改进目标

### 3.1 可靠性目标

- Kafka 消息只有在业务处理成功后才提交 offset。
- Channel、SessionRouter 或 Kafka 暂时不可用时，消息进入重试路径，不静默丢弃。
- `SendBack` 队列满时不直接丢弃普通聊天消息；需要重试、离线消息或明确的慢客户端处理策略。
- 过载测试中，数据库持久化数量、Kafka 消费数量和客户端收到数量可以相互核对。

### 3.2 性能目标

- 在当前本地 Docker 单实例环境下，达到总计 80 msg/s、持续 5 分钟、成功率 100%、P95 不超过 1s。
- 以 100 msg/s 作为进阶目标；如果无法达到，记录实际稳定边界，不扩大结论。
- 500 个 WebSocket 连接保持 5 分钟，建连成功率 100%，无异常断连或服务崩溃。
- 1000 msg/s 溢出测试中，允许发送端产生积压，但连接保持期间最终完成投递，Kafka lag 回落到 0，且无序号缺口和重复消息。

## 4. 改进任务清单

### P0：可靠性与测试有效性

| 编号 | 任务 | 建议位置 | 验收标准 |
| --- | --- | --- | --- |
| P0-1 | 关闭逐条打印原始消息和完整响应，只保留错误、采样日志和计数指标 | `internal/service/chat/client.go`、`server.go`、`kafka_server.go` | 1000 msg/s 测试期间日志不再按消息线性增长；日志文件不出现完整 payload |
| P0-2 | 将 Kafka `ReadMessage` 改为 `FetchMessage`，业务成功后显式 `CommitMessages` | `internal/service/chat/hybrid_router.go`、`internal/service/kafka/kafka_service.go` | 处理失败或进程重启后消息可重新消费；offset 不提前提交 |
| P0-3 | 为 Kafka 入队失败、SessionRouter 队列满增加阻塞重试或延迟重试 | `internal/service/chat/session_router.go`、`hybrid_router.go` | 不因一次入队错误直接丢弃；重试次数、最终失败数可观测 |
| P0-4 | 为 SendBack 满载增加可靠投递策略 | `internal/service/chat/message_processor.go`、`client.go` | 慢客户端测试中 `sendback_drop_total=0`，或每次丢弃都有持久化和补发记录 |
| P0-5 | 为压测增加数据库记录数、Kafka lag 和消息序号核对 | `test/performance/ws_load.go`、测试脚本 | 每轮结果同时包含 requested、persisted、consumed、received、duplicates、sequence gaps |

### P1：降低消息处理延迟

| 编号 | 任务 | 建议方案 | 验收标准 |
| --- | --- | --- | --- |
| P1-1 | 数据库批量写入 | 按 50～100 条或 10ms 聚合后批量 INSERT；失败批次回退重试 | 单实例 DB 写入吞吐提升；80 msg/s 场景 P95 降至 1s 内 |
| P1-2 | 配置 MySQL 连接池 | 设置 `MaxOpenConns`、`MaxIdleConns`、`ConnMaxLifetime`，并记录等待时间 | 连接池等待不成为主要耗时；参数通过压测确定，不直接套用生产值 |
| P1-3 | Redis 缓存异步化 | 将缓存更新放入有界 Worker；消息主链路只负责持久化和投递 | Redis 暂停或变慢不阻塞消息处理；缓存失败可重试 |
| P1-4 | 重构 Redis 消息缓存结构 | 使用 List/ZSet + `LTRIM`/按时间清理，避免每条消息读写完整 JSON 数组 | 单次缓存更新复杂度不随历史消息长度线性增长 |
| P1-5 | 缩小 SessionRouter 锁范围 | 不持有 `queue.mu` 执行 DB、Redis 和 WebSocket I/O；保留同一 session 的顺序状态保护 | 同一 session 仍严格有序；不同 session 的排队互不阻塞 |

### P2：提升溢出吞吐和可观测性

| 编号 | 任务 | 建议方案 | 验收标准 |
| --- | --- | --- | --- |
| P2-1 | Kafka Writer 批量参数调优 | 配置 `BatchSize`、`BatchTimeout`；可靠模式保持同步确认 | 1000 msg/s 溢出时 Kafka 写入错误为 0，吞吐和 P95 有对比数据 |
| P2-2 | 增加 Kafka 分区和消费者并行度 | 以 `session_id` 作为 key，增加分区后按分区并行消费 | 同一 session 顺序不变；跨 session 吞吐可线性或亚线性提升 |
| P2-3 | 增加运行时指标 | 记录 Channel 深度、SessionQueue 深度、Kafka lag、DB 写入耗时、Redis 耗时、SendBack 丢弃数 | 每轮压测可以定位瓶颈，而不是只看最终成功率 |
| P2-4 | 将 `CHANNEL_SIZE` 配置化 | 通过配置或环境变量设置，不再继续盲目扩大缓冲 | 不同容量下比较内存、P95、溢出触发时间和完成率 |

## 5. 实施顺序

### 阶段 A：修正基准（半天）

- 删除逐条 payload 日志。
- 确认每轮使用新启动的服务、独立 Kafka topic 或清理测试数据。
- 为压测结果补充 `delivery_duration_ms`、Kafka lag、数据库持久化数和丢弃计数。
- 按 `test.md` 重新运行 50、80、100 msg/s 三档基线。

### 阶段 B：闭环可靠投递（1 天）

- 完成 Kafka 手动提交 offset。
- 为 SessionRouter 和 Kafka 入队增加重试。
- 处理 SendBack 队列满载，不允许普通消息静默丢弃。
- 增加进程重启、Kafka 暂停、慢客户端三类故障测试。

### 阶段 C：处理链路优化（1～2 天）

- 增加 MySQL 连接池参数。
- 实现 DB 批量写入并测量批大小影响。
- 将 Redis 更新移出同步关键路径。
- 缩小 SessionRouter 锁范围，确保同一会话仍有序。

### 阶段 D：复测与记录（半天）

- Channel、Kafka、Hybrid 使用同一测试矩阵，各场景至少运行 3 次。
- 低负载记录 P50/P95/P99；过载记录 Kafka lag、恢复时间和最终完成率。
- 只将 `completed=true`、成功率 100%、环境完整且重复运行一致的结果写入简历。

## 6. 风险与取舍

| 风险 | 处理方式 |
| --- | --- |
| 批量写入增加单条消息等待时间 | 限制最大等待 10ms，并比较 P50/P95 |
| 异步 Redis 导致短时间缓存延迟 | MySQL 作为事实源，缓存失败可重建 |
| Kafka 分区增加顺序管理复杂度 | 只保证同一 `session_id` 有序，跨会话不承诺顺序 |
| 增大 Channel 导致内存和尾延迟上升 | 记录 queue depth、内存和 P99，不以缓冲容量代替处理能力 |
| 慢客户端占满 SendBack | 使用离线消息/补发机制，必要时断开慢连接 |

## 7. 完成判定

本计划完成需要同时满足：

1. 可靠投递链路有明确的 offset、重试和补发语义；
2. 80 msg/s 持续测试达到 100% 投递且 P95 不超过 1s，或记录经复测确认的实际上限；
3. 1000 msg/s overflow 测试可以区分“超时未交付”和“已持久化待恢复”，不能以超时结果宣称零丢失；
4. 500 连接保持测试记录建连、心跳、断连和资源数据；
5. 所有性能结论均可从原始 JSON、服务日志和数据库/Kafka 核对结果复现。

## 8. 暂不纳入本轮范围

- 不继续单纯扩大 `CHANNEL_SIZE`。
- 不在没有对照测试的情况下更换 RocketMQ。
- 不将 Redis ZSet 或“近七天消息缓存”写入简历，除非对应结构和 TTL 已实际实现并测试。
