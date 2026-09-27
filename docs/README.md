# KamaChat 文档导航

KamaChat 是基于 **Go + WebSocket + Kafka + Redis + MySQL** 的即时通信服务，核心亮点是 **Channel + Kafka 混合路由引擎**，并已扩展**分布式多实例投递**、**消息可靠性（幂等 + 死信）**、**Prometheus/Grafana 可观测性**、**接口限流**与 **WebSocket 心跳**。

本页是文档索引。**当前行为以代码 + 本页「现行文档」为准**；下方标为「历史计划 / 待更新」的文档用于追溯设计过程，部分结论可能已过时。

## 现行文档（与代码同步）

| 文档 | 主题 | 说明 |
| --- | --- | --- |
| [architecture.md](architecture.md) | 架构总览 | 部署拓扑、分层与组件职责、发送/投递/可靠性链路图、配置开关 |
| [distributed-spec.md](distributed-spec.md) | 分布式多实例投递 | 在线路由表 presence、实例间 Redis Stream 总线、会话亲和（一致性哈希）、配置与边界 |
| [reliability.md](reliability.md) | 消息可靠性 | 入口幂等 ID + `client_msg_id` 端到端去重、`ON CONFLICT DO NOTHING` 幂等落库、死信队列 DLQ |
| [observability.md](observability.md) | 可观测性 | `/prometheus` 指标清单、Prometheus + Grafana 编排、看板与压测观察建议 |
| [security.md](security.md) | 安全加固 | 认证接口 IP 限流（Redis 固定窗口）+ 既有安全措施汇总 |
| [websocket.md](websocket.md) | WebSocket 连接 | 建连鉴权、读写模型、心跳（ping/pong + deadline）、半开连接清理、边界 |
| [业务逻辑.md](业务逻辑.md) | 分层约定 | service 层 `(message, data, ret)` 返回约定与状态码语义 |

## 架构与设计（部分为历史计划）

| 文档 | 主题 | 说明 |
| --- | --- | --- |
| [improvement-plan.md](improvement-plan.md) | 性能与可靠性改进计划 | Hybrid 链路 P0–P2 任务清单与状态；后续的分布式/可靠性工作已独立成文 |
| [email-auth-migration-plan.md](email-auth-migration-plan.md) | 邮箱认证重构 | 手机号 → 邮箱认证迁移，文档内标注**已完成** |

## 测试与性能

| 文档 | 主题 | 说明 |
| --- | --- | --- |
| [hybrid-router-test-plan.md](hybrid-router-test-plan.md) | Hybrid 路由测试方案 | 压测指标口径（投递口径、序号、lag、失败计数器）与验收标准 |
| [2c4g-performance-test-plan.md](2c4g-performance-test-plan.md) | 2C4G 容器压测计划 | 受限资源下的压测环境与目标，结果见 `test/performance/results/` |

## 面试问答

| 文档 | 主题 | 说明 |
| --- | --- | --- |
| [项目问答记录.md](项目问答记录.md) | 面试问答（长文） | 结合代码讲解架构/数据库/Kafka/WebSocket 等；**时效性提示见文首** |

## 阅读顺序建议

1. 根目录 [README](../README.md)：项目总览、架构亮点、快速开始、API。
2. 本页：按需进入专题文档。
3. `docs/项目问答记录.md`：面向面试的深挖问答。
4. 代码：`internal/service/chat/`（路由与投递）、`internal/service/gorm/`（业务）、`api/v1/`（接口）。
