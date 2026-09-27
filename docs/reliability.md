# KamaChat 消息可靠性：幂等落库与死信队列

## 1. 问题

Kafka 消费采用 **at-least-once** 语义（处理成功才提交 offset，失败整批重试）。这意味着同一条消息可能被处理多次，来源包括：

- offset 提交失败后重试整批
- 进程在提交 offset 前崩溃重启，重放积压
- 批量落库重试

原实现中消息 UUID 在 `processText/processFile/processAudioOrVideo` **内部随机生成**，因此同一条消息被重放时会生成**不同的 UUID**，绕过 `message.uuid` 唯一索引，造成**重复消息落库**。

同时，批量落库使用普通 `CreateInBatches`，一旦批内出现重复 uuid 会触发 1062，导致**整批失败并反复重试**。

## 2. 设计：入口生成幂等 ID + ON CONFLICT DO NOTHING

### 2.1 入口生成稳定 ID

在消息**进入处理链路的最早位置**（`HybridRouter.processLocal`）生成一次消息 ID，写入 `MessageEnvelope.MsgID`：

```go
// 客户端携带 client_msg_id 时按 (send_id, client_msg_id) 派生（端到端去重）
// 否则随机生成
envelope := MessageEnvelope{
    SeqNum:  seqNum,
    MsgID:   deriveMsgID(sendID, clientMsgID),
    Payload: data,
}
```

- Kafka 溢出路径把**整个信封**写入 Kafka，重放时 `MsgID` 不变。
- 会话亲和转发的是原始 payload，抵达归属实例后才在 `processLocal` 生成 `MsgID`，仍然只生成一次。
- **端到端幂等**：客户端为每条消息生成一个 `client_msg_id`（前端 `genClientMsgId()`），网络重试/用户重复点击时保持不变，服务端派生出同一 `MsgID`，落库天然去重。不同发送者使用相同 `client_msg_id` 也会因 `send_id` 参与派生而不会碰撞。

### 2.2 透传到处理器

`MessageProcessor` 新增 `ProcessEnvelope(ctx, msgID, data, wait)`；`SessionRouter` 在识别到 `envelopeMessageProcessor` 接口时，把 `MsgID` 透传给处理器（未实现该接口的 mock 回退旧路径，保持兼容）。

处理器以 `resolveMessageUUID(msgID)` 作为 `message.Uuid`：`msgID` 非空即直接使用，否则回退随机值（兼容旧信封/测试）。

### 2.3 幂等落库

`MessageBatchWriter` 的两处写入（批量与关停兜底）改为：

```go
db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(messages, size)
```

MySQL 生成 `INSERT ... ON DUPLICATE KEY UPDATE id=id`：重复 uuid 静默跳过，**不报错、不重复、不阻塞整批**。

## 3. 效果与不变量

| 场景 | 原行为 | 现行为 |
| --- | --- | --- |
| Kafka offset 提交失败重放整批 | 重复消息 | 幂等跳过 |
| 进程重启后重放积压 | 重复消息 | 幂等跳过 |
| 批内出现重复 uuid | 1062，整批失败 | 静默跳过，其余正常落库 |
| 批量落库重试 | 可能重复/失败 | 安全重试 |

**不变量**：同一 `MessageEnvelope` 无论被处理多少次，`message` 表中最多存在一条对应记录。

## 4. 边界说明（诚实标注）

- **实时推送去重**：落库幂等，但重复处理仍会再次触发一次 WebSocket 推送。客户端应以消息 uuid 去重（前端按 uuid 判重即可）。这是"至少一次投递"的正常表现。
- **客户端重复发送**：已由 `client_msg_id` 覆盖——客户端重发同一条消息（相同 `client_msg_id`）会派生出同一 `MsgID`，落库幂等。注意需前端在发送时携带 `client_msg_id`；未携带则退化为随机 ID。
- **纯 Channel 路径**：正常负载消息不经过 Kafka，但不影响本机制——`MsgID` 在入口即生成，Channel/Kafka 两条路径共用。
- **channel / kafka 两种历史模式**：未经 `processLocal`，暂不接入；本文机制面向 hybrid 主路径。

## 5. 死信队列（DLQ）

### 5.1 问题

消费端对处理失败的整批消息**无限重试**。若某条消息是"毒消息"（payload 非法、持续触发错误），该分区消费位点被永久卡住，后续正常消息无法处理（队头阻塞）。

### 5.2 设计

- 新增死信主题 `chat_message_dlq`（`kafkaConfig.dlqTopic`，单分区）。
- 消费循环对**同一批**维护 `processAttempts` 计数；处理失败累加。
- 达到 `maxProcessRetries`（默认 5）：
  1. 调用 `SendToDLQ` 把整批转投死信主题，header 附带 `origin_topic/partition/offset`、`attempts`、`failed_at`、`cause`；
  2. 转投成功后 `CommitMessages` 提交该批 offset，放行后续消息；
  3. 转投失败则**不提交**，继续重试整批（绝不静默丢消息）。
- 成功处理一批后 `processAttempts` 归零。
- 指标：`kamachat_kafka_dlq_total`。
- `dlqTopic` 为空时禁用 DLQ，回退原无限重试行为。

### 5.3 边界说明

- DLQ 记录的是**整批**（`EnqueueMessagesAndWait` 只返回首个错误，无法定位到具体某条）；批内已成功落库的消息靠幂等去重，重放安全。
- DLQ 转投后若 offset 提交失败，下轮会重放并可能重复转投；DLQ 不保证去重，定位为**人工排查/重放**通道。
- Topic 仍是单副本（本地 Compose 环境），生产应多副本 + `min.insync.replicas`。

## 6. 代码依据

- 信封与入口生成：`internal/service/chat/hybrid_router.go`（`MessageEnvelope`、`processLocal`、`deriveMsgID`）
- 前端幂等键：`web/chat-server/src/views/chat/contact/ContactChat.vue`（`genClientMsgId`）
- 透传与处理：`internal/service/chat/session_router.go`（`envelopeMessageProcessor`）、`internal/service/chat/message_processor.go`（`ProcessEnvelope`、`resolveMessageUUID`）
- 幂等落库：`internal/service/chat/message_batch.go`（`clause.OnConflict{DoNothing: true}`）
- 死信队列：`internal/service/chat/hybrid_router.go`（消费循环 DLQ 分支）、`internal/service/kafka/kafka_service.go`（`SendToDLQ` / `buildDLQMessages`）
- 单测：`internal/service/chat/session_router_idempotency_test.go`（MsgID 透传）、`internal/service/kafka/kafka_service_test.go`（DLQ 元数据）
