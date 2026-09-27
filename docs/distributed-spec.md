# KamaChat 分布式（多实例）改造 Spec

| 项 | 内容 |
| --- | --- |
| 版本 | v1.0 |
| 状态 | 已实现（P0 + P1 + P3 会话亲和） |
| 适用范围 | Hybrid 消息路由模式 |
| 关联代码 | `internal/service/presence`、`internal/service/transport`、`internal/service/chat/distributor.go` |

## 1. 背景与目标

### 1.1 现状

当前所有实时投递都挂在**进程内**的 `map[string]*Client` 上：

- `MessageProcessor.getClient()` 只查本机 `Clients`（`message_processor.go:406`），查不到直接跳过投递。
- 群扇出 `FanoutExecutor.fanout()` 只遍历本机 `fe.clients`（`fanout_executor.go:96`）。
- 在线连接、背压状态、粘滞 session 全部是进程内状态。

结论：**单实例可水平复制，但用户连到不同实例后互相收不到消息**，无法真正横向扩展。

### 1.2 目标

1. 支持多实例部署：用户 A 连实例 1、用户 B 连实例 2，A/B 之间单聊与群聊消息可实时互通。
2. **低延迟主路径零损耗**：本机用户仍走内存快路径，不引入额外 Redis 往返。
3. **可开关、可回退**：默认关闭，关闭时行为与改造前完全一致。
4. 与现有"消息先落库、实时投递尽力而为"语义保持一致。

### 1.3 非目标（本期不做）

- 多设备同时在线（沿用现有一用户一连接语义）。
- Outbox / 死信 / 幂等表（属可靠性专项，另行立项）。
- 无状态网关层方案（改动过大，不作为本期方案）。

> 跨实例顺序由 P3「会话亲和」在 opt-in 前提下提供（见第 6 节）。

## 2. 总体设计

采用**对等有状态节点 + Redis 在线路由表 + 实例间消息总线**方案，增量改造：

```
                    ┌─────────────────────────────┐
                    │        Redis                 │
                    │  presence:user:<uuid>        │  在线路由表（value=实例ID）
                    │  chat:deliver:<instanceID>   │  每实例一条 Stream
                    └─────────────────────────────┘
                                ▲        │
              LookupMany(presence)        │ Publish(remoteInstance)
                                │        ▼
  实例 A (instanceID=A)                     实例 B (instanceID=B)
  ┌──────────────────────┐                 ┌──────────────────────┐
  │ WS Clients(A本地)     │                 │ WS Clients(B本地)     │
  │ MessageProcessor      │                 │ MessageProcessor      │
  │   └ Distributor       │                 │   └ Distributor       │
  │       ├ 本地 → 直投   │                 │       ├ 本地 → 直投   │
  │       └ 远程 → Publish│                 │       └ 远程 → Publish│
  │ InstanceBus.Run()     │◀── Stream ──────│                       │
  │   └ 本地 Enqueue      │  (B 订阅自己的) │                       │
  └──────────────────────┘                 └──────────────────────┘
```

核心抽象 **Distributor**：投递时先判断目标是否本机，本机直接投递（快路径），非本机则查 presence 定位实例并按实例聚合发布。

## 3. 组件设计

### 3.1 实例身份

- 配置 `clusterConfig.instanceId`，环境变量 `KAMA_INSTANCE_ID` 覆盖。
- 缺省值：`<hostname>:<port>`（启动时计算）。
- 所有在线表与投递 Stream 都带该 ID。

### 3.2 在线路由表（Presence）

Redis key：

| key | value | TTL | 说明 |
| --- | --- | --- | --- |
| `presence:user:<uuid>` | instanceID | `presenceTTLSeconds`（默认 60s） | 用户当前所在实例 |

- **登录**：实例将 `Clients` 写入 map 后调用 `Register`。
- **心跳**：每 `heartbeatSeconds`（默认 20s）对本机全部在线用户重新 `Register` 续期，快照 UUID 后再访问 Redis，避免持锁做网络 IO。
- **注销**：登出、读协程断开（`RemoveClient`）时 `Unregister`。
- **兜底**：即使漏删，TTL 到期自动失效，视为离线。

Presence 接口：

```go
type PresenceRegistry interface {
    Register(userID string) error
    Unregister(userID string) error
    Lookup(userID string) (instanceID string, ok bool)
    LookupMany(userIDs []string) (map[string]string, error) // MGET，群扇出一次定位
}
```

### 3.3 实例间消息总线（InstanceBus）

- 每个实例一条 Redis Stream：`chat:deliver:<instanceID>`。
- 消费组 `chat-deliver`，消费者名 = instanceID；`XGroupCreateMkStream` 起始位点 `$`（忽略历史陈旧消息）。
- `XADD` 携带 `MAXLEN ~ streamMaxLen`（默认 10000）防止无限增长。
- 消费循环：先处理 pending（`XREADGROUP ... "0"` 重投未 ack），再阻塞读取新消息（`">"`, block 1s）；**处理成功才 `XACK`**，失败保留 pending 下轮重试。
- 消息体：

```go
type DeliveryMessage struct {
    Targets  []string `json:"targets"`   // 目标用户 uuid（均在本实例在线）
    Payload  []byte   `json:"payload"`   // MessageBack.Message 原始 JSON
    DedupKey string   `json:"dedup_key"` // 消息 uuid，接收端去重 + 标记已发送
    Kind     string   `json:"kind,omitempty"` // "user" | "group"
}
```

### 3.4 投递分发器（Distributor）

```
Deliver(targets, messageBack, dedupKey):
  1. 分区：遍历 targets，本机 Clients 命中 → local；否则 pending 待定位
  2. 定位：pending 一次性 LookupMany 拿到 uuid→instanceID
  3. 投递：local 直投 EnqueueDelivery（快路径）
  4. 转发：按 instanceID 聚合，每个远程实例只 Publish 一条 DeliveryMessage
```

- 关闭状态（`enabled=false`）：退化为纯本地投递，行为等同改造前。
- 接收端 `DeliverLocalBatch(dm)`：对 `dm.Targets` 逐个本机直投；用 `DedupKey|target` 做 TTL 去重，抵御 Stream 至少一次重投。
- 指标：`cross_node_publish`、`cross_node_publish_failures`、`cross_node_received`、`cross_node_dedup_dropped`。

### 3.5 接入点改造

| 位置 | 改造 |
| --- | --- |
| `MessageProcessor.forwardToUser` | 两次 `getClient` → `Distributor.Deliver([receive, send], mb, msg.Uuid)` |
| `MessageProcessor.processAudioOrVideo` | `getClient` → `Distributor.Deliver([receive], mb, msg.Uuid)` |
| `FanoutExecutor.fanout` | 本地快照循环 → `Distributor.Deliver(members, mb, mb.Uuid)` |
| `HybridRouter.Login/Logout/RemoveClient` | presence 注册 / 注销 |
| `HybridRouter.EnableCluster` | 注入 instanceID + presence + bus，启动 bus 与心跳 |

### 3.6 会话亲和（Session Affinity，可选）

目标：把"单实例内单会话有序"扩展到"跨实例单会话有序"。

- **实例注册表**：每个实例以 `chat:instance:<instanceID>`（TTL 兜底）登记，`LiveInstances()` 通过 `SCAN` 发现存活实例。
- **一致性哈希环** `SessionRing`：对存活实例建环（每实例 128 个虚拟节点，CRC32），`Home(sessionID)` 稳定映射会话到某实例。
- **入口路由**（`HybridRouter.SendMessage`）：
  - 归属本机 → `processLocal`（分配序号 + channel/Kafka），与单机一致；
  - 归属他机 → 通过实例间总线把**原始消息**转发到归属实例，由归属实例的 `handleBusMessage` 分配序号并本地处理。
- **序号权威单点**：序号只在归属实例生成，避免多实例各自生成导致序号冲突；Kafka 路径复用信封里的序号，`SessionRouter` 仍按序号重排。
- **环刷新**：每 `heartbeatSeconds` 重新拉取存活实例重建环；实例上下线时只重映射受影响的会话（一致性哈希）。
- **故障回退**：环为空或转发失败时回退本地处理（fail-open，优先可用性，可能牺牲该会话此条消息的跨实例顺序）。
- 指标：`session_forwarded`、`session_forward_failures`、`session_forward_received`。

> 取舍：会话亲和会让跨实例会话多一次总线转发（约一个本地 Redis RTT），换取跨实例单会话严格有序。默认关闭。

## 4. 配置

```toml
[clusterConfig]
enabled = false            # 总开关，默认关闭
instanceId = ""            # 空则用 hostname:port
presenceTTLSeconds = 60
heartbeatSeconds = 20
streamMaxLen = 10000
sessionAffinity = false    # 会话亲和（需 enabled=true），保证跨实例单会话有序
```

环境变量：`KAMA_CLUSTER_ENABLED`、`KAMA_INSTANCE_ID`、`KAMA_SESSION_AFFINITY`。

## 5. 数据流

### 5.1 跨实例单聊

```
B 实例 WS 收到 A 的消息
  → SessionRouter(会话队列, 保序) → MessageProcessor.processText
  → persistMessage(落库)
  → Distributor.Deliver([A, B], mb)
      ├ B 本机 → 回显直投
      └ A 非本机 → presence Lookup→实例A → XADD chat:deliver:A
  → 实例A InstanceBus 消费 → DeliverLocalBatch → A 客户端 EnqueueDelivery
```

### 5.2 跨实例群聊扇出

```
群成员 500 人，分布 3 实例（B 本机，A/C 远程）
  → Distributor 一次 LookupMany 定位
  → 本机成员直投；远程按实例聚合：XADD 到 chat:deliver:A、chat:deliver:C 各 1 条
  → 远程实例各自扇出本地成员
  对比：远程发布次数由 O(成员数) 收敛为 O(实例数)
```

### 5.3 会话亲和（开启 sessionAffinity）

```
实例X WS 收到会话 S 的消息，环上 Home(S)=实例Y
  → SendMessage 解析 session_id → 非本机
  → XADD chat:deliver:Y {type:"session", session:{S, 原始payload}}
  → 实例Y handleBusMessage → processLocal: 分配序号 → channel/Kafka → 持久化
  → Distributor 投递给会话双方（含回到实例X 的发送方）
```

## 6. 兼容性与回退

- `clusterConfig.enabled=false`（默认）：不初始化 presence / bus，Distributor 关闭，行为与改造前**逐字节等价**。
- channel / kafka 两种消息模式本期不接入集群（仅 hybrid），文档与日志明确。

## 7. 风险与取舍

| 风险 | 处理 |
| --- | --- |
| Redis 不可用导致跨实例投递失败 | 本机投递不受影响；跨实例发布失败只计数告警，消息已落库可从历史恢复 |
| Stream 至少一次重投导致重复 | 接收端 `DedupKey` TTL 去重 |
| 群扇出大量 presence 查询 | 一次 `MGET` 批量定位，避免逐条 GET |
| 实例宕机遗留 presence | TTL 兜底自动过期 |
| 跨实例乱序 | 未开会话亲和时仅保证单实例内有序；开启后由 SessionRing 固定归属实例保证 |
| 会话亲和遇归属实例宕机 | 环刷新重映射到新实例；转发失败回退本地处理，牺牲顺序保可用 |
| 会话亲和多一跳延迟 | 默认关闭；跨实例会话约多一个本地 Redis RTT，用顺序换代价 |

## 8. 验收 / 测试

单元测试：

`internal/service/chat/distributor_test.go`
1. 关闭状态：仅本地投递，不调用 presence/publisher。
2. 开启状态：本机目标直投、远程目标按实例聚合、无在线的目标丢弃。
3. `DeliverLocalBatch`：命中去重，第二次相同 `DedupKey|target` 不重复投递。

`internal/service/chat/session_ring_test.go`
1. `Home` 确定且落在实例集合内；空环返回空。
2. 移除一个实例后，多数会话映射保持不变（一致性哈希）。

`internal/service/chat/hybrid_affinity_test.go`
1. 开启亲和：非归属会话被转发到归属实例（`type=session`）。
2. 转发失败：回退本地处理，消息进入本地 `Transmit`。

手动联调建议：

```
docker compose up -d
# 起两个实例，分别指定 instanceId，用 Nginx/gateway 负载均衡 /wss
KAMA_CLUSTER_ENABLED=true KAMA_INSTANCE_ID=node-1 ... 
KAMA_CLUSTER_ENABLED=true KAMA_INSTANCE_ID=node-2 ...
# 用户 A 连 node-1，用户 B 连 node-2，互发消息验证实时到达
# 观察 /metrics 的 cross_node_* 计数
```
