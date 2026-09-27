# KamaChat WebSocket 连接生命周期与心跳

## 1. 建立连接与鉴权

- 路由：`GET /wss?token=<JWT>`（`internal/https_server/https_server.go`）。
- 鉴权在升级前完成：解析 JWT，从 claims 取 `uuid`，**不信任 URL 中额外的 client_id**。
- 升级后为每个连接创建 `Client`，注册进对应消息模式的在线表，并启动读、写两个 goroutine。

## 2. 读写模型

| goroutine | 职责 |
| --- | --- |
| `Client.Read` | 循环 `ReadMessage`，交给 Channel / Kafka / Hybrid 路由 |
| `Client.Write` | 从有界 `SendBack` 队列取消息写回；同时负责心跳 ping |

- 业务投递统一先进 `SendBack`（容量 4096），入队最多等 500ms；满/关闭返回错误并计数，消息已落库可从历史恢复。
- **单写者约束**：gorilla/websocket 要求同一连接只能有一个并发写者。因此欢迎语/退出语等控制消息也统一走 `SendBack`，由 `Write` goroutine 串行写出，避免与心跳 ping 并发写坏连接。

## 3. 心跳与半开连接检测（本次新增）

| 参数 | 值 | 说明 |
| --- | --- | --- |
| `wsPingPeriod` | 50s | 服务端主动发送 ping 的间隔 |
| `wsPongWait` | 60s | 超过该时长未收到 pong 判定为半开连接 |
| `wsWriteWait` | 10s | 单次写入（含 ping）的超时 |
| `wsReadLimit` | 1MB | 单条入站消息大小上限 |

机制：

- `Read` 入口设置 `SetReadDeadline(now + wsPongWait)`，并注册 `SetPongHandler` 在收到 pong 时续期。
- `Write` 内以 ticker 每 `wsPingPeriod` 发送一次 ping；发送前设置 `SetWriteDeadline`。
- 若长时间无 pong，`ReadMessage` 因读超时返回错误 → `Close` + `RemoveClient`（分布式下同时注销 presence），释放半开连接资源。

效果：NAT/网络抖动导致的"僵尸连接"不再永久占用内存，也不会让用户长期显示在线。

## 4. 断开与清理

- `done` 通道通知写协程退出；`closeOnce` 保证多路径（读错误、显式登出、清理）只关闭一次，避免 panic。
- 读错误、显式登出、服务端移除都会从在线表删除并（分布式）注销 presence。

## 5. 边界（诚实标注）

- `Upgrader.CheckOrigin` 当前返回 `true`（允许任意 Origin），生产应限制可信来源。
- 客户端自动重连由前端负责，服务端只做半开检测与清理。
- 一用户一连接（`map[uuid]*Client` 覆盖语义），多设备需另行设计。
