# MygoChat — 高性能混合路由即时通讯服务

基于 Go + WebSocket + Kafka 构建的即时通讯服务，核心亮点是 **Channel + Kafka 混合路由引擎**，在通道拥塞时自动分流到 Kafka，实现零丢失消息投递。

## 架构亮点

### 混合路由引擎（Hybrid Router）

```
Client → WebSocket → HybridRouter ─┬─ Channel (正常路径, ~0.6ms P50)
                                     │
                                     └─ Kafka (背压溢出, 自动分流)
                                          │
                                          └─ Consumer → SessionQueue → Client
```

- **Channel 模式**：消息通过内存 channel 直接投递，延迟极低（P50 ~0.6ms）
- **背压检测**：监控 channel 深度，超过阈值（CHANNEL_SIZE × 4/5）持续 5 秒自动进入 overflow 模式
- **Kafka 分流**：overflow 模式下的消息路由到 Kafka 队列，消费者异步排空后投递到 WebSocket 客户端
- **零丢失恢复**：三轮 CHANNEL_SIZE=100 强制溢出测试验证，10000 条消息 100% 投递，gaps=0

### 三层路由架构

```
HybridRouter
  ├── SessionRouter（保序队列）
  │     ├── 每个 session 独立 goroutine + channel
  │     ├── 消息乱序缓存（最多 200 条，超时 30s）
  │     └── strict-FIFO 投递保证
  ├── BackpressureDetector
  │     ├── 每秒采样 channel 深度
  │     ├── 连续 5 次超阈值 → overflowMode = true
  │     └── 连续 5 次低阈值 → overflowMode = false
  └── KafkaConsumer
        ├── FetchMessage（不自动提交）
        ├── SessionRouter 处理成功后 CommitMessages
        └── 失败重试同一 offset（3 次后退避）
```

### 性能测试框架

自带完整的性能测试工具 `test/performance/ws_load.go`：

- 多用户并发 WebSocket 消息发送
- 接收方-only 延迟测量（排除发送方自回声）
- 序号完整性检测（gaps / duplicates）
- DB 持久化验证（`persistence_checked`）
- Kafka 消费者滞后验证（`kafka_lag_checked` / `kafka_consumer_lag`）
- 服务端路由计数器差值（`GET /metrics`）

## 测试结果

### 通道模式（CHANNEL_SIZE=4096）

| 场景 | 负载 | 完成率 | P50 | P95 | 路由 |
|------|------|--------|-----|-----|------|
| 基线 | 10u×20@10/s | 100% | **5.1ms** | **10.5ms** | 100% Ch |
| 持续 | 10u×100@20/s | 100% | **6.7ms** | **14.3ms** | 100% Ch |
| 持续 | 10u×100@50/s | 100% | **6.6ms** | **13.0ms** | 100% Ch |
| 突发 | 10u×100@100/s | 100% | **232ms** | **377ms** | 100% Ch |
| Burst | 10u×50@0/s | 100% | **357ms** | **661ms** | 100% Ch |

### 混合模式（CHANNEL_SIZE=4096）

| 场景 | 负载 | 完成率 | P50 | P95 | 路由 |
|------|------|--------|-----|-----|------|
| 低负载 | 10u×100@20/s | 100% | **0.6ms** | **0.8ms** | 100% Ch |
| 高负载 | 20u×100@100/s | 100% | **1.1ms** | **1.8ms** | 100% Ch |
| Burst | 20u×200@0/s | 100% | **78ms** | **112ms** | 100% Ch |
| 顺序保证 | 2u×200@0/s | 100% | **16ms** | **24ms** | gaps=0 |

### Kafka 溢出恢复（CHANNEL_SIZE=100，强制溢出）

| 轮次 | 通道 | Kafka | 总投递 | Gaps | 失败计数器 | 完成 |
|------|------|-------|--------|------|-----------|------|
| R1 | 721 | 9279 | 10000 | 0 | 全 0 | ✅ |
| R2 | 864 | 9136 | 10000 | 0 | 全 0 | ✅ |
| R3 | 304 | 9696 | 10000 | 0 | 全 0 | ✅ |

> 三轮全部 `completed=true`, `gaps=0, duplicates=0`, `kafka_lag_checked=true`, `kafka_consumer_lag=0`, `persistence_checked=true`, `metrics_collected=true`

## 功能特性

### 消息系统
- 单聊 / 群聊消息
- 文本 / 文件 / 图片 / 音视频通话
- 消息持久化（MySQL + Kafka）
- 消息历史查询

### 认证系统
- **邮箱注册**（SMTP 验证码，5 分钟有效期，一次性消费）
- 邮箱密码登录（bcrypt 哈希）
- 邮箱验证码登录
- JWT 鉴权（HS256，24h 有效期）
- 管理员 / 普通用户权限体系
- **邮箱唯一性**：`(email, deleted_at)` 复合唯一索引 + 0-based 软删除（`soft_delete` 插件），活跃邮箱在 DB 层硬性唯一（1062 兜底），软删后邮箱可重新注册

### 联系人 & 群组
- 添加 / 删除联系人
- 拉黑 / 取消拉黑
- 创建 / 加入 / 退出群聊
- 群成员管理

### 管理后台
- 用户管理（启用 / 禁用 / 删除）
- 群组管理（删除 / 设置状态）
- 管理员设置

## 技术栈

| 层 | 技术 |
|---|------|
| 语言 | Go 1.25 |
| Web 框架 | Gin + gorilla/websocket |
| 消息队列 | Apache Kafka 3.7（segmentio/kafka-go） |
| 数据库 | MySQL 8.0 + GORM（`gorm.io/plugin/soft_delete` 软删除） |
| 缓存 | Redis 7 |
| 认证 | JWT (HS256) + bcrypt |
| 邮件 | SMTP (SSL 465 / STARTTLS 587) |
| 部署 | Docker Compose |

## 快速开始

### 前置条件

- Go 1.25+
- Docker & Docker Compose
- MySQL 8.0
- Redis 7
- Kafka 3.7.0

### 启动服务

```bash
# 启动依赖服务
docker compose up -d

# 设置 JWT 密钥（必须 >= 32 字节）
export KAMA_JWT_SECRET="your-32-byte-secret-here"

# 配置 SMTP（可选，用于邮箱验证码）
# 编辑 configs/config.toml 中的 [emailConfig] 部分

# 启动服务器
go run ./cmd/kama_chat_server/
```

### 运行测试

```bash
# 单元测试和集成测试
go test ./...

# 性能测试
go run ./test/performance/ws_load.go \
  -users ./test/performance/users_20.json \
  -messages-per-user 100 \
  -rate-per-user 50 \
  -timeout 60s \
  -dsn "root:root@tcp(127.0.0.1:3307)/kama_chat_server?parseTime=true" \
  -metrics-url "https://127.0.0.1:8000/metrics"
```

### API 接口

#### 公开路由（无需认证）
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/login` | 邮箱密码登录 |
| POST | `/register` | 邮箱注册（需验证码） |
| POST | `/user/sendEmailCode` | 发送邮箱验证码 |
| POST | `/user/emailLogin` | 邮箱验证码登录 |
| GET | `/wss` | WebSocket 连接 |
| GET | `/metrics` | 路由计数器 |

#### 认证路由（需 JWT）
| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/user/updateUserInfo` | 更新用户信息 |
| POST | `/user/getUserInfo` | 获取用户信息 |
| POST | `/user/wsLogout` | WebSocket 登出 |
| POST | `/group/*` | 群组管理（9 条路由） |
| POST | `/session/*` | 会话管理（5 条路由） |
| POST | `/contact/*` | 联系人管理（12 条路由） |
| POST | `/message/*` | 消息查询（4 条路由） |

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `KAMA_JWT_SECRET` | JWT 签名密钥（必填） | — |
| `KAMA_MYSQL_HOST` | MySQL 主机 | 127.0.0.1 |
| `KAMA_MYSQL_PORT` | MySQL 端口 | 3307 |
| `KAMA_MYSQL_USER` | MySQL 用户名 | root |
| `KAMA_MYSQL_PASSWORD` | MySQL 密码 | root |
| `KAMA_KAFKA_BROKER` | Kafka 地址 | 127.0.0.1:9092 |
| `KAMA_KAFKA_CHAT_TOPIC` | Kafka 聊天 topic | chat_message |
| `KAMA_KAFKA_GROUP_ID` | Kafka 消费者组 | chat |
| `KAMA_CONFIG_PATH` | 配置文件路径 | configs/config.toml |

## 项目结构

```
├── api/v1/                 # HTTP 控制器
├── cmd/kama_chat_server/   # 入口
├── configs/                # 配置文件
├── docs/                   # 文档
├── internal/
│   ├── config/             # 配置加载
│   ├── dao/                # 数据库初始化
│   ├── dto/                # 请求/响应 DTO
│   ├── https_server/       # HTTP 路由 + 中间件
│   ├── model/              # GORM 模型
│   └── service/
│       ├── chat/           # 核心：混合路由、会话、客户端
│       ├── email/          # SMTP 邮件服务
│       ├── gorm/           # 业务服务层
│       ├── kafka/          # Kafka 客户端
│       └── redis/          # Redis 客户端
├── pkg/                    # 公共工具
└── test/                   # 测试
    ├── performance/        # 性能测试工具 + 结果
    └── integration/        # 集成测试
```