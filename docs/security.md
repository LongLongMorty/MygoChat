# KamaChat 安全加固

## 1. 接口限流（本次新增）

### 目的

对公开的认证类接口做 IP 级限流，防止：

- 密码暴力破解（`/login`）
- 批量注册与垃圾账号（`/register`）
- 验证码邮件滥发（`/user/sendEmailCode`，SMTP 成本 / 骚扰）
- 邮箱验证码暴力枚举（`/user/emailLogin`）

### 设计

`internal/https_server/middleware/ratelimit.go`：基于 Redis 的**固定窗口**计数。

```go
GE.POST("/login",               middleware.RateLimit("login", 20, time.Minute), v1.Login)
GE.POST("/register",            middleware.RateLimit("register", 10, time.Minute), v1.Register)
GE.POST("/user/sendEmailCode",  middleware.RateLimit("send_email_code", 5, time.Minute), v1.SendEmailCode)
GE.POST("/user/emailLogin",     middleware.RateLimit("email_login", 20, time.Minute), v1.EmailLogin)
```

- key：`ratelimit:<prefix>:<ip>`，`INCR` 后在首次计数时 `EXPIRE` 设置窗口。
- 计数超过阈值返回 `429` 与统一错误体，并计数 `kamachat_http_ratelimit_blocked_total`。
- 使用 `c.RemoteIP()`（直连地址）而非 `X-Forwarded-For`，避免伪造请求头绕过限流。
- **fail-open**：Redis 故障时放行，避免限流组件自身故障阻断登录核心链路（可用性优先）。

### 边界

- 固定窗口在窗口边界存在最多 2x 突刺，生产可替换为滑动窗口 / 令牌桶。
- 部署在反向代理后时，`RemoteIP` 会变成代理 IP；此时应配置 `SetTrustedProxies` 并改用 `ClientIP`。
- 未对已认证的业务接口限流（当前聚焦认证面）。

## 2. WebSocket 发送路径鉴权（本次新增）

WebSocket 消息发送此前直接信任客户端传入的 `send_id`，且不校验发送者与目标的关系，存在**冒充他人发消息**和**越权向任意会话发送**的风险。`Client.Read` 现在统一在路由前做三道校验：

1. **强制身份**：`message.SendId = c.Uuid`（JWT 解析出的当前用户），忽略客户端伪造值，并据此重新序列化后再进入路由。
2. **发送权限**（`CanSendTo`）：
   - U 目标：需存在正常好友关系（`contact_type=USER`、`status=NORMAL`、未删除，双向任一方向）；
   - G 目标：需为该群有效成员（复用投递路径同源的群成员 Set 缓存）。
3. **拉黑拦截**：复用既有 `IsBlocked`（双向拉黑/被拉黑）。

补充约定：

- 好友关系走 60s Redis 缓存，**建立/删除好友、拉黑/取消拉黑时主动失效**（`InvalidateSendAuth`）；群成员复用 `DelGroupMemberSet` 失效。
- 发送鉴权 **DB 故障时 fail-closed（拒绝）**，安全优先（与 `IsBlocked` 的 fail-open 取舍不同：拉黑是附加限制，关系校验是权限底线）。
- 全部错误提示统一经 `SendBack` 由写协程发出，保持"单写者"约束。

代码：`internal/service/chat/send_auth.go`、`internal/service/chat/client.go`（`Client.Read`）。

## 3. 既有安全措施（概览）

| 面向 | 措施 |
| --- | --- |
| 密码 | bcrypt 单向哈希（`varchar(60)`），登录用 `CompareHashAndPassword` 校验 |
| 认证 | JWT (HS256) 24h；密钥强制从环境变量加载且校验强度 |
| 邮箱唯一 | `(email, deleted_at)` 复合唯一索引，活跃邮箱 DB 层硬性唯一（1062 兜底） |
| 验证码 | 邮箱验证码 5 分钟有效、一次性消费（校验成功即删除） |
| 用户状态 | 中间件实时校验 `status`，禁用/删除用户存量 token 立即失效（60s 缓存 + 主动失效） |
| CORS | 白名单来源，非通配符 `*` |
| 文件下载 | 改为鉴权端点，按元数据/收发关系/群成员校验归属 |
| 文件上传 | 限制大小与 MIME，读取文件头内容检测，服务端重命名，拒绝可执行扩展名 |
| 参数可信 | 后端不信任请求体中的 `owner_id` 等身份字段，一律取 JWT 中的用户身份；WS 发送强制 `send_id = JWT uuid` |
| 发送权限 | WS 发送前校验好友关系 / 群成员身份，防越权发送（见第 2 节） |

## 4. 代码依据

- 限流中间件：`internal/https_server/middleware/ratelimit.go`（单测 `ratelimit_test.go`）
- 路由接入：`internal/https_server/https_server.go`
- 计数原语：`internal/service/redis/redis_service.go`（`Incr` / `Expire`）
- 发送鉴权：`internal/service/chat/send_auth.go`（单测 `send_auth_test.go`）、`internal/service/chat/client.go`
- 缓存失效：`internal/service/chat/contact_black.go`（`InvalidateContactBlack` 联动）、`internal/service/gorm/user_contact_service.go`（通过好友申请时）
