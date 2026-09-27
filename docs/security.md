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

## 2. 既有安全措施（概览）

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
| 参数可信 | 后端不信任请求体中的 `owner_id` 等身份字段，一律取 JWT 中的用户身份 |

## 3. 代码依据

- 限流中间件：`internal/https_server/middleware/ratelimit.go`（单测 `ratelimit_test.go`）
- 路由接入：`internal/https_server/https_server.go`
- 计数原语：`internal/service/redis/redis_service.go`（`Incr` / `Expire`）
