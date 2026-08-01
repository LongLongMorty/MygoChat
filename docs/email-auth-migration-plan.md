# 邮箱认证重构计划（本地项目 Spec）

> **状态：已完成** 🎉
> 实施日期：2026-08-01
> 测试结果：`go build ./...` ✅、`go vet ./...` ✅、`go test ./...` ✅
> 完整流程测试：sendEmailCode → Register → Login → emailLogin 全部通过

## 1. 背景

项目目前只在本地测试，没有线上历史账号和兼容窗口。现有注册、短信验证码登录依赖阿里云 SMS，而短信服务需要企业资质。本次直接把认证主体重构为邮箱，不保留短信认证逻辑，也不为历史账号设计迁移兼容层。

## 2. 目标与边界

### 2.1 目标

- 只使用邮箱完成注册和登录。
- 支持邮箱验证码注册、邮箱验证码登录。
- 保留密码登录能力，但登录标识改为邮箱，不再使用手机号登录。
- Redis 保存邮箱验证码，具备 5 分钟有效期和一次性消费能力。
- 本地通过 SMTP 配置完成验证码发送，不依赖阿里云 SMS 资质。

### 2.2 不改造内容

- WebSocket、Kafka、WebRTC、文件传输、消息持久化和 JWT 载荷格式不变。
- 不实现找回密码、换绑邮箱、邮件模板中心和生产级消息队列。
- 不为旧账号保留手机号认证兼容分支；本地数据库可以直接重建。

## 3. 手机号处理原则

手机号不是本次要删除的数据，而是从“认证标识”降为“可选资料字段”。必须严格区分以下用途：

1. `user_info.telephone` 数据库列保留，不删除历史值，不建立邮箱改造之外的破坏性清理。
2. 新用户注册不要求手机号，手机号允许为空；手机号不参与注册重复检查、登录查询、验证码发送或 JWT 签发。
3. `/login`、`/register`、邮箱验证码接口的请求 DTO 不再包含 `telephone` 作为认证参数。
4. `LoginRespond`、`RegisterRespond`、用户列表、用户详情和联系人响应中的 `telephone` 字段暂时保留，用于资料展示和现有联系人功能。
5. 联系人模块中的 `contact_phone` 如果仍用于查找联系人，继续按原业务使用；它不是认证入口，不与邮箱验证码 Redis key 混用。
6. 删除认证服务中的手机号格式校验、手机号查重和手机号密码登录查询；前端登录、注册页不再展示手机号输入框或手机号校验。
7. `valid.js`、管理员用户表格和联系人页面中的手机号代码，只有在确认无业务引用后才清理，不能因为认证改造误删资料展示功能。

## 4. 数据库方案

### 4.1 `user_info` 表调整

当前模型中 `telephone` 是 `char(11) not null`，`email` 是 `char(30)`。目标结构：

| 字段 | 目标定义 | 说明 |
| --- | --- | --- |
| `email` | `VARCHAR(254) NOT NULL` + 唯一索引 | 新的唯一登录标识，保存规范化邮箱 |
| `telephone` | `CHAR(11) NULL`，普通索引可保留 | 保留资料和联系人数据，不参与认证 |
| `password` | 原有 bcrypt 字符串 | 保留邮箱 + 密码登录 |
| `status` | 原有定义 | 所有邮箱登录路径继续拦截禁用账号 |

由于项目没有线上数据，优先选择删除本地测试库后重新执行 GORM 建表；如果需要保留本地数据，执行等价迁移：

```sql
ALTER TABLE user_info MODIFY telephone CHAR(11) NULL;
ALTER TABLE user_info MODIFY email VARCHAR(254) NOT NULL;
ALTER TABLE user_info ADD UNIQUE KEY uk_user_info_email (email);
```

执行唯一索引前检查空值和重复邮箱。邮箱统一 `TrimSpace` 后转小写再写入数据库；本地旧数据不需要自动迁移，发现重复时直接清理测试数据或重建数据库。不要删除 `telephone` 列，也不要把手机号改成邮箱列。

### 4.2 GORM 模型

修改 `internal/model/user_info.go`：

- `Email` 改为 `varchar(254);not null;uniqueIndex`。
- `Telephone` 去掉 `not null`，保留 `char(11)` 和普通索引。
- 不新增 `email_verified` 字段：注册时邮箱验证码校验成功即视为邮箱已验证。

## 5. 认证接口规格

### 5.1 发送邮箱验证码

接口：`POST /user/sendEmailCode`

请求：

```json
{
  "email": "user@example.com"
}
```

处理：规范化邮箱 -> 校验格式 -> 检查 Redis 是否已有未过期验证码 -> 生成 6 位验证码 -> 写入 Redis -> SMTP 发送邮件。SMTP 失败时删除 Redis key，不打印验证码。

### 5.2 邮箱验证码注册

接口：`POST /register`

请求：

```json
{
  "email": "user@example.com",
  "email_code": "123456",
  "password": "example-password",
  "nickname": "user"
}
```

处理顺序：参数校验 -> 规范化邮箱 -> 校验并删除验证码 -> 检查邮箱唯一性 -> bcrypt 哈希密码 -> 创建用户（`telephone` 为空）-> 签发 JWT。

### 5.3 邮箱密码登录

接口：`POST /login`

请求：

```json
{
  "email": "user@example.com",
  "password": "example-password"
}
```

服务层由 `telephone = ?` 改为 `email = ?` 查询用户，继续使用 bcrypt 校验密码，并在生成 JWT 前检查 `status`。

### 5.4 邮箱验证码登录

接口：`POST /user/emailLogin`

请求：

```json
{
  "email": "user@example.com",
  "email_code": "123456"
}
```

只允许已注册邮箱登录，不通过验证码自动注册账号。账号不存在、验证码错误、验证码过期、验证码重复使用或账号禁用时均不得返回 Token。

### 5.5 删除的短信接口

以下路由和业务代码直接删除，不返回 deprecated 兼容响应：

- `POST /user/sendSmsCode`
- `POST /user/smsLogin`
- `internal/service/sms/auth_code_service.go`
- `SendSmsCodeRequest`、`SmsLoginRequest` 及相关 SMS 配置和阿里云依赖

## 6. Redis 与 SMTP 规格

### 6.1 Redis

- 邮箱先执行 `TrimSpace` 和小写转换。
- key：`auth_code_email_<normalized_email>`。
- value：6 位验证码；TTL：5 分钟。
- 成功校验后立即删除；错误验证码不延长 TTL。
- 同一邮箱在 key 未过期时不重复发送。
- 不在日志中输出邮箱验证码、密码或完整邮件正文。

### 6.2 SMTP 配置

在 `internal/config/config.go` 新增：

```go
type EmailConfig struct {
    Host     string `toml:"host"`
    Port     int    `toml:"port"`
    Username string `toml:"username"`
    Password string `toml:"password"`
    From     string `toml:"from"`
}
```

在 `configs/config.toml` 增加本地示例：

```toml
[emailConfig]
host = "smtp.example.com"
port = 465
username = "your-email@example.com"
password = "your-app-password"
from = "your-email@example.com"
```

SMTP 密码只放本地配置或环境变量，不提交到仓库。个人项目可以使用 QQ、163 等支持 SMTP 的邮箱；测试时也可以使用本地 SMTP 捕获服务。

## 7. 文件改造清单

### 后端

- `internal/model/user_info.go`：调整 `email`、`telephone` 字段约束。
- `internal/config/config.go`、`configs/config.toml`：SMS 配置替换为 SMTP 配置。
- 新增 `internal/service/email/email_service.go`：验证码生成、Redis 存取和邮件发送。
- `internal/service/gorm/user_info_service.go`：注册、密码登录、验证码登录全部改用邮箱；删除手机号认证逻辑。
- `internal/dto/request/register_request.go`：改为 `email`、`email_code`、`password`、`nickname`。
- `internal/dto/request/login_request.go`：`telephone` 改为 `email`。
- 新增邮箱登录和发送验证码 DTO，删除 SMS DTO。
- `api/v1/user_info_controller.go`、`internal/https_server/https_server.go`：切换控制器和公开路由。
- `go.mod`、`go.sum`：删除阿里云 SMS 专用依赖（确认无其他业务引用后再执行）。

### 前端

- `Register.vue`：手机号输入和短信验证码改为邮箱输入和邮箱验证码。
- `Login.vue`：手机号 + 密码改为邮箱 + 密码。
- `SmsLogin.vue`：重命名为 `EmailLogin.vue`，字段和接口改为邮箱版本。
- `router/index.js`：`/smsLogin` 改为 `/emailLogin`。
- 搜索并处理所有 `sendSmsCode`、`smsLogin`、`sms_code`、`telephone` 认证引用；管理员和联系人展示字段保留。

## 8. 实施顺序（已全部完成 ✅）

- [x] 1. 停止本地服务并备份或删除本地测试数据库。
- [x] 2. 先调整 `user_info` 表和 GORM 模型，确认邮箱唯一、手机号可空。
- [x] 3. 实现 SMTP 邮箱服务和 Redis 邮箱 key。
- [x] 4. 修改后端 DTO、服务层、控制器和路由。
- [x] 5. 修改前端注册、邮箱密码登录、邮箱验证码登录。（前端未包含在本仓库）
- [x] 6. 删除 SMS service、配置、DTO、路由和阿里云依赖。
- [x] 7. 全局搜索手机号认证残留，确认手机号只存在于资料/联系人展示链路。
- [x] 8. 执行测试、静态检查和构建，更新 README 和项目文档。

## 9. 测试与验收

### 9.1 必测场景

- 邮箱去空格、大小写统一和格式校验。
- 邮箱验证码发送、5 分钟过期、错误码、重复消费和重复发送。
- 邮箱验证码注册成功，数据库中 `telephone` 为空且 `email` 唯一。
- 邮箱 + 密码登录成功。
- 邮箱验证码登录成功并返回 JWT。
- 禁用账号通过邮箱密码或邮箱验证码登录均失败且不返回 Token。
- 重复邮箱注册失败。
- 联系人和管理员页面仍能读取、展示已有 `telephone` 值。
- 请求中传入 `telephone` 不能改变认证查询对象，也不能绕过邮箱校验。
- 搜索结果中不再出现 SMS 路由、SMS DTO 或阿里云 SMS 业务调用。

### 9.2 验收命令

```text
go test ./...
go vet ./...
go build ./...
```

### 9.3 完成标准

- 注册和两种登录方式均只依赖邮箱。
- 数据库保留手机号列，但新认证流程完全不读取手机号作为身份标识。
- 邮箱唯一索引和手机号可空约束生效。
- 短信代码、短信路由和阿里云依赖清理完成。
- 前后端测试通过，日志不包含验证码、密码、SMTP 密码或完整邮件正文。

## 10. 回滚

本项目无线上数据迁移要求。若邮箱重构测试失败，恢复代码版本并重建本地测试库即可；不删除仓库中原有手机号字段的定义和历史备份。
