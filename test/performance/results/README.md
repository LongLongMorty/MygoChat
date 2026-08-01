# Channel + Kafka 混合路由性能测试结果

> 测试日期：2026-07-16
> 测试计划：docs/hybrid-router-test-plan.md

---

## 测试基础设施

### 环境配置

| 组件 | 配置 |
|---|---|
| CPU | - |
| 内存 | - |
| 磁盘 | NVMe SSD |
| 操作系统 | Windows 11 |
| Go 版本 | 1.25 |
| MySQL | 8.0 (Docker) |
| Redis | 7 (Docker) |
| Kafka | 3.7.0 (Docker，待拉取) |
| CHANNEL_SIZE | 100 |

### 依赖服务

```bash
# 已就绪：docker-compose.yml 包含 mysql, redis, kafka
docker compose up -d mysql redis   # ✅ MySQL:3307, Redis:6380
docker compose up -d kafka         # ⏳ 需先拉取 apache/kafka:3.7.0 镜像
```

### 测试用户

| 文件 | 用户数 | 消息数/用户 | 超时 |
|---|---|---|---|
| `test/performance/users_2.json` | 2 | 200 | 60s |
| `test/performance/users_10.json` | 10 | 300 | 90s |
| `test/performance/users_20.json` | 20 | 500 | 120s |

用户密码统一为 `test123`，测试工具通过 `/login` 自动登录获取 JWT Token。

---

## 测试执行步骤

### 步骤 1：Channel 模式基线

```bash
# 1. 配置 channel 模式
#    修改 configs/config.toml: messageMode = "channel"

# 2. 启动服务器
set KAMA_JWT_SECRET=your_32_byte_secret_here
set KAMA_MYSQL_PASSWORD=root
.\kama_chat_server.exe

# 3. 在另一个终端执行测试
go run .\test\performance\ws_load.go ^
  -users .\test\performance\users_20.json ^
  -messages-per-user 200 -timeout 60s ^
  -out .\test\performance\results\channel_2users.json
```

### 步骤 2：Kafka 模式基线

```bash
# 1. 先启动 Kafka
docker compose up -d kafka

# 2. 修改配置: messageMode = "kafka"

# 3. 重启服务器 + 执行测试（同上）
```

### 步骤 3：Hybrid 模式降级测试

```bash
# 1. 确保 Kafka 已启动
# 2. 修改配置: messageMode = "hybrid"
# 3. 重启服务器

# 低负载预热（10 用户 × 100 消息）
go run .\test\performance\ws_load.go ^
  -users .\test\performance\users_10.json ^
  -messages-per-user 100 -timeout 60s ^
  -out .\test\performance\results\hybrid_warmup.json

# 高负载触发降级（50 用户 × 1000 消息）
go run .\test\performance\ws_load.go ^
  -users .\test\performance\users_20.json ^
  -messages-per-user 1000 -timeout 180s ^
  -out .\test\performance\results\hybrid_overflow.json
```

---

## 预期结果（推断值）

以下数据基于架构分析推算，实际值以测试结果为准。

### Channel 模式

| 用户数 | 吞吐量 (msg/s) | P50 (ms) | P95 (ms) | P99 (ms) | 成功率 |
|---|---|---|---|---|---|
| 2 | ~3500 | <10 | <20 | <40 | >99% |
| 10 | ~4200 | <15 | <30 | <50 | >99% |
| 20 | ~3800 | <20 | <40 | <70 | >98% |
| 50 | ~3000 | <35 | <60 | <100 | >95% |

**瓶颈分析**：CHANNEL_SIZE=100 时，50 用户同时发送可能触发背压。实际成功率低于 99% 时说明需要混合路由。

### Kafka 模式

| 用户数 | 吞吐量 (msg/s) | P50 (ms) | P95 (ms) | P99 (ms) | 成功率 |
|---|---|---|---|---|---|
| 20 | ~2200 | ~30 | ~100 | ~200 | >99.5% |

### Hybrid 模式

| 阶段 | 吞吐量 (msg/s) | P95 (ms) | 降级状态 |
|---|---|---|---|
| 低负载预热（10×100） | ~3500 | <30 | Channel 模式 |
| 高负载触发（20×500） | ~3000→2200 | <50→<150 | 5s 后触发 Kafka 分流 |
| 持续高负载（20×1000） | ~2200 稳定 | <150 | Kafka 接收 |

### 关键验证点

1. **降级触发**：channel 深度 > 80 持续 5s → 日志输出"触发背压溢出模式"
2. **消息顺序**：同一 session_id 的消息按序到达（序号的连续性）
3. **消息丢失**：0%（channel 满时 HybridRouter 自动转向 Kafka）
4. **收敛恢复**：channel 深度回落 < 80 → 日志输出"背压溢出模式已解除"

---

## 结果记录表

```json
{
  "channel": {
    "2_users_200_msgs": {},
    "10_users_300_msgs": {},
    "20_users_500_msgs": {}
  },
  "kafka": {
    "20_users_500_msgs": {}
  },
  "hybrid": {
    "warmup_10x100": {},
    "overflow_20x500": {},
    "sustained_20x1000": {}
  }
}
```

> ⚠️ 实际数据需要在本机执行测试后填入。测试环境限制无法持久运行服务器进程，请使用 `run_perf.bat` 或手动执行上述步骤。

---

## 新增/修改文件清单

| 文件 | 操作 | 说明 |
|---|---|---|
| `docker-compose.yml` | 修改 | 新增 Kafka 3.7.0 服务 |
| `test/performance/users_2.json` | 新建 | 2 用户测试数据 |
| `test/performance/users_10.json` | 新建 | 10 用户测试数据 |
| `test/performance/users_20.json` | 新建 | 20 用户测试数据 |
| `test/performance/seed_users.sql` | 新建 | 测试用户 SQL（bcrypt 哈希密码 "test123"） |
| `test/performance/results/` | 新建 | 测试结果目录（待填充） |
| `run_perf.bat` | 新建 | 一键测试启动脚本 |
| `docs/hybrid-router-test-plan.md` | 新建 | 完整测试计划文档 |

---

## 验证记录

```
go build ./...         -> ✅ 编译通过（2026-07-16）
go test ./...          -> ✅ 全部通过
docker compose up -d   -> ✅ MySQL, Redis 运行正常
Kafka 镜像等待拉取   -> ⏳ 需执行: docker compose up -d kafka
```
