# 性能测试结果说明

> 结果数据见 **[results.md](./results.md)**（权威文档）
> 测试日期：2026-07-30（宿主） / 2026-08-08（2C4G 容器）
> 硬件：Intel CPU, NVMe SSD, Windows 11
> 依赖：MySQL 8.0 / Redis 7 / Kafka 3.7.0（Docker）
> Go 1.25，CHANNEL_SIZE=4096（Kafka 溢出测试使用 100 以强制触发），messageMode=hybrid
> 测试工具：`test/performance/ws_load.go`

---

## 如何复跑测试

### 方式一：2C4G 容器（推荐，对应简历数据）

```bash
# 1. 启动 2C4G 容器栈（server 限 2C4G，含 pprof 8091）
docker compose up -d

# 2. 确认资源限制
docker inspect kamachat-server --format 'CPU={{.HostConfig.NanoCpus}} 内存={{.HostConfig.Memory}}'
# CPU=2000000000 (2核)  内存=4294967296 (4GB)

# 3. 5000 QPS 积压测试（0 丢包 + 顺序一致 + 稳定）
go run ./test/performance/ws_load.go ^
  -users ./test/performance/users_20_email.json ^
  -messages-per-user 250 -rate-per-user 250 -timeout 90s ^
  -dsn "root:root@tcp(127.0.0.1:3307)/kama_chat_server?parseTime=true" ^
  -out ./test/performance/results/2c4g_5000qps.json

# 4. 压测期间采样 pprof（佐证系统稳定）
curl http://127.0.0.1:8091/debug/pprof/goroutine?debug=1
curl http://127.0.0.1:8091/debug/pprof/heap?debug=1
```

完整场景见 [docs/2c4g-performance-test-plan.md](../../docs/2c4g-performance-test-plan.md)。

### 方式二：宿主机直接跑（历史方式）

```bash
# 1. 启动依赖（Docker）与服务器（hybrid 模式）
docker compose up -d mysql redis kafka
set KAMA_JWT_SECRET=your_32_byte_secret_here
go run ./cmd/kama_chat_server/

# 2. 正常负载（Channel 全承载）
go run ./test/performance/ws_load.go ^
  -users ./test/performance/users_20_email.json ^
  -messages-per-user 100 -rate-per-user 20 -timeout 60s ^
  -out ./test/performance/results/check_run.json
```

> **注意**：`ws_load` 使用 **email 登录**（`login` 接口已改为 email 驱动）。测试用户文件须含 `email` 字段，
> 参考 `test/performance/users_20_email.json`（如 `perf1@test.local / Perf123456!`）。

测试工具参数说明：`-users` 用户文件、`-messages-per-user` 每用户消息数、`-rate-per-user` 发送速率（0=瞬时注入）、`-timeout` 最大等待、`-out` 结果输出。

---

## 关键数据摘要（详见 results.md）

| 场景 | 结果 |
|---|---|
| Channel 低负载（10u×100@20/s） | P50 **6.7ms**，100% 交付，零缺口零重复 |
| Hybrid 低负载（10u×100@20/s） | P50 **0.6ms**，全通道承载 |
| Hybrid 高负载（20u×100@100/s） | P50 **1.1ms**，**1994 msg/s**，100% 交付 |
| **2C4G 容器 5000 QPS**（2026-08-08） | **4988 msg/s，0 丢包、0 乱序、0 重复**，P50 1.6ms，pprof 稳定 |
| Kafka 溢出（10000 条瞬时注入） | **零丢失**（gaps=0/duplicates=0），Kafka 分流 ~93.7% |

> **延迟口径（诚实口径）**：Kafka 兜底路径为**可靠性通道**而非性能通道（正常负载 100% 走 Channel，
> P50 毫秒级）。溢出时消息由 Kafka 兜底保证不丢；批量消费已从 19 msg/s 提升至 ~30 msg/s，
> 单消费者仍是明确的后续优化点（并发消费者可进一步提升排空速度）。
