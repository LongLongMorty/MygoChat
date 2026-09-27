# KamaChat 可观测性（Prometheus + Grafana）

## 1. 目标

在原有轻量 `/metrics` JSON 基础上，新增标准 **Prometheus 文本 exposition** 端点，接入 Prometheus 抓取与 Grafana 看板，覆盖：

- 消息路由与投递（channel / Kafka / 跨实例 / 会话亲和）
- 可靠性与异常（处理失败、批量落库错误、offset 提交失败、队列超时）
- 运行时资源（goroutine、堆内存、GC、CPU）

采集实现**零侵入**：`internal/metrics` 以自定义 Collector 读取现有 `chat.ChatMetrics` 原子计数与 `MessageBatch.Stats()`，不改动消息链路上的业务代码。

## 2. 端点

| 路径 | 格式 | 用途 |
| --- | --- | --- |
| `/metrics` | JSON | 兼容旧压测工具 `ws_load.go`，需认证无关，仅限内网 |
| `/prometheus` | Prometheus 文本 | Prometheus 抓取端点 |

> 服务走 TLS，Prometheus 抓取需 `insecure_skip_verify: true`（本地自签证书）。

## 3. 指标清单

命名前缀 `kamachat_`，计数器以 `_total` 结尾。

### 路由与投递
- `kamachat_channel_routed_total` / `kamachat_kafka_routed_total`
- `kamachat_delivery_queue_timeouts_total` / `kamachat_delivery_client_closed_total`
- `kamachat_session_queue_timeouts_total`
- `kamachat_fanout_queue_drops_total`

### 可靠性
- `kamachat_process_failures_total`
- `kamachat_batch_flush_errors_total`
- `kamachat_kafka_commit_failures_total`
- `kamachat_kafka_dlq_total`（转投死信主题的批次数）
- `kamachat_cache_queue_drops_total`

### 批量落库
- `kamachat_batch_enqueued_total` / `kamachat_batch_flushed_total`
- `kamachat_batch_flushes_total` / `kamachat_batch_write_errors_total`

### 分布式
- `kamachat_cross_node_publish_total` / `kamachat_cross_node_publish_failures_total`
- `kamachat_cross_node_received_total` / `kamachat_cross_node_dedup_dropped_total`
- `kamachat_cross_node_lookup_failures_total`
- `kamachat_session_forwarded_total` / `kamachat_session_forward_failures_total` / `kamachat_session_forward_received_total`

### HTTP
- `kamachat_http_ratelimit_blocked_total`（被限流拦截的请求数）

### 运行时 Gauge
- `kamachat_channel_depth` / `kamachat_channel_capacity`
- `kamachat_active_sessions` / `kamachat_online_clients`
- `go_goroutines`、`go_memstats_heap_alloc_bytes`、`process_*`（Go/进程采集器）

## 4. 本地启动

```bash
docker compose up -d
```

- Prometheus：http://localhost:9090 （抓取目标 `server:8000` 的 `/prometheus`）
- Grafana：http://localhost:3000 （admin / admin，看板「KamaChat 概览」已预置）

配置位置：

```
deploy/prometheus/prometheus.yml                       # 抓取配置
deploy/grafana/provisioning/datasources/prometheus.yml  # 数据源
deploy/grafana/provisioning/dashboards/dashboard.yml    # 看板提供器
deploy/grafana/dashboards/kamachat-overview.json        # 预置看板
```

## 5. 压测观察建议

- 路由吞吐面板：确认 100% 走 channel（`rate(channel_routed_total)`），溢出时 Kafka 曲线抬升
- Channel 深度：接近容量即触发背压，配合 `channel_capacity` 判断水位
- 失败/异常面板：理想压测下应恒为 0（process_failures / batch_flush_errors / kafka_commit_failures）
- goroutine / 堆内存：压测前后回落即无泄漏（与 pprof 结论互相印证）
- 分布式面板：多实例时观察 `cross_node_*` 与 `session_forwarded` 是否符合预期

## 6. 与 pprof 的关系

- Prometheus/Grafana：**宏观趋势**（吞吐、延迟代理、资源长期曲线）
- pprof（`:8091/debug/pprof/`）：**微观定位**（某时刻 goroutine 栈、CPU/heap profile）
