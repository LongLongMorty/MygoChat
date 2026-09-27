// Package metrics 以 Prometheus 文本格式暴露 KamaChat 运行时指标。
//
// 复用 chat.ChatMetrics 的进程内原子计数（零侵入采集），并附带 Go runtime
// 与进程指标（goroutine、heap、GC、CPU），供 Prometheus 抓取 + Grafana 展示。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"kama_chat_server/internal/https_server/middleware"
	"kama_chat_server/internal/service/chat"
)

const namespace = "kamachat"

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newChatCollector(),
	)
}

// Handler 返回 Prometheus 抓取端点处理器（文本 exposition 格式）。
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

type batchStats struct {
	enqueued int64
	flushed  int64
	flushes  int64
	errors   int64
}

type counterDef struct {
	desc *prometheus.Desc
	get  func(chat.MessageMetricsSnapshot, batchStats) float64
}

type gaugeDef struct {
	desc *prometheus.Desc
	get  func() float64
}

type chatCollector struct {
	counters []counterDef
	gauges   []gaugeDef
}

func newChatCollector() *chatCollector {
	c := &chatCollector{}
	add := func(name, help string, get func(chat.MessageMetricsSnapshot, batchStats) float64) {
		c.counters = append(c.counters, counterDef{
			desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", name), help, nil, nil),
			get:  get,
		})
	}

	add("channel_routed_total", "消息经内存 channel 路由的数量", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.ChannelRouted)
	})
	add("kafka_routed_total", "消息经 Kafka 路由的数量", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.KafkaRouted)
	})
	add("delivery_queue_timeouts_total", "客户端投递队列超时次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.DeliveryQueueTimeouts)
	})
	add("delivery_client_closed_total", "投递时客户端已关闭的次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.DeliveryClientClosed)
	})
	add("delivery_status_queue_drops_total", "投递状态更新丢弃次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.DeliveryStatusQueueDrops)
	})
	add("session_queue_timeouts_total", "会话队列入队/等待超时次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.SessionQueueTimeouts)
	})
	add("process_failures_total", "消息处理失败次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.ProcessFailures)
	})
	add("kafka_commit_failures_total", "Kafka offset 提交失败次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.KafkaCommitFailures)
	})
	add("kafka_dlq_total", "批处理连续失败后转投死信主题的批次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.KafkaDLQTotal)
	})
	add("cache_queue_drops_total", "缓存更新任务丢弃次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CacheQueueDrops)
	})
	add("batch_flush_errors_total", "批量落库重试后仍失败的批次次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.BatchFlushErrors)
	})
	add("fanout_queue_drops_total", "群扇出任务丢弃次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.FanoutQueueDrops)
	})
	add("cross_node_publish_total", "跨实例投递发布批次总数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CrossNodePublish)
	})
	add("cross_node_publish_failures_total", "跨实例投递发布失败次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CrossNodePublishFailures)
	})
	add("cross_node_received_total", "从其它实例接收的投递批次总数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CrossNodeReceived)
	})
	add("cross_node_dedup_dropped_total", "跨实例重复投递去重丢弃次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CrossNodeDedupDropped)
	})
	add("cross_node_lookup_failures_total", "presence 批量定位失败次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.CrossNodeLookupFailures)
	})
	add("session_forwarded_total", "转发到会话归属实例的消息数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.SessionForwarded)
	})
	add("session_forward_failures_total", "会话转发失败回退本地的次数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.SessionForwardFailures)
	})
	add("session_forward_received_total", "本实例作为归属接收的会话消息数", func(s chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(s.SessionForwardReceived)
	})

	// 批量落库器计数
	add("batch_enqueued_total", "进入批量落库缓冲的消息数", func(_ chat.MessageMetricsSnapshot, b batchStats) float64 {
		return float64(b.enqueued)
	})
	add("batch_flushed_total", "成功落库的消息数", func(_ chat.MessageMetricsSnapshot, b batchStats) float64 {
		return float64(b.flushed)
	})
	add("batch_flushes_total", "批量落库批次总数", func(_ chat.MessageMetricsSnapshot, b batchStats) float64 {
		return float64(b.flushes)
	})
	add("batch_write_errors_total", "批量落库错误消息数", func(_ chat.MessageMetricsSnapshot, b batchStats) float64 {
		return float64(b.errors)
	})

	// HTTP 限流
	add("http_ratelimit_blocked_total", "被限流拦截的请求数", func(_ chat.MessageMetricsSnapshot, _ batchStats) float64 {
		return float64(middleware.RateLimitBlockedTotal())
	})

	c.gauges = []gaugeDef{
		{
			desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "channel_depth"), "当前 channel 缓冲深度", nil, nil),
			get:  func() float64 { return float64(len(chat.HybridChatRouter.Transmit)) },
		},
		{
			desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "channel_capacity"), "channel 缓冲容量", nil, nil),
			get:  func() float64 { return float64(cap(chat.HybridChatRouter.Transmit)) },
		},
		{
			desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "active_sessions"), "活跃会话队列数量", nil, nil),
			get:  func() float64 { return float64(chat.HybridChatRouter.SessionRouter.ActiveSessions()) },
		},
		{
			desc: prometheus.NewDesc(prometheus.BuildFQName(namespace, "", "online_clients"), "本实例在线客户端数量", nil, nil),
			get:  func() float64 { return float64(chat.HybridChatRouter.OnlineClients()) },
		},
	}
	return c
}

func (c *chatCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.counters {
		ch <- d.desc
	}
	for _, d := range c.gauges {
		ch <- d.desc
	}
}

func (c *chatCollector) Collect(ch chan<- prometheus.Metric) {
	s := chat.ChatMetrics.Snapshot()
	be, bf, bl, berr := chat.MessageBatch.Stats()
	bs := batchStats{enqueued: be, flushed: bf, flushes: bl, errors: berr}

	for _, d := range c.counters {
		ch <- prometheus.MustNewConstMetric(d.desc, prometheus.CounterValue, d.get(s, bs))
	}
	for _, d := range c.gauges {
		ch <- prometheus.MustNewConstMetric(d.desc, prometheus.GaugeValue, d.get())
	}
}
