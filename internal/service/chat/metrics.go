package chat

import "sync/atomic"

// MessageMetrics records loss-prone boundaries in the message path. They are
// intentionally process-local for now; the performance test can collect them
// from structured logs before a Prometheus endpoint is introduced.
type MessageMetrics struct {
	deliveryQueueTimeouts    atomic.Uint64
	deliveryClientClosed     atomic.Uint64
	deliveryStatusQueueDrops atomic.Uint64
	sessionQueueTimeouts     atomic.Uint64
	processFailures          atomic.Uint64
	kafkaCommitFailures      atomic.Uint64
	kafkaDLQTotal            atomic.Uint64 // 批处理连续失败后转投死信主题的批次数
	cacheQueueDrops          atomic.Uint64
	channelRouted            atomic.Uint64 // messages sent via channel path
	kafkaRouted              atomic.Uint64 // messages sent via Kafka path
	batchFlushErrors         atomic.Uint64 // batch INSERT failures after all retries
	fanoutQueueDrops         atomic.Uint64 // group fanout tasks dropped when worker queue is full

	// 分布式跨实例投递指标
	crossNodePublish         atomic.Uint64 // 成功发布到其它实例的消息批次
	crossNodePublishFailures atomic.Uint64 // 跨实例发布失败次数
	crossNodeReceived        atomic.Uint64 // 从其它实例接收的投递批次
	crossNodeDedupDropped    atomic.Uint64 // 跨实例重复投递被去重丢弃的次数
	crossNodeLookupFailures  atomic.Uint64 // presence 批量定位失败次数

	// 会话亲和指标
	sessionForwarded         atomic.Uint64 // 转发到会话归属实例的消息数
	sessionForwardFailures   atomic.Uint64 // 会话转发失败回退本地处理的次数
	sessionForwardReceived   atomic.Uint64 // 本实例作为归属实例接收的会话消息数
}

type MessageMetricsSnapshot struct {
	DeliveryQueueTimeouts    uint64
	DeliveryClientClosed     uint64
	DeliveryStatusQueueDrops uint64
	SessionQueueTimeouts     uint64
	ProcessFailures          uint64
	KafkaCommitFailures      uint64
	KafkaDLQTotal            uint64
	CacheQueueDrops          uint64
	ChannelRouted            uint64
	KafkaRouted              uint64
	BatchFlushErrors         uint64
	FanoutQueueDrops         uint64
	CrossNodePublish         uint64
	CrossNodePublishFailures uint64
	CrossNodeReceived        uint64
	CrossNodeDedupDropped    uint64
	CrossNodeLookupFailures  uint64
	SessionForwarded         uint64
	SessionForwardFailures   uint64
	SessionForwardReceived   uint64
}

var ChatMetrics MessageMetrics

func (m *MessageMetrics) Snapshot() MessageMetricsSnapshot {
	return MessageMetricsSnapshot{
		DeliveryQueueTimeouts:    m.deliveryQueueTimeouts.Load(),
		DeliveryClientClosed:     m.deliveryClientClosed.Load(),
		DeliveryStatusQueueDrops: m.deliveryStatusQueueDrops.Load(),
		SessionQueueTimeouts:     m.sessionQueueTimeouts.Load(),
		ProcessFailures:          m.processFailures.Load(),
		KafkaCommitFailures:      m.kafkaCommitFailures.Load(),
		KafkaDLQTotal:            m.kafkaDLQTotal.Load(),
		CacheQueueDrops:          m.cacheQueueDrops.Load(),
		ChannelRouted:            m.channelRouted.Load(),
		KafkaRouted:              m.kafkaRouted.Load(),
		BatchFlushErrors:         m.batchFlushErrors.Load(),
		FanoutQueueDrops:         m.fanoutQueueDrops.Load(),
		CrossNodePublish:         m.crossNodePublish.Load(),
		CrossNodePublishFailures: m.crossNodePublishFailures.Load(),
		CrossNodeReceived:        m.crossNodeReceived.Load(),
		CrossNodeDedupDropped:    m.crossNodeDedupDropped.Load(),
		CrossNodeLookupFailures:  m.crossNodeLookupFailures.Load(),
		SessionForwarded:         m.sessionForwarded.Load(),
		SessionForwardFailures:   m.sessionForwardFailures.Load(),
		SessionForwardReceived:   m.sessionForwardReceived.Load(),
	}
}
