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
	cacheQueueDrops          atomic.Uint64
	channelRouted            atomic.Uint64 // messages sent via channel path
	kafkaRouted              atomic.Uint64 // messages sent via Kafka path
	batchFlushErrors         atomic.Uint64 // batch INSERT failures after all retries
}

type MessageMetricsSnapshot struct {
	DeliveryQueueTimeouts    uint64
	DeliveryClientClosed     uint64
	DeliveryStatusQueueDrops uint64
	SessionQueueTimeouts     uint64
	ProcessFailures          uint64
	KafkaCommitFailures      uint64
	CacheQueueDrops          uint64
	ChannelRouted            uint64
	KafkaRouted              uint64
	BatchFlushErrors         uint64
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
		CacheQueueDrops:          m.cacheQueueDrops.Load(),
		ChannelRouted:            m.channelRouted.Load(),
		KafkaRouted:              m.kafkaRouted.Load(),
		BatchFlushErrors:         m.batchFlushErrors.Load(),
	}
}
