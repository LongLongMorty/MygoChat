package main

import "testing"

func TestCalculateConsumerLag(t *testing.T) {
	endOffsets := map[int]int64{0: 100, 1: 50, 2: 25}
	committedOffsets := map[int]int64{0: 100, 1: 20, 2: -1}
	if got := calculateConsumerLag(endOffsets, committedOffsets); got != 55 {
		t.Fatalf("consumer lag = %d, want 55", got)
	}
}

func TestMetricsSnapshotSubtractsBaseline(t *testing.T) {
	after := metricsSnapshot{ChannelRouted: 150, KafkaRouted: 25, BatchFlushErrors: 3, DeliveryQueueTimeouts: 7, SessionQueueTimeouts: 4}
	before := metricsSnapshot{ChannelRouted: 100, KafkaRouted: 10, BatchFlushErrors: 1, DeliveryQueueTimeouts: 2, SessionQueueTimeouts: 1}

	got := after.subtract(before)
	if got.ChannelRouted != 50 || got.KafkaRouted != 15 || got.BatchFlushErrors != 2 || got.DeliveryQueueTimeouts != 5 || got.SessionQueueTimeouts != 3 {
		t.Fatalf("unexpected metric delta: %+v", got)
	}
}

func TestMetricsSnapshotSubtractHandlesServerRestart(t *testing.T) {
	got := (metricsSnapshot{ChannelRouted: 2}).subtract(metricsSnapshot{ChannelRouted: 10})
	if got.ChannelRouted != 0 {
		t.Fatalf("restart must not underflow route counters: %d", got.ChannelRouted)
	}
}
