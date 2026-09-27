package kafka

import (
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestBuildDLQMessagesPreservesMetadata(t *testing.T) {
	original := []kafka.Message{
		{Topic: "chat_message", Partition: 2, Offset: 42, Key: []byte("S-1"), Value: []byte("payload-1")},
		{Topic: "chat_message", Partition: 2, Offset: 43, Key: []byte("S-2"), Value: []byte("payload-2")},
	}

	msgs := buildDLQMessages(original, errors.New("boom"), 5, "2026-01-01T00:00:00Z")
	if len(msgs) != 2 {
		t.Fatalf("expected 2 DLQ messages, got %d", len(msgs))
	}
	if string(msgs[0].Value) != "payload-1" || string(msgs[0].Key) != "S-1" {
		t.Fatalf("key/value not preserved: %s / %s", msgs[0].Key, msgs[0].Value)
	}

	headers := make(map[string]string, len(msgs[0].Headers))
	for _, h := range msgs[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	want := map[string]string{
		"origin_topic":     "chat_message",
		"origin_partition": "2",
		"origin_offset":    "42",
		"attempts":         "5",
		"failed_at":        "2026-01-01T00:00:00Z",
		"cause":            "boom",
	}
	for k, v := range want {
		if headers[k] != v {
			t.Errorf("header %s: expected %q got %q", k, v, headers[k])
		}
	}
}
