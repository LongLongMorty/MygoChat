package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"kama_chat_server/internal/dto/request"
)

// idCapturingProcessor 实现 envelopeMessageProcessor，记录收到的幂等 ID。
type idCapturingProcessor struct {
	mu  sync.Mutex
	ids []string
}

func (m *idCapturingProcessor) ProcessMessage([]byte) error { return nil }

func (m *idCapturingProcessor) ProcessEnvelope(_ context.Context, msgID string, _ []byte, _ bool) error {
	m.mu.Lock()
	m.ids = append(m.ids, msgID)
	m.mu.Unlock()
	return nil
}

func TestSessionRouterPropagatesMsgID(t *testing.T) {
	proc := &idCapturingProcessor{}
	router := &SessionRouter{
		sessions:    make(map[string]*SessionQueue),
		processor:   proc,
		queueSize:   10,
		stopCleanup: make(chan struct{}),
	}
	defer router.Close()

	const n = 5
	for i := 1; i <= n; i++ {
		payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: "S-1", Content: "hi"})
		env := MessageEnvelope{SeqNum: uint64(i), MsgID: fmt.Sprintf("M-msg-%d", i), Payload: payload}
		data, _ := json.Marshal(env)
		if err := router.EnqueueMessage(data); err != nil {
			t.Fatalf("EnqueueMessage: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		proc.mu.Lock()
		got := len(proc.ids)
		proc.mu.Unlock()
		if got == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	proc.mu.Lock()
	defer proc.mu.Unlock()
	if len(proc.ids) != n {
		t.Fatalf("expected %d processed, got %d", n, len(proc.ids))
	}
	for i, id := range proc.ids {
		want := fmt.Sprintf("M-msg-%d", i+1)
		if id != want {
			t.Fatalf("index %d: expected %q got %q", i, want, id)
		}
	}
}
