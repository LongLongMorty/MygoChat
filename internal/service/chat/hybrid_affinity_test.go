package chat

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kama_chat_server/internal/service/transport"
)

type fakeBus struct {
	mu        sync.Mutex
	published []publishedBusMsg
	failErr   error
}

type publishedBusMsg struct {
	inst string
	msg  *transport.BusMessage
}

func (f *fakeBus) Publish(inst string, msg *transport.BusMessage) error {
	if f.failErr != nil {
		return f.failErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, publishedBusMsg{inst: inst, msg: msg})
	return nil
}

func (f *fakeBus) Run(func(*transport.BusMessage) error) {}
func (f *fakeBus) Stop()                                 {}

func TestHybridRouterAffinityForwardsToHome(t *testing.T) {
	ring := NewSessionRing(0)
	ring.SetInstances([]string{"node-2"}) // 所有会话归属 node-2
	bus := &fakeBus{}
	h := &HybridRouter{instanceID: "node-1", affinityEnabled: true, ring: ring, bus: bus}

	if err := h.SendMessage([]byte(`{"session_id":"S-1","content":"hi"}`)); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.published) != 1 {
		t.Fatalf("expected 1 forward, got %d", len(bus.published))
	}
	p := bus.published[0]
	if p.inst != "node-2" || p.msg.Type != transport.BusTypeSession || p.msg.Session == nil {
		t.Fatalf("unexpected forward: inst=%s type=%s session=%v", p.inst, p.msg.Type, p.msg.Session)
	}
	if p.msg.Session.SessionID != "S-1" {
		t.Fatalf("session id mismatch: %s", p.msg.Session.SessionID)
	}
}

func TestHybridRouterAffinityFallbackOnPublishError(t *testing.T) {
	ring := NewSessionRing(0)
	ring.SetInstances([]string{"node-2"})
	bus := &fakeBus{failErr: errors.New("redis down")}
	h := &HybridRouter{
		instanceID:           "node-1",
		affinityEnabled:      true,
		ring:                 ring,
		bus:                  bus,
		Transmit:             make(chan []byte, 4),
		Detector:             NewBackpressureDetector(make(chan []byte, 1), 1, 1),
		sessionSeqNums:       map[string]*atomic.Uint64{},
		seqMutex:             &sync.RWMutex{},
		sessionRoutedToKafka: map[string]bool{},
		sessionRoutedAt:      map[string]time.Time{},
		sessionMutex:         &sync.RWMutex{},
	}

	if err := h.SendMessage([]byte(`{"session_id":"S-2","content":"hi"}`)); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if got := len(h.Transmit); got != 1 {
		t.Fatalf("fallback should enqueue locally, got %d", got)
	}
}
