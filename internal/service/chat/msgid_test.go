package chat

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeriveMsgID(t *testing.T) {
	// 无 client_msg_id：随机生成，两次不同
	a := deriveMsgID("U-1", "")
	b := deriveMsgID("U-1", "")
	if a == b {
		t.Fatalf("random ids should differ: %s", a)
	}
	if len(a) == 0 || len(a) > 20 {
		t.Fatalf("unexpected length %d", len(a))
	}

	// 有 client_msg_id：确定性
	x := deriveMsgID("U-1", "c-1")
	if x != deriveMsgID("U-1", "c-1") {
		t.Fatalf("derived id should be deterministic")
	}
	if len(x) > 20 || x[0] != 'M' {
		t.Fatalf("unexpected id %q", x)
	}

	// 不同发送者 + 相同 client_msg_id：不得碰撞（防跨用户抑制消息）
	if deriveMsgID("U-1", "c-1") == deriveMsgID("U-2", "c-1") {
		t.Fatalf("different senders must not collide")
	}
}

func TestProcessLocalStableMsgIDForClientMsgID(t *testing.T) {
	h := &HybridRouter{
		Transmit:             make(chan []byte, 8),
		Detector:             NewBackpressureDetector(make(chan []byte, 1), 1, 1),
		sessionSeqNums:       map[string]*atomic.Uint64{},
		seqMutex:             &sync.RWMutex{},
		sessionRoutedToKafka: map[string]bool{},
		sessionRoutedAt:      map[string]time.Time{},
		sessionMutex:         &sync.RWMutex{},
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"session_id":    "S-1",
		"send_id":       "U-1",
		"client_msg_id": "c-9",
		"content":       "hi",
	})
	if err := h.processLocal("S-1", payload); err != nil {
		t.Fatalf("processLocal: %v", err)
	}
	if err := h.processLocal("S-1", payload); err != nil {
		t.Fatalf("processLocal: %v", err)
	}

	var env1, env2 MessageEnvelope
	_ = json.Unmarshal(<-h.Transmit, &env1)
	_ = json.Unmarshal(<-h.Transmit, &env2)

	if env1.MsgID != env2.MsgID {
		t.Fatalf("same client_msg_id should yield same MsgID: %s vs %s", env1.MsgID, env2.MsgID)
	}
	if env1.SeqNum == env2.SeqNum {
		t.Fatalf("seq should still increment: %d == %d", env1.SeqNum, env2.SeqNum)
	}
}
