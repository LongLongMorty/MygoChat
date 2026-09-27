package chat

import (
	"strconv"
	"testing"
)

func TestSessionRingDeterministicAndInSet(t *testing.T) {
	r := NewSessionRing(64)
	r.SetInstances([]string{"node-1", "node-2", "node-3"})

	for i := 0; i < 1000; i++ {
		sid := "S-" + strconv.Itoa(i)
		h1 := r.Home(sid)
		h2 := r.Home(sid)
		if h1 == "" || h1 != h2 {
			t.Fatalf("home should be deterministic and non-empty: %q vs %q", h1, h2)
		}
		if h1 != "node-1" && h1 != "node-2" && h1 != "node-3" {
			t.Fatalf("home %q not an instance", h1)
		}
	}
}

func TestSessionRingEmpty(t *testing.T) {
	r := NewSessionRing(0)
	if got := r.Home("S-1"); got != "" {
		t.Fatalf("empty ring should return empty home, got %q", got)
	}
}

func TestSessionRingConsistencyOnRemoval(t *testing.T) {
	r := NewSessionRing(128)
	r.SetInstances([]string{"node-1", "node-2", "node-3", "node-4"})

	before := make(map[string]string, 5000)
	for i := 0; i < 5000; i++ {
		sid := "S-" + strconv.Itoa(i)
		before[sid] = r.Home(sid)
	}

	// 移除 node-4：一致性哈希应只重映射原属于 node-4 的会话
	r.SetInstances([]string{"node-1", "node-2", "node-3"})

	kept := 0
	for sid, home := range before {
		if r.Home(sid) == home {
			kept++
		}
	}
	// 4 选 3，理论上约 75% 的映射保持不变
	if kept < 3000 {
		t.Fatalf("consistent hashing should keep most mappings, kept %d/5000", kept)
	}
	t.Logf("removal kept %d/5000 mappings", kept)
}
