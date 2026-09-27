package chat

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

const defaultRingReplicas = 128

type ringEntry struct {
	hash     uint32
	instance string
}

// SessionRing 会话归属一致性哈希环：把会话 ID 稳定映射到某个存活实例。
//
// 使用虚拟节点（每个实例 replicas 个）降低实例增减时的重映射比例；
// 同一会话在同一实例集合下始终映射到同一实例，从而让该会话的所有消息
// 由单一实例处理，把单实例内的顺序保证扩展到跨实例。
type SessionRing struct {
	mu        sync.RWMutex
	replicas  int
	instances []string
	ring      []ringEntry
}

// NewSessionRing 创建哈希环；replicas<=0 时使用默认虚拟节点数。
func NewSessionRing(replicas int) *SessionRing {
	if replicas <= 0 {
		replicas = defaultRingReplicas
	}
	return &SessionRing{replicas: replicas}
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// SetInstances 重建哈希环（去重 + 排序，保证确定性）。
func (r *SessionRing) SetInstances(instances []string) {
	uniq := make([]string, 0, len(instances))
	seen := make(map[string]struct{}, len(instances))
	for _, inst := range instances {
		if inst == "" {
			continue
		}
		if _, ok := seen[inst]; ok {
			continue
		}
		seen[inst] = struct{}{}
		uniq = append(uniq, inst)
	}
	sort.Strings(uniq)

	ring := make([]ringEntry, 0, len(uniq)*r.replicas)
	for _, inst := range uniq {
		for i := 0; i < r.replicas; i++ {
			ring = append(ring, ringEntry{hash: hashKey(inst + "#" + strconv.Itoa(i)), instance: inst})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].hash < ring[j].hash })

	r.mu.Lock()
	r.instances = uniq
	r.ring = ring
	r.mu.Unlock()
}

// Home 返回会话归属实例；环为空时返回空字符串（调用方回退本地处理）。
func (r *SessionRing) Home(sessionID string) string {
	r.mu.RLock()
	ring := r.ring
	r.mu.RUnlock()
	if len(ring) == 0 {
		return ""
	}
	idx := sort.Search(len(ring), func(i int) bool { return ring[i].hash >= hashKey(sessionID) })
	if idx == len(ring) {
		idx = 0
	}
	return ring[idx].instance
}

// Instances 返回当前环上的实例列表（副本）。
func (r *SessionRing) Instances() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.instances))
	copy(out, r.instances)
	return out
}
