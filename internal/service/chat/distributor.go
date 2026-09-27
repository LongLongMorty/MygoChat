package chat

import (
	"sync"
	"time"

	"kama_chat_server/internal/service/transport"
	"kama_chat_server/pkg/zlog"
)

// PresenceRegistry 在线路由表接口（由 presence.Service 实现）。
type PresenceRegistry interface {
	Register(userID string) error
	Unregister(userID string) error
	Lookup(userID string) (string, bool)
	LookupMany(userIDs []string) (map[string]string, error)
	// 实例注册表：供会话亲和的一致性哈希环发现存活实例
	RegisterInstance() error
	LiveInstances() ([]string, error)
}

// InstancePublisher 实例间投递发布器（由 transport.InstanceBus 实现）。
type InstancePublisher interface {
	Publish(targetInstance string, msg *transport.BusMessage) error
}

// ClusterBus 实例间总线完整接口：发布 + 消费 + 停止。
type ClusterBus interface {
	InstancePublisher
	Run(handler func(*transport.BusMessage) error)
	Stop()
}

const (
	dedupTTL       = 2 * time.Minute
	dedupHardLimit = 200000
	crossNodeKind  = "user"
)

// Distributor 消息投递分发器：本地优先，远程按实例聚合转发。
//
// 关闭状态（enabled=false）时退化为纯本地投递，与改造前行为一致。
type Distributor struct {
	clients map[string]*Client
	mutex   *sync.RWMutex

	instanceID string
	presence   PresenceRegistry
	publisher  InstancePublisher
	enabled    bool

	dedupMu   sync.Mutex
	dedupSeen map[string]time.Time
}

// NewDistributor 创建默认（关闭）状态的分发器。
func NewDistributor(clients map[string]*Client, mutex *sync.RWMutex) *Distributor {
	return &Distributor{
		clients:   clients,
		mutex:     mutex,
		dedupSeen: make(map[string]time.Time),
	}
}

// Enable 开启集群投递：注入实例标识、在线表与发布器。
func (d *Distributor) Enable(instanceID string, presence PresenceRegistry, publisher InstancePublisher) {
	d.instanceID = instanceID
	d.presence = presence
	d.publisher = publisher
	d.enabled = true
	zlog.Info("分布式投递已启用，实例: " + instanceID)
}

// Enabled 返回是否处于集群投递模式。
func (d *Distributor) Enabled() bool {
	return d.enabled
}

// Deliver 投递一条 MessageBack 给一组目标用户。
// 本地命中直投；其余经 presence 定位后按实例聚合发布；离线目标丢弃（可从历史恢复）。
func (d *Distributor) Deliver(targets []string, mb *MessageBack, dedupKey string) {
	if mb == nil || len(targets) == 0 {
		return
	}

	// 关闭集群：纯本地投递
	if !d.enabled {
		for _, t := range targets {
			d.deliverLocal(t, mb)
		}
		return
	}

	local := make([]string, 0, len(targets))
	pending := make([]string, 0, len(targets))
	d.mutex.RLock()
	for _, t := range targets {
		if t == "" {
			continue
		}
		if _, ok := d.clients[t]; ok {
			local = append(local, t)
		} else {
			pending = append(pending, t)
		}
	}
	d.mutex.RUnlock()

	for _, t := range local {
		d.deliverLocal(t, mb)
	}
	if len(pending) == 0 {
		return
	}

	// 远程定位：一次批量 MGET
	locations, err := d.presence.LookupMany(pending)
	if err != nil {
		ChatMetrics.crossNodeLookupFailures.Add(1)
		zlog.Warn("presence 批量定位失败: " + err.Error())
		return
	}
	byInstance := make(map[string][]string)
	for _, t := range pending {
		if inst, ok := locations[t]; ok && inst != "" && inst != d.instanceID {
			byInstance[inst] = append(byInstance[inst], t)
		}
	}
	for inst, users := range byInstance {
		env := &transport.BusMessage{
			Type: transport.BusTypeDeliver,
			Deliver: &transport.DeliveryMessage{
				Targets:  users,
				Payload:  mb.Message,
				DedupKey: dedupKey,
				Kind:     crossNodeKind,
			},
		}
		if err := d.publisher.Publish(inst, env); err != nil {
			ChatMetrics.crossNodePublishFailures.Add(1)
			zlog.Warn("跨实例投递发布失败: " + err.Error())
			continue
		}
		ChatMetrics.crossNodePublish.Add(1)
	}
}

// deliverLocal 向本机在线客户端投递；返回是否命中本地连接。
func (d *Distributor) deliverLocal(uuid string, mb *MessageBack) bool {
	if uuid == "" {
		return false
	}
	d.mutex.RLock()
	client := d.clients[uuid]
	d.mutex.RUnlock()
	if client == nil {
		return false
	}
	if err := client.EnqueueDelivery(mb); err != nil {
		zlog.Warn("本地投递失败: " + err.Error())
	}
	return true
}

// DeliverLocalBatch 处理来自其它实例的投递消息：向本机目标逐个投递。
// 使用 DedupKey|target 做 TTL 去重，抵御 Stream 至少一次重投。
func (d *Distributor) DeliverLocalBatch(dm *transport.DeliveryMessage) error {
	if dm == nil || len(dm.Targets) == 0 {
		return nil
	}
	ChatMetrics.crossNodeReceived.Add(1)
	mb := &MessageBack{Message: dm.Payload, Uuid: dm.DedupKey}
	for _, t := range dm.Targets {
		if d.dedupSeenBefore(dm.DedupKey, t) {
			ChatMetrics.crossNodeDedupDropped.Add(1)
			continue
		}
		d.deliverLocal(t, mb)
	}
	return nil
}

// dedupSeenBefore 判断 (dedupKey, target) 是否在 TTL 窗口内已投递过。
func (d *Distributor) dedupSeenBefore(dedupKey, target string) bool {
	if dedupKey == "" {
		return false
	}
	key := dedupKey + "|" + target
	now := time.Now()

	d.dedupMu.Lock()
	defer d.dedupMu.Unlock()
	if seenAt, ok := d.dedupSeen[key]; ok && now.Sub(seenAt) < dedupTTL {
		return true
	}
	d.dedupSeen[key] = now
	if len(d.dedupSeen) > dedupHardLimit {
		for k, seenAt := range d.dedupSeen {
			if now.Sub(seenAt) >= dedupTTL {
				delete(d.dedupSeen, k)
			}
		}
	}
	return false
}
