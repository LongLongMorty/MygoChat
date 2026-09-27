// Package transport 提供实例间投递总线，用于把消息转发到持有目标连接的其它实例。
//
// 每个实例订阅自己的 Redis Stream（chat:deliver:<instanceID>），发布方按目标
// 用户所在实例投递一条消息，实现跨实例实时送达。
package transport

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/zlog"
)

const (
	streamPrefix = "chat:deliver:"
	consumerGrp  = "chat-deliver"
	readCount    = 100
	blockWait    = time.Second
)

// 实例间消息类型。
const (
	BusTypeDeliver = "deliver" // 投递消息给本机在线客户端
	BusTypeSession = "session" // 会话亲和：转发原始消息到会话归属实例处理
)

// BusMessage 实例间消息统一信封，按 Type 分发。
type BusMessage struct {
	Type    string           `json:"type"`
	Deliver *DeliveryMessage `json:"deliver,omitempty"`
	Session *SessionForward  `json:"session,omitempty"`
}

// DeliveryMessage 跨实例投递消息：一次携带发往同一实例的多个目标用户。
type DeliveryMessage struct {
	Targets  []string `json:"targets"`        // 目标用户 uuid（均在本实例在线）
	Payload  []byte   `json:"payload"`        // MessageBack.Message 原始 JSON
	DedupKey string   `json:"dedup_key"`      // 消息 uuid：接收端去重
	Kind     string   `json:"kind,omitempty"` // "user" | "group"
}

// SessionForward 会话亲和转发：原始消息交由会话归属实例分配序号并处理。
type SessionForward struct {
	SessionID string `json:"session_id"`
	Payload   []byte `json:"payload"` // 原始 ChatMessageRequest JSON
}

// InstanceBus 基于 Redis Stream 的实例间投递总线。
type InstanceBus struct {
	instanceID string
	stream     string
	group      string
	maxLen     int64
	stopCh     chan struct{}
	closeOnce  sync.Once
}

// NewInstanceBus 创建实例总线。maxLen<=0 时使用默认 10000。
func NewInstanceBus(instanceID string, maxLen int64) *InstanceBus {
	if maxLen <= 0 {
		maxLen = 10000
	}
	return &InstanceBus{
		instanceID: instanceID,
		stream:     streamPrefix + instanceID,
		group:      consumerGrp,
		maxLen:     maxLen,
		stopCh:     make(chan struct{}),
	}
}

// Publish 将消息投递到目标实例的 Stream。
func (b *InstanceBus) Publish(targetInstance string, msg *BusMessage) error {
	if targetInstance == "" || msg == nil {
		return nil
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return myredis.XAddMessage(streamPrefix+targetInstance, "msg", string(raw), b.maxLen)
}

// Run 阻塞运行消费循环，收到消息后交给 handler 处理，成功才 ACK。
// 建议以 goroutine 方式启动：go bus.Run(handler)。
func (b *InstanceBus) Run(handler func(*BusMessage) error) {
	if err := myredis.XGroupCreateMkStream(b.stream, b.group); err != nil {
		// BUSYGROUP：消费组已存在，属正常情况
		zlog.Info("InstanceBus 消费组: " + err.Error())
	}
	zlog.Info("InstanceBus 已启动: " + b.stream)

	for {
		select {
		case <-b.stopCh:
			return
		default:
		}

		// 1. 重投本消费者未 ack 的 pending（streamID="0"，block<0 非阻塞）
		if pending, err := myredis.XReadGroupMessages(b.stream, b.group, b.instanceID, "0", readCount, -1); err == nil {
			b.process(pending, handler)
		}

		// 2. 阻塞读取新消息（streamID=">"）
		msgs, err := myredis.XReadGroupMessages(b.stream, b.group, b.instanceID, ">", readCount, blockWait)
		if err != nil {
			zlog.Warn("InstanceBus 读取失败: " + err.Error())
			time.Sleep(200 * time.Millisecond)
			continue
		}
		b.process(msgs, handler)
	}
}

func (b *InstanceBus) process(msgs []redis.XMessage, handler func(*BusMessage) error) {
	for _, m := range msgs {
		raw, ok := m.Values["msg"].(string)
		if !ok {
			_ = myredis.XAck(b.stream, b.group, m.ID)
			continue
		}
		var dm BusMessage
		if err := json.Unmarshal([]byte(raw), &dm); err != nil {
			zlog.Warn("InstanceBus 消息解析失败，丢弃: " + err.Error())
			_ = myredis.XAck(b.stream, b.group, m.ID)
			continue
		}
		if err := handler(&dm); err != nil {
			// 处理失败：不 ACK，保留 pending 下轮重试
			zlog.Warn("InstanceBus 处理失败，将重试: " + err.Error())
			continue
		}
		_ = myredis.XAck(b.stream, b.group, m.ID)
	}
}

// Stop 停止消费循环（幂等）。
func (b *InstanceBus) Stop() {
	b.closeOnce.Do(func() {
		close(b.stopCh)
	})
}
