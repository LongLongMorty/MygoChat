package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"kama_chat_server/pkg/zlog"
)

type SessionQueue struct {
	sessionId       string
	queue           chan queuedEnvelope
	processor       IMessageProcessor
	stopCh          chan struct{}
	closeOnce       sync.Once
	lastActive      time.Time
	mu              sync.Mutex
	stateMu         sync.Mutex
	closed          bool
	nextExpectedSeq uint64
	buffer          map[uint64]queuedEnvelope
}

type queuedEnvelope struct {
	envelope MessageEnvelope
	done     chan error
	ctx      context.Context
}

type durableMessageProcessor interface {
	ProcessMessageAndWait(ctx context.Context, data []byte) error
}

type SessionRouter struct {
	sessions    map[string]*SessionQueue
	mu          sync.RWMutex
	processor   IMessageProcessor
	queueSize   int
	stopCleanup chan struct{}
	closeOnce   sync.Once
}

func NewSessionRouter(processor IMessageProcessor, queueSize int) *SessionRouter {
	router := &SessionRouter{
		sessions:    make(map[string]*SessionQueue),
		processor:   processor,
		queueSize:   queueSize,
		stopCleanup: make(chan struct{}),
	}
	go router.cleanupIdleSessions()
	return router
}

func (sr *SessionRouter) EnqueueMessage(envelopeData []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return sr.enqueue(ctx, envelopeData, nil)
}

// EnqueueMessageAndWait is used by the Kafka path. It does not acknowledge a
// Kafka record until the session worker has completed its persistence step.
func (sr *SessionRouter) EnqueueMessageAndWait(ctx context.Context, envelopeData []byte) error {
	done := make(chan error, 1)
	if err := sr.enqueue(ctx, envelopeData, done); err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		ChatMetrics.sessionQueueTimeouts.Add(1)
		return fmt.Errorf("wait for session processing: %w", ctx.Err())
	}
}

// EnqueueMessagesAndWait is used by the Kafka batch path. It enqueues a batch
// of messages (each carrying its own done channel) and waits until ALL of them
// have been processed & persisted. Ordering per session is preserved by the
// SessionQueue worker; different sessions process concurrently on their own
// goroutines. On any single failure it returns the first error, and the caller
// must retry the whole batch (seq-based dedup in the session worker makes
// re-processing already-delivered messages idempotent).
func (sr *SessionRouter) EnqueueMessagesAndWait(ctx context.Context, envelopeDatas [][]byte) error {
	dones := make([]chan error, len(envelopeDatas))
	for i, data := range envelopeDatas {
		done := make(chan error, 1)
		if err := sr.enqueue(ctx, data, done); err != nil {
			return err
		}
		dones[i] = done
	}
	// Wait for all done channels. Bail on the first error so the caller can
	// retry the batch from the last committed offset.
	for _, done := range dones {
		select {
		case err := <-done:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			ChatMetrics.sessionQueueTimeouts.Add(1)
			return fmt.Errorf("wait for session batch processing: %w", ctx.Err())
		}
	}
	return nil
}

func (sr *SessionRouter) enqueue(ctx context.Context, envelopeData []byte, done chan error) error {
	item, sessionId, err := parseQueuedEnvelope(envelopeData, done, ctx)
	if err != nil {
		return err
	}

	queue := sr.getOrCreateSessionQueue(sessionId)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.closed {
		return fmt.Errorf("session %s queue is closed", sessionId)
	}

	select {
	case queue.queue <- item:
		queue.lastActive = time.Now()
		return nil
	case <-queue.stopCh:
		return fmt.Errorf("session %s queue is stopping", sessionId)
	case <-ctx.Done():
		ChatMetrics.sessionQueueTimeouts.Add(1)
		return fmt.Errorf("enqueue session %s: %w", sessionId, ctx.Err())
	}
}

func parseQueuedEnvelope(envelopeData []byte, done chan error, ctx context.Context) (queuedEnvelope, string, error) {
	var envelope MessageEnvelope
	if err := json.Unmarshal(envelopeData, &envelope); err != nil {
		return queuedEnvelope{}, "", fmt.Errorf("parse message envelope: %w", err)
	}

	var message struct {
		SessionId string `json:"session_id"`
	}
	if err := json.Unmarshal(envelope.Payload, &message); err != nil {
		return queuedEnvelope{}, "", fmt.Errorf("parse message session_id: %w", err)
	}
	if message.SessionId == "" {
		return queuedEnvelope{}, "", fmt.Errorf("message session_id is required")
	}

	return queuedEnvelope{envelope: envelope, done: done, ctx: ctx}, message.SessionId, nil
}

func (sr *SessionRouter) getOrCreateSessionQueue(sessionId string) *SessionQueue {
	sr.mu.RLock()
	queue, exists := sr.sessions[sessionId]
	sr.mu.RUnlock()
	if exists {
		return queue
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if queue, exists = sr.sessions[sessionId]; exists {
		return queue
	}

	queue = &SessionQueue{
		sessionId:       sessionId,
		queue:           make(chan queuedEnvelope, sr.queueSize),
		processor:       sr.processor,
		stopCh:          make(chan struct{}),
		lastActive:      time.Now(),
		nextExpectedSeq: 1,
		buffer:          make(map[uint64]queuedEnvelope),
	}
	sr.sessions[sessionId] = queue
	go sr.processSessionQueue(queue)
	zlog.Info(fmt.Sprintf("创建会话队列: %s", sessionId))
	return queue
}

func (sr *SessionRouter) processSessionQueue(queue *SessionQueue) {
	defer func() {
		if recovered := recover(); recovered != nil {
			zlog.Error(fmt.Sprintf("会话队列 %s worker 异常退出: %v", queue.sessionId, recovered))
		}
	}()

	for {
		select {
		case item := <-queue.queue:
			sr.processEnvelope(queue, item)
		case <-queue.stopCh:
			sr.drainSessionQueue(queue)
			return
		}
	}
}

func (sr *SessionRouter) drainSessionQueue(queue *SessionQueue) {
	for {
		select {
		case item := <-queue.queue:
			sr.processEnvelope(queue, item)
		default:
			queue.stateMu.Lock()
			buffered := len(queue.buffer)
			queue.stateMu.Unlock()
			if buffered > 0 {
				zlog.Warn(fmt.Sprintf("会话队列 %s 关闭时仍缺少前序消息，保留 %d 条缓存等待重放", queue.sessionId, buffered))
			}
			zlog.Info(fmt.Sprintf("会话队列 %s 已停止", queue.sessionId))
			return
		}
	}
}

func (sr *SessionRouter) processEnvelope(queue *SessionQueue, item queuedEnvelope) {
	defer func() {
		if recovered := recover(); recovered != nil {
			zlog.Error(fmt.Sprintf("会话 %s 消息处理 panic: %v", queue.sessionId, recovered))
			finishQueuedEnvelope(item, fmt.Errorf("session worker panic: %v", recovered))
		}
	}()

	queue.stateMu.Lock()
	if item.envelope.SeqNum > queue.nextExpectedSeq {
		queue.buffer[item.envelope.SeqNum] = item
		buffered := len(queue.buffer)
		expected := queue.nextExpectedSeq
		queue.stateMu.Unlock()
		zlog.Warn(fmt.Sprintf("会话 %s 消息乱序缓存：期望序号 %d，实际序号 %d，buffer大小 %d", queue.sessionId, expected, item.envelope.SeqNum, buffered))
		return
	}
	if item.envelope.SeqNum < queue.nextExpectedSeq {
		expected := queue.nextExpectedSeq
		queue.stateMu.Unlock()
		zlog.Warn(fmt.Sprintf("会话 %s 丢弃重复消息：期望序号 %d，实际序号 %d", queue.sessionId, expected, item.envelope.SeqNum))
		finishQueuedEnvelope(item, nil)
		return
	}
	queue.stateMu.Unlock()

	for {
		var err error
		if item.done != nil {
			if processor, ok := queue.processor.(durableMessageProcessor); ok {
				err = processor.ProcessMessageAndWait(item.ctx, item.envelope.Payload)
			} else {
				err = queue.processor.ProcessMessage(item.envelope.Payload)
			}
		} else {
			err = queue.processor.ProcessMessage(item.envelope.Payload)
		}
		if err != nil {
			ChatMetrics.processFailures.Add(1)
			finishQueuedEnvelope(item, err)
			return
		}
		finishQueuedEnvelope(item, nil)

		queue.stateMu.Lock()
		queue.nextExpectedSeq++
		next, exists := queue.buffer[queue.nextExpectedSeq]
		if exists {
			delete(queue.buffer, queue.nextExpectedSeq)
		}
		queue.stateMu.Unlock()
		if !exists {
			return
		}
		item = next
	}
}

func finishQueuedEnvelope(item queuedEnvelope, err error) {
	if item.done == nil {
		return
	}
	item.done <- err
}

func (sr *SessionRouter) cleanupIdleSessions() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.mu.Lock()
			for sessionId, queue := range sr.sessions {
				queue.mu.Lock()
				idle := time.Since(queue.lastActive) > 5*time.Minute
				queue.mu.Unlock()
				queue.stateMu.Lock()
				empty := len(queue.queue) == 0 && len(queue.buffer) == 0
				queue.stateMu.Unlock()
				if idle && empty {
					sr.stopSessionQueue(queue)
					delete(sr.sessions, sessionId)
					zlog.Info(fmt.Sprintf("清理空闲会话队列: %s", sessionId))
				}
			}
			sr.mu.Unlock()
		case <-sr.stopCleanup:
			return
		}
	}
}

func (sr *SessionRouter) stopSessionQueue(queue *SessionQueue) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.closeOnce.Do(func() {
		queue.closed = true
		close(queue.stopCh)
	})
}

func (sr *SessionRouter) Close() {
	sr.closeOnce.Do(func() {
		close(sr.stopCleanup)
		sr.mu.Lock()
		defer sr.mu.Unlock()
		for _, queue := range sr.sessions {
			sr.stopSessionQueue(queue)
		}
		sr.sessions = make(map[string]*SessionQueue)
	})
}
