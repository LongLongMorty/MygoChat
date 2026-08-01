package chat

import (
	"fmt"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/zlog"
	"sync"
	"time"
)

const (
	// batchFlushSize triggers a flush when the buffer reaches this many messages.
	batchFlushSize = 50
	// batchFlushInterval triggers a periodic flush at this interval.
	batchFlushInterval = 10 * time.Millisecond
	// maxFlushRetries is the number of retry attempts for a failed batch INSERT.
	maxFlushRetries = 3
)

// MessageBatchWriter accumulates DB inserts and flushes them in batches.
// It is safe for concurrent use from multiple goroutines.
//
// Callers on the critical persistence path (e.g. Kafka consumer) must wait on
// the result channel returned by Enqueue before committing offsets.
type MessageBatchWriter struct {
	mu        sync.Mutex
	buffer    []batchItem
	done      chan struct{}
	ticker    *time.Ticker
	flushing  bool
	started   bool
	closed    bool
	flushCond *sync.Cond // broadcast when a flush completes
	// counters for observability
	totalEnqueued int64
	totalFlushed  int64
	totalFlushes  int64
	totalErrors   int64
}

// batchItem keeps completion state per message. A later successful batch must
// never hide an earlier failed batch from a Kafka consumer.
type batchItem struct {
	message *model.Message
	done    chan error
}

// global instance – used by all chat service paths
var MessageBatch = &MessageBatchWriter{}

// ensureStarted lazily starts the flush loop on first use.
func (bw *MessageBatchWriter) ensureStarted() {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	if bw.started || bw.closed {
		return
	}
	bw.done = make(chan struct{})
	bw.ticker = time.NewTicker(batchFlushInterval)
	bw.flushCond = sync.NewCond(&bw.mu)
	bw.started = true
	go bw.flushLoop()
}

// Enqueue adds a message to the batch buffer and returns its durability result.
// The message will be persisted to DB in a batch INSERT on the next flush.
// The caller SHOULD NOT mutate the message after enqueuing.
func (bw *MessageBatchWriter) Enqueue(msg *model.Message) <-chan error {
	bw.ensureStarted()
	done := make(chan error, 1)

	bw.mu.Lock()
	if bw.closed {
		bw.mu.Unlock()
		// fall back to single-row insert if already shut down
		err := dao.GormDB.Create(msg).Error
		if err != nil {
			zlog.Error(fmt.Sprintf("MessageBatch fallback insert: %v", err))
		}
		done <- err
		close(done)
		return done
	}
	bw.buffer = append(bw.buffer, batchItem{message: msg, done: done})
	bw.totalEnqueued++
	shouldFlush := len(bw.buffer) >= batchFlushSize
	bw.mu.Unlock()

	if shouldFlush {
		go bw.flush()
	}
	return done
}

// Flush blocks until all currently buffered messages have completed. Durable
// callers must wait on the channel returned by Enqueue, rather than relying on
// this global barrier for message-level acknowledgement.
func (bw *MessageBatchWriter) Flush() {
	bw.ensureStarted()

	bw.mu.Lock()
	for len(bw.buffer) > 0 || bw.flushing {
		if !bw.flushing && len(bw.buffer) > 0 {
			// Buffer has content but no flush is running – trigger one.
			bw.mu.Unlock()
			bw.flush()
			bw.mu.Lock()
		} else {
			// A flush is already in progress – wait for it.
			bw.flushCond.Wait()
		}
	}
	bw.mu.Unlock()
}

// Shutdown stops the flush loop and performs a final flush of any remaining messages.
func (bw *MessageBatchWriter) Shutdown() {
	bw.mu.Lock()
	if !bw.started || bw.closed {
		bw.mu.Unlock()
		return
	}
	bw.closed = true
	bw.ticker.Stop()
	close(bw.done)
	bw.mu.Unlock()

	// final flush
	bw.flush()

	zlog.Info(fmt.Sprintf("MessageBatch shutdown: enqueued=%d flushed=%d flushes=%d errors=%d",
		bw.totalEnqueued, bw.totalFlushed, bw.totalFlushes, bw.totalErrors))
}

// Stats returns current batch writer metrics.
func (bw *MessageBatchWriter) Stats() (enqueued, flushed, flushes, errors int64) {
	return bw.totalEnqueued, bw.totalFlushed, bw.totalFlushes, bw.totalErrors
}

// --- internal ---

func (bw *MessageBatchWriter) flushLoop() {
	for {
		select {
		case <-bw.ticker.C:
			bw.flush()
		case <-bw.done:
			return
		}
	}
}

func (bw *MessageBatchWriter) flush() {
	bw.mu.Lock()
	if bw.flushing || len(bw.buffer) == 0 {
		bw.mu.Unlock()
		return
	}
	bw.flushing = true
	batch := bw.buffer
	bw.buffer = nil
	bw.mu.Unlock()

	defer func() {
		bw.mu.Lock()
		bw.flushing = false
		bw.flushCond.Broadcast()
		bw.mu.Unlock()
	}()

	n := len(batch)
	messages := make([]*model.Message, n)
	for i, item := range batch {
		messages[i] = item.message
	}
	for attempt := 0; attempt < maxFlushRetries; attempt++ {
		err := dao.GormDB.CreateInBatches(messages, batchFlushSize).Error
		if err == nil {
			bw.totalFlushed += int64(n)
			bw.totalFlushes++
			completeBatch(batch, nil)
			return
		}
		if attempt == maxFlushRetries-1 {
			bw.totalErrors += int64(n)
			ChatMetrics.batchFlushErrors.Add(1)
			zlog.Error(fmt.Sprintf("MessageBatch flush FAILED after %d retries: %d messages lost, last error: %v",
				maxFlushRetries, n, err))
			completeBatch(batch, err)
			return
		}
		delay := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
		zlog.Error(fmt.Sprintf("MessageBatch flush retry %d/%d in %v: %v", attempt+1, maxFlushRetries, delay, err))
		time.Sleep(delay)
	}
}

func completeBatch(batch []batchItem, err error) {
	for _, item := range batch {
		item.done <- err
		close(item.done)
	}
}
