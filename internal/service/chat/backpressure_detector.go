package chat

import (
	"sync"
	"sync/atomic"
	"time"

	"kama_chat_server/pkg/zlog"
)

// BackpressureDetector 背压检测器
// 持续监控 channel 缓冲区深度，当持续超过高水位 N 秒时触发溢出模式
// 当低于低水位并冷却一段时间后，退出溢出模式
type BackpressureDetector struct {
	channel       chan []byte // 被监控的 channel
	highThreshold int         // 高水位阈值（如 channel 容量的 4/5）
	lowThreshold  int         // 低水位阈值（如 channel 容量的 3/5）
	duration      time.Duration // 持续超过高水位的时间窗口
	cooldown      time.Duration // 低于低水位的冷却时间窗口
	overflowMode  atomic.Bool  // 当前是否处于溢出模式
	aboveSince    time.Time    // 从何时开始超过高水位（零值表示未超过）
	belowSince    time.Time    // 从何时开始低于低水位（零值表示未低于）
	stopCh        chan struct{}
	closeOnce     sync.Once    // 防止重复关闭panic
	mu            sync.Mutex
	onOverflowEnd func()       // 溢出模式结束时的回调
}

// NewBackpressureDetector 创建背压检测器
// channel: 被监控的 Transmit channel
// highThreshold: 高水位阈值（如 80 表示 80 条消息）
// durationSec: 持续超过高水位多少秒后触发溢出（如 5 表示 5 秒）
func NewBackpressureDetector(channel chan []byte, highThreshold int, durationSec int) *BackpressureDetector {
	// 低水位设为高水位的 75%，避免频繁切换
	lowThreshold := highThreshold * 3 / 4
	if lowThreshold < 1 {
		lowThreshold = 1
	}

	return &BackpressureDetector{
		channel:       channel,
		highThreshold: highThreshold,
		lowThreshold:  lowThreshold,
		duration:      time.Duration(durationSec) * time.Second,
		cooldown:      10 * time.Second, // 冷却窗口 10 秒
		stopCh:        make(chan struct{}),
	}
}

// Start 启动检测 goroutine
func (b *BackpressureDetector) Start() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-b.stopCh:
				return
			case <-ticker.C:
				b.check()
			}
		}
	}()
	zlog.Info("背压检测器已启动")
}

// Stop 停止检测
func (b *BackpressureDetector) Stop() {
	b.closeOnce.Do(func() {
		close(b.stopCh)
	})
}

// SetOverflowEndCallback 设置溢出模式结束时的回调
func (b *BackpressureDetector) SetOverflowEndCallback(callback func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onOverflowEnd = callback
}

// IsOverflow 当前是否处于溢出模式
func (b *BackpressureDetector) IsOverflow() bool {
	return b.overflowMode.Load()
}

// CurrentDepth 当前 channel 缓冲深度
func (b *BackpressureDetector) CurrentDepth() int {
	return len(b.channel)
}

// check 每秒检查一次 channel 深度
func (b *BackpressureDetector) check() {
	depth := len(b.channel)
	b.mu.Lock()
	defer b.mu.Unlock()

	if depth > b.highThreshold {
		// 超过高水位
		if b.aboveSince.IsZero() {
			// 首次超过高水位
			b.aboveSince = time.Now()
			zlog.Info("channel 深度超过高水位")
		}

		// 检查是否持续超过高水位达 duration 秒
		if !b.overflowMode.Load() && time.Since(b.aboveSince) >= b.duration {
			b.overflowMode.Store(true)
			zlog.Info("触发背压溢出模式，消息将分流到 Kafka")
		}

		// 超过高水位时，清除低水位计时器
		if !b.belowSince.IsZero() {
			b.belowSince = time.Time{}
		}

	} else if depth <= b.lowThreshold {
		// 低于低水位
		if b.overflowMode.Load() {
			// 处于溢出模式，开始冷却计时
			if b.belowSince.IsZero() {
				b.belowSince = time.Now()
				zlog.Info("channel 深度低于低水位，开始冷却计时")
			}

			// 检查是否持续低于低水位达 cooldown 秒
			if time.Since(b.belowSince) >= b.cooldown {
				b.overflowMode.Store(false)
				b.belowSince = time.Time{}
				zlog.Info("背压溢出模式已解除，恢复纯 channel 模式")

				// 调用回调通知路由器清除会话路由状态
				if b.onOverflowEnd != nil {
					b.onOverflowEnd()
				}
			}
		}

		// 低于低水位时，清除高水位计时器
		if !b.aboveSince.IsZero() {
			b.aboveSince = time.Time{}
		}

	} else {
		// 在高低水位之间，维持当前状态，不改变计时器
		// 这是滞后区间，避免频繁切换
	}
}
