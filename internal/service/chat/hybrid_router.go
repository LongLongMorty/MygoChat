package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"kama_chat_server/internal/config"
	myKafka "kama_chat_server/internal/service/kafka"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/zlog"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

// MessageEnvelope 消息信封，包含全局序号以保证发送顺序
type MessageEnvelope struct {
	SeqNum  uint64 `json:"seq_num"` // 全局递增序号
	Payload []byte `json:"payload"` // 原始消息内容
}

// HybridRouter 混合路由器
// 以 channel 模式为主，当背压检测器发现 channel 持续高负载时，
// 自动将部分消息分流到 Kafka，实现动态负载均衡
type HybridRouter struct {
	Clients       map[string]*Client
	mutex         *sync.RWMutex
	Transmit      chan []byte           // 主 channel
	Login         chan *Client          // 登录通道
	Logout        chan *Client          // 退出通道
	Processor     *MessageProcessor     // 共享消息处理器
	SessionRouter *SessionRouter        // 会话路由器，保证单会话消息顺序
	Detector      *BackpressureDetector // 背压检测器

	// Kafka 相关（仅在溢出模式时启用）
	kafkaEnabled bool // Kafka 是否已初始化
	kafkaQuit    chan os.Signal

	// 会话路由粘性：一旦某会话进入 Kafka，在恢复前始终走 Kafka
	sessionRoutedToKafka map[string]bool
	sessionRoutedAt      map[string]time.Time // 记录粘滞开始时间，用于 TTL 清理
	sessionMutex         *sync.RWMutex

	// 每会话独立的消息序号生成器（保证会话内发送顺序）
	sessionSeqNums map[string]*atomic.Uint64
	seqMutex       *sync.RWMutex
}

var HybridChatRouter *HybridRouter

func init() {
	if HybridChatRouter == nil {
		HybridChatRouter = &HybridRouter{
			Clients:              make(map[string]*Client),
			mutex:                &sync.RWMutex{},
			Transmit:             make(chan []byte, constants.CHANNEL_SIZE),
			Login:                make(chan *Client, constants.CHANNEL_SIZE),
			Logout:               make(chan *Client, constants.CHANNEL_SIZE),
			sessionRoutedToKafka: make(map[string]bool),
			sessionRoutedAt:      make(map[string]time.Time),
			sessionMutex:         &sync.RWMutex{},
			sessionSeqNums:       make(map[string]*atomic.Uint64),
			seqMutex:             &sync.RWMutex{},
		}
		HybridChatRouter.Processor = NewMessageProcessor(HybridChatRouter.Clients, HybridChatRouter.mutex)

		// 创建会话路由器，保证单会话消息顺序
		HybridChatRouter.SessionRouter = NewSessionRouter(HybridChatRouter.Processor, constants.CHANNEL_SIZE)

		// 背压检测器：阈值 = CHANNEL_SIZE * 4/5，持续 5 秒触发溢出
		threshold := constants.CHANNEL_SIZE * 4 / 5
		if threshold < 1 {
			threshold = 1
		}
		HybridChatRouter.Detector = NewBackpressureDetector(HybridChatRouter.Transmit, threshold, 5)

		// 背压解除后（channel 低水位持续 10 秒冷却），清空黏滞 session 路由状态
		// 此时 Kafka 积压消息已被消费完毕，可安全切回 channel
		HybridChatRouter.Detector.SetOverflowEndCallback(HybridChatRouter.ClearSessionRouting)
	}
}

// getOrCreateSessionSeq 获取或创建会话的序号计数器
func (h *HybridRouter) getOrCreateSessionSeq(sessionId string) *atomic.Uint64 {
	// 先尝试读锁
	h.seqMutex.RLock()
	seq, exists := h.sessionSeqNums[sessionId]
	h.seqMutex.RUnlock()

	if exists {
		return seq
	}

	// 不存在则创建（需要写锁）
	h.seqMutex.Lock()
	defer h.seqMutex.Unlock()

	// 双重检查，避免并发创建
	if seq, exists := h.sessionSeqNums[sessionId]; exists {
		return seq
	}

	// 创建新的序号计数器（初始值为0，第一次Add(1)会返回1）
	seq = &atomic.Uint64{}
	h.sessionSeqNums[sessionId] = seq
	return seq
}

// Start 启动混合路由器
func (h *HybridRouter) Start() {
	defer func() {
		close(h.Transmit)
		close(h.Logout)
		close(h.Login)
	}()

	// 启动背压检测器
	h.Detector.Start()

	// 如果 Kafka 在配置中启用，初始化并启动消费者
	kafkaConfig := config.GetConfig().KafkaConfig
	if kafkaConfig.MessageMode == "kafka" || kafkaConfig.MessageMode == "hybrid" {
		h.initKafka()
	}

	// 启动黏滞 session TTL 自动清理（安全网）
	go h.startStickySessionCleanup()

	for {
		select {
		case client := <-h.Login:
			h.mutex.Lock()
			h.Clients[client.Uuid] = client
			h.mutex.Unlock()
			zlog.Debug(fmt.Sprintf("欢迎来到 kama 聊天服务器，用户 %s", client.Uuid))
			if err := client.Conn.WriteMessage(websocket.TextMessage, []byte("欢迎来到 kama 聊天服务器")); err != nil {
				zlog.Error(err.Error())
			}

		case client := <-h.Logout:
			h.mutex.Lock()
			delete(h.Clients, client.Uuid)
			h.mutex.Unlock()
			zlog.Info(fmt.Sprintf("用户 %s 退出登录", client.Uuid))
			if err := client.Conn.WriteMessage(websocket.TextMessage, []byte("已退出登录")); err != nil {
				zlog.Error(err.Error())
			}

		case data := <-h.Transmit:
			// channel 路径：通过会话路由器处理，保证会话内顺序
			if err := h.SessionRouter.EnqueueMessage(data); err != nil {
				zlog.Error("会话路由失败: " + err.Error())
				if h.kafkaEnabled {
					if fallbackErr := h.sendEnvelopeToKafka(data); fallbackErr != nil {
						zlog.Error("会话路由失败后的 Kafka 分流失败: " + fallbackErr.Error())
					}
				}
			}
		}
	}
}

// initKafka 初始化 Kafka writer + reader，启动消费 goroutine
func (h *HybridRouter) initKafka() {
	if h.kafkaEnabled {
		return
	}
	myKafka.KafkaService.KafkaInit()
	// Ensure the chat topic exists before the consumer group joins; otherwise
	// FetchMessage can block indefinitely with an empty partition assignment.
	myKafka.KafkaService.CreateTopic()
	h.kafkaEnabled = true
	h.kafkaQuit = make(chan os.Signal, 1)

	// 启动 Kafka 消费 goroutine
	go h.consumeKafkaMessages()

	zlog.Info("混合路由器：Kafka 已初始化")
}

// consumeKafkaMessages 消费 Kafka 消息并交给 Processor 处理
// 内部 panic 会自动恢复并重启消费者 goroutine
//
// P2-3 批量消费优化：原实现逐条 Fetch→EnqueueMessageAndWait→CommitMessages，
// 单条串行导致网络往返 + 落库等待成为瓶颈（实测 ~19 msg/s）。
// 改为批量 Fetch + 批量 EnqueueMessagesAndWait + 批量 Commit，保留逐条
// done-channel 持久化确认与"落库成功才提交 offset"的可靠性语义。
func (h *HybridRouter) consumeKafkaMessages() {
	const (
		baseDelay        = 100 * time.Millisecond
		maxDelay         = 30 * time.Second
		maxRetryCount    = 10
		kafkaBatchSize   = 200   // 单批拉取消息数
		batchFetchWindow = 200   // 批量拉取的 Fetch 次数上限（一次性快速拉满整批）
	)
	restartDelay := 1 * time.Second

	// 外层循环：自动恢复 panic
	for {
		select {
		case <-h.kafkaQuit:
			return
		default:
		}

		// 内层循环：正常消费
		func() {
			defer func() {
				if r := recover(); r != nil {
					zlog.Error(fmt.Sprintf("Kafka consumer panic，将在 %v 后重启: %v", restartDelay, r))
					time.Sleep(restartDelay)
					// 指数退避，最大 30s
					restartDelay *= 2
					if restartDelay > maxDelay {
						restartDelay = maxDelay
					}
				}
			}()

			// 恢复成功后将退避重置
			restartDelay = 1 * time.Second

			var (
				retryCount = 0
			)

			for {
				select {
				case <-h.kafkaQuit:
					return
				default:
					// 1. 批量 Fetch：最多拉取 kafkaBatchSize 条（分批避免单次阻塞整批）
					batch := make([]kafka.Message, 0, kafkaBatchSize)
					fetchErr := false
					for len(batch) < kafkaBatchSize {
						if fetchErr || len(batch) >= batchFetchWindow {
							break
						}
						m, err := myKafka.KafkaService.ChatReader.FetchMessage(ctx)
						if err != nil {
							fetchErr = true
							retryCount++
							delay := baseDelay * (1 << uint(retryCount))
							if delay > maxDelay {
								delay = maxDelay
							}
							zlog.Error(fmt.Sprintf("Kafka 读取失败 (重试 %d/%d): %v", retryCount, maxRetryCount, err))
							time.Sleep(delay)
							if retryCount >= maxRetryCount {
								retryCount = 0
								time.Sleep(maxDelay)
							}
							break
						}
						retryCount = 0
						batch = append(batch, m)
					}

					// 整批都拉取失败（无消息可读），等待后继续
					if len(batch) == 0 {
						if !fetchErr {
							time.Sleep(baseDelay)
						}
						continue
					}

					zlog.Debug(fmt.Sprintf("Kafka 批量消费: 本次 %d 条，首 offset=%d，末 offset=%d",
						len(batch), batch[0].Offset, batch[len(batch)-1].Offset))

					// 2. 批量处理并等待全部持久化（任一失败则整体重试）
					payloads := make([][]byte, len(batch))
					for i := range batch {
						payloads[i] = batch[i].Value
					}
					processCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
					err := h.SessionRouter.EnqueueMessagesAndWait(processCtx, payloads)
					cancel()
					if err != nil {
						zlog.Error("Kafka 批量消息处理失败，重试整批: " + err.Error())
						time.Sleep(baseDelay)
						continue
					}

					// 3. 批量提交 offset（提交本批最后一条即覆盖整批）
					//    kafka-go 的 CommitMessages 支持多条，提交最高位点
					if err := myKafka.KafkaService.ChatReader.CommitMessages(context.Background(), batch...); err != nil {
						ChatMetrics.kafkaCommitFailures.Add(1)
						zlog.Error("Kafka 批量 offset 提交失败，重试整批: " + err.Error())
						time.Sleep(baseDelay)
						continue
					}
				}
			}
		}()
	}
}

// SendMessage 混合发送消息
// 正常情况：写入 channel
// 背压溢出时：分流到 Kafka
// 会话粘性：一旦某会话进入 Kafka，在恢复前始终走 Kafka
func (h *HybridRouter) SendMessage(data []byte) error {
	// 提取 session_id（需要先提取才能分配会话序号）
	var msg struct {
		SessionId string `json:"session_id"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		zlog.Error("解析消息 session_id 失败: " + err.Error())
		// 无法识别会话，无法分配序号，直接拒绝
		return fmt.Errorf("消息格式错误，无法解析 session_id: %w", err)
	}

	sessionId := msg.SessionId
	if sessionId == "" {
		zlog.Error("消息缺少 session_id")
		// session_id 为空，无法路由，直接拒绝
		return fmt.Errorf("消息缺少 session_id")
	}

	// 分配该会话的独立序号（每个会话从1开始递增）
	seqNum := h.getOrCreateSessionSeq(sessionId).Add(1)
	envelope := MessageEnvelope{
		SeqNum:  seqNum,
		Payload: data,
	}
	envelopeData, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("序列化消息信封失败: %w", err)
	}

	// 检查会话是否已路由到 Kafka（会话粘性）
	h.sessionMutex.RLock()
	isRoutedToKafka := h.sessionRoutedToKafka[sessionId]
	h.sessionMutex.RUnlock()

	// 如果会话已在 Kafka 路径，或当前处于溢出模式，则走 Kafka
	if (isRoutedToKafka || h.Detector.IsOverflow()) && h.kafkaEnabled {
		// 标记会话已路由到 Kafka
		if !isRoutedToKafka {
			h.sessionMutex.Lock()
			h.sessionRoutedToKafka[sessionId] = true
			h.sessionRoutedAt[sessionId] = time.Now()
			h.sessionMutex.Unlock()
			zlog.Info(fmt.Sprintf("会话 %s 进入 Kafka 路径", sessionId))
		}

		// 使用 session_id 作为 Kafka 分区键，确保同一会话的消息顺序
		if err := myKafka.KafkaService.ChatWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(sessionId),
			Value: envelopeData, // 发送包含序号的信封
		}); err != nil {
			// 粘滞会话：Kafka 故障时不能回退到 Channel，避免乱序
			if isRoutedToKafka {
				zlog.Error(fmt.Sprintf("会话 %s 已粘滞到 Kafka，写入失败不可回退: %v", sessionId, err))
				return fmt.Errorf("Kafka 写入失败，会话 %s 已锁定到 Kafka 路径: %w", sessionId, err)
			}
			// 新会话：首次路由时可以回退
			zlog.Error("Kafka 分流失败，回退到 channel: " + err.Error())
			return h.sendToChannel(envelopeData)
		}
		ChatMetrics.kafkaRouted.Add(1)
		return nil
	}
	return h.sendToChannel(envelopeData)
}

// sendToChannel 发送到 channel，满了则尝试 Kafka
// 注意：data 是已序列化的 MessageEnvelope
func (h *HybridRouter) sendToChannel(envelopeData []byte) error {
	select {
	case h.Transmit <- envelopeData:
		ChatMetrics.channelRouted.Add(1)
		return nil
	default:
		// channel 满了
		if h.kafkaEnabled {
			return h.sendEnvelopeToKafka(envelopeData)
		}
		return fmt.Errorf("channel 满且 Kafka 未启用")
	}
}

func (h *HybridRouter) sendEnvelopeToKafka(envelopeData []byte) error {
	var envelope MessageEnvelope
	if err := json.Unmarshal(envelopeData, &envelope); err != nil {
		return fmt.Errorf("解析消息信封失败: %w", err)
	}

	var msg struct {
		SessionId string `json:"session_id"`
	}
	sessionKey := []byte("default")
	sessionId := ""
	if err := json.Unmarshal(envelope.Payload, &msg); err == nil && msg.SessionId != "" {
		sessionKey = []byte(msg.SessionId)
		sessionId = msg.SessionId
	}

	if err := myKafka.KafkaService.ChatWriter.WriteMessages(ctx, kafka.Message{
		Key:   sessionKey,
		Value: envelopeData,
	}); err != nil {
		return fmt.Errorf("Kafka 发送失败: %w", err)
	}
	ChatMetrics.kafkaRouted.Add(1)

	if sessionId != "" {
		h.sessionMutex.Lock()
		if !h.sessionRoutedToKafka[sessionId] {
			h.sessionRoutedToKafka[sessionId] = true
			h.sessionRoutedAt[sessionId] = time.Now()
			zlog.Info(fmt.Sprintf("会话 %s 进入 Kafka 路径（设置粘滞标记）", sessionId))
		}
		h.sessionMutex.Unlock()
	}
	return nil
}

// ClearSessionRouting 清除会话路由状态（当溢出模式结束时调用）
func (h *HybridRouter) ClearSessionRouting() {
	h.sessionMutex.Lock()
	defer h.sessionMutex.Unlock()

	count := len(h.sessionRoutedToKafka)
	if count > 0 {
		h.sessionRoutedToKafka = make(map[string]bool)
		h.sessionRoutedAt = make(map[string]time.Time)
		zlog.Info(fmt.Sprintf("已清除 %d 个会话的 Kafka 路由状态", count))
	}
}

// startStickySessionCleanup 定时清理过期的黏滞 session（TTL = 5 分钟）
// 安全网：防止 Kafka consumer 永久失效时 session 永远卡在 Kafka 路径
func (h *HybridRouter) startStickySessionCleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-h.kafkaQuit:
			return
		case <-ticker.C:
			h.sessionMutex.Lock()
			now := time.Now()
			for sessionId, routedAt := range h.sessionRoutedAt {
				if now.Sub(routedAt) > 5*time.Minute {
					delete(h.sessionRoutedToKafka, sessionId)
					delete(h.sessionRoutedAt, sessionId)
					zlog.Warn(fmt.Sprintf("黏滞 session %s 超过 TTL（5min），已自动清理", sessionId))
				}
			}
			h.sessionMutex.Unlock()
		}
	}
}

// Close 关闭路由器
// Close 关闭路由器
func (h *HybridRouter) Close() {
	h.Detector.Stop()
	h.SessionRouter.Close()
	h.Processor.Close()
	if h.kafkaEnabled {
		if h.kafkaQuit != nil {
			close(h.kafkaQuit)
		}
		myKafka.KafkaService.KafkaClose()
	}
	zlog.Info(fmt.Sprintf("聊天链路指标: %+v", ChatMetrics.Snapshot()))
}

// SendClientToLogin 发送客户端到登录通道
func (h *HybridRouter) SendClientToLogin(client *Client) {
	h.mutex.Lock()
	h.Login <- client
	h.mutex.Unlock()
}

// SendClientToLogout 发送客户端到登出通道
func (h *HybridRouter) SendClientToLogout(client *Client) {
	h.mutex.Lock()
	h.Logout <- client
	h.mutex.Unlock()
}

// RemoveClient 移除客户端
func (h *HybridRouter) RemoveClient(uuid string) {
	h.mutex.Lock()
	delete(h.Clients, uuid)
	h.mutex.Unlock()
}

// ensureJSONValid 确保 JSON 有效（用于调试）
func ensureJSONValid(data []byte) bool {
	var v interface{}
	return json.Unmarshal(data, &v) == nil
}
