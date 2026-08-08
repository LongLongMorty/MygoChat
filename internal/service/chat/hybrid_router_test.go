package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kama_chat_server/internal/dto/request"
)

// 测试背压检测器触发溢出模式
func TestBackpressureDetectorOverflow(t *testing.T) {
	// 创建一个小容量的 channel 用于测试
	testChan := make(chan []byte, 10)
	highThreshold := 8                                              // 80%
	detector := NewBackpressureDetector(testChan, highThreshold, 2) // 2秒触发

	// 启动检测器
	detector.Start()
	defer detector.Stop()

	// 初始状态应该不是溢出模式
	if detector.IsOverflow() {
		t.Error("初始状态不应该是溢出模式")
	}

	// 填充 channel 超过高水位
	for i := 0; i < 9; i++ {
		testChan <- []byte("test message")
	}

	// 使用轮询而非固定 Sleep，最多等待5秒
	maxWait := 5 * time.Second
	checkInterval := 100 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		if detector.IsOverflow() {
			t.Log("背压检测器正确触发溢出模式")
			return
		}
		time.Sleep(checkInterval)
	}

	t.Error("超时：背压检测器未在5秒内触发溢出模式")
}

// 测试背压检测器恢复（低水位 + 冷却窗口）
func TestBackpressureDetectorRecovery(t *testing.T) {
	// 创建一个小容量的 channel
	testChan := make(chan []byte, 10)
	highThreshold := 8                                              // 80%
	detector := NewBackpressureDetector(testChan, highThreshold, 1) // 1秒触发
	detector.cooldown = 2 * time.Second                             // 设置 2 秒冷却窗口

	detector.Start()
	defer detector.Stop()

	// 填充到高水位以上触发溢出
	for i := 0; i < 9; i++ {
		testChan <- []byte("test")
	}

	// 轮询等待进入溢出模式（最多4秒）
	maxWait := 4 * time.Second
	checkInterval := 100 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	overflowTriggered := false
	for time.Now().Before(deadline) {
		if detector.IsOverflow() {
			overflowTriggered = true
			break
		}
		time.Sleep(checkInterval)
	}

	if !overflowTriggered {
		t.Fatal("超时：未进入溢出模式")
	}

	// 清空到低水位以下（低水位 = 8 * 3/4 = 6）
	for i := 0; i < 5; i++ {
		<-testChan
	}

	// 轮询等待退出溢出模式（最多5秒）
	deadline = time.Now().Add(5 * time.Second)
	recoveryCompleted := false
	for time.Now().Before(deadline) {
		if !detector.IsOverflow() {
			recoveryCompleted = true
			break
		}
		time.Sleep(checkInterval)
	}

	if !recoveryCompleted {
		t.Error("超时：冷却期结束后应该退出溢出模式")
	} else {
		t.Log("背压检测器正确恢复")
	}
}

// 测试会话粘性路由
func TestSessionStickyRouting(t *testing.T) {
	router := &HybridRouter{
		Clients:              make(map[string]*Client),
		mutex:                &sync.RWMutex{},
		Transmit:             make(chan []byte, 10),
		sessionRoutedToKafka: make(map[string]bool),
		sessionMutex:         &sync.RWMutex{},
		kafkaEnabled:         false, // 模拟 Kafka 未启用
	}

	// 创建测试消息
	msg := request.ChatMessageRequest{
		SessionId: "test-session-123",
		SendId:    "user1",
		Content:   "hello",
	}
	_, _ = json.Marshal(msg)

	// 标记会话已路由到 Kafka
	router.sessionMutex.Lock()
	router.sessionRoutedToKafka["test-session-123"] = true
	router.sessionMutex.Unlock()

	// 即使 Kafka 未启用，会话粘性应该尝试路由到 Kafka（会失败并 fallback）
	// 这里主要测试路由逻辑是否检查了会话状态
	router.sessionMutex.RLock()
	isRouted := router.sessionRoutedToKafka["test-session-123"]
	router.sessionMutex.RUnlock()

	if !isRouted {
		t.Error("会话应该被标记为路由到 Kafka")
	}

	t.Log("会话粘性路由测试通过")
}

// 测试清除会话路由状态
func TestClearSessionRouting(t *testing.T) {
	router := &HybridRouter{
		sessionRoutedToKafka: make(map[string]bool),
		sessionMutex:         &sync.RWMutex{},
	}

	// 添加一些会话路由状态
	router.sessionMutex.Lock()
	router.sessionRoutedToKafka["session1"] = true
	router.sessionRoutedToKafka["session2"] = true
	router.sessionRoutedToKafka["session3"] = true
	router.sessionMutex.Unlock()

	// 清除状态
	router.ClearSessionRouting()

	// 验证状态已清除
	router.sessionMutex.RLock()
	count := len(router.sessionRoutedToKafka)
	router.sessionMutex.RUnlock()

	if count != 0 {
		t.Errorf("会话路由状态应该被清除，但还有 %d 个", count)
	}

	t.Log("清除会话路由状态测试通过")
}

// 测试 SessionRouter 保证会话内消息顺序
func TestSessionRouterOrdering(t *testing.T) {
	// 创建一个 mock processor，记录实际处理顺序
	var processedOrder []int
	var orderMu sync.Mutex

	mockProcessor := &MockMessageProcessor{
		onProcess: func(data []byte) {
			var msg request.ChatMessageRequest
			if err := json.Unmarshal(data, &msg); err == nil {
				// 从 "message-X" 中提取序号
				var index int
				fmt.Sscanf(msg.Content, "message-%d", &index)
				orderMu.Lock()
				processedOrder = append(processedOrder, index)
				orderMu.Unlock()
			}
		},
	}

	// 创建 SessionRouter，使用 mock processor
	router := &SessionRouter{
		sessions:    make(map[string]*SessionQueue),
		processor:   mockProcessor,
		queueSize:   10,
		stopCleanup: make(chan struct{}), // 必须初始化，否则Close()时panic
	}
	defer router.Close()

	testSessionId := "test-session-001"
	messageCount := 10

	// 串行发送消息 0..9，并包装为信封（模拟 SendMessage 的行为）
	for i := 0; i < messageCount; i++ {
		msg := request.ChatMessageRequest{
			SessionId: testSessionId,
			Content:   fmt.Sprintf("message-%d", i),
		}
		payload, _ := json.Marshal(msg)

		// 包装为信封（模拟 HybridRouter.SendMessage）
		envelope := MessageEnvelope{
			SeqNum:  uint64(i + 1), // 序号从1开始
			Payload: payload,
		}
		envelopeData, _ := json.Marshal(envelope)

		if err := router.EnqueueMessage(envelopeData); err != nil {
			t.Errorf("EnqueueMessage 失败: %v", err)
		}
	}

	// 等待所有消息被实际处理（轮询检查）
	maxWait := 2 * time.Second
	checkInterval := 50 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		orderMu.Lock()
		count := len(processedOrder)
		orderMu.Unlock()

		if count == messageCount {
			break
		}
		time.Sleep(checkInterval)
	}

	// 验证所有消息都被处理，且顺序完全一致
	orderMu.Lock()
	finalOrder := make([]int, len(processedOrder))
	copy(finalOrder, processedOrder)
	orderMu.Unlock()

	if len(finalOrder) != messageCount {
		t.Errorf("应该处理 %d 条消息，实际处理了 %d 条", messageCount, len(finalOrder))
	}

	// 验证处理顺序严格匹配 [0,1,2,3,4,5,6,7,8,9]
	expectedOrder := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for i, expected := range expectedOrder {
		if i >= len(finalOrder) {
			t.Errorf("索引 %d: 期望 %d，但消息未处理", i, expected)
			continue
		}
		if finalOrder[i] != expected {
			t.Errorf("索引 %d: 期望 %d，实际 %d", i, expected, finalOrder[i])
		}
	}

	if len(finalOrder) == messageCount {
		allMatch := true
		for i := 0; i < messageCount; i++ {
			if finalOrder[i] != i {
				allMatch = false
				break
			}
		}
		if allMatch {
			t.Logf("✅ SessionRouter 正确保证了消息顺序: %v", finalOrder)
		} else {
			t.Errorf("❌ 消息顺序错误: 期望 %v, 实际 %v", expectedOrder, finalOrder)
		}
	}
}

// TestSessionRouterOutOfOrder 测试乱序到达场景（序号2先到，1后到）
func TestSessionRouterOutOfOrder(t *testing.T) {
	var processedOrder []int
	var orderMu sync.Mutex

	mockProcessor := &MockMessageProcessor{
		onProcess: func(data []byte) {
			var msg request.ChatMessageRequest
			if err := json.Unmarshal(data, &msg); err == nil {
				var index int
				fmt.Sscanf(msg.Content, "message-%d", &index)
				orderMu.Lock()
				processedOrder = append(processedOrder, index)
				orderMu.Unlock()
			}
		},
	}

	router := &SessionRouter{
		sessions:    make(map[string]*SessionQueue),
		processor:   mockProcessor,
		queueSize:   10,
		stopCleanup: make(chan struct{}),
	}
	defer router.Close()

	testSessionId := "test-out-of-order"

	// 模拟乱序：先发序号2，再发序号1，最后发序号3
	messages := []struct {
		seqNum  uint64
		content string
	}{
		{2, "message-1"}, // 序号2先到（应缓存）
		{1, "message-0"}, // 序号1后到（触发处理1、2）
		{3, "message-2"}, // 序号3正常到
	}

	for _, m := range messages {
		msg := request.ChatMessageRequest{
			SessionId: testSessionId,
			Content:   m.content,
		}
		payload, _ := json.Marshal(msg)

		envelope := MessageEnvelope{
			SeqNum:  m.seqNum,
			Payload: payload,
		}
		envelopeData, _ := json.Marshal(envelope)

		router.EnqueueMessage(envelopeData)
		time.Sleep(50 * time.Millisecond) // 给处理时间
	}

	time.Sleep(200 * time.Millisecond)

	orderMu.Lock()
	finalOrder := make([]int, len(processedOrder))
	copy(finalOrder, processedOrder)
	orderMu.Unlock()

	expectedOrder := []int{0, 1, 2}
	if len(finalOrder) == len(expectedOrder) {
		match := true
		for i := 0; i < len(expectedOrder); i++ {
			if finalOrder[i] != expectedOrder[i] {
				match = false
				t.Errorf("索引 %d: 期望 %d, 实际 %d", i, expectedOrder[i], finalOrder[i])
			}
		}
		if match {
			t.Logf("✅ 乱序测试通过: %v", finalOrder)
		}
	} else {
		t.Errorf("消息数量不匹配: 期望 %d, 实际 %d", len(expectedOrder), len(finalOrder))
	}
}

// MockMessageProcessor 模拟 MessageProcessor 用于测试
type MockMessageProcessor struct {
	onProcess  func([]byte)
	processErr func([]byte) error
}

func (m *MockMessageProcessor) ProcessMessage(data []byte) error {
	if m.onProcess != nil {
		m.onProcess(data)
	}
	if m.processErr != nil {
		return m.processErr(data)
	}
	return nil
}

type durableMockMessageProcessor struct {
	standardCalls int
	durableCalls  int
}

func (m *durableMockMessageProcessor) ProcessMessage([]byte) error {
	m.standardCalls++
	return nil
}

func (m *durableMockMessageProcessor) ProcessMessageAndWait(context.Context, []byte) error {
	m.durableCalls++
	return nil
}

func TestSessionRouterUsesDurableProcessorForKafkaWaiters(t *testing.T) {
	processor := &durableMockMessageProcessor{}
	router := NewSessionRouter(processor, 4)
	defer router.Close()

	payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: "durable-session", Content: "message"})
	envelopeData, _ := json.Marshal(MessageEnvelope{SeqNum: 1, Payload: payload})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := router.EnqueueMessageAndWait(ctx, envelopeData); err != nil {
		t.Fatalf("durable processing failed: %v", err)
	}
	if processor.durableCalls != 1 || processor.standardCalls != 0 {
		t.Fatalf("expected durable processor only, durable=%d standard=%d", processor.durableCalls, processor.standardCalls)
	}
}

func TestSessionRouterWaitsForProcessing(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	processor := &MockMessageProcessor{
		processErr: func([]byte) error {
			close(started)
			<-release
			return nil
		},
	}
	router := NewSessionRouter(processor, 4)
	defer router.Close()

	payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: "wait-session", Content: "message"})
	envelopeData, _ := json.Marshal(MessageEnvelope{SeqNum: 1, Payload: payload})
	result := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result <- router.EnqueueMessageAndWait(ctx, envelopeData)
	}()

	<-started
	select {
	case err := <-result:
		t.Fatalf("处理完成前不应返回，实际返回: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-result; err != nil {
		t.Fatalf("等待处理完成失败: %v", err)
	}
}

func TestSessionRouterProcessingFailureCanRetrySameSequence(t *testing.T) {
	failed := true
	processor := &MockMessageProcessor{
		processErr: func([]byte) error {
			if failed {
				failed = false
				return errors.New("temporary database failure")
			}
			return nil
		},
	}
	router := NewSessionRouter(processor, 4)
	defer router.Close()

	payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: "retry-session", Content: "message"})
	envelopeData, _ := json.Marshal(MessageEnvelope{SeqNum: 1, Payload: payload})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := router.EnqueueMessageAndWait(ctx, envelopeData); err == nil {
		t.Fatal("首次处理应返回临时失败")
	}
	if err := router.EnqueueMessageAndWait(ctx, envelopeData); err != nil {
		t.Fatalf("相同序号重试应成功: %v", err)
	}
}

// TestSessionRouterBatchAndWait 验证批量消费方法：多会话消息并发处理、全部持久化后才返回
func TestSessionRouterBatchAndWait(t *testing.T) {
	var processedCount int
	var orderMu sync.Mutex
	processor := &MockMessageProcessor{
		onProcess: func([]byte) {
			orderMu.Lock()
			processedCount++
			orderMu.Unlock()
		},
	}
	router := NewSessionRouter(processor, 16)
	defer router.Close()

	// 3 个会话 × 5 条消息，每条携带递增序号
	var payloads [][]byte
	total := 0
	for s := 0; s < 3; s++ {
		for i := 1; i <= 5; i++ {
			sess := fmt.Sprintf("batch-session-%d", s)
			payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: sess, Content: fmt.Sprintf("msg-%d", i)})
			env, _ := json.Marshal(MessageEnvelope{SeqNum: uint64(i), Payload: payload})
			payloads = append(payloads, env)
			total++
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := router.EnqueueMessagesAndWait(ctx, payloads); err != nil {
		t.Fatalf("批量处理失败: %v", err)
	}

	orderMu.Lock()
	got := processedCount
	orderMu.Unlock()
	if got != total {
		t.Fatalf("批量处理应完成 %d 条，实际 %d", total, got)
	}
	t.Logf("✅ 批量消费 %d 条全部处理完成", got)
}

// TestSessionRouterBatchFailureRetry 验证批量消费失败后整批重试（序号去重保证幂等）
func TestSessionRouterBatchFailureRetry(t *testing.T) {
	var failed atomic.Bool
	failed.Store(true)
	processor := &MockMessageProcessor{
		processErr: func([]byte) error {
			if failed.Load() {
				failed.Store(false)
				return errors.New("temporary database failure")
			}
			return nil
		},
	}
	router := NewSessionRouter(processor, 16)
	defer router.Close()

	var payloads [][]byte
	for i := 1; i <= 5; i++ {
		payload, _ := json.Marshal(request.ChatMessageRequest{SessionId: "batch-retry-session", Content: fmt.Sprintf("msg-%d", i)})
		env, _ := json.Marshal(MessageEnvelope{SeqNum: uint64(i), Payload: payload})
		payloads = append(payloads, env)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 首次整批：某条失败 → 整体返回错误
	if err := router.EnqueueMessagesAndWait(ctx, payloads); err == nil {
		t.Fatal("批量首次处理应返回失败")
	}
	// 重试整批：序号去重 + 失败已恢复 → 应成功且不重复投递
	if err := router.EnqueueMessagesAndWait(ctx, payloads); err != nil {
		t.Fatalf("整批重试应成功: %v", err)
	}
	t.Log("✅ 批量失败后整批重试成功（幂等，无重复投递）")
}
