package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/dto/respond"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/enum/message/message_status_enum"
	"kama_chat_server/pkg/enum/message/message_type_enum"
	"kama_chat_server/pkg/util/random"
	"kama_chat_server/pkg/zlog"
	"sync"
	"time"
)

// IMessageProcessor 消息处理器接口（便于测试 mock）
type IMessageProcessor interface {
	ProcessMessage(data []byte) error
}

// MessageProcessor 消息处理器（从 server.go 提取的共享逻辑）
// channel 和 kafka 两条路径共用此逻辑，消除 500 行重复代码
type MessageProcessor struct {
	Clients        map[string]*Client
	mutex          *sync.RWMutex
	cacheTasks     chan func()
	stopCache      chan struct{}
	cacheCloseOnce sync.Once
}

const cacheTaskQueueSize = 2048

// NewMessageProcessor 创建消息处理器
func NewMessageProcessor(clients map[string]*Client, mutex *sync.RWMutex) *MessageProcessor {
	processor := &MessageProcessor{
		Clients:    clients,
		mutex:      mutex,
		cacheTasks: make(chan func(), cacheTaskQueueSize),
		stopCache:  make(chan struct{}),
	}
	go processor.runCacheWorker()
	return processor
}

// Close stops the best-effort cache worker. Message persistence and delivery
// do not depend on this worker; MySQL remains the source of truth.
func (mp *MessageProcessor) Close() {
	mp.cacheCloseOnce.Do(func() {
		close(mp.stopCache)
	})
}

func (mp *MessageProcessor) runCacheWorker() {
	for {
		select {
		case <-mp.stopCache:
			return
		case task := <-mp.cacheTasks:
			task()
		}
	}
}

func (mp *MessageProcessor) enqueueCacheTask(task func()) {
	select {
	case <-mp.stopCache:
		ChatMetrics.cacheQueueDrops.Add(1)
		return
	default:
	}

	select {
	case mp.cacheTasks <- task:
	case <-mp.stopCache:
		ChatMetrics.cacheQueueDrops.Add(1)
	default:
		// A stale cache is recoverable from MySQL. Do not let it block the
		// durable message path when Redis is slow or unavailable.
		ChatMetrics.cacheQueueDrops.Add(1)
	}
}

// ProcessMessage 处理一条原始 JSON 消息
// data: 从 WebSocket 读取的原始 JSON 字节
func (mp *MessageProcessor) ProcessMessage(data []byte) error {
	return mp.processMessage(context.Background(), data, false)
}

// ProcessMessageAndWait is used by the Kafka path. It returns only after this
// message's batch item has been durably written, so its offset can be committed
// without depending on unrelated batch outcomes.
func (mp *MessageProcessor) ProcessMessageAndWait(ctx context.Context, data []byte) error {
	return mp.processMessage(ctx, data, true)
}

func (mp *MessageProcessor) processMessage(ctx context.Context, data []byte, waitForPersistence bool) error {
	var chatMessageReq request.ChatMessageRequest
	if err := json.Unmarshal(data, &chatMessageReq); err != nil {
		return fmt.Errorf("parse chat message: %w", err)
	}

	switch chatMessageReq.Type {
	case message_type_enum.Text:
		return mp.processText(ctx, chatMessageReq, waitForPersistence)
	case message_type_enum.File:
		return mp.processFile(ctx, chatMessageReq, waitForPersistence)
	case message_type_enum.AudioOrVideo:
		return mp.processAudioOrVideo(ctx, chatMessageReq, waitForPersistence)
	default:
		return fmt.Errorf("unknown message type: %d", chatMessageReq.Type)
	}
}

// processText 处理文本消息
func (mp *MessageProcessor) processText(ctx context.Context, chatMessageReq request.ChatMessageRequest, waitForPersistence bool) error {
	message := model.Message{
		Uuid:       fmt.Sprintf("M%s", random.GetNowAndLenRandomString(11)),
		SessionId:  chatMessageReq.SessionId,
		Type:       chatMessageReq.Type,
		Content:    chatMessageReq.Content,
		Url:        "",
		SendId:     chatMessageReq.SendId,
		SendName:   chatMessageReq.SendName,
		SendAvatar: chatMessageReq.SendAvatar,
		ReceiveId:  chatMessageReq.ReceiveId,
		FileSize:   "0B",
		FileType:   "",
		FileName:   "",
		Status:     message_status_enum.Unsent,
		CreatedAt:  time.Now(),
		AVdata:     "",
	}
	message.SendAvatar = normalizePath(message.SendAvatar)
	if err := mp.persistMessage(ctx, &message, waitForPersistence); err != nil {
		return err
	}

	if len(message.ReceiveId) == 0 {
		return fmt.Errorf("message receiver is required")
	}
	if message.ReceiveId[0] == 'U' {
		mp.forwardToUser(message, chatMessageReq.SendAvatar)
	} else if message.ReceiveId[0] == 'G' {
		mp.forwardToGroup(message, chatMessageReq.SendAvatar)
	}
	return nil
}

// processFile 处理文件消息
func (mp *MessageProcessor) processFile(ctx context.Context, chatMessageReq request.ChatMessageRequest, waitForPersistence bool) error {
	message := model.Message{
		Uuid:       fmt.Sprintf("M%s", random.GetNowAndLenRandomString(11)),
		SessionId:  chatMessageReq.SessionId,
		Type:       chatMessageReq.Type,
		Content:    "",
		Url:        chatMessageReq.Url,
		SendId:     chatMessageReq.SendId,
		SendName:   chatMessageReq.SendName,
		SendAvatar: chatMessageReq.SendAvatar,
		ReceiveId:  chatMessageReq.ReceiveId,
		FileSize:   chatMessageReq.FileSize,
		FileType:   chatMessageReq.FileType,
		FileName:   chatMessageReq.FileName,
		Status:     message_status_enum.Unsent,
		CreatedAt:  time.Now(),
		AVdata:     "",
	}
	message.SendAvatar = normalizePath(message.SendAvatar)
	if err := mp.persistMessage(ctx, &message, waitForPersistence); err != nil {
		return err
	}

	if len(message.ReceiveId) == 0 {
		return fmt.Errorf("message receiver is required")
	}
	if message.ReceiveId[0] == 'U' {
		mp.forwardToUser(message, chatMessageReq.SendAvatar)
	} else if message.ReceiveId[0] == 'G' {
		mp.forwardToGroup(message, chatMessageReq.SendAvatar)
	}
	return nil
}

// processAudioOrVideo 处理音视频通话信令
func (mp *MessageProcessor) processAudioOrVideo(ctx context.Context, chatMessageReq request.ChatMessageRequest, waitForPersistence bool) error {
	var avData request.AVData
	if err := json.Unmarshal([]byte(chatMessageReq.AVdata), &avData); err != nil {
		return fmt.Errorf("parse audio/video payload: %w", err)
	}

	message := model.Message{
		Uuid:       fmt.Sprintf("M%s", random.GetNowAndLenRandomString(11)),
		SessionId:  chatMessageReq.SessionId,
		Type:       chatMessageReq.Type,
		Content:    "",
		Url:        "",
		SendId:     chatMessageReq.SendId,
		SendName:   chatMessageReq.SendName,
		SendAvatar: chatMessageReq.SendAvatar,
		ReceiveId:  chatMessageReq.ReceiveId,
		FileSize:   "",
		FileType:   "",
		FileName:   "",
		Status:     message_status_enum.Unsent,
		CreatedAt:  time.Now(),
		AVdata:     chatMessageReq.AVdata,
	}

	// 仅 PROXY 且特定信令类型才落库
	if avData.MessageId == "PROXY" && (avData.Type == "start_call" || avData.Type == "receive_call" || avData.Type == "reject_call") {
		message.SendAvatar = normalizePath(message.SendAvatar)
		if err := mp.persistMessage(ctx, &message, waitForPersistence); err != nil {
			return err
		}
	}

	// 音视频仅转发给单聊对方，不回显
	if len(message.ReceiveId) == 0 {
		return fmt.Errorf("message receiver is required")
	}
	if message.ReceiveId[0] == 'U' {
		messageRsp := respond.AVMessageRespond{
			SendId:     message.SendId,
			SendName:   message.SendName,
			SendAvatar: message.SendAvatar,
			ReceiveId:  message.ReceiveId,
			Type:       message.Type,
			Content:    message.Content,
			Url:        message.Url,
			FileSize:   message.FileSize,
			FileName:   message.FileName,
			FileType:   message.FileType,
			CreatedAt:  message.CreatedAt.Format("2006-01-02 15:04:05"),
			AVdata:     message.AVdata,
		}
		jsonMessage, err := json.Marshal(messageRsp)
		if err != nil {
			zlog.Error(err.Error())
		}
		var messageBack = &MessageBack{
			Message: jsonMessage,
			Uuid:    message.Uuid,
		}
		if receiveClient := mp.getClient(message.ReceiveId); receiveClient != nil {
			if err := receiveClient.EnqueueDelivery(messageBack); err != nil {
				zlog.Warn(fmt.Sprintf("用户 %s 音视频信令未实时送达: %v", message.ReceiveId, err))
			}
		}
	}
	return nil
}

func (mp *MessageProcessor) persistMessage(ctx context.Context, message *model.Message, waitForPersistence bool) error {
	done := MessageBatch.Enqueue(message)
	if !waitForPersistence {
		return nil
	}

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("persist message: %w", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for message persistence: %w", ctx.Err())
	}
}

// forwardToUser 转发消息给单聊用户（含发送方回显 + Redis 缓存）
func (mp *MessageProcessor) forwardToUser(message model.Message, rawAvatar string) {
	messageRsp := respond.GetMessageListRespond{
		SendId:     message.SendId,
		SendName:   message.SendName,
		SendAvatar: rawAvatar,
		ReceiveId:  message.ReceiveId,
		Type:       message.Type,
		Content:    message.Content,
		Url:        message.Url,
		FileSize:   message.FileSize,
		FileName:   message.FileName,
		FileType:   message.FileType,
		CreatedAt:  message.CreatedAt.Format("2006-01-02 15:04:05"),
	}
	jsonMessage, err := json.Marshal(messageRsp)
	if err != nil {
		zlog.Error(err.Error())
	}
	var messageBack = &MessageBack{
		Message: jsonMessage,
		Uuid:    message.Uuid,
	}

	// Do not hold the shared client-map lock while waiting for an outbound
	// queue. Slow clients must not block unrelated logins or deliveries.
	if receiveClient := mp.getClient(message.ReceiveId); receiveClient != nil {
		if err := receiveClient.EnqueueDelivery(messageBack); err != nil {
			zlog.Warn(fmt.Sprintf("用户 %s 消息未实时送达: %v", message.ReceiveId, err))
		}
	}
	if sendClient := mp.getClient(message.SendId); sendClient != nil {
		if err := sendClient.EnqueueDelivery(messageBack); err != nil {
			zlog.Warn(fmt.Sprintf("用户 %s 回显未实时送达: %v", message.SendId, err))
		}
	}

	// Redis is a cache, not the durability boundary. Keep its network and JSON
	// work out of the ordered message-processing path.
	mp.enqueueCacheTask(func() {
		mp.updateUserMessageCache(message, messageRsp)
	})
}

// forwardToGroup 转发消息给群成员（含发送方回显 + Redis 缓存）
func (mp *MessageProcessor) forwardToGroup(message model.Message, rawAvatar string) {
	messageRsp := respond.GetGroupMessageListRespond{
		SendId:     message.SendId,
		SendName:   message.SendName,
		SendAvatar: rawAvatar,
		ReceiveId:  message.ReceiveId,
		Type:       message.Type,
		Content:    message.Content,
		Url:        message.Url,
		FileSize:   message.FileSize,
		FileName:   message.FileName,
		FileType:   message.FileType,
		CreatedAt:  message.CreatedAt.Format("2006-01-02 15:04:05"),
	}
	jsonMessage, err := json.Marshal(messageRsp)
	if err != nil {
		zlog.Error(err.Error())
	}
	var messageBack = &MessageBack{
		Message: jsonMessage,
		Uuid:    message.Uuid,
	}

	// 查群成员列表
	var group model.GroupInfo
	if res := dao.GormDB.Where("uuid = ?", message.ReceiveId).First(&group); res.Error != nil {
		zlog.Error(res.Error.Error())
	}
	var members []string
	if err := json.Unmarshal(group.Members, &members); err != nil {
		zlog.Error(err.Error())
	}

	// Copy client references under the map lock, then enqueue outside it.
	type deliveryTarget struct {
		uuid   string
		client *Client
	}
	targets := make([]deliveryTarget, 0, len(members))
	mp.mutex.RLock()
	for _, member := range members {
		if client, ok := mp.Clients[member]; ok {
			targets = append(targets, deliveryTarget{uuid: member, client: client})
		}
	}
	mp.mutex.RUnlock()
	for _, target := range targets {
		if err := target.client.EnqueueDelivery(messageBack); err != nil {
			zlog.Warn(fmt.Sprintf("群成员 %s 消息未实时送达: %v", target.uuid, err))
		}
	}

	mp.enqueueCacheTask(func() {
		mp.updateGroupMessageCache(message, messageRsp)
	})
}

func (mp *MessageProcessor) getClient(uuid string) *Client {
	mp.mutex.RLock()
	client := mp.Clients[uuid]
	mp.mutex.RUnlock()
	return client
}

// updateUserMessageCache 更新私聊消息 Redis 缓存
func (mp *MessageProcessor) updateUserMessageCache(message model.Message, messageRsp respond.GetMessageListRespond) {
	key := "message_list_" + message.SendId + "_" + message.ReceiveId
	rspString, err := myredis.GetKeyNilIsErr(key)
	if err == nil {
		var rsp []respond.GetMessageListRespond
		if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
			zlog.Error(err.Error())
		}
		rsp = append(rsp, messageRsp)
		rspByte, err := json.Marshal(rsp)
		if err != nil {
			zlog.Error(err.Error())
		}
		if err := myredis.SetKeyEx(key, string(rspByte), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			zlog.Error(err.Error())
		}
	} else {
		if !errors.Is(err, redis.Nil) {
			zlog.Error(err.Error())
		}
	}
}

// updateGroupMessageCache 更新群聊消息 Redis 缓存
func (mp *MessageProcessor) updateGroupMessageCache(message model.Message, messageRsp respond.GetGroupMessageListRespond) {
	key := "group_messagelist_" + message.ReceiveId
	rspString, err := myredis.GetKeyNilIsErr(key)
	if err == nil {
		var rsp []respond.GetGroupMessageListRespond
		if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
			zlog.Error(err.Error())
		}
		rsp = append(rsp, messageRsp)
		rspByte, err := json.Marshal(rsp)
		if err != nil {
			zlog.Error(err.Error())
		}
		if err := myredis.SetKeyEx(key, string(rspByte), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			zlog.Error(err.Error())
		}
	} else {
		if !errors.Is(err, redis.Nil) {
			zlog.Error(err.Error())
		}
	}
}
