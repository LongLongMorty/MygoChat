package chat

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/model"
	myKafka "kama_chat_server/internal/service/kafka"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/enum/message/message_status_enum"
	"kama_chat_server/pkg/zlog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type MessageBack struct {
	Message []byte
	Uuid    string
}

type Client struct {
	Conn      *websocket.Conn
	Uuid      string
	SendTo    chan []byte       // 给server端
	SendBack  chan *MessageBack // 给前端
	done      chan struct{}
	closeOnce sync.Once
}

const (
	clientDeliveryTimeout   = 500 * time.Millisecond
	clientDeliveryQueueSize = 4096
	deliveryStatusQueueSize = 32768
	deliveryStatusBatchSize = 100
)

var errClientClosed = errors.New("websocket client is closed")

// Delivery buffering is intentionally independent from CHANNEL_SIZE. The
// latter controls router backpressure; coupling it to a client's outbound
// queue made a small router test buffer turn into avoidable WebSocket drops.
var deliveryStatusUpdates = make(chan string, deliveryStatusQueueSize)
var queuedDeliveryStatuses sync.Map

func init() {
	go flushDeliveryStatuses()
	go reconcileUnconfirmedDeliveries()
}

// reconcileUnconfirmedDeliveries 兜底：定期将长时间未收到客户端确认的消息置为已发送
// 消息已在服务端持久化（落库即视为送达），客户端断线/不确认不应让 status 永远停留在未发送
func reconcileUnconfirmedDeliveries() {
	// 每 30 秒扫描一次，5 分钟前未确认的消息统一置为已发送
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if dao.GormDB == nil {
			continue
		}
		cutoff := time.Now().Add(-5 * time.Minute)
		if res := dao.GormDB.Model(&model.Message{}).
			Where("status = ? AND created_at < ?", message_status_enum.Unsent, cutoff).
			Update("status", message_status_enum.Sent); res.Error != nil {
			zlog.Error("确认兜底扫描失败: " + res.Error.Error())
		}
	}
}

func enqueueDeliveryStatus(uuid string) {
	if uuid == "" {
		return
	}
	if _, alreadyQueued := queuedDeliveryStatuses.LoadOrStore(uuid, struct{}{}); alreadyQueued {
		return
	}
	// A sent status is user-visible state. Apply backpressure to this client's
	// write goroutine rather than silently losing the update when the batch
	// worker is temporarily slower than the delivery rate.
	deliveryStatusUpdates <- uuid
}

func flushDeliveryStatuses() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	pending := make([]string, 0, deliveryStatusBatchSize)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		for {
			if res := dao.GormDB.Model(&model.Message{}).Where("uuid IN ?", pending).Update("status", message_status_enum.Sent); res.Error == nil {
				for _, uuid := range pending {
					queuedDeliveryStatuses.Delete(uuid)
				}
				break
			} else {
				zlog.Error("batch update delivered message status, retrying: " + res.Error.Error())
				time.Sleep(100 * time.Millisecond)
			}
		}
		pending = pending[:0]
	}

	for {
		select {
		case uuid := <-deliveryStatusUpdates:
			pending = append(pending, uuid)
			if len(pending) >= deliveryStatusBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// EnqueueDelivery only waits for a bounded period. The message has already
// been persisted before this point, so a slow connection can recover it from
// history instead of blocking every session worker indefinitely.
func (c *Client) EnqueueDelivery(message *MessageBack) error {
	timer := time.NewTimer(clientDeliveryTimeout)
	defer timer.Stop()

	select {
	case <-c.done:
		ChatMetrics.deliveryClientClosed.Add(1)
		return errClientClosed
	case c.SendBack <- message:
		return nil
	case <-timer.C:
		ChatMetrics.deliveryQueueTimeouts.Add(1)
		return errors.New("websocket delivery queue is full")
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.Conn != nil {
			_ = c.Conn.Close()
		}
	})
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  2048,
	WriteBufferSize: 2048,
	// 检查连接的Origin头
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var ctx = context.Background()

var messageMode = config.GetConfig().KafkaConfig.MessageMode

// 读取websocket消息并发送给send通道
func (c *Client) Read() {
	zlog.Info("ws read goroutine start")
	for {
		// 阻塞有一定隐患，因为下面要处理缓冲的逻辑，但是可以先不做优化，问题不大
		_, jsonMessage, err := c.Conn.ReadMessage() // 阻塞状态
		if err != nil {
			zlog.Error(err.Error())
			c.Close()
			if config.GetConfig().KafkaConfig.MessageMode == "hybrid" {
				HybridChatRouter.RemoveClient(c.Uuid)
			} else if config.GetConfig().KafkaConfig.MessageMode == "channel" {
				ChatServer.RemoveClient(c.Uuid)
			} else {
				KafkaChatServer.RemoveClient(c.Uuid)
			}
			return // 直接断开websocket
		} else {
			var message = request.ChatMessageRequest{}
			if err := json.Unmarshal(jsonMessage, &message); err != nil {
				zlog.Error(err.Error())
			}
			if messageMode == "channel" {
				ChatServer.SendMessageToTransmit(jsonMessage)
			} else if messageMode == "hybrid" {
				// P1 改造：混合模式，通过背压检测器自动分流到 Kafka
				if err := HybridChatRouter.SendMessage(jsonMessage); err != nil {
					zlog.Error("混合路由发送失败: " + err.Error())
					if err := c.Conn.WriteMessage(websocket.TextMessage, []byte("消息发送失败，请稍后重试")); err != nil {
						zlog.Error(err.Error())
					}
				}
			} else {
				if err := myKafka.KafkaService.ChatWriter.WriteMessages(ctx, kafka.Message{
					Key:   []byte(strconv.Itoa(config.GetConfig().KafkaConfig.Partition)),
					Value: jsonMessage,
				}); err != nil {
					zlog.Error(err.Error())
				}
				zlog.Info("已发送消息：" + string(jsonMessage))
			}
		}
	}
}

// 从send通道读取消息发送给websocket
func (c *Client) Write() {
	zlog.Info("ws write goroutine start")
	for {
		select {
		case <-c.done:
			return
		case messageBack := <-c.SendBack:
			if messageBack == nil {
				continue
			}
			if err := c.Conn.WriteMessage(websocket.TextMessage, messageBack.Message); err != nil {
				zlog.Error(err.Error())
				c.Close()
				return
			}
			enqueueDeliveryStatus(messageBack.Uuid)
		}
	}
}

// NewClientInit 当接受到前端有登录消息时，会调用该函数
func NewClientInit(c *gin.Context, clientId string) {
	kafkaConfig := config.GetConfig().KafkaConfig
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		zlog.Error(err.Error())
		return
	}
	client := &Client{
		Conn:     conn,
		Uuid:     clientId,
		SendTo:   make(chan []byte, constants.CHANNEL_SIZE),
		SendBack: make(chan *MessageBack, clientDeliveryQueueSize),
		done:     make(chan struct{}),
	}
	if kafkaConfig.MessageMode == "channel" {
		ChatServer.SendClientToLogin(client)
	} else if kafkaConfig.MessageMode == "hybrid" {
		// P1 改造：混合模式使用 HybridChatRouter
		HybridChatRouter.SendClientToLogin(client)
	} else {
		KafkaChatServer.SendClientToLogin(client)
	}
	go client.Read()
	go client.Write()
	zlog.Info("ws连接成功")
}

// ClientLogout 当接受到前端有登出消息时，会调用该函数
func ClientLogout(clientId string) (string, int) {
	kafkaConfig := config.GetConfig().KafkaConfig
	var client *Client
	switch kafkaConfig.MessageMode {
	case "channel":
		ChatServer.mutex.RLock()
		client = ChatServer.Clients[clientId]
		ChatServer.mutex.RUnlock()
	case "hybrid":
		HybridChatRouter.mutex.RLock()
		client = HybridChatRouter.Clients[clientId]
		HybridChatRouter.mutex.RUnlock()
	default:
		KafkaChatServer.mutex.RLock()
		client = KafkaChatServer.Clients[clientId]
		KafkaChatServer.mutex.RUnlock()
	}
	if client != nil {
		if kafkaConfig.MessageMode == "channel" {
			ChatServer.SendClientToLogout(client)
		} else if kafkaConfig.MessageMode == "hybrid" {
			HybridChatRouter.SendClientToLogout(client)
		} else {
			KafkaChatServer.SendClientToLogout(client)
		}
		client.Close()
	}
	return "退出成功", 0
}
