package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	myconfig "kama_chat_server/internal/config"
	"kama_chat_server/pkg/zlog"
)

var ctx = context.Background()

type kafkaService struct {
	ChatWriter    *kafka.Writer
	ChatReader    *kafka.Reader
	ChatDLQWriter *kafka.Writer
	KafkaConn     *kafka.Conn
}

var KafkaService = new(kafkaService)

// KafkaInit 初始化kafka
func (k *kafkaService) KafkaInit() {
	kafkaConfig := myconfig.GetConfig().KafkaConfig
	groupID := kafkaConfig.GroupID
	if groupID == "" {
		groupID = "chat"
	}
	k.ChatWriter = &kafka.Writer{
		Addr:                   kafka.TCP(kafkaConfig.HostPort),
		Topic:                  kafkaConfig.ChatTopic,
		Balancer:               &kafka.Hash{},
		WriteTimeout:           kafkaConfig.Timeout * time.Second,
		RequiredAcks:           kafka.RequireAll, // 等待所有副本确认，保证可靠投递
		AllowAutoTopicCreation: kafkaConfig.AllowAutoTopicCreation,
	}
	k.ChatReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaConfig.HostPort},
		Topic:   kafkaConfig.ChatTopic,
		// FetchMessage + CommitMessages in HybridRouter controls the commit
		// boundary. Do not auto-commit before database persistence succeeds.
		CommitInterval: 0,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset, // 从头消费，避免跳过积压消息
	})

	// 死信主题 writer：批处理连续失败达到上限后转投
	if kafkaConfig.DLQTopic != "" {
		k.ChatDLQWriter = &kafka.Writer{
			Addr:                   kafka.TCP(kafkaConfig.HostPort),
			Topic:                  kafkaConfig.DLQTopic,
			Balancer:               &kafka.Hash{},
			WriteTimeout:           kafkaConfig.Timeout * time.Second,
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: kafkaConfig.AllowAutoTopicCreation,
		}
	}
}

func (k *kafkaService) KafkaClose() {
	if err := k.ChatWriter.Close(); err != nil {
		zlog.Error(err.Error())
	}
	if err := k.ChatReader.Close(); err != nil {
		zlog.Error(err.Error())
	}
	if k.ChatDLQWriter != nil {
		if err := k.ChatDLQWriter.Close(); err != nil {
			zlog.Error(err.Error())
		}
	}
}

// buildDLQMessages 构造死信消息：保留原始 key/value，并以 header 附带原始位置、
// 重试次数、失败原因与时间，供人工排查/重放。纯函数，便于单测。
func buildDLQMessages(original []kafka.Message, cause error, attempts int, failedAt string) []kafka.Message {
	dlqMessages := make([]kafka.Message, 0, len(original))
	for _, m := range original {
		dlqMessages = append(dlqMessages, kafka.Message{
			Key:   m.Key,
			Value: m.Value,
			Headers: []kafka.Header{
				{Key: "origin_topic", Value: []byte(m.Topic)},
				{Key: "origin_partition", Value: []byte(strconv.Itoa(m.Partition))},
				{Key: "origin_offset", Value: []byte(strconv.FormatInt(m.Offset, 10))},
				{Key: "attempts", Value: []byte(strconv.Itoa(attempts))},
				{Key: "failed_at", Value: []byte(failedAt)},
				{Key: "cause", Value: []byte(cause.Error())},
			},
		})
	}
	return dlqMessages
}

// SendToDLQ 将处理失败的整批 Kafka 消息转投死信主题。返回错误时调用方应继续
// 重试整批，避免消息丢失。
func (k *kafkaService) SendToDLQ(original []kafka.Message, cause error, attempts int) error {
	if k.ChatDLQWriter == nil {
		return fmt.Errorf("DLQ writer 未初始化")
	}
	dlqMessages := buildDLQMessages(original, cause, attempts, time.Now().Format(time.RFC3339Nano))
	return k.ChatDLQWriter.WriteMessages(context.Background(), dlqMessages...)
}

// CreateTopic 创建 chat topic（开发/测试环境）。
// kafkaConfig.Partition 在旧配置中表示“写入分区号”，不是分区数；
// 创建 topic 时至少保证 1 个分区，优先使用 3 以匹配本地 Compose 默认。
func (k *kafkaService) CreateTopic() {
	kafkaConfig := myconfig.GetConfig().KafkaConfig
	chatTopic := kafkaConfig.ChatTopic
	if chatTopic == "" {
		return
	}

	var err error
	k.KafkaConn, err = kafka.Dial("tcp", kafkaConfig.HostPort)
	if err != nil {
		zlog.Error("dial kafka for CreateTopic: " + err.Error())
		return
	}

	partitions := kafkaConfig.Partition
	if partitions < 1 {
		partitions = 3
	}

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             chatTopic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}
	// 同步创建死信主题（单分区即可，仅供排查/重放）
	if kafkaConfig.DLQTopic != "" && kafkaConfig.DLQTopic != chatTopic {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             kafkaConfig.DLQTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}

	if err = k.KafkaConn.CreateTopics(topicConfigs...); err != nil {
		// Topic may already exist; log and continue so the consumer can join.
		zlog.Info("CreateTopics(" + chatTopic + "): " + err.Error())
	} else {
		zlog.Info("Kafka topic ready: " + chatTopic)
	}
}
