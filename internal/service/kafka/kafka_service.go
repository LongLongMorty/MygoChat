package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	myconfig "kama_chat_server/internal/config"
	"kama_chat_server/pkg/zlog"
	"time"
)

var ctx = context.Background()

type kafkaService struct {
	ChatWriter *kafka.Writer
	ChatReader *kafka.Reader
	KafkaConn  *kafka.Conn
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
}

func (k *kafkaService) KafkaClose() {
	if err := k.ChatWriter.Close(); err != nil {
		zlog.Error(err.Error())
	}
	if err := k.ChatReader.Close(); err != nil {
		zlog.Error(err.Error())
	}
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

	if err = k.KafkaConn.CreateTopics(topicConfigs...); err != nil {
		// Topic may already exist; log and continue so the consumer can join.
		zlog.Info("CreateTopics(" + chatTopic + "): " + err.Error())
	} else {
		zlog.Info("Kafka topic ready: " + chatTopic)
	}
}
