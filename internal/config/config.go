package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

type MainConfig struct {
	AppName string `toml:"appName"`
	Host    string `toml:"host"`
	Port    int    `toml:"port"`
}

type MysqlConfig struct {
	Host                   string `toml:"host"`
	Port                   int    `toml:"port"`
	User                   string `toml:"user"`
	Password               string `toml:"password"`
	DatabaseName           string `toml:"databaseName"`
	MaxOpenConns           int    `toml:"maxOpenConns"`
	MaxIdleConns           int    `toml:"maxIdleConns"`
	ConnMaxLifetimeSeconds int    `toml:"connMaxLifetimeSeconds"`
}

type RedisConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Password string `toml:"password"`
	Db       int    `toml:"db"`
}

type EmailConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	Username string `toml:"username"`
	Password string `toml:"password"`
	From     string `toml:"from"`
}

type LogConfig struct {
	LogPath string `toml:"logPath"`
}

type KafkaConfig struct {
	MessageMode            string        `toml:"messageMode"`
	HostPort               string        `toml:"hostPort"`
	LoginTopic             string        `toml:"loginTopic"`
	LogoutTopic            string        `toml:"logoutTopic"`
	ChatTopic              string        `toml:"chatTopic"`
	GroupID                string        `toml:"groupID"`
	AllowAutoTopicCreation bool          `toml:"allowAutoTopicCreation"`
	Partition              int           `toml:"partition"`
	Timeout                time.Duration `toml:"timeout"`
	// DLQTopic 死信主题：批处理连续失败达到上限后，将整批转投此主题并提交 offset，
	// 避免毒消息无限重试阻塞分区。为空则禁用 DLQ（保持原无限重试行为）。
	DLQTopic string `toml:"dlqTopic"`
	// MaxProcessRetries 同一批消息处理失败的最大重试次数，超过则转 DLQ
	MaxProcessRetries int `toml:"maxProcessRetries"`
}

type StaticSrcConfig struct {
	StaticAvatarPath string `toml:"staticAvatarPath"`
	StaticFilePath   string `toml:"staticFilePath"`
}

// ClusterConfig 多实例（分布式）投递配置。
// enabled=false 时行为与单实例完全一致。
type ClusterConfig struct {
	Enabled            bool   `toml:"enabled"`
	InstanceID         string `toml:"instanceId"`
	PresenceTTLSeconds int    `toml:"presenceTTLSeconds"`
	HeartbeatSeconds   int    `toml:"heartbeatSeconds"`
	StreamMaxLen       int64  `toml:"streamMaxLen"`
	// SessionAffinity 开启后，同一会话的所有消息按一致性哈希固定由某个实例处理，
	// 把单实例内的会话顺序保证扩展到跨实例。默认关闭。
	SessionAffinity bool `toml:"sessionAffinity"`
}

type Config struct {
	MainConfig      `toml:"mainConfig"`
	MysqlConfig     `toml:"mysqlConfig"`
	RedisConfig     `toml:"redisConfig"`
	EmailConfig     `toml:"emailConfig"`
	LogConfig       `toml:"logConfig"`
	KafkaConfig     `toml:"kafkaConfig"`
	StaticSrcConfig `toml:"staticSrcConfig"`
	ClusterConfig   `toml:"clusterConfig"`
}

var config *Config

// LoadConfig 加载配置
func LoadConfig() error {
	if config == nil {
		config = new(Config)
	}
	// P1-1 修复：向上查找 configs/config.toml
	configPath := os.Getenv("KAMA_CONFIG_PATH")
	if configPath == "" {
		candidates := []string{
			"./configs/config.toml",
			"../configs/config.toml",
			"../../configs/config.toml",
			"../../../configs/config.toml",
			"../../../../configs/config.toml",
			"../../../../../configs/config.toml",
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				configPath = c
				break
			}
		}
	}
	if configPath == "" {
		configPath = "./configs/config.toml"
	}
	if _, err := toml.DecodeFile(configPath, config); err != nil {
		log.Fatal(err.Error())
		return err
	}

	// P1 修复：环境变量覆盖 MySQL 配置（统一 Compose 与应用配置）
	if v := os.Getenv("KAMA_MYSQL_HOST"); v != "" {
		config.MysqlConfig.Host = v
	}
	if v := os.Getenv("KAMA_MYSQL_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			config.MysqlConfig.Port = port
		}
	}
	if v := os.Getenv("KAMA_MYSQL_USER"); v != "" {
		config.MysqlConfig.User = v
	}
	if v := os.Getenv("KAMA_MYSQL_PASSWORD"); v != "" {
		config.MysqlConfig.Password = v
	}
	if v := os.Getenv("KAMA_MYSQL_DB"); v != "" {
		config.MysqlConfig.DatabaseName = v
	}

	// 环境变量覆盖 Redis 配置
	if v := os.Getenv("KAMA_REDIS_HOST"); v != "" {
		config.RedisConfig.Host = v
	}
	if v := os.Getenv("KAMA_REDIS_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			config.RedisConfig.Port = port
		}
	}
	if v := os.Getenv("KAMA_REDIS_PASSWORD"); v != "" {
		config.RedisConfig.Password = v
	}

	// Performance tests can isolate Kafka state without mutating shared TOML.
	if v := os.Getenv("KAMA_KAFKA_BROKER"); v != "" {
		config.KafkaConfig.HostPort = v
	}
	if v := os.Getenv("KAMA_KAFKA_CHAT_TOPIC"); v != "" {
		config.KafkaConfig.ChatTopic = v
	}
	if v := os.Getenv("KAMA_KAFKA_GROUP_ID"); v != "" {
		config.KafkaConfig.GroupID = v
	}
	if v := os.Getenv("KAMA_KAFKA_MESSAGE_MODE"); v != "" {
		config.KafkaConfig.MessageMode = v
	}
	if v := os.Getenv("KAMA_KAFKA_DLQ_TOPIC"); v != "" {
		config.KafkaConfig.DLQTopic = v
	}
	if v := os.Getenv("KAMA_KAFKA_MAX_PROCESS_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			config.KafkaConfig.MaxProcessRetries = n
		}
	}
	// DLQ 缺省值：空则用默认主题名；重试上限默认 5
	if config.KafkaConfig.DLQTopic == "" {
		config.KafkaConfig.DLQTopic = "chat_message_dlq"
	}
	if config.KafkaConfig.MaxProcessRetries <= 0 {
		config.KafkaConfig.MaxProcessRetries = 5
	}

	// 容器化：主服务监听地址覆盖（Docker 内须绑定 0.0.0.0 才能端口映射到宿主机）
	if v := os.Getenv("KAMA_MAIN_HOST"); v != "" {
		config.MainConfig.Host = v
	}

	// 集群（多实例）配置覆盖
	if v := os.Getenv("KAMA_CLUSTER_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			config.ClusterConfig.Enabled = enabled
		}
	}
	if v := os.Getenv("KAMA_INSTANCE_ID"); v != "" {
		config.ClusterConfig.InstanceID = v
	}
	if v := os.Getenv("KAMA_SESSION_AFFINITY"); v != "" {
		if affinity, err := strconv.ParseBool(v); err == nil {
			config.ClusterConfig.SessionAffinity = affinity
		}
	}
	// 缺省值兜底：配置缺项时不至于 TTL 为 0 导致 presence 立刻过期
	if config.ClusterConfig.PresenceTTLSeconds <= 0 {
		config.ClusterConfig.PresenceTTLSeconds = 60
	}
	if config.ClusterConfig.HeartbeatSeconds <= 0 {
		config.ClusterConfig.HeartbeatSeconds = 20
	}
	if config.ClusterConfig.StreamMaxLen <= 0 {
		config.ClusterConfig.StreamMaxLen = 10000
	}

	return nil
}

func GetConfig() *Config {
	if config == nil {
		config = new(Config)
		_ = LoadConfig()
	}
	return config
}
