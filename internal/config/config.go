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
}

type StaticSrcConfig struct {
	StaticAvatarPath string `toml:"staticAvatarPath"`
	StaticFilePath   string `toml:"staticFilePath"`
}

type Config struct {
	MainConfig      `toml:"mainConfig"`
	MysqlConfig     `toml:"mysqlConfig"`
	RedisConfig     `toml:"redisConfig"`
	EmailConfig     `toml:"emailConfig"`
	LogConfig       `toml:"logConfig"`
	KafkaConfig     `toml:"kafkaConfig"`
	StaticSrcConfig `toml:"staticSrcConfig"`
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

	// 容器化：主服务监听地址覆盖（Docker 内须绑定 0.0.0.0 才能端口映射到宿主机）
	if v := os.Getenv("KAMA_MAIN_HOST"); v != "" {
		config.MainConfig.Host = v
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
