package main

import (
	"fmt"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/https_server"
	"kama_chat_server/internal/service/chat"
	"kama_chat_server/internal/service/kafka"
	"kama_chat_server/pkg/auth"
	"kama_chat_server/pkg/zlog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// P0 修复：强制从环境变量加载 JWT 密钥，缺失或弱密钥拒绝启动
	jwtSecret := os.Getenv("KAMA_JWT_SECRET")
	if jwtSecret == "" {
		zlog.Fatal("KAMA_JWT_SECRET 环境变量未设置，拒绝启动。请设置长度 >= 32 字节的强密钥")
		return
	}
	if err := auth.SetSecret(jwtSecret); err != nil {
		zlog.Fatal("JWT 密钥不安全: " + err.Error())
		return
	}
	zlog.Info("JWT 密钥已从环境变量加载")

	// P1 修复：显式初始化数据库连接
	if err := dao.InitDB(); err != nil {
		zlog.Fatal("数据库初始化失败: " + err.Error())
		return
	}

	conf := config.GetConfig()
	host := conf.MainConfig.Host
	port := conf.MainConfig.Port
	kafkaConfig := conf.KafkaConfig

	// 根据消息模式启动对应的 Chat Server
	switch kafkaConfig.MessageMode {
	case "channel":
		go chat.ChatServer.Start()
	case "kafka":
		kafka.KafkaService.KafkaInit()
		go chat.KafkaChatServer.Start()
	case "hybrid":
		// P1 改造：混合模式，channel 为主 + 背压检测自动分流到 Kafka
		go chat.HybridChatRouter.Start()
	default:
		go chat.ChatServer.Start()
	}

	go func() {
		if err := https_server.GE.RunTLS(fmt.Sprintf("%s:%d", host, port), "pkg/ssl/server.crt", "pkg/ssl/server.key"); err != nil {
			zlog.Fatal("server running fault")
			return
		}
	}()

	// 设置信号监听
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	<-quit

	// 根据消息模式关闭对应的 Chat Server
	switch kafkaConfig.MessageMode {
	case "kafka":
		kafka.KafkaService.KafkaClose()
		chat.KafkaChatServer.Close()
	case "hybrid":
		chat.HybridChatRouter.Close()
	default:
		chat.ChatServer.Close()
	}

	// 刷新批量写入器中剩余的消息
	chat.MessageBatch.Shutdown()

	zlog.Info("关闭服务器...")

	// P1-3 修复：不再删除整个 Redis DB
	zlog.Info("服务器已关闭")
}
