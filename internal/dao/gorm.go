package dao

import (
	"fmt"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/zlog"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var GormDB *gorm.DB

// InitDB 初始化数据库连接（从 init() 移出，由 main.go 显式调用）
// 测试时可跳过，避免无 MySQL 时包初始化失败
func InitDB() error {
	conf := config.GetConfig()
	host := conf.MysqlConfig.Host
	port := conf.MysqlConfig.Port
	user := conf.MysqlConfig.User
	password := conf.MysqlConfig.Password
	database := conf.MysqlConfig.DatabaseName

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, database)

	var err error
	GormDB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("GORM 连接 MySQL 失败: %w", err)
	}
	sqlDB, err := GormDB.DB()
	if err != nil {
		return fmt.Errorf("获取 MySQL 连接池失败: %w", err)
	}
	if conf.MysqlConfig.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(conf.MysqlConfig.MaxOpenConns)
	}
	if conf.MysqlConfig.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(conf.MysqlConfig.MaxIdleConns)
	}
	if conf.MysqlConfig.ConnMaxLifetimeSeconds > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(conf.MysqlConfig.ConnMaxLifetimeSeconds) * time.Second)
	}

	zlog.Info("MySQL 连接成功: " + fmt.Sprintf("%s:%d/%s", host, port, database))

	// 自动迁移建表
	err = GormDB.AutoMigrate(
		&model.UserInfo{},
		&model.GroupInfo{},
		&model.UserContact{},
		&model.Session{},
		&model.ContactApply{},
		&model.Message{},
		&model.FileMetadata{}, // P1 修复：文件元数据表
	)
	if err != nil {
		return fmt.Errorf("数据库自动迁移建表失败: %w", err)
	}
	return nil
}

// init 保留空实现，不再在包初始化时连接数据库
// P1 修复：DB 初始化移到 InitDB()，测试无 MySQL 时不会 panic
func init() {
	// 不在此处初始化 DB，由 main.go 调用 InitDB()
}
