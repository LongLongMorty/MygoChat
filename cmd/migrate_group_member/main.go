package main

// 数据迁移脚本：将 group_info.members JSON 拆分为 group_member 关系表
// 用法：go run ./cmd/migrate_group_member/ （需 MySQL 已启动）
//
// 环境变量：
//   KAMA_MYSQL_DSN              - 完整 DSN（优先级最高）
//   KAMA_MYSQL_HOST (默认 127.0.0.1)
//   KAMA_MYSQL_PORT (默认 3307)
//   KAMA_MYSQL_USER (默认 root)
//   KAMA_MYSQL_PASSWORD (默认 root)
//   KAMA_MYSQL_DATABASE (默认 kama_chat_server)
//
// 幂等性保证：
//  1. 每个群每条记录通过 (group_id, user_id, deleted_at) 唯一索引去重
//  2. 使用 INSERT IGNORE + UPDATE 组合，重复执行不会产生重复成员
//  3. 群主（owner_id）强制 role=1 并在成员列表中
//
// 异常处理：
//  - members JSON 非法 → 记录并跳过（阻止静默成功）
//  - 群主不在成员列表 → 自动补充群主
//  - 成员 uuid 指向不存在的用户 → 记录并跳过
//  - 任意异常最终以非零退出码退出

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func getDSN() string {
	if dsn := os.Getenv("KAMA_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	host := getEnv("KAMA_MYSQL_HOST", "127.0.0.1")
	port := getEnv("KAMA_MYSQL_PORT", "3307")
	user := getEnv("KAMA_MYSQL_USER", "root")
	password := getEnv("KAMA_MYSQL_PASSWORD", "root")
	database := getEnv("KAMA_MYSQL_DATABASE", "kama_chat_server")
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, password, host, port, database)
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

type groupInfoRow struct {
	Uuid      string
	OwnerId   string
	Members   json.RawMessage
	MemberCnt *int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type memberInfo struct {
	Uuid string
}

func main() {
	dsn := getDSN()
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Warn)})
	if err != nil {
		fmt.Println("连接 MySQL 失败:", err)
		os.Exit(1)
	}

	// 0. 检测 group_info.members 旧列是否存在
	//    如果列不存在，说明已是新表结构（members 已拆分到 group_member 表），无需迁移
	var legacyColumnCount int64
	if err := db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'group_info' AND column_name IN ('members', 'member_cnt')").Scan(&legacyColumnCount).Error; err != nil {
		fmt.Println("检查旧列失败:", err)
		os.Exit(1)
	}
	if legacyColumnCount == 0 {
		fmt.Println("group_info 已无 members/member_cnt 旧列，数据迁移已完成，无需执行")
		return
	}

	// 1. 执行建表，确保 group_member 表和索引已存在（独立于项目 AutoMigrate）
	if err := db.Exec(`CREATE TABLE IF NOT EXISTS group_member (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		group_id CHAR(20) NOT NULL,
		user_id CHAR(20) NOT NULL,
		role TINYINT DEFAULT 0,
		status TINYINT DEFAULT 0,
		mute_until DATETIME NULL,
		joined_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		deleted_at BIGINT NOT NULL DEFAULT 0,
		INDEX idx_group_member_group_id (group_id),
		INDEX idx_group_member_user_id (user_id),
		INDEX idx_group_member_created_at (created_at),
		UNIQUE INDEX idx_group_member (group_id, user_id, deleted_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`).Error; err != nil {
		fmt.Println("建表失败:", err)
		os.Exit(1)
	}
	fmt.Println("group_member 表结构就绪")

	// 2. 查出所有活跃群（deleted_at = 0）
	var groups []groupInfoRow
	if err := db.Table("group_info").
		Select("uuid, owner_id, members, member_cnt, created_at, updated_at").
		Where("deleted_at = 0").
		Find(&groups).Error; err != nil {
		fmt.Println("查询群列表失败:", err)
		os.Exit(1)
	}
	fmt.Printf("共 %d 个活跃群待迁移\n", len(groups))

	totalMembers := 0
	skipped := 0
	anomalies := 0

	for _, g := range groups {
		var members []string
		if len(g.Members) > 0 {
			if err := json.Unmarshal(g.Members, &members); err != nil {
				fmt.Printf("[异常] 群 %s 的 members JSON 非法，跳过: %v\n", g.Uuid, err)
				anomalies++
				continue
			}
		}

		// 3. 去重
		seen := map[string]bool{}
		deduped := []string{}
		for _, m := range members {
			if m == "" || seen[m] {
				continue
			}
			seen[m] = true
			deduped = append(deduped, m)
		}

		// 4. 校验群主：不在成员列表则补充
		if !seen[g.OwnerId] {
			fmt.Printf("[警告] 群 %s 群主 %s 不在成员列表，自动补充\n", g.Uuid, g.OwnerId)
			deduped = append(deduped, g.OwnerId)
			anomalies++
		}

		// 5. 校验成员用户存在性
		validMembers := []string{}
		for _, m := range deduped {
			var userCount int64
			db.Table("user_info").Where("uuid = ?", m).Count(&userCount)
			if userCount == 0 {
				fmt.Printf("[异常] 群 %s 成员 %s 在 user_info 中不存在，跳过\n", g.Uuid, m)
				anomalies++
				skipped++
				continue
			}
			validMembers = append(validMembers, m)
		}

		// 6. 写入成员表（幂等）
		now := time.Now()
		for _, m := range validMembers {
			role := 0
			if m == g.OwnerId {
				role = 1 // 群主
			}
			var existing int64
			db.Table("group_member").
				Where("group_id = ? AND user_id = ? AND deleted_at = 0", g.Uuid, m).
				Count(&existing)
			if existing > 0 {
				// 已存在，更新 role（限定 deleted_at=0 的活跃记录）
				if err := db.Table("group_member").
					Where("group_id = ? AND user_id = ? AND deleted_at = 0", g.Uuid, m).
					Updates(map[string]interface{}{"role": role, "updated_at": now}).Error; err != nil {
					fmt.Printf("[异常] 群 %s 成员 %s 更新失败: %v\n", g.Uuid, m, err)
					anomalies++
				}
				totalMembers++
				continue
			}
			if err := db.Table("group_member").Create(map[string]interface{}{
				"group_id":   g.Uuid,
				"user_id":    m,
				"role":       role,
				"status":     0,
				"joined_at":  g.CreatedAt,
				"created_at": now,
				"updated_at": now,
				"deleted_at": 0,
			}).Error; err != nil {
				fmt.Printf("[异常] 群 %s 成员 %s 插入失败: %v\n", g.Uuid, m, err)
				anomalies++
				continue
			}
			totalMembers++
		}

		// 7. 校验 member_cnt
		var cnt int64
		db.Table("group_member").Where("group_id = ? AND deleted_at = 0", g.Uuid).Count(&cnt)
		if g.MemberCnt != nil && *g.MemberCnt != int(cnt) {
			fmt.Printf("[校验] 群 %s 旧 member_cnt=%d，实际成员数=%d\n", g.Uuid, *g.MemberCnt, cnt)
			anomalies++
		}
	}

	fmt.Printf("迁移完成: 共写入 %d 条成员记录，跳过 %d 条，异常 %d 处\n", totalMembers, skipped, anomalies)
	if anomalies > 0 {
		fmt.Println("存在异常，请检查上方日志")
		os.Exit(1)
	}
}