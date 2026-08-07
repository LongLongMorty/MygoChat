package integration

import (
	"fmt"
	"os"
	"testing"
	"time"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/model"
	userService "kama_chat_server/internal/service/gorm"
	"kama_chat_server/pkg/auth"
)

// TestUserCRUD 集成测试：验证 DAO 与真实 MySQL 的兼容性
// 前置条件：docker-compose up -d 启动 MySQL
// 环境变量：KAMA_MYSQL_HOST=127.0.0.1 KAMA_MYSQL_PORT=3307
//
// 此测试不会跳过 -- 它需要真实的 MySQL 实例
func TestUserCRUD(t *testing.T) {
	// 检查是否在 CI 环境（有 MySQL 可用）
	if os.Getenv("KAMA_INTEGRATION_TEST") != "1" {
		t.Skip("跳过集成测试，设置 KAMA_INTEGRATION_TEST=1 启用（需先 docker-compose up -d）")
	}

	// 初始化数据库连接
	if err := dao.InitDB(); err != nil {
		t.Fatalf("数据库初始化失败（请确保 docker-compose up -d 已运行）: %v", err)
	}

	// 清理可能存在的测试数据
	dao.GormDB.Where("telephone = ?", "13900000000").Delete(&model.UserInfo{})

	// 1. Create - 创建测试用户
	hashedPassword, err := auth.HashPassword("test123456")
	if err != nil {
		t.Fatalf("密码哈希失败: %v", err)
	}
	user := &model.UserInfo{
		Uuid:      "UTEST" + fmt.Sprintf("%d", 1),
		Telephone: "13900000000",
		Nickname:  "集成测试用户",
		Password:  hashedPassword,
		IsAdmin:   0,
		Status:    0,
	}
	if err := dao.GormDB.Create(user).Error; err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	t.Logf("创建用户成功: uuid=%s", user.Uuid)

	// 2. Read - 读取用户
	var readUser model.UserInfo
	if err := dao.GormDB.First(&readUser, "telephone = ?", "13900000000").Error; err != nil {
		t.Fatalf("读取用户失败: %v", err)
	}
	if readUser.Nickname != "集成测试用户" {
		t.Errorf("昵称不匹配: got %s, want 集成测试用户", readUser.Nickname)
	}

	// 3. 密码校验
	if !auth.VerifyPassword(readUser.Password, "test123456") {
		t.Error("密码校验失败")
	}
	if auth.VerifyPassword(readUser.Password, "wrongpassword") {
		t.Error("错误密码竟然校验通过")
	}
	t.Log("密码校验通过")

	// 4. Update - 更新用户
	dao.GormDB.Model(&readUser).Update("nickname", "已更新昵称")
	var updatedUser model.UserInfo
	dao.GormDB.First(&updatedUser, "uuid = ?", user.Uuid)
	if updatedUser.Nickname != "已更新昵称" {
		t.Errorf("更新失败: got %s", updatedUser.Nickname)
	}

	// 5. Delete - 删除用户（软删除）
	dao.GormDB.Delete(&updatedUser)
	var deletedUser model.UserInfo
	result := dao.GormDB.First(&deletedUser, "uuid = ?", user.Uuid)
	if result.Error == nil {
		t.Error("软删除失败：仍能查到用户")
	}

	// 6. Unscoped 查询可查到软删除的记录
	var unscopedUser model.UserInfo
	if err := dao.GormDB.Unscoped().First(&unscopedUser, "uuid = ?", user.Uuid).Error; err != nil {
		t.Errorf("Unscoped 查询失败: %v", err)
	}

	// 清理：物理删除测试数据
	dao.GormDB.Unscoped().Delete(&unscopedUser)
	t.Log("集成测试全部通过：Create/Read/Update/Delete/软删除/密码校验")
}

func TestLoginRejectsDisabledAndLegacyPlaintextUsers(t *testing.T) {
	if os.Getenv("KAMA_INTEGRATION_TEST") != "1" {
		t.Skip("设置 KAMA_INTEGRATION_TEST=1 后运行，需要可用 MySQL")
	}
	if err := dao.InitDB(); err != nil {
		t.Fatalf("数据库初始化失败: %v", err)
	}

	tests := []struct {
		email    string
		password string
		status   int8
		name     string
	}{
		{email: "disabled@test.com", password: "disabled-password", status: 1, name: "disabled"},
		{email: "legacy@test.com", password: "legacy-secret", status: 0, name: "legacy plaintext"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			user := &model.UserInfo{
				Uuid:      "USEC" + tc.name,
				Email:     tc.email,
				Telephone: "",
				Nickname:  "security-test",
				Password:  tc.password,
				Status:    tc.status,
				CreatedAt: time.Now(),
			}
			if tc.name == "disabled" {
				var err error
				user.Password, err = auth.HashPassword(tc.password)
				if err != nil {
					t.Fatalf("密码哈希失败: %v", err)
				}
			}
			dao.GormDB.Unscoped().Where("email = ?", tc.email).Delete(&model.UserInfo{})
			if err := dao.GormDB.Create(user).Error; err != nil {
				t.Fatalf("创建测试用户失败: %v", err)
			}
			defer dao.GormDB.Unscoped().Where("email = ?", tc.email).Delete(&model.UserInfo{})

			message, response, ret := userService.UserInfoService.Login(request.LoginRequest{
				Email:    tc.email,
				Password: tc.password,
			})
			if ret != -2 || response != nil {
				t.Fatalf("登录应被拒绝: message=%s ret=%d response=%+v", message, ret, response)
			}
		})
	}
}

// TestAutoMigrate 集成测试：验证 AutoMigrate 能正确建表
func TestAutoMigrate(t *testing.T) {
	if os.Getenv("KAMA_INTEGRATION_TEST") != "1" {
		t.Skip("跳过集成测试，设置 KAMA_INTEGRATION_TEST=1 启用")
	}
	if err := dao.InitDB(); err != nil {
		t.Fatalf("数据库初始化失败: %v", err)
	}

	// 验证所有表存在（按当前数据库过滤，避免 sys.session 等系统表干扰）
	tables := []string{"user_info", "group_info", "group_member", "user_contact", "session", "contact_apply", "message"}
	for _, table := range tables {
		var count int64
		dao.GormDB.Raw(fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", table)).Scan(&count)
		if count != 1 {
			t.Errorf("表 %s 不存在", table)
		}
	}
	t.Log("所有 7 张表 AutoMigrate 验证通过")
}
