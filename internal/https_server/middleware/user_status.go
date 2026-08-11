package middleware

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"gorm.io/gorm"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/enum/user_info/user_status_enum"
	"kama_chat_server/pkg/zlog"
)

const (
	// userStatusCachePrefix 用户状态缓存 key 前缀
	userStatusCachePrefix = "auth_user_status_"
	// userStatusCacheTTL 状态缓存 TTL：禁用/删除/启用时主动失效，正常 60s 内最多一次 DB 查询
	userStatusCacheTTL = 60 * time.Second
	// userStatusDeleted 缓存标记：用户不存在或已删除（软删行 GORM 默认查不到）
	userStatusDeleted = -1
)

// CheckUserActive 校验用户是否可用（存在且未被禁用）。
// 用于堵住"JWT 无状态 + 中间件只验签"的漏洞：禁用/删除用户的存量 token（24h 内有效）
// 无法继续访问接口。
// 返回 (是否允许, HTTP 状态码)；DB 故障时 fail-closed 拒绝访问（500），安全优先。
func CheckUserActive(uuid string) (bool, int) {
	key := userStatusCachePrefix + uuid

	// 1. 缓存命中
	cached, err := myredis.GetKey(key)
	if err == nil && cached != "" {
		if status, convErr := strconv.Atoi(cached); convErr == nil {
			if status == userStatusDeleted || status != int(user_status_enum.NORMAL) {
				return false, http.StatusUnauthorized
			}
			return true, http.StatusOK
		}
	}

	// GormDB 未初始化（仅测试环境；生产由 main.go 启动时 InitDB，监听前必就绪）→ 仅凭 token 放行
	if dao.GormDB == nil {
		return true, http.StatusOK
	}

	// 2. 缓存未命中 → 查 DB（GORM 默认排除软删行：已删除用户查不到，命中 ErrRecordNotFound）
	var user model.UserInfo
	if res := dao.GormDB.Select("status").Where("uuid = ?", uuid).First(&user); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			_ = myredis.SetKeyEx(key, strconv.Itoa(userStatusDeleted), userStatusCacheTTL)
			return false, http.StatusUnauthorized
		}
		// DB 故障：fail-closed，宁可拒绝访问也不放行可能已被禁用的账号
		zlog.Error("用户状态查询失败: " + res.Error.Error())
		return false, http.StatusInternalServerError
	}

	_ = myredis.SetKeyEx(key, strconv.Itoa(int(user.Status)), userStatusCacheTTL)
	if user.Status != user_status_enum.NORMAL {
		return false, http.StatusUnauthorized
	}
	return true, http.StatusOK
}

// InvalidateUserStatusCache 主动失效用户状态缓存。
// 管理端 启用/禁用/删除 用户后调用，使状态变更即时生效，无需等待 60s TTL。
func InvalidateUserStatusCache(uuid string) {
	_ = myredis.DelKeyIfExists(userStatusCachePrefix + uuid)
}
