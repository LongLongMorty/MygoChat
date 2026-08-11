package chat

import (
	"errors"
	"time"

	"gorm.io/gorm"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/enum/contact/contact_status_enum"
	"kama_chat_server/pkg/zlog"
)

const (
	// contactBlackCachePrefix 拉黑关系缓存 key 前缀：contact_black_<sendId>_<receiveId>
	contactBlackCachePrefix = "contact_black_"
	// contactBlackCacheTTL 缓存 TTL：拉黑/取消拉黑/删除好友时主动失效，正常 60s 内每对关系最多一次 DB 查询
	contactBlackCacheTTL = 60 * time.Second
)

// IsBlocked 检查 sendId 给 receiveId 发消息是否被拉黑关系拦截。
// 检查 (sendId → receiveId) 方向的联系人记录：
//   - status = BLACK（sendId 拉黑了 receiveId）→ 拦截
//   - status = BE_BLACK（sendId 被 receiveId 拉黑）→ 拦截
//
// 一条记录覆盖两个方向的语义。Redis 缓存 60s，变更时主动失效（InvalidateContactBlack）。
// DB 故障时 fail-open 放行：消息链路是核心通道，DB 抖动不应导致全站消息发送失败
// （与鉴权 CheckUserActive 的 fail-closed 是有意区分：鉴权安全优先，消息可用性优先）。
func IsBlocked(sendId, receiveId string) bool {
	key := contactBlackKey(sendId, receiveId)

	// 1. 缓存命中
	cached, err := myredis.GetKey(key)
	if err == nil && cached != "" {
		return cached == "1"
	}

	// 2. 缓存未命中 → 查 DB（GORM 默认过滤软删行：删除好友后记录被软删，按"未拉黑"放行）
	var contact model.UserContact
	if res := dao.GormDB.Select("status").Where("user_id = ? AND contact_id = ?", sendId, receiveId).First(&contact); res.Error != nil {
		if !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			zlog.Error("拉黑状态查询失败: " + res.Error.Error())
		}
		// 无记录（非联系人）或 DB 故障 → 放行
		_ = myredis.SetKeyEx(key, "0", contactBlackCacheTTL)
		return false
	}

	blocked := contact.Status == contact_status_enum.BLACK || contact.Status == contact_status_enum.BE_BLACK
	value := "0"
	if blocked {
		value = "1"
	}
	_ = myredis.SetKeyEx(key, value, contactBlackCacheTTL)
	return blocked
}

// InvalidateContactBlack 主动失效两个方向的拉黑关系缓存。
// 拉黑 / 取消拉黑 / 删除好友 后调用，使变更即时生效。
func InvalidateContactBlack(a, b string) {
	_ = myredis.DelKeyIfExists(contactBlackKey(a, b))
	_ = myredis.DelKeyIfExists(contactBlackKey(b, a))
}

func contactBlackKey(a, b string) string {
	return contactBlackCachePrefix + a + "_" + b
}
