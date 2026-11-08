package chat

import (
	"errors"
	"time"

	"gorm.io/gorm"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/enum/contact/contact_status_enum"
	"kama_chat_server/pkg/enum/contact/contact_type_enum"
	"kama_chat_server/pkg/zlog"
)

const (
	// sendAuthCachePrefix 用户发送鉴权缓存：send_auth_<sendId>_<receiveId>
	sendAuthCachePrefix = "send_auth_"
	// sendAuthCacheTTL 缓存 TTL：好友关系变化时主动失效（InvalidateSendAuth），正常 60s 内每对关系最多一次 DB 查询
	sendAuthCacheTTL = 60 * time.Second
)

// CanSendTo 校验 senderID 是否有权向 receiveID 发送消息：
//   - U 目标：需存在正常好友关系（contact_type=USER、status=NORMAL、未删除，双向任一方向）
//   - G 目标：需为该群有效成员（复用群成员 Set 缓存，与投递路径同源）
//
// 安全：DB 故障时 fail-closed（拒绝），优先阻断越权发送。
func CanSendTo(senderID, receiveID string) bool {
	if senderID == "" || receiveID == "" {
		return false
	}
	// 非 DB 环境（部分单测）fail-closed，避免空指针
	if dao.GormDB == nil {
		return false
	}
	switch receiveID[0] {
	case 'U':
		return canSendToUser(senderID, receiveID)
	case 'G':
		return canSendToGroup(senderID, receiveID)
	default:
		return false
	}
}

// canSendToGroup 复用 getActiveGroupMemberIDs（Redis Set 缓存 + miss 回源 DB），
// 成员变更路径已通过 DelGroupMemberSet 主动失效，故此处无需额外缓存。
func canSendToGroup(senderID, groupID string) bool {
	members, err := getActiveGroupMemberIDs(groupID)
	if err != nil {
		zlog.Error("群成员发送鉴权查询失败，拒绝发送: " + err.Error())
		return false
	}
	for _, member := range members {
		if member == senderID {
			return true
		}
	}
	return false
}

func canSendToUser(senderID, receiveID string) bool {
	key := sendAuthCachePrefix + senderID + "_" + receiveID
	if cached, err := myredis.GetKey(key); err == nil && cached != "" {
		return cached == "1"
	}

	// 正常好友关系：双向任一方向存在即可（好友关系可能单向存储）
	var contact model.UserContact
	res := dao.GormDB.
		Where("contact_type = ? AND status = ? AND ((user_id = ? AND contact_id = ?) OR (user_id = ? AND contact_id = ?))",
			contact_type_enum.USER, contact_status_enum.NORMAL, senderID, receiveID, receiveID, senderID).
		First(&contact)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			_ = myredis.SetKeyEx(key, "0", sendAuthCacheTTL)
			return false
		}
		zlog.Error("好友关系发送鉴权查询失败，拒绝发送: " + res.Error.Error())
		return false // fail-closed
	}
	_ = myredis.SetKeyEx(key, "1", sendAuthCacheTTL)
	return true
}

// InvalidateSendAuth 失效两个方向的用户发送鉴权缓存（好友关系变化时调用）。
func InvalidateSendAuth(a, b string) {
	_ = myredis.DelKeyIfExists(sendAuthCachePrefix + a + "_" + b)
	_ = myredis.DelKeyIfExists(sendAuthCachePrefix + b + "_" + a)
}
