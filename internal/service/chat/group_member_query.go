package chat

import (
	"time"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/enum/group_member/group_member_status_enum"
	"kama_chat_server/pkg/zlog"
)

const (
	// groupMemberSetPrefix 群成员 Set 缓存 key 前缀：group_member_set_<groupId>
	groupMemberSetPrefix = "group_member_set_"
	// groupMemberSetTTL 缓存兜底 TTL：成员变更路径主动失效（DelGroupMemberSet），
	// TTL 仅兜底防"某条变更路径漏了失效"导致的长期不一致；最坏 10 分钟恢复
	groupMemberSetTTL = 10 * time.Minute
)

// getActiveGroupMemberIDs 获取群所有有效成员 uuid 列表（供消息投递链路统一使用）。
// 与 service/gorm.GroupMemberService.GetActiveGroupMemberIDs 保持相同查询条件：
// 只投递给 status=正常 且未删除的成员。
//
// 改造：Redis Set 缓存（key = group_member_set_<groupId>），命中零 DB 查询；
// miss 时查 DB 重建（批量 SAdd + TTL 兜底）。成员变更路径调用 DelGroupMemberSet 主动失效。
// 函数签名不变，5 个调用点（Processor/Server/KafkaServer）零改动。
func getActiveGroupMemberIDs(groupID string) ([]string, error) {
	key := groupMemberSetPrefix + groupID

	// 1. 缓存命中（SMembers 对不存在的 key 返回空 slice 且无错误 → 空即 miss）
	members, err := myredis.SMembers(key)
	if err == nil && len(members) > 0 {
		return members, nil
	}
	if err != nil {
		// Redis 故障：fail-open 回退查 DB，不阻塞投递
		zlog.Error("群成员缓存读取失败，回退 DB: " + err.Error())
	}

	// 2. 缓存未命中 → 查 DB
	var memberIDs []string
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ? AND status = ?", groupID, group_member_status_enum.NORMAL).
		Pluck("user_id", &memberIDs); res.Error != nil {
		zlog.Error("查询群成员失败: " + res.Error.Error())
		return nil, res.Error
	}

	// 3. 重建缓存（空群不写缓存，避免与 miss 无法区分；群至少含群主，实际不会为空）
	if len(memberIDs) > 0 {
		if err := myredis.SAdd(key, memberIDs); err != nil {
			zlog.Error("群成员缓存写入失败: " + err.Error())
		} else {
			_ = myredis.Expire(key, groupMemberSetTTL)
		}
	}
	return memberIDs, nil
}

// DelGroupMemberSet 主动失效群成员 Set 缓存。
// 所有 group_member 表写入路径（加人/进群/退群/踢人/解散/删群/群申请通过）在事务提交后调用，
// 使成员变更即时生效，无需等待 TTL。
func DelGroupMemberSet(groupID string) {
	_ = myredis.DelKeyIfExists(groupMemberSetPrefix + groupID)
}
