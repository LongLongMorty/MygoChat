package chat

import (
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/enum/group_member/group_member_status_enum"
	"kama_chat_server/pkg/zlog"
)

// getActiveGroupMemberIDs 获取群所有有效成员 uuid 列表
// 与 service/gorm.GroupMemberService.GetActiveGroupMemberIDs 保持相同查询条件，
// 供消息投递链路（Channel/Kafka/Processor）统一使用：
// 只投递给 status=正常 且未删除的成员
func getActiveGroupMemberIDs(groupID string) ([]string, error) {
	var memberIDs []string
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ? AND status = ?", groupID, group_member_status_enum.NORMAL).
		Pluck("user_id", &memberIDs); res.Error != nil {
		zlog.Error("查询群成员失败: " + res.Error.Error())
		return nil, res.Error
	}
	return memberIDs, nil
}
