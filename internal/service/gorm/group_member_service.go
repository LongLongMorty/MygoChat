package gorm

import (
	"errors"
	"time"

	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/enum/group_member/group_member_role_enum"
	"kama_chat_server/pkg/enum/group_member/group_member_status_enum"
	"kama_chat_server/pkg/zlog"

	"gorm.io/gorm"
)

type groupMemberService struct {
}

var GroupMemberService = new(groupMemberService)

// ErrMemberKicked 被踢成员禁止重新加入的 sentinel 错误
var ErrMemberKicked = errors.New("用户已被移出群聊，无法自动重新加入，请联系群主")

// GetActiveGroupMemberIDs 获取群所有有效成员 uuid 列表
// 统一入口：聊天投递（Channel/Kafka/Processor）和业务查询都走这里，
// 保证只投递给 status=正常 且未删除的成员
func (g *groupMemberService) GetActiveGroupMemberIDs(groupID string) ([]string, error) {
	var memberIDs []string
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ? AND status = ?", groupID, group_member_status_enum.NORMAL).
		Pluck("user_id", &memberIDs); res.Error != nil {
		zlog.Error("查询群成员失败: " + res.Error.Error())
		return nil, res.Error
	}
	return memberIDs, nil
}

// AddMember 添加成员到群（幂等：已存在则恢复，不存在则插入）
// ownerId 参数仅为日志与调用点区分，实际以 tx 执行
func (g *groupMemberService) addMember(tx *gorm.DB, groupID, userID string, role int8) error {
	now := time.Now()
	// 1. 已存在活跃记录（deleted_at=0）→ 幂等更新状态，不重复插入
	var active int64
	if res := tx.Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Count(&active); res.Error != nil {
		return res.Error
	}
	if active > 0 {
		if res := tx.Model(&model.GroupMember{}).
			Where("group_id = ? AND user_id = ?", groupID, userID).
			Updates(map[string]interface{}{
				"status":     group_member_status_enum.NORMAL,
				"role":       role,
				"joined_at":  now,
				"updated_at": now,
			}); res.Error != nil {
			return res.Error
		}
		return nil
	}
	// 2. 无活跃记录，但有历史记录（退群/被踢，deleted_at≠0）→ 恢复最新一条为活跃
	var history model.GroupMember
	if res := tx.Unscoped().Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ? AND deleted_at <> 0", groupID, userID).
		Order("id DESC").First(&history); res.Error == nil {
		// 被踢成员禁止重新加入（需群主手动操作或走审核流程）
		if history.Status == group_member_status_enum.KICKED {
			return ErrMemberKicked
		}
		if res := tx.Unscoped().Model(&model.GroupMember{}).
			Where("id = ?", history.Id).
			Updates(map[string]interface{}{
				"status":     group_member_status_enum.NORMAL,
				"role":       role,
				"deleted_at": 0,
				"joined_at":  now,
				"updated_at": now,
			}); res.Error != nil {
			return res.Error
		}
		return nil
	}
	// 3. 全新成员 → 插入
	member := model.GroupMember{
		GroupId:   groupID,
		UserId:    userID,
		Role:      role,
		Status:    group_member_status_enum.NORMAL,
		JoinedAt:  now,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if res := tx.Create(&member); res.Error != nil {
		return res.Error
	}
	return nil
}

// updateMemberStatus 更新成员状态（退群/被移出）
// 软删除保留历史记录，方便后续黑名单等扩展
func (g *groupMemberService) updateMemberStatus(groupID, userID string, status int8) error {
	now := time.Now()
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ? AND user_id = ?", groupID, userID).
		Updates(map[string]interface{}{
			"status":     status,
			"deleted_at": now.UnixNano(),
			"updated_at": now,
		}); res.Error != nil {
		zlog.Error("更新群成员状态失败: " + res.Error.Error())
		return res.Error
	}
	return nil
}

// CountActiveMembers 统计群有效成员数
func (g *groupMemberService) CountActiveMembers(groupID string) (int64, error) {
	var cnt int64
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ? AND status = ?", groupID, group_member_status_enum.NORMAL).
		Count(&cnt); res.Error != nil {
		zlog.Error("统计群成员数失败: " + res.Error.Error())
		return 0, res.Error
	}
	return cnt, nil
}

// Init 包级占位，确保枚举包被引用（供 gofmt/vet 检查通过时使用）
var _ = group_member_role_enum.OWNER
var _ = group_member_status_enum.NORMAL
