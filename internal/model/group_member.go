package model

import (
	"time"

	"gorm.io/plugin/soft_delete"
)

// GroupMember 群成员关系表
// 从 group_info.members JSON 字段拆分而来，支持角色、禁言、成员状态等扩展
type GroupMember struct {
	Id        int64                 `gorm:"column:id;primaryKey;comment:自增id"`
	GroupId   string                `gorm:"column:group_id;type:char(20);not null;uniqueIndex:idx_group_member,priority:1;index:idx_group_member_group_id;comment:群组uuid"`
	UserId    string                `gorm:"column:user_id;type:char(20);not null;uniqueIndex:idx_group_member,priority:2;index:idx_group_member_user_id;comment:用户uuid"`
	Role      int8                  `gorm:"column:role;default:0;comment:角色，0.普通成员，1.群主，2.管理员"`
	Status    int8                  `gorm:"column:status;default:0;comment:状态，0.正常，1.被移出，2.已退群"`
	MuteUntil *time.Time            `gorm:"column:mute_until;type:datetime;comment:禁言截止时间，null表示不禁言"`
	JoinedAt  time.Time             `gorm:"column:joined_at;type:datetime;not null;comment:加入时间"`
	CreatedAt time.Time             `gorm:"column:created_at;index;type:datetime;not null;comment:创建时间"`
	UpdatedAt time.Time             `gorm:"column:updated_at;type:datetime;not null;comment:更新时间"`
	DeletedAt soft_delete.DeletedAt `gorm:"column:deleted_at;uniqueIndex:idx_group_member,priority:3;type:bigint;not null;default:0;softDelete:nano;comment:删除时间"`
}

func (GroupMember) TableName() string {
	return "group_member"
}
