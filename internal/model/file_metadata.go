package model

import (
	"time"

	"gorm.io/gorm"
)

// FileMetadata 文件元数据（P1 修复：附件归属校验）
type FileMetadata struct {
	Id            int64          `gorm:"column:id;primaryKey;autoIncrement"`
	Uuid          string         `gorm:"column:uuid;uniqueIndex;type:char(20);comment:文件业务ID"`
	UploaderUuid  string         `gorm:"column:uploader_uuid;index;type:char(20);not null;comment:上传者UUID"`
	ServerName    string         `gorm:"column:server_name;type:varchar(255);not null;comment:服务端存储文件名"`
	OriginalName  string         `gorm:"column:original_name;type:varchar(255);comment:客户端原始文件名"`
	FileType      string         `gorm:"column:file_type;type:varchar(64);comment:文件MIME类型"`
	FileSize      int64          `gorm:"column:file_size;comment:文件大小(字节)"`
	IsAvatar      int8           `gorm:"column:is_avatar;default:0;comment:0普通文件 1头像"`
	CreatedAt     time.Time      `gorm:"column:created_at;type:datetime;not null;comment:上传时间"`
	DeletedAt     gorm.DeletedAt `gorm:"column:deleted_at;type:datetime;index;comment:删除时间"`
}

func (FileMetadata) TableName() string {
	return "file_metadata"
}
