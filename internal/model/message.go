package model

import (
	"database/sql"
	"gorm.io/gorm"
	"time"
)

type Message struct {
	Id         int64     `gorm:"column:id;primaryKey;comment:自增id"`
	Uuid       string    `gorm:"column:uuid;uniqueIndex;type:char(20);not null;comment:消息uuid"`
	SessionId  string    `gorm:"column:session_id;index;type:varchar(100);not null;comment:会话uuid"`
	Type       int8      `gorm:"column:type;not null;comment:消息类型，0.文本，1.语音，2.文件，3.通话"` // 通话不用存消息内容或者url
	Content    string    `gorm:"column:content;type:TEXT;comment:消息内容"`
	Url        string    `gorm:"column:url;type:char(255);comment:消息url"`
	SendId     string    `gorm:"column:send_id;index;type:char(20);not null;comment:发送者uuid;index:idx_msg_send_receive_created,priority:1"`
	SendName   string    `gorm:"column:send_name;type:varchar(20);not null;comment:发送者昵称"`
	SendAvatar string    `gorm:"column:send_avatar;type:varchar(255);not null;comment:发送者头像"`
	ReceiveId  string    `gorm:"column:receive_id;index;type:char(20);not null;comment:接受者uuid;index:idx_msg_send_receive_created,priority:2"`
	FileType   string    `gorm:"column:file_type;type:char(10);comment:文件类型"`
	FileName   string    `gorm:"column:file_name;type:varchar(50);comment:文件名"`
	FileSize   string    `gorm:"column:file_size;type:char(20);comment:文件大小(展示用字符串，如 1.5MB)"`
	// FileSizeBytes 文件大小的字节数（bigint），供排序/统计/校验使用；
	// 与 FileSize 展示字符串并存，写入时由服务端解析或由上传接口回填
	FileSizeBytes int64 `gorm:"column:file_size_bytes;type:bigint;not null;default:0;comment:文件大小(字节数)"`
	Status        int8  `gorm:"column:status;not null;comment:状态，0.未发送，1.已发送"`
	// ReadStatus 已读状态：0.未读，1.已读（单聊：接收方拉取历史时批量置已读；群聊已读需明细表，当前仅单聊生效）
	ReadStatus int8 `gorm:"column:read_status;not null;default:0;comment:已读状态，0.未读，1.已读"`
	// ReadAt 首次被读取时间
	ReadAt    sql.NullTime `gorm:"column:read_at;type:datetime;comment:已读时间"`
	CreatedAt time.Time    `gorm:"column:created_at;not null;comment:创建时间;index:idx_msg_send_receive_created,priority:3"`
	SendAt    sql.NullTime `gorm:"column:send_at;comment:发送时间"`
	AVdata    string       `gorm:"column:av_data;type:longtext;comment:通话传递数据(WebRTC信令, 瞬时数据, 建议短TTL清理)"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index;type:datetime;comment:软删除时间(支持撤回/删除消息)"`
}

func (Message) TableName() string {
	return "message"
}
