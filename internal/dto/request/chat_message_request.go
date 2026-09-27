package request

type ChatMessageRequest struct {
	SessionId  string `json:"session_id"`
	Type       int8   `json:"type"`
	Content    string `json:"content"`
	Url        string `json:"url"`
	SendId     string `json:"send_id"`
	SendName   string `json:"send_name"`
	SendAvatar string `json:"send_avatar"`
	ReceiveId  string `json:"receive_id"`
	FileSize   string `json:"file_size"`
	// FileSizeBytes 文件大小字节数（可选，优先使用；为 0 时服务端尝试解析 FileSize 字符串）
	FileSizeBytes int64  `json:"file_size_bytes"`
	FileType      string `json:"file_type"`
	FileName      string `json:"file_name"`
	AVdata        string `json:"av_data"`
	// ClientMsgId 客户端生成的幂等键（可选）。同一用户重试发送时保持不变，
	// 服务端据此派生稳定消息 ID，实现端到端去重；为空则服务端生成随机 ID。
	ClientMsgId string `json:"client_msg_id"`
}
