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
}
