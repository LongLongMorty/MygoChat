package respond

type GetMessageListRespond struct {
	Id           int64  `json:"id"`
	SendId       string `json:"send_id"`
	SendName     string `json:"send_name"`
	SendAvatar   string `json:"send_avatar"`
	ReceiveId    string `json:"receive_id"`
	Type         int8   `json:"type"`
	Content      string `json:"content"`
	Url          string `json:"url"`
	FileType     string `json:"file_type"`
	FileName     string `json:"file_name"`
	FileSize     string `json:"file_size"`
	FileSizeBytes int64 `json:"file_size_bytes"`
	ReadStatus   int8   `json:"read_status"`
	CreatedAt    string `json:"created_at"` // 先用CreatedAt排序，后面考虑改成SentAt
}
