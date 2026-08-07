package request

type GetMessageListRequest struct {
	UserOneId string `json:"user_one_id"`
	UserTwoId string `json:"user_two_id"`
	// Limit 每页条数，默认 50，最大 100
	Limit int `json:"limit"`
	// BeforeId 游标分页：返回 id < BeforeId 的最近 Limit 条；0 表示第一页（最近消息）
	BeforeId int64 `json:"before_id"`
}
