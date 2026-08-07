package v1

import (
	"github.com/gin-gonic/gin"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/model"
	"kama_chat_server/internal/service/gorm"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/zlog"
	"net/http"
)

// GetMessageList 获取聊天记录
// P0 修复：强制 user_one_id 为当前 JWT 用户，校验 user_two_id 为有效联系人
func GetMessageList(c *gin.Context) {
	var req request.GetMessageListRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    500,
			"message": constants.SYSTEM_ERROR,
		})
		return
	}

	// P0: 强制当前用户为一方，忽略请求体中的 user_one_id
	currentUuid := c.GetString("uuid")
	req.UserOneId = currentUuid

	// P0: 自己不能查自己
	if req.UserOneId == req.UserTwoId {
		c.JSON(http.StatusOK, gin.H{
			"code":    -2,
			"message": "不能查看与自己的聊天记录",
		})
		return
	}

	// P1 修复：校验另一方是当前用户的有效联系人（必须 contact_type=USER, status=NORMAL）
	// 拉黑（BLACK/BE_BLACK）和删除（DELETE/BE_DELETE）的联系人不能查看聊天记录
	var contact model.UserContact
	res := dao.GormDB.Where(
		"((user_id = ? AND contact_id = ?) OR (user_id = ? AND contact_id = ?)) AND contact_type = 0 AND status = 0 AND deleted_at IS NULL",
		req.UserOneId, req.UserTwoId, req.UserTwoId, req.UserOneId).First(&contact)
	if res.Error != nil {
		zlog.Error("非联系人/已拉黑/已删除用户尝试查看聊天记录: " + currentUuid + " -> " + req.UserTwoId)
		c.JSON(http.StatusOK, gin.H{
			"code":    403,
			"message": "无权查看该聊天记录",
		})
		return
	}

	message, rsp, ret := gorm.MessageService.GetMessageList(req.UserOneId, req.UserTwoId, req.Limit, req.BeforeId)
	JsonBack(c, message, ret, rsp)
}

// RevokeMessage 撤回消息（仅发送者本人）
func RevokeMessage(c *gin.Context) {
	var req request.RevokeMessageRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    500,
			"message": constants.SYSTEM_ERROR,
		})
		return
	}
	message, ret := gorm.MessageService.RevokeMessage(c.GetString("uuid"), req.MessageUuid)
	JsonBack(c, message, ret, nil)
}

// GetGroupMessageList 获取群聊消息记录
// P0 修复：校验当前用户为该群有效成员
func GetGroupMessageList(c *gin.Context) {
	var req request.GetGroupMessageListRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    500,
			"message": constants.SYSTEM_ERROR,
		})
		return
	}

	currentUuid := c.GetString("uuid")

	// P0: 校验当前用户是否是该群的有效成员
	// 修复：改用 group_member 权威表（status=正常 且未删除），
	// 原实现查 user_contact，被踢/退群成员可能因两表不同步仍通过校验
	var memberCount int64
	memberRes := dao.GormDB.Table("group_member").
		Where("group_id = ? AND user_id = ? AND status = 0 AND deleted_at = 0",
			req.GroupId, currentUuid).
		Count(&memberCount)
	if memberRes.Error != nil || memberCount == 0 {
		zlog.Error("非群成员尝试查看群聊记录: " + currentUuid + " -> " + req.GroupId)
		c.JSON(http.StatusOK, gin.H{
			"code":    403,
			"message": "无权查看该群聊记录",
		})
		return
	}

	message, rsp, ret := gorm.MessageService.GetGroupMessageList(req.GroupId, req.Limit, req.BeforeId)
	JsonBack(c, message, ret, rsp)
}

// UploadAvatar 上传头像
func UploadAvatar(c *gin.Context) {
	message, data, ret := gorm.MessageService.UploadAvatar(c)
	JsonBack(c, message, ret, data)
}

// UploadFile 上传文件
func UploadFile(c *gin.Context) {
	message, data, ret := gorm.MessageService.UploadFile(c)
	JsonBack(c, message, ret, data)
}
