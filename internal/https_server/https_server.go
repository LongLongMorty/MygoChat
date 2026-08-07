package https_server

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	v1 "kama_chat_server/api/v1"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/https_server/middleware"
	"kama_chat_server/internal/service/chat"
	"kama_chat_server/pkg/ssl"
)

var GE *gin.Engine

func init() {
	GE = gin.Default()

	// CORS 配置
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowOrigins = []string{"*"} // TODO: 生产环境改为白名单
	corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	GE.Use(cors.New(corsConfig))
	GE.Use(ssl.TlsHandler(config.GetConfig().MainConfig.Host, config.GetConfig().MainConfig.Port))

	// 静态资源：头像公开（注册/登录前需展示），文件改为鉴权下载
	GE.Static("/static/avatars", config.GetConfig().StaticAvatarPath)
	// /static/files 已移除公开路由，改用 GET /file/download?name=<filename>（需认证）

	// === 公开路由（无需认证）===
	GE.POST("/login", v1.Login)
	GE.POST("/register", v1.Register)
	GE.POST("/user/sendEmailCode", v1.SendEmailCode)
	GE.POST("/user/emailLogin", v1.EmailLogin)
	GE.GET("/wss", v1.WsLogin) // WebSocket 在 handler 内部校验 JWT

	// === 运维路由（无需认证，仅限内网）===
	GE.GET("/metrics", func(c *gin.Context) {
		s := chat.ChatMetrics.Snapshot()
		c.JSON(200, gin.H{
			"channel_routed":              s.ChannelRouted,
			"kafka_routed":                s.KafkaRouted,
			"delivery_queue_timeouts":     s.DeliveryQueueTimeouts,
			"delivery_client_closed":      s.DeliveryClientClosed,
			"delivery_status_queue_drops": s.DeliveryStatusQueueDrops,
			"session_queue_timeouts":      s.SessionQueueTimeouts,
			"process_failures":            s.ProcessFailures,
			"kafka_commit_failures":       s.KafkaCommitFailures,
			"cache_queue_drops":           s.CacheQueueDrops,
			"batch_flush_errors":          s.BatchFlushErrors,
		})
	})

	// === 认证路由（需要 JWT）===
	authGroup := GE.Group("/")
	authGroup.Use(middleware.AuthMiddleware())
	{
		// 用户
		authGroup.POST("/user/updateUserInfo", v1.UpdateUserInfo)
		authGroup.POST("/user/getUserInfo", v1.GetUserInfo)
		authGroup.POST("/user/wsLogout", v1.WsLogout)

		// 群聊
		authGroup.POST("/group/createGroup", v1.CreateGroup)
		authGroup.POST("/group/loadMyGroup", v1.LoadMyGroup)
		authGroup.POST("/group/checkGroupAddMode", v1.CheckGroupAddMode)
		authGroup.POST("/group/enterGroupDirectly", v1.EnterGroupDirectly)
		authGroup.POST("/group/leaveGroup", v1.LeaveGroup)
		authGroup.POST("/group/dismissGroup", v1.DismissGroup)
		authGroup.POST("/group/getGroupInfo", v1.GetGroupInfo)
		authGroup.POST("/group/updateGroupInfo", v1.UpdateGroupInfo)
		authGroup.POST("/group/getGroupMemberList", v1.GetGroupMemberList)
		authGroup.POST("/group/removeGroupMembers", v1.RemoveGroupMembers)

		// 会话
		authGroup.POST("/session/openSession", v1.OpenSession)
		authGroup.POST("/session/getUserSessionList", v1.GetUserSessionList)
		authGroup.POST("/session/getGroupSessionList", v1.GetGroupSessionList)
		authGroup.POST("/session/deleteSession", v1.DeleteSession)
		authGroup.POST("/session/checkOpenSessionAllowed", v1.CheckOpenSessionAllowed)

		// 联系人
		authGroup.POST("/contact/getUserList", v1.GetUserList)
		authGroup.POST("/contact/loadMyJoinedGroup", v1.LoadMyJoinedGroup)
		authGroup.POST("/contact/getContactInfo", v1.GetContactInfo)
		authGroup.POST("/contact/deleteContact", v1.DeleteContact)
		authGroup.POST("/contact/applyContact", v1.ApplyContact)
		authGroup.POST("/contact/getNewContactList", v1.GetNewContactList)
		authGroup.POST("/contact/passContactApply", v1.PassContactApply)
		authGroup.POST("/contact/blackContact", v1.BlackContact)
		authGroup.POST("/contact/cancelBlackContact", v1.CancelBlackContact)
		authGroup.POST("/contact/getAddGroupList", v1.GetAddGroupList)
		authGroup.POST("/contact/refuseContactApply", v1.RefuseContactApply)
		authGroup.POST("/contact/blackApply", v1.BlackApply)

		// 消息（含上传，P0-4: 上传现在受认证保护）
		authGroup.POST("/message/getMessageList", v1.GetMessageList)
		authGroup.POST("/message/getGroupMessageList", v1.GetGroupMessageList)
		authGroup.POST("/message/revokeMessage", v1.RevokeMessage)
		authGroup.POST("/message/uploadAvatar", v1.UploadAvatar)
		authGroup.POST("/message/uploadFile", v1.UploadFile)
		// P1 修复：文件下载改为鉴权端点
		authGroup.GET("/file/download", v1.DownloadFile)

		// 聊天室（空壳，保留路由）
		authGroup.POST("/chatroom/getCurContactListInChatRoom", v1.GetCurContactListInChatRoom)
	}

	// === 管理员路由（需要 JWT + 管理员权限）===
	adminGroup := GE.Group("/")
	adminGroup.Use(middleware.AuthMiddleware(), middleware.AdminMiddleware())
	{
		adminGroup.POST("/user/getUserInfoList", v1.GetUserInfoList)
		adminGroup.POST("/user/ableUsers", v1.AbleUsers)
		adminGroup.POST("/user/disableUsers", v1.DisableUsers)
		adminGroup.POST("/user/deleteUsers", v1.DeleteUsers)
		adminGroup.POST("/user/setAdmin", v1.SetAdmin)
		adminGroup.POST("/group/getGroupInfoList", v1.GetGroupInfoList)
		adminGroup.POST("/group/deleteGroups", v1.DeleteGroups)
		adminGroup.POST("/group/setGroupsStatus", v1.SetGroupsStatus)
	}
}
