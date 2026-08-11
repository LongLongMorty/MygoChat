package v1

import (
	"github.com/gin-gonic/gin"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/https_server/middleware"
	"kama_chat_server/internal/service/chat"
	"kama_chat_server/pkg/auth"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/zlog"
	"net/http"
)

// WsLogin wss登录 Get
// P0-3 修复：握手前校验 JWT，不再信任 client_id 参数
func WsLogin(c *gin.Context) {
	// 从 query 参数获取 token
	token := c.Query("token")
	if token == "" {
		// 兼容：也支持 Authorization 头
		token = c.GetHeader("Authorization")
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
	}
	if token == "" {
		zlog.Error("WebSocket 握手失败：缺少 token")
		c.JSON(http.StatusOK, gin.H{
			"code":    401,
			"message": "缺少认证信息",
		})
		return
	}

	// 解析 JWT 获取用户身份
	claims, err := auth.ParseToken(token)
	if err != nil {
		// 无效/过期 token 属正常客户端行为（token 24h 过期），Warn 级别避免日志风暴
		zlog.Warn("WebSocket JWT 校验失败: " + err.Error())
		c.JSON(http.StatusOK, gin.H{
			"code":    401,
			"message": "无效或过期的认证信息",
		})
		return
	}

	// 校验用户状态：禁用/删除用户不能凭存量 token 重新握手建立连接
	if allowed, _ := middleware.CheckUserActive(claims.Uuid); !allowed {
		zlog.Warn("WebSocket 用户状态异常，拒绝握手: " + claims.Uuid)
		c.JSON(http.StatusOK, gin.H{
			"code":    401,
			"message": "账号不可用，请联系管理员",
		})
		return
	}

	// 使用 JWT 中的 uuid 作为 clientId，忽略 URL 中的 client_id
	clientId := claims.Uuid
	zlog.Info("WebSocket 握手成功，用户: " + clientId)
	chat.NewClientInit(c, clientId)
}

// WsLogout wss登出
func WsLogout(c *gin.Context) {
	var req request.WsLogoutRequest
	if err := c.BindJSON(&req); err != nil {
		zlog.Error(err.Error())
		c.JSON(http.StatusOK, gin.H{
			"code":    500,
			"message": constants.SYSTEM_ERROR,
		})
		return
	}
	message, ret := chat.ClientLogout(c.GetString("uuid"))
	JsonBack(c, message, ret, nil)
}
