package middleware

import (
	"kama_chat_server/pkg/auth"
	"kama_chat_server/pkg/zlog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware JWT 认证中间件
// 从 Authorization: Bearer <token> 头解析 JWT；WebSocket 场景浏览器无法设置自定义
// Header（浏览器 WebSocket API 限制），回退从 query 参数 ?token=<token> 获取。
// 校验通过后注入 uuid 和 is_admin 到 context
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		var tokenString string
		if authHeader != "" {
			// 期望格式: Bearer <token>
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || parts[0] != "Bearer" {
				c.JSON(http.StatusUnauthorized, gin.H{
					"code":    401,
					"message": "认证格式错误",
				})
				c.Abort()
				return
			}
			tokenString = parts[1]
		} else {
			// WebSocket 兼容：浏览器 WebSocket API 无法设置自定义 Header，
			// 从 query 参数取 token（与 /wss handler 的兼容逻辑保持一致）
			tokenString = c.Query("token")
		}
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "缺少认证信息",
			})
			c.Abort()
			return
		}
		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			// 无效/过期 token 属正常客户端行为（token 24h 过期、客户端残留旧 token），
			// 使用 Warn 级别，避免无效请求刷屏触发日志风暴
			zlog.Warn("JWT 解析失败: " + err.Error())
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "无效或过期的认证信息",
			})
			c.Abort()
			return
		}
		// 校验用户状态：禁用/删除用户的存量 token 立即失效（JWT 无状态 + 只验签的 24h 漏洞）
		if allowed, statusCode := CheckUserActive(claims.Uuid); !allowed {
			zlog.Warn("用户状态异常，拒绝访问: " + claims.Uuid)
			c.JSON(statusCode, gin.H{
				"code":    statusCode,
				"message": "账号不可用，请联系管理员",
			})
			c.Abort()
			return
		}
		// 注入用户信息到 context
		c.Set("uuid", claims.Uuid)
		c.Set("is_admin", claims.IsAdmin)
		c.Next()
	}
}

// AdminMiddleware 管理员授权中间件
// 必须在 AuthMiddleware 之后使用
func AdminMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		isAdmin, exists := c.Get("is_admin")
		if !exists {
			c.JSON(http.StatusForbidden, gin.H{
				"code":    403,
				"message": "禁止访问",
			})
			c.Abort()
			return
		}
		if isAdmin.(int8) != 1 {
			c.JSON(http.StatusForbidden, gin.H{
				"code":    403,
				"message": "需要管理员权限",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
