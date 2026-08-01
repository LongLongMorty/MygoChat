package middleware

import (
	"kama_chat_server/pkg/auth"
	"kama_chat_server/pkg/zlog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware JWT 认证中间件
// 从 Authorization: Bearer <token> 头解析 JWT，校验通过后注入 uuid 和 is_admin 到 context
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "缺少认证信息",
			})
			c.Abort()
			return
		}
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
		tokenString := parts[1]
		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			zlog.Error("JWT 解析失败: " + err.Error())
			c.JSON(http.StatusUnauthorized, gin.H{
				"code":    401,
				"message": "无效或过期的认证信息",
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
