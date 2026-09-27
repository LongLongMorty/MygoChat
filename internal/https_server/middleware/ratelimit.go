package middleware

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/zlog"
)

// 可替换的计数器实现，便于单测注入内存实现（默认走 Redis）。
var (
	ratelimitIncr   = myredis.Incr
	ratelimitExpire = myredis.Expire
)

var rateLimitBlocked atomic.Uint64

// RateLimitBlockedTotal 返回被限流拦截的请求总数（供指标采集）。
func RateLimitBlockedTotal() uint64 {
	return rateLimitBlocked.Load()
}

// RateLimit 基于 Redis 固定窗口的限流中间件，按客户端 IP 计数。
//
// prefix 区分不同接口；limit 为窗口内允许的最大请求数；window 为窗口长度。
// 使用 RemoteIP（直连地址）而非 X-Forwarded-For，避免伪造头绕过。
// Redis 故障时 fail-open（放行），避免限流组件故障拖垮登录等核心链路。
func RateLimit(prefix string, limit int, window time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := fmt.Sprintf("ratelimit:%s:%s", prefix, c.RemoteIP())

		count, err := ratelimitIncr(key)
		if err != nil {
			zlog.Warn("限流计数失败，放行请求: " + err.Error())
			c.Next()
			return
		}
		if count == 1 {
			// 首次计数时开启窗口
			if err := ratelimitExpire(key, window); err != nil {
				zlog.Warn("限流窗口设置失败: " + err.Error())
			}
		}
		if count > int64(limit) {
			rateLimitBlocked.Add(1)
			c.JSON(http.StatusTooManyRequests, gin.H{
				"code":    http.StatusTooManyRequests,
				"message": "请求过于频繁，请稍后重试",
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
