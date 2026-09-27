package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestRateLimitBlocksOverLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 注入内存计数器，避免依赖 Redis
	var mu sync.Mutex
	counts := map[string]int64{}
	origIncr, origExpire := ratelimitIncr, ratelimitExpire
	ratelimitIncr = func(key string) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		counts[key]++
		return counts[key], nil
	}
	ratelimitExpire = func(string, time.Duration) error { return nil }
	defer func() { ratelimitIncr = origIncr; ratelimitExpire = origExpire }()

	r := gin.New()
	r.Use(RateLimit("test", 3, time.Minute))
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	do := func() int {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/ping", nil))
		return w.Code
	}

	for i := 1; i <= 3; i++ {
		if code := do(); code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, code)
		}
	}
	if code := do(); code != http.StatusTooManyRequests {
		t.Fatalf("4th request: expected 429, got %d", code)
	}
}

func TestRateLimitFailOpenOnRedisError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origIncr := ratelimitIncr
	ratelimitIncr = func(string) (int64, error) { return 0, errors.New("redis down") }
	defer func() { ratelimitIncr = origIncr }()

	r := gin.New()
	r.Use(RateLimit("test", 1, time.Minute))
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/ping", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("redis error should fail-open, got %d", w.Code)
	}
}
