package middleware

import (
	"kama_chat_server/pkg/auth"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
	auth.SetSecret("this_is_a_test_secret_at_least_32_bytes_long!!")
}

// TestAuthMiddlewareNoToken 无 Token 应返回 401
func TestAuthMiddlewareNoToken(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(200, gin.H{"code": 0})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("无 Token 应返回 401, got %d", w.Code)
	}
}

// TestAuthMiddlewareBadFormat 格式错误应返回 401
func TestAuthMiddlewareBadFormat(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(200, gin.H{"code": 0})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "InvalidFormat token123")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("格式错误应返回 401, got %d", w.Code)
	}
}

// TestAuthMiddlewareInvalidToken 无效 Token 应返回 401
func TestAuthMiddlewareInvalidToken(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		c.JSON(200, gin.H{"code": 0})
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer invalid.token.here")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("无效 Token 应返回 401, got %d", w.Code)
	}
}

// TestAuthMiddlewareValidToken 有效 Token 应通过并注入 uuid
func TestAuthMiddlewareValidToken(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/protected", func(c *gin.Context) {
		uuid := c.GetString("uuid")
		c.JSON(200, gin.H{"code": 0, "uuid": uuid})
	})

	token, _ := auth.GenerateToken("UTEST123", 0)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/protected", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("有效 Token 应返回 200, got %d", w.Code)
	}
}

// TestAuthMiddlewareQueryToken WebSocket 场景：浏览器无法设置自定义 Header，
// query 参数携带 Token 应通过并注入 uuid
func TestAuthMiddlewareQueryToken(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware())
	r.GET("/wss", func(c *gin.Context) {
		uuid := c.GetString("uuid")
		c.JSON(200, gin.H{"code": 0, "uuid": uuid})
	})

	token, _ := auth.GenerateToken("UWS123", 0)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/wss?token="+token, nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("query token 应返回 200, got %d", w.Code)
	}
}

// TestAdminMiddlewareNonAdmin 非管理员应返回 403
func TestAdminMiddlewareNonAdmin(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware(), AdminMiddleware())
	r.GET("/admin", func(c *gin.Context) {
		c.JSON(200, gin.H{"code": 0})
	})

	// 普通用户 token (is_admin=0)
	token, _ := auth.GenerateToken("UTEST123", 0)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/admin", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("非管理员应返回 403, got %d", w.Code)
	}
}

// TestAdminMiddlewareAdmin 管理员应通过
func TestAdminMiddlewareAdmin(t *testing.T) {
	r := gin.New()
	r.Use(AuthMiddleware(), AdminMiddleware())
	r.GET("/admin", func(c *gin.Context) {
		c.JSON(200, gin.H{"code": 0})
	})

	// 管理员 token (is_admin=1)
	token, _ := auth.GenerateToken("UADMIN123", 1)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/admin", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("管理员应返回 200, got %d", w.Code)
	}
}
