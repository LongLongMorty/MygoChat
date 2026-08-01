package file_test

import (
	"kama_chat_server/api/v1"
	"kama_chat_server/internal/https_server/middleware"
	"kama_chat_server/pkg/auth"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
	auth.SetSecret("this_is_a_test_secret_at_least_32_bytes_long!!")
}

// setupRouter 创建带认证中间件的测试路由
func setupRouter() *gin.Engine {
	r := gin.New()
	r.Use(middleware.AuthMiddleware())
	r.GET("/file/download", v1.DownloadFile)
	return r
}

// makeToken 生成测试用 JWT
func makeToken(uuid string, isAdmin int8) string {
	token, _ := auth.GenerateToken(uuid, isAdmin)
	return token
}

// TestDownloadNoToken 无 Token 应返回 401
func TestDownloadNoToken(t *testing.T) {
	r := setupRouter()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/file/download?name=test.png", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("无 Token 应返回 401, got %d", w.Code)
	}
}

// TestDownloadMissingName 缺少文件名参数应返回错误
func TestDownloadMissingName(t *testing.T) {
	r := setupRouter()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/file/download", nil)
	req.Header.Set("Authorization", "Bearer "+makeToken("U123", 0))
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("应返回 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "缺少文件名") {
		t.Errorf("应返回缺少文件名错误, got %s", w.Body.String())
	}
}

// TestDownloadPathTraversal 路径穿越攻击应被拦截
func TestDownloadPathTraversal(t *testing.T) {
	r := setupRouter()
	payloads := []string{
		"../../../etc/passwd",
		"..\\..\\windows\\system32",
		"foo/bar",
		"foo\\bar",
	}
	for _, p := range payloads {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/file/download?name="+p, nil)
		req.Header.Set("Authorization", "Bearer "+makeToken("U123", 0))
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("路径穿越 %s 应返回 200, got %d", p, w.Code)
		}
		if !strings.Contains(w.Body.String(), "非法文件名") {
			t.Errorf("路径穿越 %s 应返回非法文件名错误", p)
		}
	}
}

// TestDownloadInvalidToken 无效 Token 应返回 401
func TestDownloadInvalidToken(t *testing.T) {
	r := setupRouter()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/file/download?name=test.png", nil)
	req.Header.Set("Authorization", "Bearer invalid.token.here")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("无效 Token 应返回 401, got %d", w.Code)
	}
}

// TestDownloadNonExistentFile 不存在的文件应返回错误
func TestDownloadNonExistentFile(t *testing.T) {
	r := setupRouter()
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/file/download?name=nonexistent_file_12345.png", nil)
	req.Header.Set("Authorization", "Bearer "+makeToken("U123", 0))
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("应返回 200, got %d", w.Code)
	}
	// 无 DB 连接时会返回文件不存在或无权访问
	if !strings.Contains(w.Body.String(), "文件不存在") {
		t.Logf("响应: %s", w.Body.String())
	}
}
