package auth

import (
	"strings"
	"testing"
)

func TestGenerateAndParseToken(t *testing.T) {
	// 必须先设置 secret
	if err := SetSecret("this_is_a_test_secret_at_least_32_bytes_long!!"); err != nil {
		t.Fatalf("SetSecret 失败: %v", err)
	}

	token, err := GenerateToken("U123456789", 1)
	if err != nil {
		t.Fatalf("GenerateToken 失败: %v", err)
	}
	if token == "" {
		t.Error("token 为空")
	}
	if strings.Count(token, ".") != 2 {
		t.Error("JWT 格式错误（应为 header.payload.signature）")
	}

	// 解析 token
	claims, err := ParseToken(token)
	if err != nil {
		t.Fatalf("ParseToken 失败: %v", err)
	}
	if claims.Uuid != "U123456789" {
		t.Errorf("uuid 不匹配: got %s", claims.Uuid)
	}
	if claims.IsAdmin != 1 {
		t.Errorf("is_admin 不匹配: got %d", claims.IsAdmin)
	}
}

func TestParseTokenInvalid(t *testing.T) {
	SetSecret("this_is_a_test_secret_at_least_32_bytes_long!!")

	// 无效 token
	_, err := ParseToken("invalid.token.here")
	if err == nil {
		t.Error("无效 token 应返回错误")
	}

	// 篡改 token
	token, _ := GenerateToken("U123", 0)
	tampered := token[:len(token)-5] + "XXXXX"
	_, err = ParseToken(tampered)
	if err == nil {
		t.Error("篡改 token 应返回错误")
	}
}

func TestSetSecretTooShort(t *testing.T) {
	// 短密钥应拒绝
	err := SetSecret("short")
	if err == nil {
		t.Error("短密钥应拒绝启动")
	}
	if !strings.Contains(err.Error(), "32") {
		t.Errorf("错误信息应包含长度要求: %v", err)
	}
}

func TestGenerateTokenNoSecret(t *testing.T) {
	// 重置 secret 为空
	jwtSecret = ""
	_, err := GenerateToken("U123", 0)
	if err == nil {
		t.Error("未设置 secret 应返回错误")
	}
}
