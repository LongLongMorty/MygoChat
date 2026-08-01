package auth

import (
	"testing"
)

func TestHashAndVerifyPassword(t *testing.T) {
	hashed, err := HashPassword("mypassword")
	if err != nil {
		t.Fatalf("HashPassword 失败: %v", err)
	}
	if hashed == "mypassword" {
		t.Error("哈希后仍为明文")
	}
	if len(hashed) < 50 {
		t.Error("哈希长度异常")
	}

	// 正确密码校验
	if !VerifyPassword(hashed, "mypassword") {
		t.Error("正确密码校验失败")
	}

	// 错误密码校验
	if VerifyPassword(hashed, "wrongpassword") {
		t.Error("错误密码竟然校验通过")
	}
}

func TestHashPasswordUnique(t *testing.T) {
	h1, _ := HashPassword("same")
	h2, _ := HashPassword("same")
	if h1 == h2 {
		t.Error("相同密码哈希应不同（salt 随机）")
	}
}
