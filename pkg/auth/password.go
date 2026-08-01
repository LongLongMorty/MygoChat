package auth

import "golang.org/x/crypto/bcrypt"

// HashPassword 使用 bcrypt 哈希密码
// cost=10，生成的哈希固定 60 字节
func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

// VerifyPassword 校验密码
// 使用 bcrypt.CompareHashAndPassword（常量时间比较，防时序攻击）
func VerifyPassword(hashedPassword, password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	return err == nil
}
