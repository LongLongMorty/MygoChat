package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JWTClaims JWT 载荷
type JWTClaims struct {
	Uuid    string `json:"uuid"`
	IsAdmin int8   `json:"is_admin"`
	jwt.RegisteredClaims
}

// jwtSecret JWT 密钥，必须通过 SetSecret 设置，不再有默认值
var jwtSecret = ""

// SetSecret 设置 JWT 密钥（必须在 main.go 启动时调用）
// 密钥长度必须 >= 32 字节，否则返回错误
func SetSecret(secret string) error {
	if len(secret) < 32 {
		return errors.New("JWT secret 长度不足 32 字节，拒绝启动")
	}
	jwtSecret = secret
	return nil
}

// GenerateToken 生成 JWT Token
func GenerateToken(uuid string, isAdmin int8) (string, error) {
	if jwtSecret == "" {
		return "", errors.New("JWT secret 未设置，请调用 SetSecret")
	}
	claims := JWTClaims{
		Uuid:    uuid,
		IsAdmin: isAdmin,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(jwtSecret))
}

// ParseToken 解析并验证 JWT Token
func ParseToken(tokenString string) (*JWTClaims, error) {
	if jwtSecret == "" {
		return nil, errors.New("JWT secret 未设置")
	}
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(jwtSecret), nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, errors.New("invalid token")
}
