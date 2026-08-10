package auth

import "golang.org/x/crypto/bcrypt"

// dummyBcryptHash 固定假密码的 bcrypt 哈希，启动时生成一次。
// 用于用户不存在时执行等价耗时比较，抹平响应时间差异，防账号枚举的时间侧信道。
var dummyBcryptHash = func() string {
	hashed, err := bcrypt.GenerateFromPassword([]byte("dummy-password-for-timing"), bcrypt.DefaultCost)
	if err != nil {
		panic("bcrypt dummy hash 生成失败: " + err.Error())
	}
	return string(hashed)
}()

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

// VerifyDummy 对固定假密码执行 bcrypt 比较（耗时与真实校验一致）。
// 用户不存在时调用，保证"邮箱不存在"与"密码错误"的响应时间无法区分，防账号枚举。
func VerifyDummy(password string) bool {
	return VerifyPassword(dummyBcryptHash, password)
}
