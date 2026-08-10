package email

import (
	"crypto/tls"
	"fmt"
	"kama_chat_server/internal/config"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/util/random"
	"kama_chat_server/pkg/zlog"
	"net/smtp"
	"strconv"
	"strings"
	"time"
)

const (
	// EmailCodeTTL 验证码有效期
	EmailCodeTTL = 5 * time.Minute
	// EmailCodeCooldown 重发冷却时间，与验证码有效期解耦：
	// 验证码 5 分钟有效，但 60 秒冷却期过后即可重发，无需等验证码过期
	EmailCodeCooldown = 60 * time.Second
)

// codeKey 验证码存储 key（TTL = EmailCodeTTL）
func codeKey(email string) string {
	return "auth_code_email_" + email
}

// cooldownKey 重发冷却 key（TTL = EmailCodeCooldown），仅用于防重复发送
func cooldownKey(email string) string {
	return "auth_code_cd_" + email
}

// SendEmailCode sends a 6-digit verification code to the given email address.
// Returns (message, retCode) where retCode=0 means success.
func SendEmailCode(email string) (string, int) {
	normalized := normalizeEmail(email)

	// 原子抢占重发冷却锁（SET NX EX 60）：并发请求只有一个能拿到锁，
	// 其余立即被拒，避免同一邮箱同时发出多封验证码邮件
	ok, err := myredis.SetNX(cooldownKey(normalized), "1", EmailCodeCooldown)
	if err != nil {
		zlog.Error(err.Error())
		return "系统错误", -1
	}
	if !ok {
		return "发送过于频繁，请稍后再试", -2
	}

	// Generate 6-digit code
	code := strconv.Itoa(random.GetRandomInt(6))
	if len(code) != 6 {
		code = fmt.Sprintf("%06s", code)
	}
	if len(code) > 6 {
		code = code[:6]
	}

	// Write to Redis with 5-minute TTL
	if err := myredis.SetKeyEx(codeKey(normalized), code, EmailCodeTTL); err != nil {
		// 验证码写入失败，释放冷却锁，允许用户立即重试
		_ = myredis.DelKeyIfExists(cooldownKey(normalized))
		zlog.Error(err.Error())
		return "系统错误", -1
	}

	// Send email via SMTP
	if err := sendSMTP(normalized, code); err != nil {
		// SMTP failed — remove both keys so the user can retry immediately
		_ = myredis.DelKeyIfExists(codeKey(normalized))
		_ = myredis.DelKeyIfExists(cooldownKey(normalized))
		zlog.Error("SMTP发送失败: " + err.Error())
		return "邮件发送失败，请稍后重试", -1
	}

	return "验证码已发送到邮箱，请查收", 0
}

// VerifyEmailCode checks the code for the given email and deletes it on success.
// Returns true if the code is correct and not expired.
func VerifyEmailCode(email, code string) bool {
	normalized := normalizeEmail(email)

	stored, err := myredis.GetKey(codeKey(normalized))
	if err != nil || stored == "" {
		return false
	}
	if stored != code {
		return false
	}
	// One-time consumption: delete immediately on success
	_ = myredis.DelKeyIfExists(codeKey(normalized))
	return true
}

// normalizeEmail trims spaces and converts to lowercase.
func normalizeEmail(email string) string {
	return strings.TrimSpace(strings.ToLower(email))
}

// sendSMTP sends the verification code email via SMTP.
func sendSMTP(to, code string) error {
	cfg := config.GetConfig().EmailConfig
	if cfg.Host == "" || cfg.Username == "" {
		return fmt.Errorf("SMTP not configured")
	}

	subject := "邮箱验证码"
	body := fmt.Sprintf("您的验证码是：%s\n\n验证码有效期为5分钟，请勿泄露给他人。", code)
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		cfg.From, to, subject, body)

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)

	if cfg.Port == 465 {
		// SSL/TLS direct
		tlsConfig := &tls.Config{
			ServerName: cfg.Host,
			InsecureSkipVerify: false,
		}
		conn, err := tls.Dial("tcp", addr, tlsConfig)
		if err != nil {
			return err
		}
		client, err := smtp.NewClient(conn, cfg.Host)
		if err != nil {
			return err
		}
		defer client.Close()
		if err = client.Auth(auth); err != nil {
			return err
		}
		if err = client.Mail(cfg.From); err != nil {
			return err
		}
		if err = client.Rcpt(to); err != nil {
			return err
		}
		w, err := client.Data()
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(msg))
		if err != nil {
			return err
		}
		return w.Close()
	}

	// STARTTLS (587) or plain (25)
	return smtp.SendMail(addr, auth, cfg.From, []string{to}, []byte(msg))
}