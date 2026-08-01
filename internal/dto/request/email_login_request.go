package request

type EmailLoginRequest struct {
	Email    string `json:"email"`
	EmailCode string `json:"email_code"`
}