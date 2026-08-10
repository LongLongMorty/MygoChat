package gorm

import (
	"encoding/json"
	"errors"
	"fmt"
	redis "github.com/go-redis/redis/v8"
	"gorm.io/gorm"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/dto/respond"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	myemail "kama_chat_server/internal/service/email"
	"kama_chat_server/pkg/auth"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/enum/user_info/user_status_enum"
	"kama_chat_server/pkg/util/random"
	"kama_chat_server/pkg/zlog"
	"regexp"
	"strings"
	"time"

	"gorm.io/plugin/soft_delete"
)

type userInfoService struct {
}

var UserInfoService = new(userInfoService)

// normalizeEmail trims spaces and converts to lowercase.
func normalizeEmail(email string) string {
	return strings.TrimSpace(strings.ToLower(email))
}

// checkEmailValid 校验邮箱是否有效
func (u *userInfoService) checkEmailValid(email string) bool {
	pattern := `^[^\s@]+@[^\s@]+\.[^\s@]+$`
	match, err := regexp.MatchString(pattern, email)
	if err != nil {
		zlog.Error(err.Error())
	}
	return match
}

// checkUserIsAdminOrNot 检验用户是否为管理员
func (u *userInfoService) checkUserIsAdminOrNot(user model.UserInfo) int8 {
	return user.IsAdmin
}

// Login 邮箱密码登录
func (u *userInfoService) Login(loginReq request.LoginRequest) (string, *respond.LoginRespond, int) {
	email := normalizeEmail(loginReq.Email)
	if !u.checkEmailValid(email) {
		return "邮箱格式不正确", nil, -2
	}
	password := loginReq.Password
	var user model.UserInfo
	res := dao.GormDB.First(&user, "email = ?", email)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			// 防账号枚举：与密码错误返回相同文案；dummy bcrypt 抹平响应时间差异
			zlog.Info("登录失败：邮箱未注册 " + email)
			_ = auth.VerifyDummy(password)
			return "邮箱或密码错误", nil, -2
		}
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	if user.Status == user_status_enum.DISABLE {
		message := "账号已被禁用，无法登录"
		zlog.Info(message)
		return message, nil, -2
	}
	if !auth.VerifyPassword(user.Password, password) {
		message := "邮箱或密码错误"
		zlog.Info("登录失败：密码错误 " + email)
		return message, nil, -2
	}

	// P0-1: 签发 JWT Token
	token, err := auth.GenerateToken(user.Uuid, user.IsAdmin)
	if err != nil {
		zlog.Error("JWT 生成失败: " + err.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}

	loginRsp := &respond.LoginRespond{
		Uuid:      user.Uuid,
		Telephone: user.Telephone,
		Nickname:  user.Nickname,
		Email:     user.Email,
		Avatar:    user.Avatar,
		Gender:    user.Gender,
		Birthday:  user.Birthday,
		Signature: user.Signature,
		IsAdmin:   user.IsAdmin,
		Status:    user.Status,
		Token:     token,
	}
	year, month, day := user.CreatedAt.Date()
	loginRsp.CreatedAt = fmt.Sprintf("%d.%d.%d", year, month, day)

	return "登陆成功", loginRsp, 0
}

// EmailLogin 邮箱验证码登录
func (u *userInfoService) EmailLogin(req request.EmailLoginRequest) (string, *respond.LoginRespond, int) {
	email := normalizeEmail(req.Email)
	if !u.checkEmailValid(email) {
		return "邮箱格式不正确", nil, -2
	}
	var user model.UserInfo
	res := dao.GormDB.First(&user, "email = ?", email)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			// 防账号枚举：与验证码错误返回相同文案
			zlog.Info("验证码登录失败：邮箱未注册 " + email)
			return "邮箱或验证码错误", nil, -2
		}
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	if user.Status == user_status_enum.DISABLE {
		message := "账号已被禁用，无法登录"
		zlog.Info(message)
		return message, nil, -2
	}

	if !myemail.VerifyEmailCode(email, req.EmailCode) {
		message := "邮箱或验证码错误"
		zlog.Info(message)
		return message, nil, -2
	}

	token, err := auth.GenerateToken(user.Uuid, user.IsAdmin)
	if err != nil {
		zlog.Error("JWT 生成失败: " + err.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}

	loginRsp := &respond.LoginRespond{
		Uuid:      user.Uuid,
		Telephone: user.Telephone,
		Nickname:  user.Nickname,
		Email:     user.Email,
		Avatar:    user.Avatar,
		Gender:    user.Gender,
		Birthday:  user.Birthday,
		Signature: user.Signature,
		IsAdmin:   user.IsAdmin,
		Status:    user.Status,
		Token:     token,
	}
	year, month, day := user.CreatedAt.Date()
	loginRsp.CreatedAt = fmt.Sprintf("%d.%d.%d", year, month, day)

	return "登陆成功", loginRsp, 0
}

// SendEmailCode 发送邮箱验证码
func (u *userInfoService) SendEmailCode(email string) (string, int) {
	normalized := normalizeEmail(email)
	if !u.checkEmailValid(normalized) {
		return "邮箱格式不正确", -2
	}
	return myemail.SendEmailCode(normalized)
}

// checkEmailExist 检查邮箱是否存在
func (u *userInfoService) checkEmailExist(email string) (string, int) {
	var user model.UserInfo
	if res := dao.GormDB.Where("email = ?", email).First(&user); res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			zlog.Info("该邮箱不存在，可以注册")
			return "", 0
		}
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	zlog.Info("该邮箱已经存在，注册失败")
	return "该邮箱已经存在，注册失败", -2
}

// Register 注册
func (u *userInfoService) Register(registerReq request.RegisterRequest) (string, *respond.RegisterRespond, int) {
	email := normalizeEmail(registerReq.Email)
	if !u.checkEmailValid(email) {
		return "邮箱格式不正确", nil, -2
	}

	if !myemail.VerifyEmailCode(email, registerReq.EmailCode) {
		message := "验证码不正确或已过期，请重试"
		zlog.Info(message)
		return message, nil, -2
	}

	message, ret := u.checkEmailExist(email)
	if ret != 0 {
		return message, nil, ret
	}

	var newUser model.UserInfo
	newUser.Uuid = "U" + random.GetNowAndLenRandomString(11)
	newUser.Email = email
	newUser.Telephone = ""
	hashedPassword, err := auth.HashPassword(registerReq.Password)
	if err != nil {
		zlog.Error("密码哈希失败: " + err.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	newUser.Password = hashedPassword
	newUser.Nickname = registerReq.Nickname
	newUser.Avatar = "https://cube.elemecdn.com/0/88/03b0d39583f48206768a7534e55bcpng.png"
	newUser.CreatedAt = time.Now()
	newUser.IsAdmin = u.checkUserIsAdminOrNot(newUser)
	newUser.Status = user_status_enum.NORMAL
	// 手机号验证，最后一步才调用api，省钱hhh

	res := dao.GormDB.Create(&newUser)
	if res.Error != nil {
		// MySQL 1062: Duplicate entry — 并发注册兜底，Service 层 checkEmailExist 已拦截非删除行
		if strings.Contains(res.Error.Error(), "1062") || strings.Contains(res.Error.Error(), "Duplicate entry") {
			zlog.Info("并发注册冲突: " + email + " 已存在")
			return "该邮箱已经存在，注册失败", nil, -2
		}
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	// P0-1: 注册成功后签发 JWT Token
	token, err := auth.GenerateToken(newUser.Uuid, newUser.IsAdmin)
	if err != nil {
		zlog.Error("JWT 生成失败: " + err.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	registerRsp := &respond.RegisterRespond{
		Uuid:      newUser.Uuid,
		Telephone: newUser.Telephone,
		Nickname:  newUser.Nickname,
		Email:     newUser.Email,
		Avatar:    newUser.Avatar,
		Gender:    newUser.Gender,
		Birthday:  newUser.Birthday,
		Signature: newUser.Signature,
		IsAdmin:   newUser.IsAdmin,
		Status:    newUser.Status,
		Token:     token,
	}
	year, month, day := newUser.CreatedAt.Date()
	registerRsp.CreatedAt = fmt.Sprintf("%d.%d.%d", year, month, day)

	return "注册成功", registerRsp, 0
}

// UpdateUserInfo 修改用户信息
// 某用户修改了信息，可能会影响contact_user_list，不需要删除redis的contact_user_list，timeout之后会自己更新
// 但是需要更新redis的user_info，因为可能影响用户搜索
func (u *userInfoService) UpdateUserInfo(updateReq request.UpdateUserInfoRequest) (string, int) {
	var user model.UserInfo
	if res := dao.GormDB.First(&user, "uuid = ?", updateReq.Uuid); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	if updateReq.Email != "" {
		user.Email = updateReq.Email
	}
	if updateReq.Nickname != "" {
		user.Nickname = updateReq.Nickname
	}
	if updateReq.Birthday != "" {
		user.Birthday = updateReq.Birthday
	}
	if updateReq.Signature != "" {
		user.Signature = updateReq.Signature
	}
	if updateReq.Avatar != "" {
		user.Avatar = updateReq.Avatar
	}
	if res := dao.GormDB.Save(&user); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	//if err := myredis.DelKeysWithPattern("user_info_" + updateReq.Uuid); err != nil {
	//	zlog.Error(err.Error())
	//}
	return "修改用户信息成功", 0
}

// GetUserInfoList 获取用户列表除了ownerId之外 - 管理员
// 管理员少，而且如果用户更改了，那么管理员会一直频繁删除redis，更新redis，比较麻烦，所以管理员暂时不使用redis缓存
func (u *userInfoService) GetUserInfoList(ownerId string) (string, []respond.GetUserListRespond, int) {
	// redis中没有数据，从数据库中获取
	var users []model.UserInfo
	// 获取所有的用户
	if res := dao.GormDB.Unscoped().Where("uuid != ?", ownerId).Find(&users); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	var rsp []respond.GetUserListRespond
	for _, user := range users {
		rp := respond.GetUserListRespond{
			Uuid:      user.Uuid,
			Telephone: user.Telephone,
			Nickname:  user.Nickname,
			Status:    user.Status,
			IsAdmin:   user.IsAdmin,
		}
		if user.DeletedAt != 0 {
			rp.IsDeleted = true
		} else {
			rp.IsDeleted = false
		}
		rsp = append(rsp, rp)
	}
	return "获取用户列表成功", rsp, 0
}

// AbleUsers 启用用户
// 用户是否启用禁用需要实时更新contact_user_list状态，所以redis的contact_user_list需要删除
func (u *userInfoService) AbleUsers(uuidList []string) (string, int) {
	var users []model.UserInfo
	if res := dao.GormDB.Model(model.UserInfo{}).Where("uuid in (?)", uuidList).Find(&users); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, user := range users {
		user.Status = user_status_enum.NORMAL
		if res := dao.GormDB.Save(&user); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}
	// 删除所有"contact_user_list"开头的key
	//if err := myredis.DelKeysWithPrefix("contact_user_list"); err != nil {
	//	zlog.Error(err.Error())
	//}
	return "启用用户成功", 0
}

// DisableUsers 禁用用户
// 用户是否启用禁用需要实时更新contact_user_list状态，所以redis的contact_user_list需要删除
func (u *userInfoService) DisableUsers(uuidList []string) (string, int) {
	var users []model.UserInfo
	if res := dao.GormDB.Model(model.UserInfo{}).Where("uuid in (?)", uuidList).Find(&users); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, user := range users {
		user.Status = user_status_enum.DISABLE
		if res := dao.GormDB.Save(&user); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		var sessionList []model.Session
		if res := dao.GormDB.Where("send_id = ? or receive_id = ?", user.Uuid, user.Uuid).Find(&sessionList); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		for _, session := range sessionList {
			var deletedAt gorm.DeletedAt
			deletedAt.Time = time.Now()
			deletedAt.Valid = true
			session.DeletedAt = deletedAt
			if res := dao.GormDB.Save(&session); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
	}
	// 删除所有"contact_user_list"开头的key
	//if err := myredis.DelKeysWithPrefix("contact_user_list"); err != nil {
	//	zlog.Error(err.Error())
	//}
	return "禁用用户成功", 0
}

// DeleteUsers 删除用户
// 用户是否启用禁用需要实时更新contact_user_list状态，所以redis的contact_user_list需要删除
func (u *userInfoService) DeleteUsers(uuidList []string) (string, int) {
	var users []model.UserInfo
	if res := dao.GormDB.Model(model.UserInfo{}).Where("uuid in (?)", uuidList).Find(&users); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, user := range users {
		user.DeletedAt = soft_delete.DeletedAt(time.Now().UnixNano())
		if res := dao.GormDB.Save(&user); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}

		// 删除会话
		var sessionList []model.Session
		if res := dao.GormDB.Where("send_id = ? or receive_id = ?", user.Uuid, user.Uuid).Find(&sessionList); res.Error != nil {
			if errors.Is(res.Error, gorm.ErrRecordNotFound) {
				zlog.Info(res.Error.Error())
			} else {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
		for _, session := range sessionList {
			var deletedAt gorm.DeletedAt
			deletedAt.Time = time.Now()
			deletedAt.Valid = true
			session.DeletedAt = deletedAt
			if res := dao.GormDB.Save(&session); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}

		// 删除联系人
		var contactList []model.UserContact
		if res := dao.GormDB.Where("user_id = ? or contact_id = ?", user.Uuid, user.Uuid).Find(&contactList); res.Error != nil {
			if errors.Is(res.Error, gorm.ErrRecordNotFound) {
				zlog.Info(res.Error.Error())
			} else {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
		for _, contact := range contactList {
			var deletedAt gorm.DeletedAt
			deletedAt.Time = time.Now()
			deletedAt.Valid = true
			contact.DeletedAt = deletedAt
			if res := dao.GormDB.Save(&contact); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}

		// 删除申请记录
		var applyList []model.ContactApply
		if res := dao.GormDB.Where("user_id = ? or contact_id = ?", user.Uuid, user.Uuid).Find(&applyList); res.Error != nil {
			if errors.Is(res.Error, gorm.ErrRecordNotFound) {
				zlog.Info(res.Error.Error())
			} else {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
		for _, apply := range applyList {
			var deletedAt gorm.DeletedAt
			deletedAt.Time = time.Now()
			deletedAt.Valid = true
			apply.DeletedAt = deletedAt
			if res := dao.GormDB.Save(&apply); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}

	}
	// 删除所有"contact_user_list"开头的key
	//if err := myredis.DelKeysWithPrefix("contact_user_list"); err != nil {
	//	zlog.Error(err.Error())
	//}
	return "删除用户成功", 0
}

// GetUserInfo 获取用户信息
func (u *userInfoService) GetUserInfo(uuid string) (string, *respond.GetUserInfoRespond, int) {
	// redis
	zlog.Info(uuid)
	rspString, err := myredis.GetKeyNilIsErr("user_info_" + uuid)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			zlog.Info(err.Error())
			var user model.UserInfo
			if res := dao.GormDB.Where("uuid = ?", uuid).Find(&user); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			rsp := respond.GetUserInfoRespond{
				Uuid:      user.Uuid,
				Telephone: user.Telephone,
				Nickname:  user.Nickname,
				Avatar:    user.Avatar,
				Birthday:  user.Birthday,
				Email:     user.Email,
				Gender:    user.Gender,
				Signature: user.Signature,
				CreatedAt: user.CreatedAt.Format("2006-01-02 15:04:05"),
				IsAdmin:   user.IsAdmin,
				Status:    user.Status,
			}
			//rspString, err := json.Marshal(rsp)
			//if err != nil {
			//	zlog.Error(err.Error())
			//}
			//if err := myredis.SetKeyEx("user_info_"+uuid, string(rspString), constants.REDIS_TIMEOUT*time.Minute); err != nil {
			//	zlog.Error(err.Error())
			//}
			return "获取用户信息成功", &rsp, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var rsp respond.GetUserInfoRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取用户信息成功", &rsp, 0
}

// SetAdmin 设置管理员
func (u *userInfoService) SetAdmin(uuidList []string, isAdmin int8) (string, int) {
	var users []model.UserInfo
	if res := dao.GormDB.Where("uuid = (?)", uuidList).Find(&users); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, user := range users {
		user.IsAdmin = isAdmin
		if res := dao.GormDB.Save(&user); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}
	return "设置管理员成功", 0
}
