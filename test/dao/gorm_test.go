package dao

import (
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/util/random"
	"strconv"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	// P1 修复：无 MySQL 时 skip，不 fail
	if err := dao.InitDB(); err != nil {
		t.Skip("MySQL 不可用，跳过 DAO 测试: " + err.Error())
	}
	userInfo := &model.UserInfo{
		Uuid:      "U" + strconv.Itoa(random.GetRandomInt(11)),
		Nickname:  "apylee",
		Telephone: "180323532112",
		Email:     "1212312312@qq.com",
		Password:  "123456",
		CreatedAt: time.Now(),
		IsAdmin:   1,
	}
	_ = dao.GormDB.Create(userInfo)
}
