package gorm

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/request"
	"kama_chat_server/internal/dto/respond"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/enum/contact/contact_status_enum"
	"kama_chat_server/pkg/enum/contact/contact_type_enum"
	"kama_chat_server/pkg/enum/group_info/group_status_enum"
	"kama_chat_server/pkg/enum/group_member/group_member_role_enum"
	"kama_chat_server/pkg/enum/group_member/group_member_status_enum"
	"kama_chat_server/pkg/util/random"
	"kama_chat_server/pkg/zlog"
	"log"
	"time"

	"gorm.io/plugin/soft_delete"
)

type groupInfoService struct {
}

var GroupInfoService = new(groupInfoService)

// SaveGroup 保存群聊
//func (g *groupInfoService) SaveGroup(groupReq request.SaveGroupRequest) error {
//	var group model.GroupInfo
//	res := dao.GormDB.First(&group, "uuid = ?", groupReq.Uuid)
//	if res.Error != nil {
//		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
//			// 创建群聊
//			group.Uuid = groupReq.Uuid
//			group.Name = groupReq.Name
//			group.Notice = groupReq.Notice
//			group.AddMode = groupReq.AddMode
//			group.Avatar = groupReq.Avatar
//			group.MemberCnt = 1
//			group.Members = append(group.Members, groupReq.OwnerId)
//			group.OwnerId = groupReq.OwnerId
//			group.CreatedAt = time.Now()
//			group.UpdatedAt = time.Now()
//			if res := dao.GormDB.Create(&group); res.Error != nil {
//				zlog.Error(res.Error.Error())
//				return res.Error
//			}
//			return nil
//		} else {
//			zlog.Error(res.Error.Error())
//			return res.Error
//		}
//	}
//	// 更新群聊
//	group.Uuid = groupReq.Uuid
//	group.Name = groupReq.Name
//	group.Notice = groupReq.Notice
//	group.AddMode = groupReq.AddMode
//	group.Avatar = groupReq.Avatar
//	group.MemberCnt = 1
//	group.Members = append(group.Members, groupReq.OwnerId)
//	group.OwnerId = groupReq.OwnerId
//	group.CreatedAt = time.Now()
//	group.UpdatedAt = time.Now()
//	return nil
//}

// CreateGroup 创建群聊
func (g *groupInfoService) CreateGroup(groupReq request.CreateGroupRequest) (string, int) {
	group := model.GroupInfo{
		Uuid:      fmt.Sprintf("G%s", random.GetNowAndLenRandomString(11)),
		Name:      groupReq.Name,
		Notice:    groupReq.Notice,
		OwnerId:   groupReq.OwnerId,
		AddMode:   groupReq.AddMode,
		Avatar:    groupReq.Avatar,
		Status:    group_status_enum.NORMAL,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		if res := tx.Create(&group); res.Error != nil {
			return res.Error
		}
		// 群主即第一个成员，role=1
		if err := GroupMemberService.addMember(tx, group.Uuid, groupReq.OwnerId, group_member_role_enum.OWNER); err != nil {
			return err
		}
		// 添加联系人
		contact := model.UserContact{
			UserId:      groupReq.OwnerId,
			ContactId:   group.Uuid,
			ContactType: contact_type_enum.GROUP,
			Status:      contact_status_enum.NORMAL,
			CreatedAt:   time.Now(),
			UpdateAt:    time.Now(),
		}
		if res := tx.Create(&contact); res.Error != nil {
			return res.Error
		}
		return nil
	})
	if err != nil {
		zlog.Error("创建群聊事务失败: " + err.Error())
		return constants.SYSTEM_ERROR, -1
	}

	if err := myredis.DelKeysWithPattern("contact_mygroup_list_" + groupReq.OwnerId); err != nil {
		zlog.Error(err.Error())
	}

	return "创建成功", 0
}

// GetAllMembers 获取所有成员信息
//func (g *groupInfoService) GetAllMembers(groupId string) ([]string, int) {
//	var group model.GroupInfo
//	res := dao.GormDB.Preload("Members").First(&group, "uuid = ?", groupId)
//	if res.Error != nil {
//		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
//			zlog.Error("群组不存在")
//			return nil, -1
//		} else {
//			zlog.Error(res.Error.Error())
//			return nil, -1
//		}
//	} else {
//		var members []string
//		if err := json.Unmarshal(group.Members, members); err != nil {
//			zlog.Error(err.Error())
//			return nil, -1
//		}
//		return members, 0
//	}
//}

// LoadMyGroup 获取我创建的群聊
func (g *groupInfoService) LoadMyGroup(ownerId string) (string, []respond.LoadMyGroupRespond, int) {
	rspString, err := myredis.GetKeyNilIsErr("contact_mygroup_list_" + ownerId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			var groupList []model.GroupInfo
			if res := dao.GormDB.Order("created_at DESC").Where("owner_id = ?", ownerId).Find(&groupList); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			var groupListRsp []respond.LoadMyGroupRespond
			for _, group := range groupList {
				groupListRsp = append(groupListRsp, respond.LoadMyGroupRespond{
					GroupId:   group.Uuid,
					GroupName: group.Name,
					Avatar:    group.Avatar,
				})
			}
			rspString, err := json.Marshal(groupListRsp)
			if err != nil {
				zlog.Error(err.Error())
			}
			if err := myredis.SetKeyEx("contact_mygroup_list_"+ownerId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
				zlog.Error(err.Error())
			}
			return "获取成功", groupListRsp, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var groupListRsp []respond.LoadMyGroupRespond
	if err := json.Unmarshal([]byte(rspString), &groupListRsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取成功", groupListRsp, 0
}

// GetGroupInfo 获取群聊详情
func (g *groupInfoService) GetGroupInfo(groupId string) (string, *respond.GetGroupInfoRespond, int) {
	rspString, err := myredis.GetKeyNilIsErr("group_info_" + groupId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			var group model.GroupInfo
			if res := dao.GormDB.First(&group, "uuid = ?", groupId); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			memberCnt, err := GroupMemberService.CountActiveMembers(group.Uuid)
			if err != nil {
				return constants.SYSTEM_ERROR, nil, -1
			}
			rsp := &respond.GetGroupInfoRespond{
				Uuid:      group.Uuid,
				Name:      group.Name,
				Notice:    group.Notice,
				Avatar:    group.Avatar,
				MemberCnt: int(memberCnt),
				OwnerId:   group.OwnerId,
				AddMode:   group.AddMode,
				Status:    group.Status,
			}
			if group.DeletedAt != 0 {
				rsp.IsDeleted = true
			} else {
				rsp.IsDeleted = false
			}
			//rspString, err := json.Marshal(rsp)
			//if err != nil {
			//	zlog.Error(err.Error())
			//}
			//if err := myredis.SetKeyEx("group_info_"+groupId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			//	zlog.Error(err.Error())
			//}
			return "获取成功", rsp, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var rsp *respond.GetGroupInfoRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取成功", rsp, 0
}

// GetGroupInfoList 获取群聊列表 - 管理员
// 管理员少，而且如果用户更改了，那么管理员会一直频繁删除redis，更新redis，比较麻烦，所以管理员暂时不使用redis缓存
func (g *groupInfoService) GetGroupInfoList() (string, []respond.GetGroupListRespond, int) {
	var groupList []model.GroupInfo
	if res := dao.GormDB.Unscoped().Find(&groupList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}
	var rsp []respond.GetGroupListRespond
	for _, group := range groupList {
		rp := respond.GetGroupListRespond{
			Uuid:    group.Uuid,
			Name:    group.Name,
			OwnerId: group.OwnerId,
			Status:  group.Status,
		}
		if group.DeletedAt != 0 {
			rp.IsDeleted = true
		} else {
			rp.IsDeleted = false
		}
		rsp = append(rsp, rp)
	}
	return "获取成功", rsp, 0
}

//func (g *groupInfoService) checkUserAndGroupValid(userId string, groupId string)

// GetGroupInfo4Chat 获取聊天会话群聊详情
//func (g *groupInfoService) GetGroupInfo4Chat() error {
//
//}

// LeaveGroup 退群
func (g *groupInfoService) LeaveGroup(userId string, groupId string) (string, int) {
	// 校验群存在
	var group model.GroupInfo
	if res := dao.GormDB.First(&group, "uuid = ?", groupId); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	// 群主不能直接退群，须先转让群主或解散群
	if group.OwnerId == userId {
		return "群主不能退群，请先转让群主或解散群", -2
	}
	var deletedAt gorm.DeletedAt
	deletedAt.Time = time.Now()
	deletedAt.Valid = true

	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		// 更新成员状态为已退群
		if err := tx.Model(&model.GroupMember{}).
			Where("group_id = ? AND user_id = ?", groupId, userId).
			Updates(map[string]interface{}{
				"status":     group_member_status_enum.QUIT,
				"deleted_at": time.Now().UnixNano(),
				"updated_at": time.Now(),
			}).Error; err != nil {
			return err
		}
		// 删除会话
		if res := tx.Model(&model.Session{}).Where("send_id = ? AND receive_id = ?", userId, groupId).Update("deleted_at", deletedAt); res.Error != nil {
			return res.Error
		}
		// 删除联系人
		if res := tx.Model(&model.UserContact{}).Where("user_id = ? AND contact_id = ?", userId, groupId).Updates(map[string]interface{}{
			"deleted_at": deletedAt,
			"status":     contact_status_enum.QUIT_GROUP, // 退群
		}); res.Error != nil {
			return res.Error
		}
		// 删除申请记录，后面还可以加
		if res := tx.Model(&model.ContactApply{}).Where("contact_id = ? AND user_id = ?", groupId, userId).Update("deleted_at", deletedAt); res.Error != nil {
			return res.Error
		}
		return nil
	})
	if err != nil {
		zlog.Error("退群事务失败: " + err.Error())
		return constants.SYSTEM_ERROR, -1
	}

	if err := myredis.DelKeysWithPattern("group_session_list_" + userId); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPattern("my_joined_group_list_" + userId); err != nil {
		zlog.Error(err.Error())
	}
	return "退群成功", 0
}

// DismissGroup 解散群聊
func (g *groupInfoService) DismissGroup(ownerId, groupId string) (string, int) {
	// 校验群主权限
	var groupCheck model.GroupInfo
	if res := dao.GormDB.First(&groupCheck, "uuid = ?", groupId); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	if groupCheck.OwnerId != ownerId {
		return "无权解散该群聊", -2
	}

	var deletedAt gorm.DeletedAt
	deletedAt.Time = time.Now()
	deletedAt.Valid = true
	// group_info 使用 0-based 软删除(bigint), 其他表仍用 NULL 语义 gorm.DeletedAt
	groupDeletedAt := soft_delete.DeletedAt(time.Now().UnixNano())
	if res := dao.GormDB.Model(&model.GroupInfo{}).Where("uuid = ?", groupId).Updates(
		map[string]interface{}{
			"deleted_at": groupDeletedAt,
			"updated_at": deletedAt.Time,
		}); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}

	// 软删群成员记录：解散后成员关系一并失效，避免 getActiveGroupMemberIDs 继续返回已解散群的成员
	if res := dao.GormDB.Model(&model.GroupMember{}).
		Where("group_id = ?", groupId).
		Updates(map[string]interface{}{
			"deleted_at": int64(groupDeletedAt),
			"updated_at": deletedAt.Time,
		}); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}

	var sessionList []model.Session
	if res := dao.GormDB.Model(&model.Session{}).Where("receive_id = ?", groupId).Find(&sessionList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, session := range sessionList {
		if res := dao.GormDB.Model(&session).Updates(
			map[string]interface{}{
				"deleted_at": deletedAt,
			}); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}

	var userContactList []model.UserContact
	if res := dao.GormDB.Model(&model.UserContact{}).Where("contact_id = ?", groupId).Find(&userContactList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}

	for _, userContact := range userContactList {
		if res := dao.GormDB.Model(&userContact).Update("deleted_at", deletedAt); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}

	var contactApplys []model.ContactApply
	if res := dao.GormDB.Model(&contactApplys).Where("contact_id = ?", groupId).Find(&contactApplys); res.Error != nil {
		if res.Error != gorm.ErrRecordNotFound {
			zlog.Info(res.Error.Error())
			return "无响应的申请记录需要删除", 0
		}
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, contactApply := range contactApplys {
		if res := dao.GormDB.Model(&contactApply).Update("deleted_at", deletedAt); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}
	//if err := myredis.DelKeysWithPattern("group_info_" + groupId); err != nil {
	//	zlog.Error(err.Error())
	//}
	//if err := myredis.DelKeysWithPattern("groupmember_list_" + groupId); err != nil {
	//	zlog.Error(err.Error())
	//}
	if err := myredis.DelKeysWithPattern("contact_mygroup_list_" + ownerId); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPattern("group_session_list_" + ownerId); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPrefix("my_joined_group_list"); err != nil {
		zlog.Error(err.Error())
	}
	return "解散群聊成功", 0
}

// DeleteGroups 删除列表中群聊 - 管理员
func (g *groupInfoService) DeleteGroups(uuidList []string) (string, int) {
	for _, uuid := range uuidList {
		var deletedAt gorm.DeletedAt
		deletedAt.Time = time.Now()
		deletedAt.Valid = true
		// group_info 使用 0-based 软删除(bigint), 其他表仍用 NULL 语义 gorm.DeletedAt
		groupDeletedAt := soft_delete.DeletedAt(time.Now().UnixNano())
		if res := dao.GormDB.Model(&model.GroupInfo{}).Where("uuid = ?", uuid).Update("deleted_at", groupDeletedAt); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		// 软删群成员记录：删群后成员关系一并失效
		if res := dao.GormDB.Model(&model.GroupMember{}).
			Where("group_id = ?", uuid).
			Updates(map[string]interface{}{
				"deleted_at": int64(groupDeletedAt),
				"updated_at": deletedAt.Time,
			}); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		// 删除会话
		var sessionList []model.Session
		if res := dao.GormDB.Model(&model.Session{}).Where("receive_id = ?", uuid).Find(&sessionList); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		for _, session := range sessionList {
			if res := dao.GormDB.Model(&session).Update("deleted_at", deletedAt); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
		// 删除联系人
		var userContactList []model.UserContact
		if res := dao.GormDB.Model(&model.UserContact{}).Where("contact_id = ?", uuid).Find(&userContactList); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}

		for _, userContact := range userContactList {
			if res := dao.GormDB.Model(&userContact).Update("deleted_at", deletedAt); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}

		var contactApplys []model.ContactApply
		if res := dao.GormDB.Model(&contactApplys).Where("contact_id = ?", uuid).Find(&contactApplys); res.Error != nil {
			if res.Error != gorm.ErrRecordNotFound {
				zlog.Info(res.Error.Error())
				return "无响应的申请记录需要删除", 0
			}
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		for _, contactApply := range contactApplys {
			if res := dao.GormDB.Model(&contactApply).Update("deleted_at", deletedAt); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
		}
	}
	//for _, uuid := range uuidList {
	//	if err := myredis.DelKeysWithPattern("group_info_" + uuid); err != nil {
	//		zlog.Error(err.Error())
	//	}
	//	if err := myredis.DelKeysWithPattern("groupmember_list_" + uuid); err != nil {
	//		zlog.Error(err.Error())
	//	}
	//}
	if err := myredis.DelKeysWithPrefix("contact_mygroup_list"); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPrefix("group_session_list"); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPrefix("group_session_list"); err != nil {
		zlog.Error(err.Error())
	}
	return "解散/删除群聊成功", 0
}

// CheckGroupAddMode 检查群聊加群方式
func (g *groupInfoService) CheckGroupAddMode(groupId string) (string, int8, int) {
	rspString, err := myredis.GetKeyNilIsErr("group_info_" + groupId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			var group model.GroupInfo
			if res := dao.GormDB.First(&group, "uuid = ?", groupId); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1, -1
			}
			return "加群方式获取成功", group.AddMode, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, -1, -1
		}
	}
	var rsp respond.GetGroupInfoRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "加群方式获取成功", rsp.AddMode, 0
}

// EnterGroupDirectly 直接进群
// ownerId 是群聊id，contactId 是进群的用户
func (g *groupInfoService) EnterGroupDirectly(ownerId, contactId string) (string, int) {
	var group model.GroupInfo
	if res := dao.GormDB.First(&group, "uuid = ?", ownerId); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	// 服务端校验 add_mode：直接进群仅允许 add_mode=0
	if group.AddMode != 0 {
		return "该群不允许直接加入，请通过审核方式申请", -2
	}

	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		// 幂等加成员：已存在则恢复，不存在则插入
		if err := GroupMemberService.addMember(tx, ownerId, contactId, group_member_role_enum.MEMBER); err != nil {
			return err
		}
		// 幂等处理联系人：已存在则恢复，不存在则插入
		var existingContact model.UserContact
		if err := tx.Unscoped().Where("user_id = ? AND contact_id = ?", contactId, ownerId).First(&existingContact).Error; err == nil {
			// 已存在（可能软删过），恢复
			if err := tx.Unscoped().Model(&existingContact).UpdateColumn("deleted_at", nil).Error; err != nil {
				return err
			}
			if err := tx.Model(&existingContact).Updates(map[string]interface{}{
				"status":    contact_status_enum.NORMAL,
				"update_at": time.Now(),
			}).Error; err != nil {
				return err
			}
		} else {
			newContact := model.UserContact{
				UserId:      contactId,
				ContactId:   ownerId,
				ContactType: contact_type_enum.GROUP,
				Status:      contact_status_enum.NORMAL,
				CreatedAt:   time.Now(),
				UpdateAt:    time.Now(),
			}
			if res := tx.Create(&newContact); res.Error != nil {
				return res.Error
			}
		}
		return nil
	})
	if err != nil {
		zlog.Error("进群事务失败: " + err.Error())
		if errors.Is(err, ErrMemberKicked) {
			return err.Error(), -2
		}
		return constants.SYSTEM_ERROR, -1
	}

	// 清理当前用户的缓存（contactId 是进群的用户，不是群 id）
	if err := myredis.DelKeysWithPattern("group_session_list_" + contactId); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPattern("my_joined_group_list_" + contactId); err != nil {
		zlog.Error(err.Error())
	}
	return "进群成功", 0
}

// SetGroupsStatus 设置群聊是否启用
func (g *groupInfoService) SetGroupsStatus(uuidList []string, status int8) (string, int) {
	var deletedAt gorm.DeletedAt
	deletedAt.Time = time.Now()
	deletedAt.Valid = true
	for _, uuid := range uuidList {
		if res := dao.GormDB.Model(&model.GroupInfo{}).Where("uuid = ?", uuid).Update("status", status); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
		if status == group_status_enum.DISABLE {
			var sessionList []model.Session
			if res := dao.GormDB.Model(&sessionList).Where("receive_id = ?", uuid).Find(&sessionList); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, -1
			}
			for _, session := range sessionList {
				if res := dao.GormDB.Model(&session).Update("deleted_at", deletedAt); res.Error != nil {
					zlog.Error(res.Error.Error())
					return constants.SYSTEM_ERROR, -1
				}
			}
		}
	}
	//for _, uuid := range uuidList {
	//	if err := myredis.DelKeysWithPattern("group_info_" + uuid); err != nil {
	//		zlog.Error(err.Error())
	//	}
	//}
	return "设置成功", 0
}

// UpdateGroupInfo 更新群聊消息
func (g *groupInfoService) UpdateGroupInfo(req request.UpdateGroupInfoRequest) (string, int) {
	var group model.GroupInfo
	if res := dao.GormDB.First(&group, "uuid = ?", req.Uuid); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	// 校验群主权限
	if group.OwnerId != req.OwnerId {
		return "无权修改该群聊", -2
	}
	if req.Name != "" {
		group.Name = req.Name
	}
	if req.AddMode != -1 {
		group.AddMode = req.AddMode
	}
	if req.Notice != "" {
		group.Notice = req.Notice
	}
	if req.Avatar != "" {
		group.Avatar = req.Avatar
	}
	if res := dao.GormDB.Save(&group); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	// 修改会话
	var sessionList []model.Session
	if res := dao.GormDB.Where("receive_id = ?", req.Uuid).Find(&sessionList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	for _, session := range sessionList {
		session.ReceiveName = group.Name
		session.Avatar = group.Avatar
		log.Println(session)
		if res := dao.GormDB.Save(&session); res.Error != nil {
			zlog.Error(res.Error.Error())
			return constants.SYSTEM_ERROR, -1
		}
	}

	//if err := myredis.DelKeysWithPattern("group_info_" + req.Uuid); err != nil {
	//	zlog.Error(err.Error())
	//}
	//if err := myredis.SetKeyEx("contact_mygroup_list_"+ req.OwnerId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
	//	zlog.Error(err.Error())
	//}
	return "更新成功", 0
}

// GetGroupMemberList 获取群聊成员列表
func (g *groupInfoService) GetGroupMemberList(groupId string) (string, []respond.GetGroupMemberListRespond, int) {
	rspString, err := myredis.GetKeyNilIsErr("group_memberlist_" + groupId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 校验群存在
			var group model.GroupInfo
			if res := dao.GormDB.First(&group, "uuid = ?", groupId); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			// 关联查询：group_member JOIN user_info，只返回正常状态成员
			var rspList []respond.GetGroupMemberListRespond
			if res := dao.GormDB.Table("group_member AS gm").
				Select("gm.user_id AS user_id, ui.nickname AS nickname, ui.avatar AS avatar, gm.role AS role").
				Joins("JOIN user_info AS ui ON ui.uuid = gm.user_id").
				Where("gm.group_id = ? AND gm.status = ? AND gm.deleted_at = 0", groupId, group_member_status_enum.NORMAL).
				Order("gm.role DESC, gm.joined_at ASC").
				Scan(&rspList); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			//rspString, err := json.Marshal(rspList)
			//if err != nil {
			//	zlog.Error(err.Error())
			//}
			//if err := myredis.SetKeyEx("group_memberlist_"+groupId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			//	zlog.Error(err.Error())
			//}
			return "获取群聊成员列表成功", rspList, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var rsp []respond.GetGroupMemberListRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取群聊成员列表成功", rsp, 0
}

// RemoveGroupMembers 移除群聊成员
// 使用数据库事务，保证中途失败时全部回滚
func (g *groupInfoService) RemoveGroupMembers(req request.RemoveGroupMembersRequest) (string, int) {
	var group model.GroupInfo
	if res := dao.GormDB.First(&group, "uuid = ?", req.GroupId); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	if group.OwnerId != req.OwnerId {
		return "无权移除该群成员", -2
	}

	for _, uuid := range req.UuidList {
		if req.OwnerId == uuid {
			return "不能移除群主", -2
		}
	}

	var deletedAt gorm.DeletedAt
	deletedAt.Time = time.Now()
	deletedAt.Valid = true
	nowNano := time.Now().UnixNano()
	now := time.Now()
	zlog.Info(fmt.Sprintf("移除群成员: group=%s, operator=%s, targets=%v", req.GroupId, req.OwnerId, req.UuidList))

	err := dao.GormDB.Transaction(func(tx *gorm.DB) error {
		for _, uuid := range req.UuidList {
			// 更新成员状态为被移出
			if res := tx.Model(&model.GroupMember{}).
				Where("group_id = ? AND user_id = ?", req.GroupId, uuid).
				Updates(map[string]interface{}{
					"status":     group_member_status_enum.KICKED,
					"deleted_at": nowNano,
					"updated_at": now,
				}); res.Error != nil {
				return res.Error
			}
			// 删除会话
			if res := tx.Model(&model.Session{}).Where("send_id = ? AND receive_id = ?", uuid, req.GroupId).Update("deleted_at", deletedAt); res.Error != nil {
				return res.Error
			}
			// 删除联系人
			if res := tx.Model(&model.UserContact{}).Where("user_id = ? AND contact_id = ?", uuid, req.GroupId).Update("deleted_at", deletedAt); res.Error != nil {
				return res.Error
			}
			// 删除申请记录
			if res := tx.Model(&model.ContactApply{}).Where("user_id = ? AND contact_id = ?", uuid, req.GroupId).Update("deleted_at", deletedAt); res.Error != nil {
				return res.Error
			}
		}
		return nil
	})
	if err != nil {
		zlog.Error("移除群成员事务失败: " + err.Error())
		return constants.SYSTEM_ERROR, -1
	}

	if err := myredis.DelKeysWithPrefix("group_session_list"); err != nil {
		zlog.Error(err.Error())
	}
	if err := myredis.DelKeysWithPrefix("my_joined_group_list"); err != nil {
		zlog.Error(err.Error())
	}
	return "移除群聊成员成功", 0
}
