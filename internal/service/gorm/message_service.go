package gorm

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
	"io"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/dto/respond"
	"kama_chat_server/internal/model"
	myredis "kama_chat_server/internal/service/redis"
	"kama_chat_server/pkg/constants"
	"kama_chat_server/pkg/util/random"
	"kama_chat_server/pkg/zlog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// P0-4: 文件上传安全配置
var allowedImageTypes = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
	"image/gif":  true,
	"image/webp": true,
}

var allowedFileTypes = map[string]bool{
	"image/jpeg":       true,
	"image/png":        true,
	"image/gif":        true,
	"image/webp":       true,
	"video/mp4":        true,
	"audio/mpeg":       true,
	"audio/wav":        true,
	"application/pdf":  true,
	"text/plain":       true,
	"application/zip":  true,
	"application/x-zip-compressed": true,
}

// maxAvatarSize 头像大小上限 2MB
const maxAvatarSize = 2 * 1024 * 1024

// maxFileSize 文件大小上限 50MB
const maxFileSize = 50 * 1024 * 1024

type messageService struct {
}

var MessageService = new(messageService)

// defaultPageLimit 历史消息分页默认/最大条数
const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

// normalizePageLimit 规范化分页参数：非法值回退默认，超上限截断
func normalizePageLimit(limit int) int {
	if limit <= 0 {
		return defaultPageLimit
	}
	if limit > maxPageLimit {
		return maxPageLimit
	}
	return limit
}

// GetMessageList 获取聊天记录（游标分页）
// limit 默认 50 最大 100；beforeId>0 时返回 id<beforeId 的最近 limit 条（上翻页）
func (m *messageService) GetMessageList(userOneId, userTwoId string, limit int, beforeId int64) (string, []respond.GetMessageListRespond, int) {
	limit = normalizePageLimit(limit)
	// 仅第一页（无游标）走缓存；翻页查询实时查库
	cacheKey := "message_list_" + userOneId + "_" + userTwoId
	if beforeId <= 0 {
		rspString, err := myredis.GetKeyNilIsErr(cacheKey)
		if err == nil {
			var rsp []respond.GetMessageListRespond
			if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
				zlog.Error(err.Error())
			}
			return "获取聊天记录成功", rsp, 0
		} else if !errors.Is(err, redis.Nil) {
			zlog.Error(err.Error())
		}
	}

	// 双向查询：两侧各自带游标条件（分组写法避免 GORM Or 链优先级问题），
	// 每侧都能命中 (send_id, receive_id) 复合索引
	var messageList []model.Message
	var query *gorm.DB
	if beforeId > 0 {
		query = dao.GormDB.Where(
			"(send_id = ? AND receive_id = ? AND id < ?) OR (send_id = ? AND receive_id = ? AND id < ?)",
			userOneId, userTwoId, beforeId, userTwoId, userOneId, beforeId,
		)
	} else {
		query = dao.GormDB.Where(
			"(send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?)",
			userOneId, userTwoId, userTwoId, userOneId,
		)
	}
	if res := query.Order("created_at DESC, id DESC").Limit(limit).Find(&messageList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}

	var rspList []respond.GetMessageListRespond
	for _, message := range messageList {
		rspList = append(rspList, respond.GetMessageListRespond{
			Id:            message.Id,
			SendId:        message.SendId,
			SendName:      message.SendName,
			SendAvatar:    message.SendAvatar,
			ReceiveId:     message.ReceiveId,
			Content:       message.Content,
			Url:           message.Url,
			Type:          message.Type,
			FileType:      message.FileType,
			FileName:      message.FileName,
			FileSize:      message.FileSize,
			FileSizeBytes: message.FileSizeBytes,
			ReadStatus:    message.ReadStatus,
			CreatedAt:     message.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}
	// 倒序查询后反转回正序
	for i, j := 0, len(rspList)-1; i < j; i, j = i+1, j-1 {
		rspList[i], rspList[j] = rspList[j], rspList[i]
	}

	// 拉取历史即视为已读：把"对方发给我"的消息批量置已读
	if beforeId <= 0 {
		now := time.Now()
		dao.GormDB.Model(&model.Message{}).
			Where("send_id = ? AND receive_id = ? AND read_status = 0", userTwoId, userOneId).
			Updates(map[string]interface{}{
				"read_status": 1,
				"read_at":     now,
			})
		// 同步刷新缓存中的已读状态
		for i := range rspList {
			if rspList[i].SendId == userTwoId {
				rspList[i].ReadStatus = 1
			}
		}
		// 第一页写缓存（TTL 1 分钟，发送消息时主动失效）
		rspByte, err := json.Marshal(rspList)
		if err == nil {
			if err := myredis.SetKeyEx(cacheKey, string(rspByte), time.Minute*constants.REDIS_TIMEOUT); err != nil {
				zlog.Error(err.Error())
			}
		}
	}
	return "获取聊天记录成功", rspList, 0
}

// GetGroupMessageList 获取群聊消息记录（游标分页）
// limit 默认 50 最大 100；beforeId>0 时返回 id<beforeId 的最近 limit 条（上翻页）
func (m *messageService) GetGroupMessageList(groupId string, limit int, beforeId int64) (string, []respond.GetGroupMessageListRespond, int) {
	limit = normalizePageLimit(limit)
	// 仅第一页（无游标）走缓存
	cacheKey := "group_messagelist_" + groupId
	if beforeId <= 0 {
		rspString, err := myredis.GetKeyNilIsErr(cacheKey)
		if err == nil {
			var rsp []respond.GetGroupMessageListRespond
			if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
				zlog.Error(err.Error())
			}
			return "获取聊天记录成功", rsp, 0
		} else if !errors.Is(err, redis.Nil) {
			zlog.Error(err.Error())
		}
	}

	var messageList []model.Message
	query := dao.GormDB.Where("receive_id = ?", groupId).Order("created_at DESC, id DESC")
	if beforeId > 0 {
		query = query.Where("id < ?", beforeId)
	}
	if res := query.Limit(limit).Find(&messageList); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, nil, -1
	}

	var rspList []respond.GetGroupMessageListRespond
	for _, message := range messageList {
		rspList = append(rspList, respond.GetGroupMessageListRespond{
			Id:            message.Id,
			SendId:        message.SendId,
			SendName:      message.SendName,
			SendAvatar:    message.SendAvatar,
			ReceiveId:     message.ReceiveId,
			Content:       message.Content,
			Url:           message.Url,
			Type:          message.Type,
			FileType:      message.FileType,
			FileName:      message.FileName,
			FileSize:      message.FileSize,
			FileSizeBytes: message.FileSizeBytes,
			CreatedAt:     message.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}
	// 倒序查询后反转回正序
	for i, j := 0, len(rspList)-1; i < j; i, j = i+1, j-1 {
		rspList[i], rspList[j] = rspList[j], rspList[i]
	}

	// 第一页写缓存
	if beforeId <= 0 {
		rspByte, err := json.Marshal(rspList)
		if err == nil {
			if err := myredis.SetKeyEx(cacheKey, string(rspByte), time.Minute*constants.REDIS_TIMEOUT); err != nil {
				zlog.Error(err.Error())
			}
		}
	}
	return "获取聊天记录成功", rspList, 0
}

// UploadFileResponse 上传响应（P1 修复：返回服务端文件名和下载 URL）
type UploadFileResponse struct {
	Name string `json:"name"` // 服务端生成的文件名
	URL  string `json:"url"`  // 下载 URL（/file/download?name=xxx）
}

// UploadAvatar 上传头像
// P0-4 修复：限制大小 2MB、校验 MIME、随机文件名防覆盖
// P1 修复：返回文件名和 URL，保存元数据
func (m *messageService) UploadAvatar(c *gin.Context) (string, interface{}, int) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxAvatarSize+1024)
	if err := c.Request.ParseMultipartForm(maxAvatarSize); err != nil {
		zlog.Error("头像上传失败（大小超限或解析错误）: " + err.Error())
		return "文件大小超过限制（最大2MB）", nil, -2
	}
	mForm := c.Request.MultipartForm
	for key, _ := range mForm.File {
		file, fileHeader, err := c.Request.FormFile(key)
		if err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		defer file.Close()

		if fileHeader.Size > maxAvatarSize {
			return "头像大小超过限制（最大2MB）", nil, -2
		}
		contentType := fileHeader.Header.Get("Content-Type")
		if !allowedImageTypes[contentType] {
			return "头像格式不允许，仅支持 jpeg/png/gif/webp", nil, -2
		}
		head := make([]byte, 512)
		n, _ := io.ReadFull(file, head)
		detectedType := http.DetectContentType(head[:n])
		if !allowedImageTypes[detectedType] {
			return "文件内容与声明类型不符，拒绝上传", nil, -2
		}
		file.Seek(0, io.SeekStart)

		ext := filepath.Ext(fileHeader.Filename)
		randomName := fmt.Sprintf("avatar_%d%s", time.Now().UnixNano(), ext)
		localFileName := config.GetConfig().StaticAvatarPath + "/" + randomName
		out, err := os.Create(localFileName)
		if err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		defer out.Close()
		if _, err := io.Copy(out, file); err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		zlog.Info("完成头像上传: " + randomName)

		// P1 修复：保存文件元数据 + 返回文件名
		uploaderUuid := c.GetString("uuid")
		fileMeta := &model.FileMetadata{
			Uuid:         "F" + random.GetNowAndLenRandomString(11),
			UploaderUuid: uploaderUuid,
			ServerName:   randomName,
			OriginalName: fileHeader.Filename,
			FileType:     detectedType,
			FileSize:     fileHeader.Size,
			IsAvatar:     1,
			CreatedAt:    time.Now(),
		}
		// P2 修复：元数据写入失败时删除已落盘文件
		if err := dao.GormDB.Create(fileMeta).Error; err != nil {
			zlog.Error("头像元数据写入失败，删除已落盘文件: " + err.Error())
			os.Remove(localFileName)
			return "头像上传失败（元数据错误）", nil, -1
		}

		// 头像走公开静态路由 /static/avatars/<name>
		rsp := &UploadFileResponse{
			Name: randomName,
			URL:  "/static/avatars/" + randomName,
		}
		return "上传成功", rsp, 0
	}
	return "上传成功", nil, 0
}

// UploadFile 上传文件
// P0-4 修复：限制大小 50MB、校验 MIME、随机文件名防覆盖
// P1 修复：返回文件名和鉴权下载 URL，保存元数据
func (m *messageService) UploadFile(c *gin.Context) (string, interface{}, int) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxFileSize+1024)
	if err := c.Request.ParseMultipartForm(maxFileSize); err != nil {
		zlog.Error("文件上传失败（大小超限或解析错误）: " + err.Error())
		return "文件大小超过限制（最大50MB）", nil, -2
	}
	mForm := c.Request.MultipartForm
	for key, _ := range mForm.File {
		file, fileHeader, err := c.Request.FormFile(key)
		if err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		defer file.Close()

		if fileHeader.Size > maxFileSize {
			return "文件大小超过限制（最大50MB）", nil, -2
		}
		contentType := fileHeader.Header.Get("Content-Type")
		if !allowedFileTypes[contentType] {
			return "文件类型不允许: " + contentType, nil, -2
		}
		head := make([]byte, 512)
		n, _ := io.ReadFull(file, head)
		detectedType := http.DetectContentType(head[:n])
		if !allowedFileTypes[detectedType] {
			return "文件内容与声明类型不符，拒绝上传", nil, -2
		}
		file.Seek(0, io.SeekStart)

		ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
		if ext == ".html" || ext == ".htm" || ext == ".svg" || ext == ".js" || ext == ".exe" || ext == ".bat" || ext == ".sh" {
			return "不允许上传可执行内容", nil, -2
		}

		randomName := fmt.Sprintf("file_%d%s", time.Now().UnixNano(), ext)
		localFileName := config.GetConfig().StaticFilePath + "/" + randomName
		out, err := os.Create(localFileName)
		if err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		defer out.Close()
		if _, err := io.Copy(out, file); err != nil {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
		zlog.Info("完成文件上传: " + randomName)

		// P1 修复：保存文件元数据 + 返回鉴权下载 URL
		// P2 修复：元数据写入失败时删除已落盘文件，防止孤儿文件
		uploaderUuid := c.GetString("uuid")
		fileMeta := &model.FileMetadata{
			Uuid:         "F" + random.GetNowAndLenRandomString(11),
			UploaderUuid: uploaderUuid,
			ServerName:   randomName,
			OriginalName: fileHeader.Filename,
			FileType:     detectedType,
			FileSize:     fileHeader.Size,
			IsAvatar:     0,
			CreatedAt:    time.Now(),
		}
		if err := dao.GormDB.Create(fileMeta).Error; err != nil {
			zlog.Error("文件元数据写入失败，删除已落盘文件: " + err.Error())
			os.Remove(localFileName)
			return "文件上传失败（元数据错误）", nil, -1
		}

		rsp := &UploadFileResponse{
			Name: randomName,
			URL:  "/file/download?name=" + randomName,
		}
		return "上传成功", rsp, 0
	}
	return "上传成功", nil, 0
}

// RevokeMessage 撤回消息（仅发送者本人，软删除）
func (m *messageService) RevokeMessage(ownerId, messageUuid string) (string, int) {
	var message model.Message
	if res := dao.GormDB.Where("uuid = ?", messageUuid).First(&message); res.Error != nil {
		zlog.Error(res.Error.Error())
		return "消息不存在", -2
	}
	if message.SendId != ownerId {
		return "无权撤回他人消息", -2
	}
	// 软删除：历史查询自动过滤
	if res := dao.GormDB.Delete(&message); res.Error != nil {
		zlog.Error(res.Error.Error())
		return constants.SYSTEM_ERROR, -1
	}
	// 失效相关历史缓存
	if message.ReceiveId[0] == 'U' {
		_ = myredis.DelKeysWithPattern("message_list_" + message.SendId + "_" + message.ReceiveId)
		_ = myredis.DelKeysWithPattern("message_list_" + message.ReceiveId + "_" + message.SendId)
	} else {
		_ = myredis.DelKeysWithPattern("group_messagelist_" + message.ReceiveId)
	}
	return "撤回成功", 0
}
