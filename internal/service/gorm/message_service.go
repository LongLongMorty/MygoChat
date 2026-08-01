package gorm

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
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

// GetMessageList 获取聊天记录
func (m *messageService) GetMessageList(userOneId, userTwoId string) (string, []respond.GetMessageListRespond, int) {
	rspString, err := myredis.GetKeyNilIsErr("message_list_" + userOneId + "_" + userTwoId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			zlog.Info(err.Error())
			zlog.Info(fmt.Sprintf("%s %s", userTwoId, userTwoId))
			var messageList []model.Message
			if res := dao.GormDB.Where("(send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?)", userOneId, userTwoId, userTwoId, userOneId).Order("created_at ASC").Find(&messageList); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			var rspList []respond.GetMessageListRespond
			for _, message := range messageList {
				rspList = append(rspList, respond.GetMessageListRespond{
					SendId:     message.SendId,
					SendName:   message.SendName,
					SendAvatar: message.SendAvatar,
					ReceiveId:  message.ReceiveId,
					Content:    message.Content,
					Url:        message.Url,
					Type:       message.Type,
					FileType:   message.FileType,
					FileName:   message.FileName,
					FileSize:   message.FileSize,
					CreatedAt:  message.CreatedAt.Format("2006-01-02 15:04:05"),
				})
			}
			//rspString, err := json.Marshal(rspList)
			//if err != nil {
			//	zlog.Error(err.Error())
			//}
			//if err := myredis.SetKeyEx("message_list_"+userOneId+"_"+userTwoId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			//	zlog.Error(err.Error())
			//}
			return "获取聊天记录成功", rspList, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var rsp []respond.GetMessageListRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取群聊记录成功", rsp, 0
}

// GetGroupMessageList 获取群聊消息记录
func (m *messageService) GetGroupMessageList(groupId string) (string, []respond.GetGroupMessageListRespond, int) {
	rspString, err := myredis.GetKeyNilIsErr("group_messagelist_" + groupId)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			var messageList []model.Message
			if res := dao.GormDB.Where("receive_id = ?", groupId).Order("created_at ASC").Find(&messageList); res.Error != nil {
				zlog.Error(res.Error.Error())
				return constants.SYSTEM_ERROR, nil, -1
			}
			var rspList []respond.GetGroupMessageListRespond
			for _, message := range messageList {
				rsp := respond.GetGroupMessageListRespond{
					SendId:     message.SendId,
					SendName:   message.SendName,
					SendAvatar: message.SendAvatar,
					ReceiveId:  message.ReceiveId,
					Content:    message.Content,
					Url:        message.Url,
					Type:       message.Type,
					FileType:   message.FileType,
					FileName:   message.FileName,
					FileSize:   message.FileSize,
					CreatedAt:  message.CreatedAt.Format("2006-01-02 15:04:05"),
				}
				rspList = append(rspList, rsp)
			}
			//rspString, err := json.Marshal(rspList)
			//if err != nil {
			//	zlog.Error(err.Error())
			//}
			//if err := myredis.SetKeyEx("group_messagelist_"+groupId, string(rspString), time.Minute*constants.REDIS_TIMEOUT); err != nil {
			//	zlog.Error(err.Error())
			//}
			return "获取聊天记录成功", rspList, 0
		} else {
			zlog.Error(err.Error())
			return constants.SYSTEM_ERROR, nil, -1
		}
	}
	var rsp []respond.GetGroupMessageListRespond
	if err := json.Unmarshal([]byte(rspString), &rsp); err != nil {
		zlog.Error(err.Error())
	}
	return "获取聊天记录成功", rsp, 0
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
