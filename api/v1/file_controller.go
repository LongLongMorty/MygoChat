package v1

import (
	"github.com/gin-gonic/gin"
	"kama_chat_server/internal/config"
	"kama_chat_server/internal/dao"
	"kama_chat_server/internal/model"
	"kama_chat_server/pkg/zlog"
	"net/http"
	"path/filepath"
	"strings"
)

// DownloadFile 鉴权下载端点
// P1 修复：JWT 认证 + 附件归属校验（发送者/私聊接收者/群成员）
// 请求：GET /file/download?name=<server_filename>
func DownloadFile(c *gin.Context) {
	filename := c.Query("name")
	if filename == "" {
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "缺少文件名参数",
		})
		return
	}

	// 防止路径穿越攻击：先检查原始输入，再取 Base
	if strings.Contains(filename, "..") || strings.Contains(filename, "/") || strings.Contains(filename, "\\") {
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "非法文件名",
		})
		return
	}
	filename = filepath.Base(filename)

	// P1 修复：附件归属校验
	requesterUuid := c.GetString("uuid")

	// P2 修复：DB 未初始化时返回系统错误，不 panic
	if dao.GormDB == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    -2,
			"message": "服务暂不可用",
		})
		return
	}

	var fileMeta model.FileMetadata
	result := dao.GormDB.Where("server_name = ?", filename).First(&fileMeta)
	if result.Error != nil {
		zlog.Error("文件元数据不存在: " + filename + " by " + requesterUuid)
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "文件不存在或无权访问",
		})
		return
	}

	// 授权检查：上传者可直接下载
	if fileMeta.UploaderUuid == requesterUuid {
		serveFile(c, filename, requesterUuid)
		return
	}

	// 授权检查：查 message 表，验证 requester 是否为该文件的发送方或接收方
	// url 字段包含文件名，匹配 server_name
	var msg model.Message
	msgResult := dao.GormDB.Where("url LIKE ? AND (send_id = ? OR receive_id = ?)",
		"%"+filename+"%", requesterUuid, requesterUuid).First(&msg)
	if msgResult.Error == nil {
		// 找到包含此文件的消息，且 requester 是发送方或接收方
		serveFile(c, filename, requesterUuid)
		return
	}

	// 授权检查：群成员可下载（receive_id 以 G 开头时，查 group_member 表）
	// 查找包含此文件的群消息
	var groupMsg model.Message
	groupResult := dao.GormDB.Where("url LIKE ? AND receive_id LIKE 'G%'",
		"%"+filename+"%").First(&groupMsg)
	if groupResult.Error == nil {
		// 检查 requester 是否是该群的成员
		var count int64
		dao.GormDB.Table("user_contact").Where("user_id = ? AND contact_id = ? AND deleted_at IS NULL",
			requesterUuid, groupMsg.ReceiveId).Count(&count)
		if count > 0 {
			serveFile(c, filename, requesterUuid)
			return
		}
	}

	zlog.Error("归属校验失败: file=" + filename + " requester=" + requesterUuid + " uploader=" + fileMeta.UploaderUuid)
	c.JSON(http.StatusOK, gin.H{
		"code":    403,
		"message": "无权下载此文件",
	})
}

// serveFile 提供文件下载
func serveFile(c *gin.Context, filename string, requesterUuid string) {
	filePath := filepath.Join(config.GetConfig().StaticFilePath, filename)
	zlog.Info("文件下载: " + filename + " by user " + requesterUuid)
	c.File(filePath)
}
