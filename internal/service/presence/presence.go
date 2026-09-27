// Package presence 维护"用户当前所在实例"的在线路由表，供分布式投递定位使用。
//
// Redis key: presence:user:<uuid>  ->  instanceID（TTL 兜底，心跳续期）
package presence

import (
	"strings"
	"time"

	myredis "kama_chat_server/internal/service/redis"
)

const (
	userKeyPrefix     = "presence:user:"
	instanceKeyPrefix = "chat:instance:"
)

// Service 在线状态服务：值语义无状态，所有数据在 Redis。
type Service struct {
	instanceID string
	ttl        time.Duration
}

// New 创建在线状态服务。instanceID 为本实例标识，ttl 为路由记录存活时间。
func New(instanceID string, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &Service{instanceID: instanceID, ttl: ttl}
}

// InstanceID 返回本实例标识。
func (s *Service) InstanceID() string {
	return s.instanceID
}

func userKey(userID string) string {
	return userKeyPrefix + userID
}

// Register 登记/续期用户所在实例（登录与心跳调用）。
func (s *Service) Register(userID string) error {
	if userID == "" {
		return nil
	}
	return myredis.SetKeyEx(userKey(userID), s.instanceID, s.ttl)
}

// Unregister 注销用户的在线路由（登出、断开调用）。
func (s *Service) Unregister(userID string) error {
	if userID == "" {
		return nil
	}
	return myredis.DelKeyIfExists(userKey(userID))
}

// Lookup 查询用户当前所在实例，未在线时 ok=false。
func (s *Service) Lookup(userID string) (string, bool) {
	if userID == "" {
		return "", false
	}
	value, err := myredis.GetKeyNilIsErr(userKey(userID))
	if err != nil || value == "" {
		return "", false
	}
	return value, true
}

// RegisterInstance 登记/续期本实例（供一致性哈希环发现存活实例）。
func (s *Service) RegisterInstance() error {
	return myredis.SetKeyEx(instanceKeyPrefix+s.instanceID, time.Now().Format(time.RFC3339Nano), s.ttl)
}

// LiveInstances 返回当前存活实例 ID 列表（SCAN chat:instance:*，去掉前缀）。
func (s *Service) LiveInstances() ([]string, error) {
	keys, err := myredis.ScanKeys(instanceKeyPrefix)
	if err != nil {
		return nil, err
	}
	instances := make([]string, 0, len(keys))
	for _, k := range keys {
		instances = append(instances, strings.TrimPrefix(k, instanceKeyPrefix))
	}
	return instances, nil
}

// LookupMany 批量查询用户所在实例（一次 MGET），返回 uuid -> instanceID。
// 未在线的用户不会出现在结果中；Redis 故障返回错误，调用方按"全部离线"降级。
func (s *Service) LookupMany(userIDs []string) (map[string]string, error) {
	result := make(map[string]string, len(userIDs))
	if len(userIDs) == 0 {
		return result, nil
	}
	keys := make([]string, len(userIDs))
	for i, id := range userIDs {
		keys[i] = userKey(id)
	}
	values, err := myredis.MGet(keys)
	if err != nil {
		return result, err
	}
	for i, v := range values {
		if v != "" {
			result[userIDs[i]] = v
		}
	}
	return result, nil
}
