package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"kama_chat_server/internal/config"
	"kama_chat_server/pkg/zlog"
	"strconv"
	"time"
)

var redisClient *redis.Client
var ctx = context.Background()

func init() {
	conf := config.GetConfig()
	host := conf.RedisConfig.Host
	port := conf.RedisConfig.Port
	password := conf.RedisConfig.Password
	db := conf.Db
	addr := host + ":" + strconv.Itoa(port)

	redisClient = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

func SetKeyEx(key string, value string, timeout time.Duration) error {
	err := redisClient.Set(ctx, key, value, timeout).Err()
	if err != nil {
		return err
	}
	return nil
}

// Incr 原子自增 key 的整数值（key 不存在时从 0 自增到 1），用于固定窗口限流计数。
func Incr(key string) (int64, error) {
	return redisClient.Incr(ctx, key).Result()
}

// SetNX 原子设置 key（仅当 key 不存在时），返回是否设置成功。
// 对应 Redis 的 SET key value NX EX timeout，用于防并发抢占场景。
func SetNX(key string, value string, timeout time.Duration) (bool, error) {
	return redisClient.SetNX(ctx, key, value, timeout).Result()
}

// SAdd 向集合批量添加成员（覆盖 Set 数据结构的写扩散场景）
func SAdd(key string, members []string) error {
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return redisClient.SAdd(ctx, key, args...).Err()
}

// SRem 从集合移除成员
func SRem(key string, members []string) error {
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return redisClient.SRem(ctx, key, args...).Err()
}

// SMembers 获取集合全部成员（key 不存在时返回空 slice 且无错误）
func SMembers(key string) ([]string, error) {
	return redisClient.SMembers(ctx, key).Result()
}

// Expire 设置 key 过期时间（用于 Set 等结构的 TTL 兜底）
func Expire(key string, timeout time.Duration) error {
	return redisClient.Expire(ctx, key, timeout).Err()
}

func GetKey(key string) (string, error) {
	value, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			zlog.Info("该key不存在")
			return "", nil
		}
		return "", err
	}
	return value, nil
}

func GetKeyNilIsErr(key string) (string, error) {
	value, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return value, nil
}

// ScanKeys 返回所有匹配 prefix* 的完整 key（SCAN 迭代，避免 KEYS 阻塞 Redis）。
func ScanKeys(prefix string) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		ks, c, err := redisClient.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, ks...)
		cursor = c
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func GetKeyWithPrefixNilIsErr(prefix string) (string, error) {
	// P2-1 修复：使用 SCAN 替代 KEYS，避免阻塞 Redis
	var keys []string
	var cursor uint64
	for {
		ks, c, err := redisClient.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return "", err
		}
		keys = append(keys, ks...)
		cursor = c
		if cursor == 0 {
			break
		}
	}

	if len(keys) == 0 {
		zlog.Info("没有找到相关前缀key")
		return "", redis.Nil
	}

	if len(keys) == 1 {
		zlog.Info(fmt.Sprintln("成功找到了相关前缀key", keys))
		return keys[0], nil
	}
	zlog.Error("找到了数量大于1的key，查找异常")
	return "", errors.New("找到了数量大于1的key，查找异常")
}

func GetKeyWithSuffixNilIsErr(suffix string) (string, error) {
	// P2-1 修复：使用 SCAN 替代 KEYS
	var keys []string
	var cursor uint64
	for {
		ks, c, err := redisClient.Scan(ctx, cursor, "*"+suffix, 100).Result()
		if err != nil {
			return "", err
		}
		keys = append(keys, ks...)
		cursor = c
		if cursor == 0 {
			break
		}
	}

	if len(keys) == 0 {
		zlog.Info("没有找到相关后缀key")
		return "", redis.Nil
	}

	if len(keys) == 1 {
		zlog.Info(fmt.Sprintln("成功找到了相关后缀key", keys))
		return keys[0], nil
	}
	zlog.Error("找到了数量大于1的key，查找异常")
	return "", errors.New("找到了数量大于1的key，查找异常")
}

func DelKeyIfExists(key string) error {
	exists, err := redisClient.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 1 { // 键存在
		delErr := redisClient.Del(ctx, key).Err()
		if delErr != nil {
			return delErr
		}
	}
	// 无论键是否存在，都不返回错误
	return nil
}

func DelKeysWithPattern(pattern string) error {
	// P2-1 修复：使用 SCAN 替代 KEYS
	var cursor uint64
	for {
		keys, c, err := redisClient.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			_, err = redisClient.Del(ctx, keys...).Result()
			if err != nil {
				return err
			}
			zlog.Info(fmt.Sprintf("成功删除匹配 %s 的key: %v", pattern, keys))
		}
		cursor = c
		if cursor == 0 {
			break
		}
	}
	return nil
}

func DelKeysWithPrefix(prefix string) error {
	// P2-1 修复：使用 SCAN 替代 KEYS
	var cursor uint64
	for {
		keys, c, err := redisClient.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			_, err = redisClient.Del(ctx, keys...).Result()
			if err != nil {
				return err
			}
			zlog.Info(fmt.Sprintf("成功删除前缀 %s 的key: %v", prefix, keys))
		}
		cursor = c
		if cursor == 0 {
			break
		}
	}
	return nil
}

func DelKeysWithSuffix(suffix string) error {
	// P2-1 修复：使用 SCAN 替代 KEYS
	var cursor uint64
	for {
		keys, c, err := redisClient.Scan(ctx, cursor, "*"+suffix, 100).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			_, err = redisClient.Del(ctx, keys...).Result()
			if err != nil {
				return err
			}
			zlog.Info(fmt.Sprintf("成功删除后缀 %s 的key: %v", suffix, keys))
		}
		cursor = c
		if cursor == 0 {
			break
		}
	}
	return nil
}

// MGet 批量读取多个 key（对应 Redis MGET）。
// 返回切片与 keys 等长，key 不存在时对应元素为空字符串。
// 用于分布式在线路由表的一次性批量定位（避免逐条 GET）。
func MGet(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	result := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		if s, ok := v.(string); ok {
			result[i] = s
		}
	}
	return result, nil
}

// --- Redis Stream 操作（分布式实例间投递总线）---

// XGroupCreateMkStream 创建消费组（stream 不存在则自动创建）。
// BUSYGROUP 表示组已存在，调用方应忽略。
func XGroupCreateMkStream(stream, group string) error {
	return redisClient.XGroupCreateMkStream(ctx, stream, group, "$").Err()
}

// XAddMessage 向 stream 追加一条消息，使用 MAXLEN ~ maxLen 近似裁剪防止无限增长。
func XAddMessage(stream string, key string, value string, maxLen int64) error {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{key: value},
	}
	if maxLen > 0 {
		args.MaxLen = maxLen
		args.Approx = true
	}
	return redisClient.XAdd(ctx, args).Err()
}

// XReadGroupMessages 以消费组身份读取 stream。
// streamID 用 ">" 读取从未投递的新消息；用 "0" 读取本消费者已投递但未 ack 的 pending。
// block < 0 时不下发 BLOCK 选项（非阻塞）；无消息时返回空切片且无错误。
func XReadGroupMessages(stream, group, consumer, streamID string, count int64, block time.Duration) ([]redis.XMessage, error) {
	res, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, streamID},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

// XAck 确认（删除）消费组 pending 中的消息。
func XAck(stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return redisClient.XAck(ctx, stream, group, ids...).Err()
}

func DeleteAllRedisKeys() error {
	var cursor uint64 = 0
	for {
		keys, nextCursor, err := redisClient.Scan(ctx, cursor, "*", 0).Result()
		if err != nil {
			return err
		}
		cursor = nextCursor

		if len(keys) > 0 {
			_, err := redisClient.Del(ctx, keys...).Result()
			if err != nil {
				return err
			}
		}

		if cursor == 0 {
			break
		}
	}
	return nil
}
