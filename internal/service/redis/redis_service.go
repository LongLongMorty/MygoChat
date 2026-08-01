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
