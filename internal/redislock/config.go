package redislock

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	redisv9 "github.com/redis/go-redis/v9"
	"time"
)

const (
	defaultExpireTime = 10 * time.Second // 默认过期时间10s
	defaultRetryDelay = 500 * time.Millisecond
	defaultRetryTries = 3
	defaultKeyFront   = "{redis-lock}"
)

func getLockerKeyName(key string) string {
	return fmt.Sprintf("%s%s", defaultKeyFront, key)
}

func RedisClientV8(redisClient *redis.Client) (*redis.Client, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	return redisClient, nil
}
func RedisClientV9(redisClient *redisv9.Client) (*redisv9.Client, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return redisClient, nil
}
