package limiter

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"golang.org/x/time/rate"
	"time"
)

// KeySecondLimiter 按key秒级限流的工具
type KeySecondLimiter struct {
	limitCache cache.CommCache[*rate.Limiter]
	cacheTime  time.Duration

	rate  rate.Limit // 每秒生成多少令牌
	burst int        // 最大突发量
}

// NewKeySecondLimiter 创建一个key限流器
// r: 每秒允许多少请求
// burst: 最大允许突发多少
func NewKeySecondLimiter(cacheTime time.Duration, r rate.Limit, burst int) *KeySecondLimiter {
	if cacheTime < 10*time.Second {
		cacheTime = 5 * time.Minute
	}
	return &KeySecondLimiter{
		cacheTime:  cacheTime,
		limitCache: cache.NewMemGoCache[*rate.Limiter](cacheTime, 10*time.Minute),
		rate:       r,
		burst:      burst,
	}
}

// getLimiter 获取或创建一个key的限流器
func (kl *KeySecondLimiter) getLimiter(ctx context.Context, key string) *rate.Limiter {
	limiter, err := kl.limitCache.Get(ctx, key)
	if err == nil && limiter != nil {
		return limiter
	}
	limiter = rate.NewLimiter(kl.rate, kl.burst)
	setTrue, err := kl.limitCache.Set(ctx, key, limiter, kl.cacheTime)
	if err == nil && setTrue {
		return limiter
	}
	return nil
}

// Allow 判断这个key是否允许通过（非阻塞）
func (kl *KeySecondLimiter) Allow(ctx context.Context, key string) bool {
	limiter := kl.getLimiter(ctx, key)
	if limiter == nil {
		return true
	}
	return limiter.Allow()
}

// Wait 阻塞等待直到获取令牌（带ctx超时）
func (kl *KeySecondLimiter) Wait(ctx context.Context, key string) error {
	limiter := kl.getLimiter(ctx, key)
	if limiter == nil {
		return fmt.Errorf("%s", "no access")
	}
	return limiter.Wait(ctx)
}
