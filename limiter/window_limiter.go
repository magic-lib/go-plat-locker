package limiter

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-cache/cache"
	"sync"
	"time"
)

// WindowLimiter 基于滑动窗口的限流器（严格的时间窗口限制）
type WindowLimiter struct {
	cacheTime   time.Duration
	windowCache cache.CommCache[*windowCounter]
	maxRequests int
	period      time.Duration
}

type windowCounter struct {
	mu         sync.Mutex
	timestamps []time.Time
}

// NewWindowLimiter 创建一个严格的时间窗口限流器
// cacheTime: 限流器缓存时间（建议设置为 period 的2倍以上）
// maxRequests: 时间窗口内允许的最大请求数
// period: 时间窗口长度
// 示例：NewWindowLimiter(10*time.Minute, 5, 2*time.Minute) 表示任意2分钟内最多5次请求
func NewWindowLimiter(cacheTime time.Duration, maxRequests int, period time.Duration) *WindowLimiter {
	if cacheTime < period*2 {
		cacheTime = period * 2
	}
	return &WindowLimiter{
		cacheTime:   cacheTime,
		windowCache: cache.NewMemGoCache[*windowCounter](cacheTime, cacheTime*2),
		maxRequests: maxRequests,
		period:      period,
	}
}

func (wl *WindowLimiter) getCounter(ctx context.Context, key string) *windowCounter {
	counter, err := wl.windowCache.Get(ctx, key)
	if err == nil && counter != nil {
		_, _ = wl.windowCache.Set(ctx, key, counter, wl.cacheTime)
		return counter
	}
	counter = &windowCounter{
		timestamps: make([]time.Time, 0),
	}
	setTrue, err := wl.windowCache.Set(ctx, key, counter, wl.cacheTime)
	if err == nil && setTrue {
		return counter
	}
	return nil
}

// Allow 判断这个key是否允许通过（非阻塞）
func (wl *WindowLimiter) Allow(ctx context.Context, key string) bool {
	counter := wl.getCounter(ctx, key)
	if counter == nil {
		return true
	}

	counter.mu.Lock()
	defer counter.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-wl.period)

	// 移除窗口外的时间戳
	validTimestamps := make([]time.Time, 0)
	for _, ts := range counter.timestamps {
		if ts.After(windowStart) {
			validTimestamps = append(validTimestamps, ts)
		}
	}

	// 检查是否在限制内
	if len(validTimestamps) >= wl.maxRequests {
		counter.timestamps = validTimestamps
		return false
	}

	// 添加当前请求时间戳
	counter.timestamps = append(validTimestamps, now)
	return true
}

// Wait 阻塞等待直到获取许可（带ctx超时）
func (wl *WindowLimiter) Wait(ctx context.Context, key string) error {
	for {
		if wl.Allow(ctx, key) {
			return nil
		}

		// 计算需要等待的时间
		counter := wl.getCounter(ctx, key)
		if counter == nil {
			return fmt.Errorf("no access")
		}

		counter.mu.Lock()
		if len(counter.timestamps) == 0 {
			counter.mu.Unlock()
			continue
		}

		// 找到最早的时间戳，计算它何时会移出窗口
		oldest := counter.timestamps[0]
		waitTime := oldest.Add(wl.period).Sub(time.Now())
		counter.mu.Unlock()

		if waitTime <= 0 {
			continue
		}

		// 等待或直到ctx取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// 继续循环重试
		}
	}
}
