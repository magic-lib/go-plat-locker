package lock

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/magic-lib/go-plat-locker/internal/gmlock"
	"github.com/magic-lib/go-plat-locker/internal/redislock"
	"sync"
	"time"
)

var (
	defaultClientMux   sync.Mutex
	defaultRedisClient *redis.Client
	defaultExpiration         = 30 * time.Second
	_                  Locker = (*gmlock.MemLock)(nil)
	_                  Locker = (*redislock.BsmRedisLock)(nil)
	_                  Locker = (*redislock.RedSyncLock)(nil)
	_                  Locker = (*redislock.RedisLock)(nil)
	_                  Locker = (*commLocker)(nil)
)

// SetLockerDefRedisClient 新建redis锁
func SetLockerDefRedisClient(redisClient *redis.Client) {
	if redisClient != nil {
		go func() {
			var err error
			if redisClient, err = redislock.RedisClientV8(redisClient); err != nil {
				return
			}
			defaultClientMux.Lock()
			defer defaultClientMux.Unlock()
			defaultRedisClient = redisClient
		}()
	}
}

type commLocker struct {
	redisClient *redis.Client
	locker      Locker
	expiration  time.Duration
}

func (c *commLocker) LockerType() string {
	return c.locker.LockerType()
}

func (c *commLocker) TryLock(ctx context.Context) (bool, error) {
	return c.locker.TryLock(ctx)
}

func (c *commLocker) Lock(ctx context.Context) (bool, error) {
	return c.locker.Lock(ctx)
}

func (c *commLocker) UnLock(ctx context.Context) (bool, error) {
	return c.locker.UnLock(ctx)
}

func (c *commLocker) LockFunc(ctx context.Context, f func()) error {
	_, err := c.Lock(ctx)
	if err != nil {
		return err
	}
	defer c.UnLock(ctx)
	f()
	return nil
}
func (c *commLocker) TryLockFunc(ctx context.Context, f func()) (bool, error) {
	ok, err := c.TryLock(ctx)
	if err != nil {
		return false, err
	}
	if ok {
		defer c.UnLock(ctx)
		f()
		return true, nil
	}
	return false, nil
}

type Option func(*commLocker)

// WithExpiration 设置过期时间
func WithExpiration(expiration time.Duration) Option {
	return func(c *commLocker) {
		c.expiration = expiration
	}
}
func WithRedisClient(redisClient *redis.Client) Option {
	return func(c *commLocker) {
		c.redisClient = redisClient
	}
}
func WithLocker(locker Locker) Option {
	return func(c *commLocker) {
		c.locker = locker
	}
}

func NewLocker(key string, options ...Option) Locker {
	locker := &commLocker{
		expiration: defaultExpiration,
	}
	for _, opt := range options {
		opt(locker)
	}
	if locker.locker != nil {
		return locker
	}

	var redisClient *redis.Client
	if locker.redisClient != nil {
		redisClient = locker.redisClient
		if defaultRedisClient == nil {
			SetLockerDefRedisClient(locker.redisClient)
		}
	} else {
		if defaultRedisClient != nil {
			redisClient = defaultRedisClient
		}
	}
	if redisClient != nil {
		var err error
		locker.locker, err = redislock.NewRedSyncLock(redisClient, key, locker.expiration)
		if err == nil {
			return locker
		}
		locker.locker, err = redislock.NewRedisLock(redisClient, key, locker.expiration)
		if err == nil {
			return locker
		}
	}

	locker.locker = gmlock.NewMemLock(key)
	return locker
}
