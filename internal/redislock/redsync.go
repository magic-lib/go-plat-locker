package redislock

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/magic-lib/go-plat-locker/internal/config"
	"time"
)

// RedSyncLock 	redis锁
type RedSyncLock struct {
	redisClient *redis.Client
	rs          *redsync.Redsync
	mx          *redsync.Mutex
	key         string
	expiration  time.Duration
}

// NewRedSyncLock 新的锁
func NewRedSyncLock(redisClient *redis.Client, key string, expiration time.Duration) (*RedSyncLock, error) {
	var err error
	redisClient, err = RedisClientV8(redisClient)
	if err != nil {
		return nil, err
	}

	rLock := new(RedSyncLock)
	rLock.redisClient = redisClient
	rLock.key = getLockerKeyName(key)

	// implements the `redis.Pool` interface.
	client := redis.NewClient(&redis.Options{
		Addr:     redisClient.Options().Addr,
		Username: redisClient.Options().Username,
		Password: redisClient.Options().Password,
	})
	pool := goredis.NewPool(client)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rLock.rs = redsync.New(pool)

	//redis时间不能太短，避免大量的redis操作
	if expiration < defaultExpireTime {
		expiration = defaultExpireTime
	}

	rLock.expiration = expiration

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	rLock.mx = rLock.rs.NewMutex(
		rLock.key,
		redsync.WithExpiry(expiration),            // 锁过期时间
		redsync.WithRetryDelay(defaultRetryDelay), // 重试间隔
		redsync.WithTries(defaultRetryTries),      // 重试次数
	)

	return rLock, nil
}

// Lock 上锁
func (l *RedSyncLock) Lock(ctx context.Context) error {
	if err := l.mx.LockContext(ctx); err != nil {
		return err
	}
	return nil
}

// UnLock 解锁
func (l *RedSyncLock) UnLock(ctx context.Context) error {
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := l.mx.UnlockContext(ctx); !ok || err != nil {
		return err
	}
	return nil
}

// TryLock 尝试加锁
func (l *RedSyncLock) TryLock(ctx context.Context) (bool, error) {
	// Release the lock so other processes or threads can obtain a lock.
	if err := l.mx.TryLockContext(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (l *RedSyncLock) LockFunc(ctx context.Context, f func()) error {
	err := l.Lock(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = l.UnLock(ctx)
	}()
	f()
	return nil
}
func (l *RedSyncLock) TryLockFunc(ctx context.Context, f func()) (bool, error) {
	ok, err := l.TryLock(ctx)
	if err != nil {
		return false, err
	}
	if ok {
		defer func() {
			_ = l.UnLock(ctx)
		}()
		f()
		return true, nil
	}
	return false, nil
}

func (l *RedSyncLock) LockerType() string {
	return config.LockerTypeRedis
}
