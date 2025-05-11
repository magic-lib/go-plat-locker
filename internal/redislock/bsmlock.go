package redislock

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/magic-lib/go-plat-locker/internal/config"
	"github.com/redis/go-redis/v9"
	"time"
)

// BsmRedisLock 	redis锁
type BsmRedisLock struct {
	redisClient *redis.Client
	key         string
	cli         *redislock.Client
	mx          *redislock.Lock
	expiration  time.Duration
}

// NewBsmRedisLock 新的锁
func NewBsmRedisLock(redisClient *redis.Client, key string, expiration time.Duration) (*BsmRedisLock, error) {
	var err error
	redisClient, err = RedisClientV9(redisClient)
	if err != nil {
		return nil, err
	}

	rLock := new(BsmRedisLock)
	rLock.redisClient = redisClient
	rLock.key = getLockerKeyName(key)

	client := redis.NewClient(&redis.Options{
		Addr:     redisClient.Options().Addr,
		Username: redisClient.Options().Username,
		Password: redisClient.Options().Password,
	})

	rLock.cli = redislock.New(client)

	if expiration < defaultExpireTime {
		expiration = defaultExpireTime
	}

	rLock.expiration = expiration

	return rLock, nil
}

// Lock 上锁
func (l *BsmRedisLock) Lock(ctx context.Context) error {
	lock, err := l.cli.Obtain(ctx, l.key, l.expiration, nil)
	if errors.Is(err, redislock.ErrNotObtained) {
		fmt.Println("Could not obtain lock!")
	} else if err != nil {
		return err
	}
	l.mx = lock
	return nil
}

// UnLock 解锁
func (l *BsmRedisLock) UnLock(ctx context.Context) error {
	if err := l.mx.Release(ctx); err != nil {
		return err
	}
	return nil
}

// TryLock 尝试加锁
func (l *BsmRedisLock) TryLock(ctx context.Context) (bool, error) {
	// Release the lock so other processes or threads can obtain a lock.
	t, err := l.mx.TTL(ctx)
	if err != nil {
		return false, err
	}
	if t == 0 {
		err = l.Lock(ctx)
		if err == nil {

		}
		err = l.Lock(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (l *BsmRedisLock) LockFunc(ctx context.Context, f func()) error {
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
func (l *BsmRedisLock) TryLockFunc(ctx context.Context, f func()) (bool, error) {
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

func (l *BsmRedisLock) LockerType() string {
	return config.LockerTypeRedis
}
