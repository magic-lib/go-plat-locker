package lock

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-locker/internal/config"
	"github.com/magic-lib/go-plat-locker/internal/gmlock"
	"time"
)

// Lock 加锁
func Lock(key string, f func(), expiration ...time.Duration) (bool, error) {
	exp := defaultExpiration
	if expiration != nil && len(expiration) > 0 {
		exp = expiration[0]
	}
	return lockFunc(nil, key, f, false, exp)
}

// TryLock 如果锁住了，就不执行f了, 同一时刻只执行一次
// 返回true表示执行了，false 表示没有执行
func TryLock(key string, f func(), expiration ...time.Duration) (bool, error) {
	exp := time.Duration(0)
	if expiration != nil && len(expiration) > 0 {
		exp = expiration[0]
	}
	return lockFunc(nil, key, f, true, exp)
}

// lockFunc 返回锁目前的状态
func lockFunc(locker Locker, key string, f func(), useTry bool, expiration time.Duration) (isRun bool, err error) {
	if f == nil {
		return false, fmt.Errorf("f is nil")
	}
	opts := make([]Option, 0)
	if locker != nil {
		opts = append(opts, WithLocker(locker))
	}
	if expiration > 0 {
		opts = append(opts, WithExpiration(expiration))
	}

	ctx := context.Background()
	nl := NewLocker(key, opts...)

	if useTry {
		if nl.LockerType() == config.LockerTypeMemory {
			return nl.TryLockFunc(ctx, f)
		}

		ok, err := nl.TryLock(ctx)
		if err != nil {
			return lockFunc(gmlock.NewMemLock(key), key, f, useTry, expiration)
		}

		if ok {
			defer func(nl Locker, ctx context.Context) {
				err = nl.UnLock(ctx)
				if err != nil {
					fmt.Println("unlock error:", err)
				}
			}(nl, ctx)
			f()
			return true, nil
		}
		return false, nil
	}

	if nl.LockerType() == config.LockerTypeMemory {
		return true, nl.LockFunc(ctx, f)
	}

	err = nl.Lock(ctx)
	if err != nil {
		return lockFunc(gmlock.NewMemLock(key), key, f, useTry, expiration)
	}
	defer func(nl Locker, ctx context.Context) {
		err = nl.UnLock(ctx)
		if err != nil {
			fmt.Println("unlock error:", err)
		}
	}(nl, ctx)
	f()
	return true, nil
}
