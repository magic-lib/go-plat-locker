package lock

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-locker/internal/config"
	"github.com/magic-lib/go-plat-locker/internal/gmlock"
)

// Lock 加锁
func Lock(key string, f func(), opts ...Option) (bool, error) {
	return lockCommon(key, f, false, opts...)
}

// TryLock 如果锁住了，就不执行f了, 同一时刻只执行一次
// 返回true表示执行了，false 表示没有执行
func TryLock(key string, f func(), opts ...Option) (bool, error) {
	return lockCommon(key, f, true, opts...)
}
func lockCommon(key string, f func(), useTry bool, opts ...Option) (bool, error) {
	newLocker := NewLocker(key, opts...)
	// 类型断言为具体结构体
	if concrete, ok := newLocker.(*commLocker); ok {
		if concrete.expiration == 0 {
			concrete.expiration = defaultExpiration
		}
		return lockFunc(concrete, key, f, useTry)
	}
	return lockFunc(newLocker, key, f, useTry)
}

// lockFunc 返回锁目前的状态
func lockFunc(locker Locker, key string, f func(), useTry bool) (isRun bool, err error) {
	if f == nil {
		return false, fmt.Errorf("f is nil")
	}
	opts := make([]Option, 0)
	if locker != nil {
		opts = append(opts, WithLocker(locker))
	}

	ctx := context.Background()
	nl := NewLocker(key, opts...)

	if useTry {
		if nl.LockerType() == config.LockerTypeMemory {
			return nl.TryLockFunc(ctx, f)
		}

		ok, err := nl.TryLock(ctx)
		if err != nil {
			return lockFunc(gmlock.NewMemLock(key), key, f, useTry)
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
		return lockFunc(gmlock.NewMemLock(key), key, f, useTry)
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
