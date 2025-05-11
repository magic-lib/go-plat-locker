package lock

import (
	"context"
)

type Locker interface {
	TryLock(ctx context.Context) (bool, error)
	TryLockFunc(ctx context.Context, f func()) (bool, error)
	Lock(ctx context.Context) (bool, error)
	LockFunc(ctx context.Context, f func()) error
	UnLock(ctx context.Context) (bool, error)
	LockerType() string
}
