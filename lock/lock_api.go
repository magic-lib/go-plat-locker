package lock

import (
	"context"
)

type Locker interface {
	TryLock(ctx context.Context) (bool, error)
	Lock(ctx context.Context) error
	UnLock(ctx context.Context) error
	TryLockFunc(ctx context.Context, f func()) (bool, error)
	LockFunc(ctx context.Context, f func()) error
	LockerType() string
}
