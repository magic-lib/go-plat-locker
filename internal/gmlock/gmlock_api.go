// Copyright GoFrame Author(https://goframe.org). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

// Package gmlock implements a concurrent-safe memory-based locker.
package gmlock

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-locker/internal/config"
)

type MemLock struct {
	key string
}

const (
	DefaultKeyFront = "{mem-lock}"
)

func getLockerKeyName(key string) string {
	return fmt.Sprintf("%s%s", DefaultKeyFront, key)
}

// NewMemLock 新的锁
func NewMemLock(key string) *MemLock {
	return &MemLock{
		key: getLockerKeyName(key),
	}
}

// Lock 上锁
func (m *MemLock) Lock(_ context.Context) error {
	Lock(m.key)
	return nil
}

// UnLock 解锁
func (m *MemLock) UnLock(_ context.Context) error {
	Unlock(m.key)
	Remove(m.key)
	return nil
}

// TryLock 尝试加锁
func (m *MemLock) TryLock(_ context.Context) (bool, error) {
	return TryLock(m.key), nil
}
func (m *MemLock) LockFunc(_ context.Context, f func()) error {
	LockFunc(m.key, f)
	return nil
}
func (m *MemLock) TryLockFunc(_ context.Context, f func()) (bool, error) {
	return TryLockFunc(m.key, f), nil
}

func (m *MemLock) LockerType() string {
	return config.LockerTypeMemory
}
