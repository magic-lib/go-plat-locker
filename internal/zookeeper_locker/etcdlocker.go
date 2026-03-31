package zookeeper_locker

import (
	"time"
)

//go.etcd.io/etcd/client/v3/concurrency

// EtcdLock 	zookeeper锁
type EtcdLock struct {
	key        string
	expiration time.Duration
}

// NewEtcdLock 新的锁
func NewEtcdLock(key string, expiration time.Duration) (*EtcdLock, error) {
	//cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	//session, _ := concurrency.NewSession(cli, concurrency.WithTTL(10))
	//mutex := concurrency.NewMutex(session, "/my-lock/")
	//
	//mutex.Lock(context.Background())
	//defer mutex.Unlock(context.Background())
	return nil, nil
}
