package zookeeper_locker

import (
	"time"
)

//"github.com/go-zookeeper/zk"

// ZooKeeperLock 	zookeeper锁
type ZooKeeperLock struct {
	key        string
	expiration time.Duration
}

// NewZooKeepterLock 新的锁
func NewZooKeepterLock(key string, expiration time.Duration) (*ZooKeeperLock, error) {
	//conn, _, _ := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5)
	//lock := zk.NewLock(conn, "/my-lock", zk.WorldACL(zk.PermAll))
	//if err := lock.Lock(); err != nil {
	//	return
	//}
	//defer lock.Unlock()
	return nil, nil
}
