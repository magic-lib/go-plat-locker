package lock_test

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/magic-lib/go-plat-locker/lock"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	var redClient = redis.NewClient(&redis.Options{
		Addr: "192.168.10.37:16379",
	})
	lock.SetLockerDefRedisClient(redClient)

	key1 := "aaaa"
	//key2 := "bb"

	go func() {
		//不同key
		//mm, err := lock.LockedFunc(key2, func() {
		//fmt.Println("不同key内 11111111")
		//})
		//fmt.Println("不同key内", mm, err)
	}()

	go func() {
		fmt.Println("内1 aaaaaa")
		mm, err := lock.Lock(key1, func() {
			fmt.Println("内1 11111111")
			time.Sleep(2 * time.Second)
		})
		fmt.Println("内1", mm, err)
	}()
	go func() {
		fmt.Println("内2 aaaaaa")
		mm, err := lock.Lock(key1, func() {
			fmt.Println("内2 11111111")
			time.Sleep(1 * time.Second)
		})
		fmt.Println("内2", mm, err)
	}()
	mm, err := lock.Lock(key1, func() {
		//fmt.Println("外 11111111")
		time.Sleep(1 * time.Second)
	})
	fmt.Println("外", mm, err)

	time.Sleep(10 * time.Second)
}

func TestDeadLock2(t *testing.T) {
	//var redClient = redis.NewClient(&redis.Options{
	//	Addr: "192.168.10.37:16379",
	//})
	//SetRedisClient(redClient)

	key1 := "aaaa"

	mm, err := lock.Lock(key1, func() {
		fmt.Println("内1 11111111")
		time.Sleep(1 * time.Second)
		fmt.Println("内1 222222222")

		go func() {
			mm, err := lock.Lock(key1, func() {
				fmt.Println("内内11 11111111")
				fmt.Println("内内11 222222222")
			})
			fmt.Println("内内11", mm, err)
		}()

		time.Sleep(3 * time.Second)

	})
	fmt.Println("内1", mm, err)

	time.Sleep(5 * time.Second)
}

// 因为有锁，只能执行一个
func TestDeadLock3(t *testing.T) {
	//var redClient = redis.NewClient(&redis.Options{
	//	Addr: "192.168.10.37:16379",
	//})
	//SetRedisClient(redClient)

	//key1 := "aaaa"

	//go func() {
	//	mm, err := LockOnce(key1, func() {
	//		fmt.Println("内1 11111111")
	//		time.Sleep(1 * time.Second)
	//		fmt.Println("内1 222222222")
	//	})
	//	fmt.Println("内1", mm, err)
	//}()
	//go func() {
	//	mm, err := LockOnce(key1, func() {
	//		fmt.Println("内2 11111111")
	//		time.Sleep(1 * time.Second)
	//		fmt.Println("内2 222222222")
	//	})
	//	fmt.Println("内2", mm, err)
	//}()

	time.Sleep(5 * time.Second)
}
