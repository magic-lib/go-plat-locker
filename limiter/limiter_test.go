package limiter_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-locker/limiter"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	limitData := limiter.NewKeySecondLimiter(1*time.Minute, 5, 1)

	key := "userKey"

	for i := 0; i < 100; i++ {
		if i < 20 && !limitData.Allow(context.Background(), key) {
			fmt.Println("no")
			continue
		}
		fmt.Println("yes")
		if i > 30 {
			time.Sleep(1 * time.Second)
		}
	}

}
