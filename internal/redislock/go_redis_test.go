package redislock

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewTimer(t *testing.T) {
	var timer *time.Timer
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	for i := 0; i < 5; i++ {
		if i != 0 {
			if timer == nil {
				timer = time.NewTimer(100 * time.Millisecond)
			} else {
				timer.Reset(100 * time.Millisecond)
			}

			select {
			case <-ctx.Done():
				fmt.Println("done")
				timer.Stop()
				// Exit early if the context is done.
				fmt.Println("error:", ctx.Err())
				return
			case <-timer.C:
				fmt.Println("cccc:")
				// Fall-through when the delay timer completes.
			}
		}

		if i == 3 {
			cancel()
			continue
		}

		fmt.Println("run:", i)
	}

	select {}
}
