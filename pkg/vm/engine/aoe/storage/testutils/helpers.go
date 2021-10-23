package testutils

import (
	"time"
)

func WaitExpect(timeout int, expect func() bool) {
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)
	interval := time.Duration(timeout) * time.Millisecond / 20
	for time.Now().Before(end) && !expect() {
		time.Sleep(interval)
	}
}
