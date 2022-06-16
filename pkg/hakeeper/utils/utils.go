package utils

import "time"

const (
	TickPerSecond = 10
	StoreTimeout  = 10 * time.Minute
)

func ExpiredTick(start uint64, timeout time.Duration) uint64 {
	return uint64(timeout/time.Second)*TickPerSecond + start
}
