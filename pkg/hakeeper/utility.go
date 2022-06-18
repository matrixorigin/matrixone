package hakeeper

import "time"

const (
	// FIXME: configuration item or some other
	TickPerSecond = 10
)

func ExpiredTick(start uint64, timeout time.Duration) uint64 {
	return uint64(timeout/time.Second)*TickPerSecond + start
}
