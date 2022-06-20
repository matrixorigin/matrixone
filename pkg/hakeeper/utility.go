package hakeeper

import "time"

const (
	// FIXME: configuration item or some other
	TickPerSecond   = 10
	LogStoreTimeout = 10 * time.Minute
	DnStoreTimeout  = 10 * time.Second
)

func ExpiredTick(start uint64, timeout time.Duration) uint64 {
	return uint64(timeout/time.Second)*TickPerSecond + start
}
