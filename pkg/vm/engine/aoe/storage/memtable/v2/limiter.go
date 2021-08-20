package memtable

import (
	"fmt"
	"sync/atomic"
)

type sizeLimiter struct {
	maxactivesize, activesize uint64
}

func newSizeLimiter(maxactivesize uint64) *sizeLimiter {
	return &sizeLimiter{
		maxactivesize: maxactivesize,
	}
}

func (l *sizeLimiter) RetuernQuota(size uint64) uint64 {
	return atomic.AddUint64(&l.activesize, ^uint64(size-1))
}

func (l *sizeLimiter) ApplyQuota(size uint64) bool {
	pre := atomic.LoadUint64(&l.activesize)
	post := pre + size
	if post > l.maxactivesize {
		return false
	}
	for !atomic.CompareAndSwapUint64(&l.activesize, pre, post) {
		pre = atomic.LoadUint64(&l.activesize)
		post = pre + size
		if post > l.activesize {
			return false
		}
	}
	return true
}

func (l *sizeLimiter) Total() uint64 {
	return atomic.LoadUint64(&l.activesize)
}

func (l *sizeLimiter) String() string {
	s := fmt.Sprintf("<sizeLimiter>[Size=(%d/%d)]",
		l.Total(), l.maxactivesize)
	return s
}
