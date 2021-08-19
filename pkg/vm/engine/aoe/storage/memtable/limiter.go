package memtable

import (
	"fmt"
	"sync/atomic"
)

type memtableLimiter struct {
	maxactivesize, activesize uint64
}

func newMemtableLimiter(maxactivesize uint64) *memtableLimiter {
	return &memtableLimiter{
		maxactivesize: maxactivesize,
	}
}

func (l *memtableLimiter) ApplySizeQuota(size uint64) bool {
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

func (l *memtableLimiter) ActiveSize() uint64 {
	return atomic.LoadUint64(&l.activesize)
}

func (l *memtableLimiter) String() string {
	s := fmt.Sprintf("<memtableLimiter>[Size=(%d/%d)]",
		l.ActiveSize(), l.maxactivesize)
	return s
}
