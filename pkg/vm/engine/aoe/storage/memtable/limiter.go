package memtable

import (
	"fmt"
	"sync/atomic"
)

type memtableLimiter struct {
	maxactivecnt, activecnt   uint32
	maxactivesize, activesize uint64
}

func newMemtableLimiter(maxactivecnt uint32, maxactivesize uint64) *memtableLimiter {
	return &memtableLimiter{
		maxactivecnt:  maxactivecnt,
		maxactivesize: maxactivesize,
	}
}

func (l *memtableLimiter) ApplyCntQuota(cnt uint32) bool {
	precnt := atomic.LoadUint32(&l.activecnt)
	postcnt := precnt + cnt
	if postcnt > l.maxactivecnt {
		return false
	}
	for !atomic.CompareAndSwapUint32(&l.activecnt, precnt, postcnt) {
		precnt = atomic.LoadUint32(&l.activecnt)
		postcnt = precnt + cnt
		if postcnt > l.activecnt {
			return false
		}
	}
	return true
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

func (l *memtableLimiter) ActiveCnt() uint32 {
	return atomic.LoadUint32(&l.activecnt)
}

func (l *memtableLimiter) ActiveSize() uint64 {
	return atomic.LoadUint64(&l.activesize)
}

func (l *memtableLimiter) String() string {
	s := fmt.Sprintf("<memtableLimiter>[Cnt=(%d/%d),Size=(%d/%d)]",
		l.ActiveCnt(), l.maxactivecnt, l.ActiveSize(), l.maxactivesize)
	return s
}
