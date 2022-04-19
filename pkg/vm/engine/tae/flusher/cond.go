package flusher

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type FlushCond struct {
	*sync.Cond
	mask *roaring.Bitmap
}

func NewFlushCond() *FlushCond {
	return &FlushCond{
		Cond: sync.NewCond(new(sync.Mutex)),
		mask: roaring.NewBitmap(),
	}
}

func (cond *FlushCond) Acquire(ver int) bool {
	cond.L.Lock()
	defer cond.L.Unlock()
	owner := !cond.mask.Contains(uint32(ver))
	if owner {
		cond.mask.Add(uint32(ver))
	}
	return owner
}

func (cond *FlushCond) Release(ver int) {
	cond.L.Lock()
	cond.mask.Remove(uint32(ver))
	cond.Broadcast()
	cond.L.Unlock()
}

func (cond *FlushCond) WaitDone(ver int) {
	cond.L.Lock()
	for !cond.mask.Contains(uint32(ver)) {
		cond.Wait()
	}
	cond.L.Unlock()
}
