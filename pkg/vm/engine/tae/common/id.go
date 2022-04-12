package common

import "sync/atomic"

type IdAllocator struct {
	id uint64
}

func (id *IdAllocator) Get() uint64 {
	return atomic.LoadUint64(&id.id)
}

func (id *IdAllocator) Alloc() uint64 {
	return atomic.AddUint64(&id.id, uint64(1))
}

func (id *IdAllocator) Set(val uint64) {
	atomic.StoreUint64(&id.id, val)
}
