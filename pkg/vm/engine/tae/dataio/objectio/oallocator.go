package objectio

import "sync"

type ObjectAllocator struct {
	mutex     sync.RWMutex
	available uint64
	pageSize  uint32
}

func p2align(x uint64, align uint64) uint64 {
	return x & -align
}

func p2roundup(x uint64, align uint64) uint64 {
	return -(-x & -align)
}

func NewObjectAllocator(capacity uint64, pageSize uint32) *ObjectAllocator {
	allocator := &ObjectAllocator{
		pageSize: pageSize,
	}
	allocator.Init(capacity, pageSize)
	return allocator
}

func (o *ObjectAllocator) Init(capacity uint64, pageSize uint32) {
	o.pageSize = pageSize
	o.available = 0
}

func (o *ObjectAllocator) Allocate(needLen uint64) (uint64, uint64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	offset := o.available
	o.available += needLen
	return offset, o.available
}
