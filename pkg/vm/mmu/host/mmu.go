package host

import (
	"matrixbase/pkg/vm/mmu"
	"sync/atomic"
)

func New(limit int64) *Mmu {
	return &Mmu{
		limit: limit,
	}
}

func (m *Mmu) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

func (m *Mmu) Free(size int64) {
	atomic.AddInt64(&m.size, size*-1)
}

func (m *Mmu) Alloc(size int64) error {
	if atomic.LoadInt64(&m.size)+size > m.limit {
		return mmu.OutOfMemory
	}
	for v := atomic.LoadInt64(&m.size); !atomic.CompareAndSwapInt64(&m.size, v, v+size); v = atomic.LoadInt64(&m.size) {
	}
	return nil
}
