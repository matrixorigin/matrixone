package guest

import (
	"matrixone/pkg/vm/mmu"
	"matrixone/pkg/vm/mmu/host"
	"sync/atomic"
)

func New(limit int64, mmu *host.Mmu) *Mmu {
	return &Mmu{
		Mmu:   mmu,
		limit: limit,
	}
}

func (m *Mmu) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

func (m *Mmu) HostSize() int64 {
	return m.Mmu.Size()
}

func (m *Mmu) Free(size int64) {
	if size == 0 {
		return
	}
	m.Mmu.Free(size)
	atomic.AddInt64(&m.size, size*-1)
}

func (m *Mmu) Alloc(size int64) error {
	if size == 0 {
		return nil
	}
	if atomic.LoadInt64(&m.size)+size > m.limit {
		return mmu.OutOfMemory
	}
	if err := m.Mmu.Alloc(size); err != nil {
		return err
	}
	for v := atomic.LoadInt64(&m.size); !atomic.CompareAndSwapInt64(&m.size, v, v+size); v = atomic.LoadInt64(&m.size) {
	}
	return nil
}
