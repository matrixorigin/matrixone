package guest

import (
	"matrixone/pkg/vm/mmu"
	"matrixone/pkg/vm/mmu/host"
)

func New(limit int64, mmu *host.Mmu) *Mmu {
	return &Mmu{
		Mmu:   mmu,
		Limit: limit,
	}
}

func (m *Mmu) Size() int64 {
	return m.size
}

func (m *Mmu) HostSize() int64 {
	return m.Mmu.Size()
}

func (m *Mmu) Free(size int64) {
	if size == 0 {
		return
	}
	m.size -= size
	m.Mmu.Free(size)
}

func (m *Mmu) Alloc(size int64) error {
	if size == 0 {
		return nil
	}
	if m.size+size > m.Limit {
		return mmu.OutOfMemory
	}
	if err := m.Mmu.Alloc(size); err != nil {
		return err
	}
	m.size += size
	return nil
}
