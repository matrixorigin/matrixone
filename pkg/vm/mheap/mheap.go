package mheap

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

func New(gm *guest.Mmu) *Mheap {
	return &Mheap{
		Gm: gm,
		Mp: mempool.New(),
	}
}

func Size(m *Mheap) int64 {
	return m.Gm.Size()
}

func HostSize(m *Mheap) int64 {
	return m.Gm.HostSize()
}

func Free(m *Mheap, data []byte) {
	m.Gm.Free(int64(cap(data)))
}

func Alloc(m *Mheap, size int64) ([]byte, error) {
	data := mempool.Alloc(m.Mp, int(size))
	if err := m.Gm.Alloc(int64(cap(data))); err != nil {
		return nil, err
	}
	return data[:size], nil
}

func Grow(m *Mheap, old []byte, size int64) ([]byte, error) {
	data, err := Alloc(m, mempool.Realloc(old, size))
	if err != nil {
		return nil, err
	}
	copy(data, old)
	return data[:size], nil
}
