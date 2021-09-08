package process

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

func New(gm *guest.Mmu) *Process {
	return &Process{
		Gm: gm,
	}
}

func (p *Process) Size() int64 {
	return p.Gm.Size()
}

func (p *Process) HostSize() int64 {
	return p.Gm.HostSize()
}

func (p *Process) Free(data []byte) {
	p.Mp.Free(data)
	p.Gm.Free(int64(cap(data)))
}

func (p *Process) Alloc(size int64) ([]byte, error) {
	data := p.Mp.Alloc(int(size))
	if err := p.Gm.Alloc(int64(cap(data))); err != nil {
		p.Mp.Free(data)
		return nil, err
	}
	return data[:size], nil
}

func (p *Process) Grow(old []byte, size int64) ([]byte, error) {
	data, err := p.Alloc(mempool.Realloc(old, size))
	if err != nil {
		return nil, err
	}
	copy(data, old)
	return data, nil
}
