package process

import (
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

func New(gm *guest.Mmu, mp *mempool.Mempool) *Process {
	return &Process{
		Gm: gm,
		Mp: mp,
	}
}

func (p *Process) Size() int64 {
	return p.Gm.Size()
}

func (p *Process) HostSize() int64 {
	return p.Gm.HostSize()
}

func (p *Process) Free(data []byte) bool {
	end := p.Mp.Free(data)
	if end {
		p.Gm.Free(int64(cap(data)))
	}
	return end
}

func (p *Process) Alloc(size int64) ([]byte, error) {
	data := p.Mp.Alloc(int(size))
	if err := p.Gm.Alloc(int64(cap(data))); err != nil {
		p.Mp.Free(data)
		return nil, err
	}
	return data, nil
}
