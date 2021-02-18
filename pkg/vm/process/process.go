package process

import "matrixbase/pkg/vm/mmu/guest"

func New(gm *guest.Mmu) *Process {
	return &Process{
		Gm: gm,
	}
}

func (p *Process) Destroy() {
	p.Gm.Destroy()
}

func (p *Process) Size() int64 {
	return p.Gm.Size()
}

func (p *Process) HostSize() int64 {
	return p.Gm.HostSize()
}

func (p *Process) Free(size int64) {
	p.Gm.Free(size)
}

func (p *Process) Alloc(size int64) error {
	return p.Gm.Alloc(size)
}
