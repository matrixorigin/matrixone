package pipeline

import (
	"bytes"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		cs:    cs,
		ins:   ins,
		attrs: attrs,
	}
}

func NewMerge(ins vm.Instructions) *Pipeline {
	return &Pipeline{
		ins: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.ins, &buf)
	return buf.String()
}

func (p *Pipeline) Run(segs []engine.Segment, proc *process.Process) (bool, error) {
	var end bool
	var err error

	proc.Mp = mempool.Pool.Get().(*mempool.Mempool)
	defer func() {
		proc.Reg.Ax = nil
		vm.Run(p.ins, proc)
		for i := range p.cds {
			proc.Free(p.cds[i].Bytes())
		}
		for i := range p.cds {
			proc.Free(p.dds[i].Bytes())
		}
		mempool.Pool.Put(proc.Mp)
		proc.Mp = nil
	}()
	if err = vm.Prepare(p.ins, proc); err != nil {
		return false, err
	}
	q := p.prefetch(segs, proc)
	p.cds, p.dds = make([]*bytes.Buffer, 0, len(p.cs)), make([]*bytes.Buffer, 0, len(p.cs))
	{
		for _ = range p.cs {
			data, err := proc.Alloc(CompressedBlockSize)
			if err != nil {
				return false, err
			}
			p.cds = append(p.cds, bytes.NewBuffer(data))
		}
		for _ = range p.cs {
			data, err := proc.Alloc(CompressedBlockSize)
			if err != nil {
				return false, err
			}
			p.dds = append(p.dds, bytes.NewBuffer(data))
		}
	}
	for i, j := 0, len(q.bs); i < j; i++ {
		if err := q.prefetch(p.attrs); err != nil {
			return false, err
		}
		bat, err := q.bs[i].blk.Read(p.cs, p.attrs, p.cds, p.dds)
		if err != nil {
			return false, err
		}
		proc.Reg.Ax = bat
		if end, err = vm.Run(p.ins, proc); err != nil {
			return end, err
		}
		if end {
			break
		}
	}
	return end, err
}

func (p *Pipeline) RunMerge(proc *process.Process) (bool, error) {
	proc.Mp = mempool.Pool.Get().(*mempool.Mempool)
	defer func() {
		proc.Reg.Ax = nil
		vm.Run(p.ins, proc)
		mempool.Pool.Put(proc.Mp)
		proc.Mp = nil
	}()
	if err := vm.Prepare(p.ins, proc); err != nil {
		vm.Clean(p.ins, proc)
		return false, err
	}
	for {
		proc.Reg.Ax = nil
		if end, err := vm.Run(p.ins, proc); err != nil || end {
			return end, err
		}
		return false, nil
	}
}

func (p *Pipeline) prefetch(segs []engine.Segment, proc *process.Process) *queue {
	q := new(queue)
	q.bs = make([]block, 0, 8) // prefetch block list
	{
		for _, seg := range segs {
			ids := seg.Blocks()
			for _, id := range ids {
				q.bs = append(q.bs, block{blk: seg.Block(id, proc)})
			}
		}
	}
	return q
}

func (q *queue) prefetch(attrs []string) error {
	if q.pi == len(q.bs) {
		return nil
	}
	start := q.pi
	for i, j := q.pi, len(q.bs); i < j; i++ {
		if i > PrefetchNum+start {
			break
		}
		q.bs[i].blk.Prefetch(attrs)
		q.pi = i + 1
	}
	return nil
}
