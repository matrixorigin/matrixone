package pipeline

import (
	"bytes"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
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

	defer func() {
		{
			proc.Reg.Ax = nil
			_, err = vm.Run(p.ins, proc)
		}
	}()
	if err = vm.Prepare(p.ins, proc); err != nil {
		vm.Clean(p.ins, proc)
		return false, err
	}
	q, err := p.prefetch(segs, proc)
	if err != nil {
		return false, err
	}
	for i, j := 0, len(q.bs); i < j; i++ {
		if err := q.prefetch(p.cs, p.attrs, proc); err != nil {
			return false, err
		}
		if q.bs[i].bat == nil {
			bat, err := q.bs[i].blk.Prefetch(p.cs, p.attrs, proc)
			if err != nil {
				return false, err
			}
			q.bs[i].bat = bat
			q.pi = i + 1
		}
		proc.Reg.Ax = q.bs[i].bat
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
	if err := vm.Prepare(p.ins, proc); err != nil {
		vm.Clean(p.ins, proc)
		return false, err
	}
	for {
		if end, err := vm.Run(p.ins, proc); err != nil || end {
			return end, err
		}
	}
	return false, nil
}

func (p *Pipeline) prefetch(segs []engine.Segment, proc *process.Process) (*queue, error) {
	q := new(queue)
	q.bs = make([]block, 0, 8) // prefetch block list
	{
		for _, seg := range segs {
			ids := seg.Blocks()
			for _, id := range ids {
				b := seg.Block(id, proc)
				var siz int64
				{
					for _, attr := range p.attrs {
						siz += b.Size(attr)
					}
				}
				q.bs = append(q.bs, block{siz: siz, blk: b})
			}
		}
	}
	if err := q.prefetch(p.cs, p.attrs, proc); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *queue) prefetch(cs []uint64, attrs []string, proc *process.Process) error {
	if q.pi == len(q.bs) {
		return nil
	}
	for i, j := q.pi, len(q.bs); i < j; i++ {
		if q.siz >= proc.Lim.BatchSize {
			break
		}
		bat, err := q.bs[i].blk.Prefetch(cs, attrs, proc)
		if err != nil {
			return err
		}
		q.bs[i].bat = bat
		q.pi = i + 1
	}
	return nil
}
