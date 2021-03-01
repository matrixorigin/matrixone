package pipeline

import (
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		cs:    cs,
		ins:   ins,
		attrs: attrs,
	}
}

func (p *Pipeline) Run(segs []engine.Segment, proc *process.Process) (bool, error) {
	if err := vm.Prepare(p.ins, proc); err != nil {
		return false, err
	}
	for _, seg := range segs {
		bat, err := seg.Read(p.cs, p.attrs, proc)
		if err != nil {
			return false, err
		}
		proc.Reg.Ax = bat
		if end, err := vm.Run(p.ins, proc); err != nil || end {
			return end, err
		}
	}
	return false, nil
}
