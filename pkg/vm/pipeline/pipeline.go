package pipeline

import (
	"bytes"
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

	if err = vm.Prepare(p.ins, proc); err != nil {
		vm.Clean(p.ins, proc)
		return false, err
	}
	for _, seg := range segs {
		bat, err := seg.Read(p.cs, p.attrs, proc)
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
	{
		proc.Reg.Ax = nil
		_, err = vm.Run(p.ins, proc)
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
