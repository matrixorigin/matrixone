package unittest

import (
	"fmt"
	"log"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/engine"
	"matrixbase/pkg/vm/engine/memEngine"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/mmu/host"
	"matrixbase/pkg/vm/pipeline"
	"matrixbase/pkg/vm/process"
	"testing"
)

func TestProjection(t *testing.T) {
	var ins vm.Instructions

	proc := process.New(guest.New(1<<20, host.New(1<<20)), mempool.New(1<<32, 8))
	{
		ins = append(ins, vm.Instruction{vm.Projection, projection.Argument{[]string{"uid"}}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.New([]uint64{1}, []string{"uid"}, ins)
	p.Run(segments(proc), proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
}

func segments(proc *process.Process) []engine.Segment {
	e := memEngine.NewTestEngine()
	r, err := e.Relation("test")
	if err != nil {
		log.Fatal(err)
	}
	ids := r.Segments()
	segs := make([]engine.Segment, len(ids))
	for i, id := range ids {
		segs[i] = r.Segment(id, proc)
	}
	return segs
}
