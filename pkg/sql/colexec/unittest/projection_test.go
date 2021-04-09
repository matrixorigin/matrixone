package unittest

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"testing"
)

func TestProjection(t *testing.T) {
	var ins vm.Instructions

	proc := process.New(guest.New(1<<20, host.New(1<<20)), mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)
	}
	{
		var es []extend.Extend

		{
			es = append(es, &extend.Attribute{"uid", types.T_varchar})
		}
		ins = append(ins, vm.Instruction{vm.Projection, &projection.Argument{[]string{"uid"}, es}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.New([]uint64{1, 1}, []string{"uid", "orderId"}, ins)
	fmt.Printf("%s\n", p)
	p.Run(segments("R", proc), proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	fmt.Printf("************\n")
}
