package unittest

import (
	"fmt"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/sql/colexec/extend"
	"matrixbase/pkg/sql/colexec/offset"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/mmu/host"
	"matrixbase/pkg/vm/pipeline"
	"matrixbase/pkg/vm/process"
	"testing"
)

func TestOffset(t *testing.T) {
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
	}
	{
		ins = append(ins, vm.Instruction{vm.Offset, &offset.Argument{Offset: 19}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.New([]uint64{1, 1}, []string{"uid", "orderId"}, ins)
	fmt.Printf("%s\n", p)
	p.Run(segments("R", proc), proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
}
