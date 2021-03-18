package unittest

import (
	"fmt"
	"log"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/sql/colexec/extend"
	"matrixbase/pkg/sql/colexec/extend/overload"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/mmu/host"
	"matrixbase/pkg/vm/pipeline"
	"matrixbase/pkg/vm/process"
	"testing"
)

func TestExtendProjection(t *testing.T) {
	var ins vm.Instructions

	proc := process.New(guest.New(1<<20, host.New(1<<20)), mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)
	}
	{
		var es []extend.Extend

		{
			es = append(es, &extend.UnaryExtend{overload.UnaryMinus, &extend.Attribute{"price", types.T_float64}})
		}
		ins = append(ins, vm.Instruction{vm.Projection, &projection.Argument{[]string{"neg"}, es}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
		proc.Refer["neg"] = 1
	}
	p := pipeline.New([]uint64{1}, []string{"price"}, ins)
	fmt.Printf("%s\n", p)
	if _, err := p.Run(segments(proc), proc); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
}
