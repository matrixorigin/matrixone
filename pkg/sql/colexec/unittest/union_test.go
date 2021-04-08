package unittest

import (
	"fmt"
	"matrixbase/pkg/sql/colexec/set/union"
	"matrixbase/pkg/sql/colexec/transfer"
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/mmu/host"
	"matrixbase/pkg/vm/pipeline"
	"matrixbase/pkg/vm/process"
	"sync"
	"testing"
)

func TestSetUnion(t *testing.T) {
	var wg sync.WaitGroup
	var ins vm.Instructions

	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm, mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)
		proc.Reg.Ws = make([]*process.WaitRegister, 2)
		for i := 0; i < 2; i++ {
			proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
		}
	}
	{
		var rins vm.Instructions

		rproc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
		{
			rproc.Refer = make(map[string]uint64)
		}
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1}, []string{"orderId"}, rins)
		wg.Add(1)
		go func() {
			fmt.Printf("R: %s\n", rp)
			rp.Run(segments("R", rproc), rproc)
			fmt.Printf("R - guest: %v, host: %v\n", rproc.Size(), rproc.HostSize())
			wg.Done()
		}()
	}
	{
		var sins vm.Instructions

		sproc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
		{
			sproc.Refer = make(map[string]uint64)
		}
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{1}, []string{"orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S: %s\n", sp)
			sp.Run(segments("S", sproc), sproc)
			fmt.Printf("S - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		ins = append(ins, vm.Instruction{vm.SetUnion, &union.Argument{R: "R", S: "S"}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
