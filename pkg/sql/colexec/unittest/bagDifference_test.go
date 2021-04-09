package unittest

import (
	"fmt"
	"matrixone/pkg/sql/colexec/bag/difference"
	"matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"sync"
	"testing"
)

func TestBagDifference(t *testing.T) {
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
		rins = append(rins, vm.Instruction{vm.Limit, &limit.Argument{Limit: 5}})
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1}, []string{"uid"}, rins)
		wg.Add(1)
		go func() {
			fmt.Printf("S: %s\n", rp)
			rp.Run(segments("S", rproc), rproc)
			fmt.Printf("S - guest: %v, host: %v\n", rproc.Size(), rproc.HostSize())
			wg.Done()
		}()
	}
	{
		var sins vm.Instructions

		sproc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
		{
			sproc.Refer = make(map[string]uint64)
		}
		sins = append(sins, vm.Instruction{vm.Limit, &limit.Argument{Limit: 5}})
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{1}, []string{"uid"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("R: %s\n", sp)
			sp.Run(segments("R", sproc), sproc)
			fmt.Printf("R - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		ins = append(ins, vm.Instruction{vm.BagDifference, &difference.Argument{R: "S", S: "R"}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
