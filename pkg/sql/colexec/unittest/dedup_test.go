package unittest

import (
	"fmt"
	"matrixbase/pkg/sql/colexec/dedup"
	"matrixbase/pkg/sql/colexec/mergededup"
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

func TestDedup(t *testing.T) {
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
		rins = append(rins, vm.Instruction{vm.Dedup, &dedup.Argument{Attrs: []string{"uid"}}})
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1, 1, 1}, []string{"orderId", "uid", "price"}, rins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 0]: %s\n", rp)
			rp.Run(segments("S", rproc)[:1], rproc)
			fmt.Printf("S[segment 0] - guest: %v, host: %v\n", rproc.Size(), rproc.HostSize())
			wg.Done()
		}()

	}
	{
		var sins vm.Instructions

		sproc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
		{
			sproc.Refer = make(map[string]uint64)
		}
		sins = append(sins, vm.Instruction{vm.Dedup, &dedup.Argument{Attrs: []string{"uid"}}})
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{1, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 1]: %s\n", sp)
			sp.Run(segments("S", sproc)[1:2], sproc)
			fmt.Printf("S[segment 1] - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		ins = append(ins, vm.Instruction{vm.MergeDedup, &mergededup.Argument{Attrs: []string{"uid"}}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
