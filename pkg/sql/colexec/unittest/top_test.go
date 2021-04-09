package unittest

import (
	"fmt"
	"matrixone/pkg/sql/colexec/mergetop"
	"matrixone/pkg/sql/colexec/top"
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

func TestTop(t *testing.T) {
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
		{
			var fs []top.Field

			fs = append(fs, top.Field{"orderId", top.Descending})
			rins = append(rins, vm.Instruction{vm.Top, &top.Argument{Limit: 3, Fs: fs}})
		}
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1, 1, 1}, []string{"orderId", "uid", "price"}, rins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 0]: %s\n", rp)
			rp.Run(segments("R", rproc)[:1], rproc)
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
		{
			var fs []top.Field

			fs = append(fs, top.Field{"orderId", top.Descending})
			sins = append(sins, vm.Instruction{vm.Top, &top.Argument{Limit: 3, Fs: fs}})
		}
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{1, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 1]: %s\n", sp)
			sp.Run(segments("R", sproc)[1:2], sproc)
			fmt.Printf("S[segment 1] - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		var fs []mergetop.Field

		fs = append(fs, mergetop.Field{"orderId", mergetop.Descending})
		ins = append(ins, vm.Instruction{vm.MergeTop, &mergetop.Argument{Limit: 3, Fs: fs}})
	}
	ins = append(ins, vm.Instruction{vm.Output, nil})
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
