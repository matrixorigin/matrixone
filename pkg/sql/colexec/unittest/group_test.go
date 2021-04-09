package unittest

import (
	"fmt"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/group"
	"matrixone/pkg/sql/colexec/mergegroup"
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

func TestGroup(t *testing.T) {
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
		proc.Refer = make(map[string]uint64)
		proc.Refer["x"] = 1
		proc.Refer["y"] = 1
		proc.Refer["z"] = 1
	}
	{
		var rins vm.Instructions

		rproc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
		{
			rproc.Refer = make(map[string]uint64)
			rproc.Refer["x"] = 1
			rproc.Refer["y"] = 1
			rproc.Refer["z"] = 1
		}
		var es []aggregation.Extend

		{
			es = append(es, aggregation.Extend{Op: aggregation.SumCount, Name: "price", Alias: "x"})
			es = append(es, aggregation.Extend{Op: aggregation.Max, Name: "orderId", Alias: "y"})
			es = append(es, aggregation.Extend{Op: aggregation.Min, Name: "orderId", Alias: "z"})
		}
		rins = append(rins, vm.Instruction{vm.Group, &group.Argument{Gs: []string{"uid"}, Es: es}})
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1, 1, 2}, []string{"orderId", "price", "uid"}, rins)
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
			sproc.Refer["x"] = 1
			sproc.Refer["y"] = 1
			sproc.Refer["z"] = 1
		}
		var es []aggregation.Extend

		{
			es = append(es, aggregation.Extend{Op: aggregation.SumCount, Name: "price", Alias: "x"})
			es = append(es, aggregation.Extend{Op: aggregation.Max, Name: "orderId", Alias: "y"})
			es = append(es, aggregation.Extend{Op: aggregation.Min, Name: "orderId", Alias: "z"})
		}
		sins = append(sins, vm.Instruction{vm.Group, &group.Argument{Gs: []string{"uid"}, Es: es}})
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{2, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 1]: %s\n", sp)
			sp.Run(segments("S", sproc)[1:], sproc)
			fmt.Printf("S[segment 1] - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		var es []aggregation.Extend

		{
			es = append(es, aggregation.Extend{Op: aggregation.Avg, Name: "x", Alias: "x"})
			es = append(es, aggregation.Extend{Op: aggregation.Max, Name: "y", Alias: "y"})
			es = append(es, aggregation.Extend{Op: aggregation.Min, Name: "z", Alias: "z"})
		}
		ins = append(ins, vm.Instruction{vm.MergeGroup, &mergegroup.Argument{Gs: []string{"uid"}, Es: es}})
		ins = append(ins, vm.Instruction{vm.Output, nil})
	}
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
