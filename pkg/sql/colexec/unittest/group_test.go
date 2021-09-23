package unittest

import (
	"fmt"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/group"
	"matrixone/pkg/sql/colexec/mergegroup"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/testutil"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"sync"
	"testing"
)

func TestGroup(t *testing.T) {
	return
	var wg sync.WaitGroup
	var ins vm.Instructions

	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm)
	{
		proc.Refer = make(map[string]uint64)
		proc.Reg.MergeReceivers = make([]*process.WaitRegister, 2)
		for i := 0; i < 2; i++ {
			proc.Reg.MergeReceivers[i] = &process.WaitRegister{
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

		rproc := process.New(guest.New(1<<20, hm))
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
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Proc: proc, Reg: proc.Reg.MergeReceivers[0]}})
		rp := pipeline.New([]uint64{1, 1, 2}, []string{"orderId", "price", "uid"}, rins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 0]: %s\n", rp)
			segments, err := testutil.NewTestSegments("S", rproc)
			if err != nil {
				panic(err)
			}
			rp.Run(segments[:1], rproc)
			fmt.Printf("S[segment 0] - guest: %v, host: %v\n", rproc.Size(), rproc.HostSize())
			wg.Done()
		}()

	}
	{
		var sins vm.Instructions

		sproc := process.New(guest.New(1<<20, hm))
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
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Proc: proc, Reg: proc.Reg.MergeReceivers[0]}})
		sp := pipeline.New([]uint64{2, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S[segment 1]: %s\n", sp)
			segments, err := testutil.NewTestSegments("S", sproc)
			if err != nil {
				panic(err)
			}
			sp.Run(segments[1:], sproc)
			fmt.Printf("S[segment 1] - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	wg.Wait()
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

	fmt.Printf("************\n")
}
