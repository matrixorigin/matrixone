package unittest

import (
	"fmt"
	"matrixbase/pkg/sql/colexec/aggregation"
	"matrixbase/pkg/sql/colexec/exchange"
	"matrixbase/pkg/sql/colexec/mergegroup"
	"matrixbase/pkg/vm"
	"matrixbase/pkg/vm/mempool"
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/mmu/host"
	"matrixbase/pkg/vm/pipeline"
	"matrixbase/pkg/vm/process"
	"sync"
	"testing"
)

func TestShuffle(t *testing.T) {
	var wg sync.WaitGroup
	var ms []*guest.Mmu
	var ws []*process.WaitRegister

	hm := host.New(1 << 20)

	{
		var ins vm.Instructions

		ms = append(ms, guest.New(1<<20, hm))
		proc := process.New(ms[0], mempool.New(1<<32, 8))
		{
			proc.Refer = make(map[string]uint64)
			proc.Reg.Ws = make([]*process.WaitRegister, 1)
			proc.Reg.Ws[0] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
			ws = append(ws, proc.Reg.Ws[0])
			proc.Refer = make(map[string]uint64)
			proc.Refer["x"] = 1
			proc.Refer["y"] = 1
			proc.Refer["z"] = 1
		}
		{
			var es []aggregation.Extend

			{
				es = append(es, aggregation.Extend{Op: aggregation.Avg, Name: "price", Alias: "x"})
				es = append(es, aggregation.Extend{Op: aggregation.Max, Name: "orderId", Alias: "y"})
				es = append(es, aggregation.Extend{Op: aggregation.Min, Name: "orderId", Alias: "z"})
			}
			ins = append(ins, vm.Instruction{vm.MergeGroup, &mergegroup.Argument{Gs: []string{"uid"}, Es: es}})
			ins = append(ins, vm.Instruction{vm.Output, nil})
		}
		p := pipeline.NewMerge(ins)
		wg.Add(1)
		go func() {
			fmt.Printf("0 - %s\n", p)
			p.RunMerge(proc)
			fmt.Printf("0 - guest: %v, host: %v\n", proc.Size(), proc.HostSize())
			fmt.Printf("************\n")
			wg.Done()
		}()
	}
	{
		var ins vm.Instructions

		ms = append(ms, guest.New(1<<20, hm))
		proc := process.New(ms[1], mempool.New(1<<32, 8))
		{
			proc.Refer = make(map[string]uint64)
			proc.Reg.Ws = make([]*process.WaitRegister, 1)
			proc.Reg.Ws[0] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
			ws = append(ws, proc.Reg.Ws[0])
			proc.Refer = make(map[string]uint64)
			proc.Refer["x"] = 1
			proc.Refer["y"] = 1
			proc.Refer["z"] = 1
		}
		{
			var es []aggregation.Extend

			{
				es = append(es, aggregation.Extend{Op: aggregation.Avg, Name: "price", Alias: "x"})
				es = append(es, aggregation.Extend{Op: aggregation.Max, Name: "orderId", Alias: "y"})
				es = append(es, aggregation.Extend{Op: aggregation.Min, Name: "orderId", Alias: "z"})
			}
			ins = append(ins, vm.Instruction{vm.MergeGroup, &mergegroup.Argument{Gs: []string{"uid"}, Es: es}})
			ins = append(ins, vm.Instruction{vm.Output, nil})
		}
		p := pipeline.NewMerge(ins)
		wg.Add(1)
		go func() {
			fmt.Printf("1 - %s\n", p)
			p.RunMerge(proc)
			fmt.Printf("1 - guest: %v, host: %v\n", proc.Size(), proc.HostSize())
			fmt.Printf("************\n")
			wg.Done()
		}()
	}
	var ins vm.Instructions

	proc := process.New(guest.New(1<<20, hm), mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)
	}
	ins = append(ins, vm.Instruction{vm.Exchange, &exchange.Argument{Attrs: []string{"uid"}, Ms: ms, Ws: ws}})
	p := pipeline.New([]uint64{1, 1, 2}, []string{"orderId", "price", "uid"}, ins)
	fmt.Printf("%s\n", p)
	p.Run(segments("S", proc), proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
}
