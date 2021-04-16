package spilltest

import (
	"fmt"
	"log"
	"matrixone/pkg/sql/colexec/mergeorder"
	"matrixone/pkg/sql/colexec/order"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine/spillEngine"
	"matrixone/pkg/vm/engine/spillEngine/kv"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"sync"
	"testing"
)

func TestOrder(t *testing.T) {
	var wg sync.WaitGroup
	var ins vm.Instructions

	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm, mempool.New(1<<32, 8))
	{
		proc.Refer = make(map[string]uint64)
		proc.Reg.Ws = make([]*process.WaitRegister, 2)
		proc.Id = "0"
		proc.Lim.BatchRows = 10
		proc.Lim.PartitionRows = 10
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
			var fs []order.Field

			fs = append(fs, order.Field{"orderId", order.Descending})
			rins = append(rins, vm.Instruction{vm.Order, &order.Argument{Fs: fs}})
		}
		rins = append(rins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[0]}})
		rp := pipeline.New([]uint64{1, 1, 1}, []string{"orderId", "uid", "price"}, rins)
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
		{
			var fs []order.Field

			fs = append(fs, order.Field{"orderId", order.Descending})
			sins = append(sins, vm.Instruction{vm.Order, &order.Argument{Fs: fs}})
		}
		sins = append(sins, vm.Instruction{vm.Transfer, &transfer.Argument{Mmu: gm, Reg: proc.Reg.Ws[1]}})
		sp := pipeline.New([]uint64{1, 1, 1}, []string{"uid", "price", "orderId"}, sins)
		wg.Add(1)
		go func() {
			fmt.Printf("S: %s\n", sp)
			sp.Run(segments("S", sproc), sproc)
			fmt.Printf("S - guest: %v, host: %v\n", sproc.Size(), sproc.HostSize())
			wg.Done()
		}()
	}
	{
		var fs []mergeorder.Field

		db, err := kv.New("test.db")
		if err != nil {
			log.Fatal(err)
		}
		fs = append(fs, mergeorder.Field{"orderId", mergeorder.Descending})
		ins = append(ins, vm.Instruction{vm.MergeOrder, &mergeorder.Argument{E: spillEngine.New("test.db", db), Fs: fs}})
	}
	ins = append(ins, vm.Instruction{vm.Output, nil})
	p := pipeline.NewMerge(ins)
	fmt.Printf("%s\n", p)
	p.RunMerge(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
	wg.Wait()
	fmt.Printf("************\n")
}
