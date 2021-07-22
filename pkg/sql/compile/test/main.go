package main

import (
	"fmt"
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/engine/memEngine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
)

func Print(_ interface{}, bat *batch.Batch) error {
	fmt.Printf("%s\n", bat)
	for i, vec := range bat.Vecs {
		fmt.Printf("\t[%v] = %v\n", bat.Attrs[i], vec.Data[:8])
	}
	return nil
}

func main() {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm, mempool.New(1<<40, 8))
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	c := compile.New("test", os.Args[1], "tom", memEngine.NewTestEngine(), metadata.Nodes{}, proc)
	es, err := c.Compile()
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			log.Fatal(err)
		}
		if err := e.Run(); err != nil {
			log.Fatal(err)
		}
	}
}
