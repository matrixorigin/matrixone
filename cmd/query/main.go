package main

import (
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/engine/logEngine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"runtime/trace"
)

func Print(_ interface{}, bat *batch.Batch) error {
	//	fmt.Printf("%s\n", bat)
	return nil
}

func main() {
	f, err := os.Create("trace.out")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	trace.Start(f)
	defer trace.Stop()

	db, err := logEngine.New("test.db")
	if err != nil {
		log.Fatal(err)
	}
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
	//sql := os.Args[1]
	sql := "select avg(LO_QUANTITY) from lineorder_flat group by S_NAME"
	es, err := compile.New("test", sql, "tom", db, proc).Compile()
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
