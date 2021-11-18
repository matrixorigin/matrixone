package main

import (
	"fmt"
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/engine/memEngine"
	"matrixone/pkg/vm/mheap"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
)

func Print(_ interface{}, bat *batch.Batch) error {
	fmt.Printf("%v\n", bat.Zs)
	fmt.Printf("%v\n", bat)
	return nil
}

func main() {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	e := memEngine.NewTestEngine()
	sql := os.Args[1]
	c := compile.New("test", sql, "", e, proc)
	es, err := c.Build()
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			log.Fatal(err)
		}
		attrs := e.Columns()
		fmt.Printf("result:\n")
		for i, attr := range attrs {
			fmt.Printf("\t[%v] = %v:%v\n", i, attr.Name, attr.Typ)
		}
		if err := e.Run(0); err != nil {
			log.Fatal(err)
		}
	}
}
