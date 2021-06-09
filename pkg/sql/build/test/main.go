package main

import (
	"fmt"
	"log"
	"matrixone/pkg/sql/build"
	"matrixone/pkg/vm/engine/memEngine"
	"os"
)

func main() {
	os, err := build.New("default", os.Args[1], memEngine.NewTestEngine(), nil).Build()
	if err != nil {
		log.Fatal(err)
	}
	for _, o := range os {
		if o != nil {
			fmt.Printf("%s\n", o)
			fmt.Printf("\t%v\n", o.Attribute())
		} else {
			fmt.Printf("NULL\n")
		}
	}
}
