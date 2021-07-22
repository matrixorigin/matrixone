package main

import (
	"fmt"
	"github.com/fagongzi/util/format"
)

func main() {
	s := fmt.Sprintf("%8d", 1)
	fmt.Printf("%v\n", []byte(s))
	t := format.Uint64ToBytes(1)
	fmt.Printf("%v\n", t)
}
