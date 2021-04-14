package uint32s

import (
	"fmt"
	"math/rand"
	"testing"
)

const (
	Num   = 10
	Limit = 100
)

func generate() ([]uint32, []int64) {
	os := make([]int64, Num)
	xs := make([]uint32, Num)
	{
		for i := 0; i < Num; i++ {
			os[i] = int64(i)
			xs[i] = uint32(rand.Int63() % Limit)
		}
	}
	return xs, os
}

func TestSort(t *testing.T) {
	vs, os := generate()
	for i, o := range os {
		fmt.Printf("[%v] = %v\n", i, vs[o])
	}
	Sort(vs, os[2:])
	fmt.Printf("\n")
	for i, o := range os {
		fmt.Printf("[%v] = %v\n", i, vs[o])
	}
}
