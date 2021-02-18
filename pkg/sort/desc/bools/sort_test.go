package bools

import (
	"fmt"
	"testing"
)

const (
	Num = 10
)

func generate() ([]bool, []int64) {
	os := make([]int64, Num)
	xs := make([]bool, Num)
	{
		for i := 0; i < Num; i++ {
			os[i] = int64(i)
			if i%2 == 0 {
				xs[i] = true
			}
		}
	}
	return xs, os
}

func TestSort(t *testing.T) {
	vs, os := generate()
	for i, o := range os {
		fmt.Printf("[%v] = %v\n", i, vs[o])
	}
	Sort(vs, os[:Num-2])
	fmt.Printf("\n")
	for i, o := range os {
		fmt.Printf("[%v] = %v\n", i, vs[o])
	}
}
