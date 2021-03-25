package sum

import (
	"fmt"
	"testing"
)

func makeIbuffer(l int) []int64 {
	buf := make([]int64, l)
	for i := range buf {
		buf[i] = int64(i)
	}
	return buf
}

func makeFbuffer(l int) []float64 {
	buf := make([]float64, l)
	for i := range buf {
		buf[i] = float64(i)
	}
	return buf
}

func TestF64Sum(t *testing.T) {
	xs := makeFbuffer(10000)
	fmt.Printf("sum: %v\n", Float64Sum(xs))
	fmt.Printf("pure sum: %v\n", float64SumPure(xs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(10000)
	fmt.Printf("sum: %v\n", Int64Sum(xs))
	fmt.Printf("pure sum: %v\n", int64SumPure(xs))
}
