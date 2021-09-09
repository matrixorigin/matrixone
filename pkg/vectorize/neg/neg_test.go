package neg

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

func makeFbuffer(l int) []float32 {
	buf := make([]float32, l)
	for i := range buf {
		buf[i] = float32(i)
	}
	return buf
}

func TestF64Sum(t *testing.T) {
	xs := makeFbuffer(100)
	rs := make([]float32, 100)
	fmt.Printf("float neg: %v\n", Float32Neg(xs, rs))
	fmt.Printf("pure float neg: %v\n", float32Neg(xs, rs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(100)
	rs := make([]int64, 100)
	fmt.Printf("int neg: %v\n", Int64Neg(xs, rs))
	fmt.Printf("pure int neg: %v\n", int64Neg(xs, rs))
}
