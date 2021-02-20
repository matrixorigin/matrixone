package plus

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

func TestF64Plus(t *testing.T) {
	xs := makeFbuffer(13)
	res := make([]float64, 13)
	fmt.Printf("sum:\n\t%v\n", f64PlusAvx(xs, xs, res))
	fmt.Printf("pure sum:\n\t%v\n", f64PlusPure(xs, xs, res))
}

func TestI64Plus(t *testing.T) {
	xs := makeIbuffer(100)
	res := make([]int64, 50)
	fmt.Printf("sum: %v\n", i64PlusAvx(xs[:50], xs[50:], res))
	fmt.Printf("pure sum: %v\n", i64PlusPure(xs[:50], xs[50:], res))
}
