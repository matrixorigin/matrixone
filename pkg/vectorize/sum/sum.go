package sum

import "matrixbase/pkg/internal/cpu"

var (
	i64Sum     func([]int64) int64
	f64Sum     func([]float64) float64
	i64SumSels func([]int64, []int64) int64
	f64SumSels func([]float64, []int64) float64
)

func init() {
	if cpu.X86.HasAVX2 {
		i64Sum = i64SumAvx
		f64Sum = f64SumAvx
	} else {
		i64Sum = i64SumPure
		f64Sum = f64SumPure
	}
	i64SumSels = i64SumSelsPure
	f64SumSels = f64SumSelsPure
}

func iSumAvx([]int64) int64
func fSumAvx([]float64) float64

func I64Sum(xs []int64) int64 {
	return i64Sum(xs)
}

func F64Sum(xs []float64) float64 {
	return f64Sum(xs)
}

func I64SumSels(xs []int64, sels []int64) int64 {
	return i64SumSels(xs, sels)
}

func F64SumSels(xs []float64, sels []int64) float64 {
	return f64SumSels(xs, sels)
}

func i64SumAvx(xs []int64) int64 {
	n := len(xs) / 8
	r := iSumAvx(xs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		r += xs[i]
	}
	return r
}

func i64SumPure(xs []int64) int64 {
	var sum int64

	for _, x := range xs {
		sum += x
	}
	return sum
}

func f64SumAvx(xs []float64) float64 {
	n := len(xs) / 8
	r := fSumAvx(xs[:n*8])
	for i, j := n*8, len(xs); i < j; i++ {
		r += xs[i]
	}
	return r
}

func f64SumPure(xs []float64) float64 {
	var sum float64

	for _, x := range xs {
		sum += x
	}
	return sum
}

func i64SumSelsPure(xs []int64, sels []int64) int64 {
	var sum int64

	for _, sel := range sels {
		sum += xs[sel]
	}
	return sum
}

func f64SumSelsPure(xs []float64, sels []int64) float64 {
	var sum float64

	for _, sel := range sels {
		sum += xs[sel]
	}
	return sum
}
