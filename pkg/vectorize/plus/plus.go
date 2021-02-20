package plus

import "matrixbase/pkg/internal/cpu"

var (
	i64PlusOne func(int64, []int64, []int64) []int64
	i64Plus    func([]int64, []int64, []int64) []int64

	f64PlusOne func(float64, []float64, []float64) []float64
	f64Plus    func([]float64, []float64, []float64) []float64
)

func init() {
	if cpu.X86.HasAVX2 {
		i64Plus = i64PlusAvx
		i64PlusOne = i64PlusOneAvx

		f64Plus = f64PlusAvx
		f64PlusOne = f64PlusOneAvx
	} else {
		i64Plus = i64PlusPure
		i64PlusOne = i64PlusOnePure

		f64Plus = f64PlusPure
		f64PlusOne = f64PlusOnePure
	}
}

func iPlusAvx([]int64, []int64, []int64)
func iPlusOneAvx(int64, []int64, []int64)
func fPlusAvx([]float64, []float64, []float64)
func fPlusOneAvx(float64, []float64, []float64)

func I64Plus(xs, ys, rs []int64) []int64 {
	return i64Plus(xs, ys, rs)
}

func I64PlusOne(x int64, ys, rs []int64) []int64 {
	return i64PlusOnePure(x, ys, rs)
}

func F64Plus(xs, ys, rs []float64) []float64 {
	return f64Plus(xs, ys, rs)
}

func F64PlusOne(x float64, ys, rs []float64) []float64 {
	return f64PlusOnePure(x, ys, rs)
}

func i64PlusAvx(xs, ys, rs []int64) []int64 {
	n := len(xs) / 4
	iPlusAvx(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func i64PlusOneAvx(x int64, ys, rs []int64) []int64 {
	n := len(ys) / 4
	iPlusOneAvx(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func f64PlusAvx(xs, ys, rs []float64) []float64 {
	n := len(xs) / 4
	fPlusAvx(xs[:n*4], ys[:n*4], rs[:n*4])
	for i, j := n*4, len(xs); i < j; i++ {
		rs[i] = xs[i] + ys[i]
	}
	return rs
}

func f64PlusOneAvx(x float64, ys, rs []float64) []float64 {
	n := len(ys) / 4
	fPlusOneAvx(x, ys[:n*4], rs[:n*4])
	for i, j := n*4, len(ys); i < j; i++ {
		rs[i] = x + ys[i]
	}
	return rs
}

func i64PlusPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func i64PlusOnePure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func f64PlusPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func f64PlusOnePure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}
