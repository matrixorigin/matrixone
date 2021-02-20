package mod

import (
	"math"
)

var (
	i64ModOne     func(int64, []int64, []int64) []int64
	i64ModOneBy   func(int64, []int64, []int64) []int64
	i64Mod        func([]int64, []int64, []int64) []int64
	i64ModOneSels func(int64, []int64, []int64, []int64) []int64
	i64ModSels    func([]int64, []int64, []int64, []int64) []int64

	f64ModOne     func(float64, []float64, []float64) []float64
	f64ModOneBy   func(float64, []float64, []float64) []float64
	f64Mod        func([]float64, []float64, []float64) []float64
	f64ModOneSels func(float64, []float64, []float64, []int64) []float64
	f64ModSels    func([]float64, []float64, []float64, []int64) []float64
)

func init() {
	i64Mod = i64ModPure
	i64ModOne = i64ModOnePure
	i64ModOneBy = i64ModOneByPure
	i64ModSels = i64ModSelsPure
	i64ModOneSels = i64ModOneSelsPure

	f64Mod = f64ModPure
	f64ModOne = f64ModOnePure
	i64ModOneBy = i64ModOneByPure
	f64ModSels = f64ModSelsPure
	f64ModOneSels = f64ModOneSelsPure
}

// check if 0 exists before the function call
func I64Mod(xs, ys, rs []int64) []int64 {
	return i64Mod(xs, ys, rs)
}

// check if 0 exists before the function call
func I64ModOne(x int64, ys, rs []int64) []int64 {
	return i64ModOnePure(x, ys, rs)
}

// check if 0 exists before the function call
func I64ModOneBy(x int64, ys, rs []int64) []int64 {
	return i64ModOneByPure(x, ys, rs)
}

// check if 0 exists before the function call
func I64ModSels(xs, ys, rs []int64, sels []int64) []int64 {
	return i64ModSels(xs, ys, rs, sels)
}

// check if 0 exists before the function call
func I64ModOneSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return i64ModOneSels(x, ys, rs, sels)
}

// check if 0 exists before the function call
func F64Mod(xs, ys, rs []float64) []float64 {
	return f64Mod(xs, ys, rs)
}

// check if 0 exists before the function call
func F64ModOne(x float64, ys, rs []float64) []float64 {
	return f64ModOnePure(x, ys, rs)
}

// check if 0 exists before the function call
func F64ModOneBy(x float64, ys, rs []float64) []float64 {
	return f64ModOneByPure(x, ys, rs)
}

// check if 0 exists before the function call
func F64ModSels(xs, ys, rs []float64, sels []int64) []float64 {
	return f64ModSels(xs, ys, rs, sels)
}

// check if 0 exists before the function call
func F64ModOneSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return f64ModOneSels(x, ys, rs, sels)
}

func i64ModPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func i64ModOnePure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func i64ModOneByPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func i64ModOneSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func i64ModSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func f64ModPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = math.Mod(x, ys[i])
	}
	return rs
}

func f64ModOnePure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = math.Mod(x, y)
	}
	return rs
}

func f64ModOneByPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = math.Mod(y, x)
	}
	return rs
}

func f64ModOneSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = math.Mod(x, ys[sel])
	}
	return rs
}

func f64ModSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = math.Mod(xs[sel], ys[sel])
	}
	return rs
}
