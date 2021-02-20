package mult

var (
	i64MultOne func(int64, []int64, []int64) []int64
	i64Mult    func([]int64, []int64, []int64) []int64

	f64MultOne func(float64, []float64, []float64) []float64
	f64Mult    func([]float64, []float64, []float64) []float64
)

func init() {
	i64Mult = i64MultPure
	i64MultOne = i64MultOnePure

	f64Mult = f64MultPure
	f64MultOne = f64MultOnePure
}

func I64Mult(xs, ys, rs []int64) []int64 {
	return i64Mult(xs, ys, rs)
}

func I64MultOne(x int64, ys, rs []int64) []int64 {
	return i64MultOnePure(x, ys, rs)
}

func F64Mult(xs, ys, rs []float64) []float64 {
	return f64Mult(xs, ys, rs)
}

func F64MultOne(x float64, ys, rs []float64) []float64 {
	return f64MultOnePure(x, ys, rs)
}

func i64MultPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func i64MultOnePure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func f64MultPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func f64MultOnePure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}
