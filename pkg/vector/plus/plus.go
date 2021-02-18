package plus

var (
	i64PlusOne func(int64, []int64, []int64) []int64
	i64Plus    func([]int64, []int64, []int64) []int64

	f64PlusOne func(float64, []float64, []float64) []float64
	f64Plus    func([]float64, []float64, []float64) []float64
)

func init() {
	i64Plus = i64PlusPure
	i64PlusOne = i64PlusOnePure

	f64Plus = f64PlusPure
	f64PlusOne = f64PlusOnePure
}

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
