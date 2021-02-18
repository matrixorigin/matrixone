package minus

var (
	i64MinusOne   func(int64, []int64, []int64) []int64
	i64MinusOneBy func(int64, []int64, []int64) []int64
	i64Minus      func([]int64, []int64, []int64) []int64

	f64MinusOne   func(float64, []float64, []float64) []float64
	f64MinusOneBy func(float64, []float64, []float64) []float64
	f64Minus      func([]float64, []float64, []float64) []float64
)

func init() {
	i64Minus = i64MinusPure
	i64MinusOne = i64MinusOnePure
	i64MinusOneBy = i64MinusOneByPure

	f64Minus = f64MinusPure
	f64MinusOne = f64MinusOnePure
	i64MinusOneBy = i64MinusOneByPure
}

func I64Minus(xs, ys, rs []int64) []int64 {
	return i64Minus(xs, ys, rs)
}

func I64MinusOne(x int64, ys, rs []int64) []int64 {
	return i64MinusOnePure(x, ys, rs)
}

func I64MinusOneBy(x int64, ys, rs []int64) []int64 {
	return i64MinusOneByPure(x, ys, rs)
}

func F64Minus(xs, ys, rs []float64) []float64 {
	return f64Minus(xs, ys, rs)
}

func F64MinusOne(x float64, ys, rs []float64) []float64 {
	return f64MinusOnePure(x, ys, rs)
}

func F64MinusOneBy(x float64, ys, rs []float64) []float64 {
	return f64MinusOneByPure(x, ys, rs)
}

func i64MinusPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func i64MinusOnePure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func i64MinusOneByPure(x int64, ys, rs []int64) []int64 {
	for i, y := range rs {
		rs[i] = y - x
	}
	return rs
}

func f64MinusPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func f64MinusOnePure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func f64MinusOneByPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}
