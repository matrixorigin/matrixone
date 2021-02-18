package neg

var (
	i64Neg func([]int64, []int64) []int64
	f64Neg func([]float64, []float64) []float64
)

func init() {
	i64Neg = i64NegPure
	f64Neg = f64NegPure
}

func I64Neg(xs, rs []int64) []int64 {
	return i64Neg(xs, rs)
}

func F64Neg(xs, rs []float64) []float64 {
	return f64Neg(xs, rs)
}

func i64NegPure(xs, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func f64NegPure(xs, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}
