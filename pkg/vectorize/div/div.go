package div

var (
	i64DivOne     func(int64, []int64, []int64) []int64
	i64DivOneBy   func(int64, []int64, []int64) []int64
	i64Div        func([]int64, []int64, []int64) []int64
	i64DivOneSels func(int64, []int64, []int64, []int64) []int64
	i64DivSels    func([]int64, []int64, []int64, []int64) []int64

	f64DivOne     func(float64, []float64, []float64) []float64
	f64DivOneBy   func(float64, []float64, []float64) []float64
	f64Div        func([]float64, []float64, []float64) []float64
	f64DivOneSels func(float64, []float64, []float64, []int64) []float64
	f64DivSels    func([]float64, []float64, []float64, []int64) []float64
)

func init() {
	i64Div = i64DivPure
	i64DivOne = i64DivOnePure
	i64DivOneBy = i64DivOneByPure
	i64DivSels = i64DivSelsPure
	i64DivOneSels = i64DivOneSelsPure

	f64Div = f64DivPure
	f64DivOne = f64DivOnePure
	i64DivOneBy = i64DivOneByPure
	f64DivSels = f64DivSelsPure
	f64DivOneSels = f64DivOneSelsPure
}

// check if 0 exists before the function call
func I64Div(xs, ys, rs []int64) []int64 {
	return i64Div(xs, ys, rs)
}

// check if 0 exists before the function call
func I64DivOne(x int64, ys, rs []int64) []int64 {
	return i64DivOnePure(x, ys, rs)
}

// check if 0 exists before the function call
func I64DivOneBy(x int64, ys, rs []int64) []int64 {
	return i64DivOneByPure(x, ys, rs)
}

// check if 0 exists before the function call
func I64DivSels(xs, ys, rs []int64, sels []int64) []int64 {
	return i64DivSels(xs, ys, rs, sels)
}

// check if 0 exists before the function call
func I64DivOneSels(x int64, ys, rs []int64, sels []int64) []int64 {
	return i64DivOneSels(x, ys, rs, sels)
}

// check if 0 exists before the function call
func F64Div(xs, ys, rs []float64) []float64 {
	return f64Div(xs, ys, rs)
}

// check if 0 exists before the function call
func F64DivOne(x float64, ys, rs []float64) []float64 {
	return f64DivOnePure(x, ys, rs)
}

// check if 0 exists before the function call
func F64DivOneBy(x float64, ys, rs []float64) []float64 {
	return f64DivOneByPure(x, ys, rs)
}

// check if 0 exists before the function call
func F64DivSels(xs, ys, rs []float64, sels []int64) []float64 {
	return f64DivSels(xs, ys, rs, sels)
}

// check if 0 exists before the function call
func F64DivOneSels(x float64, ys, rs []float64, sels []int64) []float64 {
	return f64DivOneSels(x, ys, rs, sels)
}

func i64DivPure(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func i64DivOnePure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func i64DivOneByPure(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func i64DivOneSelsPure(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func i64DivSelsPure(xs, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func f64DivPure(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func f64DivOnePure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func f64DivOneByPure(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func f64DivOneSelsPure(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func f64DivSelsPure(xs, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}
