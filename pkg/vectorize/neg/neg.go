package neg

var (
	Int8Neg    func([]int8, []int8) []int8
	Int16Neg   func([]int16, []int16) []int16
	Int32Neg   func([]int32, []int32) []int32
	Int64Neg   func([]int64, []int64) []int64
	Float32Neg func([]float32, []float32) []float32
	Float64Neg func([]float64, []float64) []float64
)

func int8Neg(xs, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int16Neg(xs, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int32Neg(xs, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func int64Neg(xs, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float32Neg(xs, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func float64Neg(xs, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}
