package neg

import "matrixbase/pkg/container/types"

var (
	i8Neg  func([]int8, []int8) []int8
	i16Neg func([]int16, []int16) []int16
	i32Neg func([]int32, []int32) []int32
	i64Neg func([]int64, []int64) []int64

	u8Neg  func([]uint8, []uint8) []uint8
	u16Neg func([]uint16, []uint16) []uint16
	u32Neg func([]uint32, []uint32) []uint32
	u64Neg func([]uint64, []uint64) []uint64

	f32Neg func([]float32, []float32) []float32
	f64Neg func([]float64, []float64) []float64

	decimalNeg func([]types.Decimal, []types.Decimal) []types.Decimal

	dateNeg     func([]types.Date, []types.Date) []types.Date
	datetimeNeg func([]types.Datetime, []types.Datetime) []types.Datetime
)

func init() {
	i8Neg = i8NegPure
	i16Neg = i16NegPure
	i32Neg = i32NegPure
	i64Neg = i64NegPure

	f32Neg = f32NegPure
	f64Neg = f64NegPure

	decimalNeg = decimalNegPure
}

func I8Neg(xs, rs []int8) []int8 {
	return i8Neg(xs, rs)
}

func I16Neg(xs, rs []int16) []int16 {
	return i16Neg(xs, rs)
}

func I32Neg(xs, rs []int32) []int32 {
	return i32Neg(xs, rs)
}

func I64Neg(xs, rs []int64) []int64 {
	return i64Neg(xs, rs)
}

func F32Neg(xs, rs []float32) []float32 {
	return f32Neg(xs, rs)
}

func F64Neg(xs, rs []float64) []float64 {
	return f64Neg(xs, rs)
}

func DecimalNeg(xs, rs []types.Decimal) []types.Decimal {
	return decimalNegPure(xs, rs)
}

func i8NegPure(xs, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func i16NegPure(xs, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func i32NegPure(xs, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func i64NegPure(xs, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = -x
	}
	return rs
}

func f32NegPure(xs, rs []float32) []float32 {
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

func decimalNegPure(xs, rs []types.Decimal) []types.Decimal {
	return rs
}
