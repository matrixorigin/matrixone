package shuffle

import "matrixbase/pkg/container/types"

var (
	i8Shuffle  func([]int8, []int64) []int8
	i16Shuffle func([]int16, []int64) []int16
	i32Shuffle func([]int32, []int64) []int32
	i64Shuffle func([]int64, []int64) []int64

	ui8Shuffle  func([]uint8, []int64) []uint8
	ui16Shuffle func([]uint16, []int64) []uint16
	ui32Shuffle func([]uint32, []int64) []uint32
	ui64Shuffle func([]uint64, []int64) []uint64

	float32Shuffle func([]float32, []int64) []float32
	float64Shuffle func([]float64, []int64) []float64

	decimalShuffle func([]types.Decimal, []int64) []types.Decimal

	dateShuffle     func([]types.Date, []int64) []types.Date
	datetimeShuffle func([]types.Datetime, []int64) []types.Datetime

	tupleShuffle func([][]interface{}, []int64) [][]interface{}

	sShuffle func(*types.Bytes, []int64) *types.Bytes
)

func init() {
	i8Shuffle = i8ShufflePure
	i16Shuffle = i16ShufflePure
	i32Shuffle = i32ShufflePure
	i64Shuffle = i64ShufflePure

	ui8Shuffle = ui8ShufflePure
	ui16Shuffle = ui16ShufflePure
	ui32Shuffle = ui32ShufflePure
	ui64Shuffle = ui64ShufflePure

	float32Shuffle = float32ShufflePure
	float64Shuffle = float64ShufflePure

	decimalShuffle = decimalShufflePure

	dateShuffle = dateShufflePure
	datetimeShuffle = datetimeShufflePure

	tupleShuffle = tupleShufflePure

	sShuffle = sShufflePure
}

func I8Shuffle(vs []int8, sels []int64) []int8 {
	return i8Shuffle(vs, sels)
}

func I16Shuffle(vs []int16, sels []int64) []int16 {
	return i16Shuffle(vs, sels)
}

func I32Shuffle(vs []int32, sels []int64) []int32 {
	return i32Shuffle(vs, sels)
}

func I64Shuffle(vs []int64, sels []int64) []int64 {
	return i64Shuffle(vs, sels)
}

func Ui8Shuffle(vs []uint8, sels []int64) []uint8 {
	return ui8Shuffle(vs, sels)
}

func Ui16Shuffle(vs []uint16, sels []int64) []uint16 {
	return ui16Shuffle(vs, sels)
}

func Ui32Shuffle(vs []uint32, sels []int64) []uint32 {
	return ui32Shuffle(vs, sels)
}

func Ui64Shuffle(vs []uint64, sels []int64) []uint64 {
	return ui64Shuffle(vs, sels)
}

func Float32Shuffle(vs []float32, sels []int64) []float32 {
	return float32Shuffle(vs, sels)
}

func Float64Shuffle(vs []float64, sels []int64) []float64 {
	return float64Shuffle(vs, sels)
}

func DecimalShuffle(vs []types.Decimal, sels []int64) []types.Decimal {
	return decimalShuffle(vs, sels)
}

func DateShuffle(vs []types.Date, sels []int64) []types.Date {
	return dateShuffle(vs, sels)
}

func DatetimeShuffle(vs []types.Datetime, sels []int64) []types.Datetime {
	return datetimeShuffle(vs, sels)
}

func TupleShuffle(vs [][]interface{}, sels []int64) [][]interface{} {
	return tupleShuffle(vs, sels)
}

func SShuffle(vs *types.Bytes, sels []int64) *types.Bytes {
	return sShuffle(vs, sels)
}

func i8ShufflePure(vs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func i16ShufflePure(vs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func i32ShufflePure(vs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func i64ShufflePure(vs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func ui8ShufflePure(vs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func ui16ShufflePure(vs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func ui32ShufflePure(vs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func ui64ShufflePure(vs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func float32ShufflePure(vs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func float64ShufflePure(vs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func decimalShufflePure(vs []types.Decimal, sels []int64) []types.Decimal {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func dateShufflePure(vs []types.Date, sels []int64) []types.Date {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func datetimeShufflePure(vs []types.Datetime, sels []int64) []types.Datetime {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func tupleShufflePure(vs [][]interface{}, sels []int64) [][]interface{} {
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	return vs[:len(sels)]
}

func sShufflePure(vs *types.Bytes, sels []int64) *types.Bytes {
	os, ns := vs.Os, vs.Ns
	for i, sel := range sels {
		os[i] = os[sel]
		ns[i] = ns[sel]
	}
	vs.Os = os[:len(sels)]
	vs.Ns = ns[:len(sels)]
	return vs
}
