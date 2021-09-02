package eq

import (
	"bytes"
	"matrixone/pkg/container/types"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Eq                      func([]int8, []int8, []int64) []int64
	Int8EqNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8EqSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8EqNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8EqScalar                func(int8, []int8, []int64) []int64
	Int8EqNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8EqScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8EqNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Eq                     func([]int16, []int16, []int64) []int64
	Int16EqNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16EqSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16EqNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16EqScalar               func(int16, []int16, []int64) []int64
	Int16EqNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16EqScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16EqNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Eq                     func([]int32, []int32, []int64) []int64
	Int32EqNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32EqSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32EqNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32EqScalar               func(int32, []int32, []int64) []int64
	Int32EqNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32EqScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32EqNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Eq                     func([]int64, []int64, []int64) []int64
	Int64EqNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64EqSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64EqNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64EqScalar               func(int64, []int64, []int64) []int64
	Int64EqNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64EqScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64EqNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Eq                     func([]uint8, []uint8, []int64) []int64
	Uint8EqNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8EqSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8EqNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8EqScalar               func(uint8, []uint8, []int64) []int64
	Uint8EqNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8EqScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8EqNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Eq                    func([]uint16, []uint16, []int64) []int64
	Uint16EqNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16EqSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16EqNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16EqScalar              func(uint16, []uint16, []int64) []int64
	Uint16EqNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16EqScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16EqNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Eq                    func([]uint32, []uint32, []int64) []int64
	Uint32EqNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32EqSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32EqNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32EqScalar              func(uint32, []uint32, []int64) []int64
	Uint32EqNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32EqScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32EqNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Eq                    func([]uint64, []uint64, []int64) []int64
	Uint64EqNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64EqSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64EqNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64EqScalar              func(uint64, []uint64, []int64) []int64
	Uint64EqNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64EqScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64EqNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Eq                   func([]float32, []float32, []int64) []int64
	Float32EqNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32EqSels               func([]float32, []float32, []int64, []int64) []int64
	Float32EqNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32EqScalar             func(float32, []float32, []int64) []int64
	Float32EqNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32EqScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32EqNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Eq                   func([]float64, []float64, []int64) []int64
	Float64EqNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64EqSels               func([]float64, []float64, []int64, []int64) []int64
	Float64EqNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64EqScalar             func(float64, []float64, []int64) []int64
	Float64EqNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64EqScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64EqNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrEq                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrEqNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrEqSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrEqNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrEqScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrEqNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrEqScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrEqNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Eq = int8Eq
	Int8EqNullable = int8EqNullable
	Int8EqSels = int8EqSels
	Int8EqNullableSels = int8EqNullableSels
	Int8EqScalar = int8EqScalar
	Int8EqNullableScalar = int8EqNullableScalar
	Int8EqScalarSels = int8EqScalarSels
	Int8EqNullableScalarSels = int8EqNullableScalarSels
	Int16Eq = int16Eq
	Int16EqNullable = int16EqNullable
	Int16EqSels = int16EqSels
	Int16EqNullableSels = int16EqNullableSels
	Int16EqScalar = int16EqScalar
	Int16EqNullableScalar = int16EqNullableScalar
	Int16EqScalarSels = int16EqScalarSels
	Int16EqNullableScalarSels = int16EqNullableScalarSels
	Int32Eq = int32Eq
	Int32EqNullable = int32EqNullable
	Int32EqSels = int32EqSels
	Int32EqNullableSels = int32EqNullableSels
	Int32EqScalar = int32EqScalar
	Int32EqNullableScalar = int32EqNullableScalar
	Int32EqScalarSels = int32EqScalarSels
	Int32EqNullableScalarSels = int32EqNullableScalarSels
	Int64Eq = int64Eq
	Int64EqNullable = int64EqNullable
	Int64EqSels = int64EqSels
	Int64EqNullableSels = int64EqNullableSels
	Int64EqScalar = int64EqScalar
	Int64EqNullableScalar = int64EqNullableScalar
	Int64EqScalarSels = int64EqScalarSels
	Int64EqNullableScalarSels = int64EqNullableScalarSels
	Uint8Eq = uint8Eq
	Uint8EqNullable = uint8EqNullable
	Uint8EqSels = uint8EqSels
	Uint8EqNullableSels = uint8EqNullableSels
	Uint8EqScalar = uint8EqScalar
	Uint8EqNullableScalar = uint8EqNullableScalar
	Uint8EqScalarSels = uint8EqScalarSels
	Uint8EqNullableScalarSels = uint8EqNullableScalarSels
	Uint16Eq = uint16Eq
	Uint16EqNullable = uint16EqNullable
	Uint16EqSels = uint16EqSels
	Uint16EqNullableSels = uint16EqNullableSels
	Uint16EqScalar = uint16EqScalar
	Uint16EqNullableScalar = uint16EqNullableScalar
	Uint16EqScalarSels = uint16EqScalarSels
	Uint16EqNullableScalarSels = uint16EqNullableScalarSels
	Uint32Eq = uint32Eq
	Uint32EqNullable = uint32EqNullable
	Uint32EqSels = uint32EqSels
	Uint32EqNullableSels = uint32EqNullableSels
	Uint32EqScalar = uint32EqScalar
	Uint32EqNullableScalar = uint32EqNullableScalar
	Uint32EqScalarSels = uint32EqScalarSels
	Uint32EqNullableScalarSels = uint32EqNullableScalarSels
	Uint64Eq = uint64Eq
	Uint64EqNullable = uint64EqNullable
	Uint64EqSels = uint64EqSels
	Uint64EqNullableSels = uint64EqNullableSels
	Uint64EqScalar = uint64EqScalar
	Uint64EqNullableScalar = uint64EqNullableScalar
	Uint64EqScalarSels = uint64EqScalarSels
	Uint64EqNullableScalarSels = uint64EqNullableScalarSels
	Float32Eq = float32Eq
	Float32EqNullable = float32EqNullable
	Float32EqSels = float32EqSels
	Float32EqNullableSels = float32EqNullableSels
	Float32EqScalar = float32EqScalar
	Float32EqNullableScalar = float32EqNullableScalar
	Float32EqScalarSels = float32EqScalarSels
	Float32EqNullableScalarSels = float32EqNullableScalarSels
	Float64Eq = float64Eq
	Float64EqNullable = float64EqNullable
	Float64EqSels = float64EqSels
	Float64EqNullableSels = float64EqNullableSels
	Float64EqScalar = float64EqScalar
	Float64EqNullableScalar = float64EqNullableScalar
	Float64EqScalarSels = float64EqScalarSels
	Float64EqNullableScalarSels = float64EqNullableScalarSels
	StrEq = strEq
	StrEqNullable = strEqNullable
	StrEqSels = strEqSels
	StrEqNullableSels = strEqNullableSels
	StrEqScalar = strEqScalar
	StrEqNullableScalar = strEqNullableScalar
	StrEqScalarSels = strEqScalarSels
	StrEqNullableScalarSels = strEqNullableScalarSels
}

func int8Eq(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqNullable(xs, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqSels(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqNullableSels(xs, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqScalar(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqNullableScalar(x int8, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8EqNullableScalarSels(x int8, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16Eq(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqNullable(xs, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqSels(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqNullableSels(xs, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqScalar(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqNullableScalar(x int16, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16EqNullableScalarSels(x int16, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32Eq(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqNullable(xs, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqSels(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqNullableSels(xs, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqScalar(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqNullableScalar(x int32, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32EqNullableScalarSels(x int32, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64Eq(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqNullable(xs, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqSels(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqNullableSels(xs, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqScalar(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqNullableScalar(x int64, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64EqNullableScalarSels(x int64, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8Eq(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqNullable(xs, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqSels(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqNullableSels(xs, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqScalar(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqNullableScalar(x uint8, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8EqNullableScalarSels(x uint8, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16Eq(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqNullable(xs, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqSels(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqNullableSels(xs, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqScalar(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqNullableScalar(x uint16, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16EqNullableScalarSels(x uint16, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32Eq(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqNullable(xs, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqSels(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqNullableSels(xs, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqScalar(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqNullableScalar(x uint32, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32EqNullableScalarSels(x uint32, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64Eq(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqNullable(xs, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqSels(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqNullableSels(xs, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqScalar(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqNullableScalar(x uint64, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64EqNullableScalarSels(x uint64, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32Eq(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqNullable(xs, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqSels(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqNullableSels(xs, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqScalar(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqNullableScalar(x float32, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32EqNullableScalarSels(x float32, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64Eq(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, x := range xs {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, y := range ys {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEq(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
