package lt

import (
	"bytes"
	"matrixone/pkg/container/types"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Lt                      func([]int8, []int8, []int64) []int64
	Int8LtNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8LtSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8LtNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8LtScalar                func(int8, []int8, []int64) []int64
	Int8LtNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8LtScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8LtNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Lt                     func([]int16, []int16, []int64) []int64
	Int16LtNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16LtSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16LtNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16LtScalar               func(int16, []int16, []int64) []int64
	Int16LtNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16LtScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16LtNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Lt                     func([]int32, []int32, []int64) []int64
	Int32LtNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32LtSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32LtNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32LtScalar               func(int32, []int32, []int64) []int64
	Int32LtNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32LtScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32LtNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Lt                     func([]int64, []int64, []int64) []int64
	Int64LtNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64LtSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64LtNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64LtScalar               func(int64, []int64, []int64) []int64
	Int64LtNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64LtScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64LtNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Lt                     func([]uint8, []uint8, []int64) []int64
	Uint8LtNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8LtSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8LtNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8LtScalar               func(uint8, []uint8, []int64) []int64
	Uint8LtNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8LtScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8LtNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Lt                    func([]uint16, []uint16, []int64) []int64
	Uint16LtNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16LtSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16LtNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16LtScalar              func(uint16, []uint16, []int64) []int64
	Uint16LtNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16LtScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16LtNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Lt                    func([]uint32, []uint32, []int64) []int64
	Uint32LtNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32LtSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32LtNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32LtScalar              func(uint32, []uint32, []int64) []int64
	Uint32LtNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32LtScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32LtNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Lt                    func([]uint64, []uint64, []int64) []int64
	Uint64LtNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64LtSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64LtNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64LtScalar              func(uint64, []uint64, []int64) []int64
	Uint64LtNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64LtScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64LtNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Lt                   func([]float32, []float32, []int64) []int64
	Float32LtNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32LtSels               func([]float32, []float32, []int64, []int64) []int64
	Float32LtNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32LtScalar             func(float32, []float32, []int64) []int64
	Float32LtNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32LtScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32LtNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Lt                   func([]float64, []float64, []int64) []int64
	Float64LtNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64LtSels               func([]float64, []float64, []int64, []int64) []int64
	Float64LtNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64LtScalar             func(float64, []float64, []int64) []int64
	Float64LtNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64LtScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64LtNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrLt                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrLtNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrLtSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrLtNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrLtScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrLtNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrLtScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrLtNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Lt = int8Lt
	Int8LtNullable = int8LtNullable
	Int8LtSels = int8LtSels
	Int8LtNullableSels = int8LtNullableSels
	Int8LtScalar = int8LtScalar
	Int8LtNullableScalar = int8LtNullableScalar
	Int8LtScalarSels = int8LtScalarSels
	Int8LtNullableScalarSels = int8LtNullableScalarSels
	Int16Lt = int16Lt
	Int16LtNullable = int16LtNullable
	Int16LtSels = int16LtSels
	Int16LtNullableSels = int16LtNullableSels
	Int16LtScalar = int16LtScalar
	Int16LtNullableScalar = int16LtNullableScalar
	Int16LtScalarSels = int16LtScalarSels
	Int16LtNullableScalarSels = int16LtNullableScalarSels
	Int32Lt = int32Lt
	Int32LtNullable = int32LtNullable
	Int32LtSels = int32LtSels
	Int32LtNullableSels = int32LtNullableSels
	Int32LtScalar = int32LtScalar
	Int32LtNullableScalar = int32LtNullableScalar
	Int32LtScalarSels = int32LtScalarSels
	Int32LtNullableScalarSels = int32LtNullableScalarSels
	Int64Lt = int64Lt
	Int64LtNullable = int64LtNullable
	Int64LtSels = int64LtSels
	Int64LtNullableSels = int64LtNullableSels
	Int64LtScalar = int64LtScalar
	Int64LtNullableScalar = int64LtNullableScalar
	Int64LtScalarSels = int64LtScalarSels
	Int64LtNullableScalarSels = int64LtNullableScalarSels
	Uint8Lt = uint8Lt
	Uint8LtNullable = uint8LtNullable
	Uint8LtSels = uint8LtSels
	Uint8LtNullableSels = uint8LtNullableSels
	Uint8LtScalar = uint8LtScalar
	Uint8LtNullableScalar = uint8LtNullableScalar
	Uint8LtScalarSels = uint8LtScalarSels
	Uint8LtNullableScalarSels = uint8LtNullableScalarSels
	Uint16Lt = uint16Lt
	Uint16LtNullable = uint16LtNullable
	Uint16LtSels = uint16LtSels
	Uint16LtNullableSels = uint16LtNullableSels
	Uint16LtScalar = uint16LtScalar
	Uint16LtNullableScalar = uint16LtNullableScalar
	Uint16LtScalarSels = uint16LtScalarSels
	Uint16LtNullableScalarSels = uint16LtNullableScalarSels
	Uint32Lt = uint32Lt
	Uint32LtNullable = uint32LtNullable
	Uint32LtSels = uint32LtSels
	Uint32LtNullableSels = uint32LtNullableSels
	Uint32LtScalar = uint32LtScalar
	Uint32LtNullableScalar = uint32LtNullableScalar
	Uint32LtScalarSels = uint32LtScalarSels
	Uint32LtNullableScalarSels = uint32LtNullableScalarSels
	Uint64Lt = uint64Lt
	Uint64LtNullable = uint64LtNullable
	Uint64LtSels = uint64LtSels
	Uint64LtNullableSels = uint64LtNullableSels
	Uint64LtScalar = uint64LtScalar
	Uint64LtNullableScalar = uint64LtNullableScalar
	Uint64LtScalarSels = uint64LtScalarSels
	Uint64LtNullableScalarSels = uint64LtNullableScalarSels
	Float32Lt = float32Lt
	Float32LtNullable = float32LtNullable
	Float32LtSels = float32LtSels
	Float32LtNullableSels = float32LtNullableSels
	Float32LtScalar = float32LtScalar
	Float32LtNullableScalar = float32LtNullableScalar
	Float32LtScalarSels = float32LtScalarSels
	Float32LtNullableScalarSels = float32LtNullableScalarSels
	Float64Lt = float64Lt
	Float64LtNullable = float64LtNullable
	Float64LtSels = float64LtSels
	Float64LtNullableSels = float64LtNullableSels
	Float64LtScalar = float64LtScalar
	Float64LtNullableScalar = float64LtNullableScalar
	Float64LtScalarSels = float64LtScalarSels
	Float64LtNullableScalarSels = float64LtNullableScalarSels
	StrLt = strLt
	StrLtNullable = strLtNullable
	StrLtSels = strLtSels
	StrLtNullableSels = strLtNullableSels
	StrLtScalar = strLtScalar
	StrLtNullableScalar = strLtNullableScalar
	StrLtScalarSels = strLtScalarSels
	StrLtNullableScalarSels = strLtNullableScalarSels
}

func int8Lt(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtNullable(xs, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtSels(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtNullableSels(xs, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtScalar(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtNullableScalar(x int8, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LtNullableScalarSels(x int8, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16Lt(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtNullable(xs, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtSels(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtNullableSels(xs, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtScalar(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtNullableScalar(x int16, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LtNullableScalarSels(x int16, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32Lt(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtNullable(xs, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtSels(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtNullableSels(xs, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtScalar(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtNullableScalar(x int32, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LtNullableScalarSels(x int32, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64Lt(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtNullable(xs, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtSels(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtNullableSels(xs, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtScalar(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtNullableScalar(x int64, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LtNullableScalarSels(x int64, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8Lt(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtNullable(xs, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtSels(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtNullableSels(xs, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtScalar(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtNullableScalar(x uint8, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LtNullableScalarSels(x uint8, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16Lt(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtNullable(xs, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtSels(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtNullableSels(xs, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtScalar(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtNullableScalar(x uint16, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LtNullableScalarSels(x uint16, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32Lt(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtNullable(xs, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtSels(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtNullableSels(xs, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtScalar(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtNullableScalar(x uint32, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LtNullableScalarSels(x uint32, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64Lt(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtNullable(xs, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtSels(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtNullableSels(xs, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtScalar(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtNullableScalar(x uint64, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LtNullableScalarSels(x uint64, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32Lt(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtNullable(xs, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtSels(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtNullableSels(xs, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtScalar(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtNullableScalar(x float32, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LtNullableScalarSels(x float32, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64Lt(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LtNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLt(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
