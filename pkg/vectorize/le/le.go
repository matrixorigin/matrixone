package le

import (
	"bytes"
	"matrixone/pkg/container/types"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Le                      func([]int8, []int8, []int64) []int64
	Int8LeNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8LeSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8LeNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8LeScalar                func(int8, []int8, []int64) []int64
	Int8LeNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8LeScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8LeNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Le                     func([]int16, []int16, []int64) []int64
	Int16LeNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16LeSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16LeNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16LeScalar               func(int16, []int16, []int64) []int64
	Int16LeNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16LeScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16LeNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Le                     func([]int32, []int32, []int64) []int64
	Int32LeNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32LeSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32LeNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32LeScalar               func(int32, []int32, []int64) []int64
	Int32LeNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32LeScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32LeNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Le                     func([]int64, []int64, []int64) []int64
	Int64LeNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64LeSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64LeNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64LeScalar               func(int64, []int64, []int64) []int64
	Int64LeNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64LeScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64LeNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Le                     func([]uint8, []uint8, []int64) []int64
	Uint8LeNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8LeSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8LeNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8LeScalar               func(uint8, []uint8, []int64) []int64
	Uint8LeNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8LeScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8LeNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Le                    func([]uint16, []uint16, []int64) []int64
	Uint16LeNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16LeSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16LeNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16LeScalar              func(uint16, []uint16, []int64) []int64
	Uint16LeNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16LeScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16LeNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Le                    func([]uint32, []uint32, []int64) []int64
	Uint32LeNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32LeSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32LeNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32LeScalar              func(uint32, []uint32, []int64) []int64
	Uint32LeNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32LeScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32LeNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Le                    func([]uint64, []uint64, []int64) []int64
	Uint64LeNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64LeSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64LeNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64LeScalar              func(uint64, []uint64, []int64) []int64
	Uint64LeNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64LeScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64LeNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Le                   func([]float32, []float32, []int64) []int64
	Float32LeNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32LeSels               func([]float32, []float32, []int64, []int64) []int64
	Float32LeNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32LeScalar             func(float32, []float32, []int64) []int64
	Float32LeNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32LeScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32LeNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Le                   func([]float64, []float64, []int64) []int64
	Float64LeNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64LeSels               func([]float64, []float64, []int64, []int64) []int64
	Float64LeNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64LeScalar             func(float64, []float64, []int64) []int64
	Float64LeNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64LeScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64LeNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrLe                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrLeNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrLeSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrLeNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrLeScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrLeNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrLeScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrLeNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Le = int8Le
	Int8LeNullable = int8LeNullable
	Int8LeSels = int8LeSels
	Int8LeNullableSels = int8LeNullableSels
	Int8LeScalar = int8LeScalar
	Int8LeNullableScalar = int8LeNullableScalar
	Int8LeScalarSels = int8LeScalarSels
	Int8LeNullableScalarSels = int8LeNullableScalarSels
	Int16Le = int16Le
	Int16LeNullable = int16LeNullable
	Int16LeSels = int16LeSels
	Int16LeNullableSels = int16LeNullableSels
	Int16LeScalar = int16LeScalar
	Int16LeNullableScalar = int16LeNullableScalar
	Int16LeScalarSels = int16LeScalarSels
	Int16LeNullableScalarSels = int16LeNullableScalarSels
	Int32Le = int32Le
	Int32LeNullable = int32LeNullable
	Int32LeSels = int32LeSels
	Int32LeNullableSels = int32LeNullableSels
	Int32LeScalar = int32LeScalar
	Int32LeNullableScalar = int32LeNullableScalar
	Int32LeScalarSels = int32LeScalarSels
	Int32LeNullableScalarSels = int32LeNullableScalarSels
	Int64Le = int64Le
	Int64LeNullable = int64LeNullable
	Int64LeSels = int64LeSels
	Int64LeNullableSels = int64LeNullableSels
	Int64LeScalar = int64LeScalar
	Int64LeNullableScalar = int64LeNullableScalar
	Int64LeScalarSels = int64LeScalarSels
	Int64LeNullableScalarSels = int64LeNullableScalarSels
	Uint8Le = uint8Le
	Uint8LeNullable = uint8LeNullable
	Uint8LeSels = uint8LeSels
	Uint8LeNullableSels = uint8LeNullableSels
	Uint8LeScalar = uint8LeScalar
	Uint8LeNullableScalar = uint8LeNullableScalar
	Uint8LeScalarSels = uint8LeScalarSels
	Uint8LeNullableScalarSels = uint8LeNullableScalarSels
	Uint16Le = uint16Le
	Uint16LeNullable = uint16LeNullable
	Uint16LeSels = uint16LeSels
	Uint16LeNullableSels = uint16LeNullableSels
	Uint16LeScalar = uint16LeScalar
	Uint16LeNullableScalar = uint16LeNullableScalar
	Uint16LeScalarSels = uint16LeScalarSels
	Uint16LeNullableScalarSels = uint16LeNullableScalarSels
	Uint32Le = uint32Le
	Uint32LeNullable = uint32LeNullable
	Uint32LeSels = uint32LeSels
	Uint32LeNullableSels = uint32LeNullableSels
	Uint32LeScalar = uint32LeScalar
	Uint32LeNullableScalar = uint32LeNullableScalar
	Uint32LeScalarSels = uint32LeScalarSels
	Uint32LeNullableScalarSels = uint32LeNullableScalarSels
	Uint64Le = uint64Le
	Uint64LeNullable = uint64LeNullable
	Uint64LeSels = uint64LeSels
	Uint64LeNullableSels = uint64LeNullableSels
	Uint64LeScalar = uint64LeScalar
	Uint64LeNullableScalar = uint64LeNullableScalar
	Uint64LeScalarSels = uint64LeScalarSels
	Uint64LeNullableScalarSels = uint64LeNullableScalarSels
	Float32Le = float32Le
	Float32LeNullable = float32LeNullable
	Float32LeSels = float32LeSels
	Float32LeNullableSels = float32LeNullableSels
	Float32LeScalar = float32LeScalar
	Float32LeNullableScalar = float32LeNullableScalar
	Float32LeScalarSels = float32LeScalarSels
	Float32LeNullableScalarSels = float32LeNullableScalarSels
	Float64Le = float64Le
	Float64LeNullable = float64LeNullable
	Float64LeSels = float64LeSels
	Float64LeNullableSels = float64LeNullableSels
	Float64LeScalar = float64LeScalar
	Float64LeNullableScalar = float64LeNullableScalar
	Float64LeScalarSels = float64LeScalarSels
	Float64LeNullableScalarSels = float64LeNullableScalarSels
	StrLe = strLe
	StrLeNullable = strLeNullable
	StrLeSels = strLeSels
	StrLeNullableSels = strLeNullableSels
	StrLeScalar = strLeScalar
	StrLeNullableScalar = strLeNullableScalar
	StrLeScalarSels = strLeScalarSels
	StrLeNullableScalarSels = strLeNullableScalarSels
}

func int8Le(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeNullable(xs, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeSels(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeNullableSels(xs, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeScalar(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeNullableScalar(x int8, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8LeNullableScalarSels(x int8, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16Le(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeNullable(xs, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeSels(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeNullableSels(xs, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeScalar(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeNullableScalar(x int16, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16LeNullableScalarSels(x int16, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32Le(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeNullable(xs, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeSels(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeNullableSels(xs, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeScalar(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeNullableScalar(x int32, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32LeNullableScalarSels(x int32, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64Le(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeNullable(xs, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeSels(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeNullableSels(xs, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeScalar(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeNullableScalar(x int64, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64LeNullableScalarSels(x int64, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8Le(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeNullable(xs, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeNullableSels(xs, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeNullableScalar(x uint8, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8LeNullableScalarSels(x uint8, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16Le(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeNullable(xs, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeNullableSels(xs, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeNullableScalar(x uint16, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16LeNullableScalarSels(x uint16, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32Le(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeNullable(xs, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeNullableSels(xs, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeNullableScalar(x uint32, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32LeNullableScalarSels(x uint32, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64Le(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeNullable(xs, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeNullableSels(xs, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeNullableScalar(x uint64, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64LeNullableScalarSels(x uint64, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32Le(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeNullable(xs, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeSels(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeNullableSels(xs, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeScalar(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeNullableScalar(x float32, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32LeNullableScalarSels(x float32, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64Le(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64LeNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLe(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
