package ge

import (
	"bytes"
	"matrixone/pkg/container/types"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Ge                      func([]int8, []int8, []int64) []int64
	Int8GeNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8GeSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8GeNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8GeScalar                func(int8, []int8, []int64) []int64
	Int8GeNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8GeScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8GeNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Ge                     func([]int16, []int16, []int64) []int64
	Int16GeNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16GeSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16GeNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16GeScalar               func(int16, []int16, []int64) []int64
	Int16GeNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16GeScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16GeNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Ge                     func([]int32, []int32, []int64) []int64
	Int32GeNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32GeSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32GeNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32GeScalar               func(int32, []int32, []int64) []int64
	Int32GeNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32GeScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32GeNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Ge                     func([]int64, []int64, []int64) []int64
	Int64GeNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64GeSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64GeNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64GeScalar               func(int64, []int64, []int64) []int64
	Int64GeNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64GeScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64GeNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Ge                     func([]uint8, []uint8, []int64) []int64
	Uint8GeNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8GeSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8GeNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8GeScalar               func(uint8, []uint8, []int64) []int64
	Uint8GeNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8GeScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8GeNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Ge                    func([]uint16, []uint16, []int64) []int64
	Uint16GeNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16GeSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16GeNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16GeScalar              func(uint16, []uint16, []int64) []int64
	Uint16GeNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16GeScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16GeNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Ge                    func([]uint32, []uint32, []int64) []int64
	Uint32GeNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32GeSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32GeNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32GeScalar              func(uint32, []uint32, []int64) []int64
	Uint32GeNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32GeScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32GeNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Ge                    func([]uint64, []uint64, []int64) []int64
	Uint64GeNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64GeSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64GeNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64GeScalar              func(uint64, []uint64, []int64) []int64
	Uint64GeNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64GeScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64GeNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Ge                   func([]float32, []float32, []int64) []int64
	Float32GeNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32GeSels               func([]float32, []float32, []int64, []int64) []int64
	Float32GeNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32GeScalar             func(float32, []float32, []int64) []int64
	Float32GeNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32GeScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32GeNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Ge                   func([]float64, []float64, []int64) []int64
	Float64GeNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64GeSels               func([]float64, []float64, []int64, []int64) []int64
	Float64GeNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64GeScalar             func(float64, []float64, []int64) []int64
	Float64GeNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64GeScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64GeNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrGe                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrGeNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrGeSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrGeNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrGeScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrGeNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrGeScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrGeNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Ge = int8Ge
	Int8GeNullable = int8GeNullable
	Int8GeSels = int8GeSels
	Int8GeNullableSels = int8GeNullableSels
	Int8GeScalar = int8GeScalar
	Int8GeNullableScalar = int8GeNullableScalar
	Int8GeScalarSels = int8GeScalarSels
	Int8GeNullableScalarSels = int8GeNullableScalarSels
	Int16Ge = int16Ge
	Int16GeNullable = int16GeNullable
	Int16GeSels = int16GeSels
	Int16GeNullableSels = int16GeNullableSels
	Int16GeScalar = int16GeScalar
	Int16GeNullableScalar = int16GeNullableScalar
	Int16GeScalarSels = int16GeScalarSels
	Int16GeNullableScalarSels = int16GeNullableScalarSels
	Int32Ge = int32Ge
	Int32GeNullable = int32GeNullable
	Int32GeSels = int32GeSels
	Int32GeNullableSels = int32GeNullableSels
	Int32GeScalar = int32GeScalar
	Int32GeNullableScalar = int32GeNullableScalar
	Int32GeScalarSels = int32GeScalarSels
	Int32GeNullableScalarSels = int32GeNullableScalarSels
	Int64Ge = int64Ge
	Int64GeNullable = int64GeNullable
	Int64GeSels = int64GeSels
	Int64GeNullableSels = int64GeNullableSels
	Int64GeScalar = int64GeScalar
	Int64GeNullableScalar = int64GeNullableScalar
	Int64GeScalarSels = int64GeScalarSels
	Int64GeNullableScalarSels = int64GeNullableScalarSels
	Uint8Ge = uint8Ge
	Uint8GeNullable = uint8GeNullable
	Uint8GeSels = uint8GeSels
	Uint8GeNullableSels = uint8GeNullableSels
	Uint8GeScalar = uint8GeScalar
	Uint8GeNullableScalar = uint8GeNullableScalar
	Uint8GeScalarSels = uint8GeScalarSels
	Uint8GeNullableScalarSels = uint8GeNullableScalarSels
	Uint16Ge = uint16Ge
	Uint16GeNullable = uint16GeNullable
	Uint16GeSels = uint16GeSels
	Uint16GeNullableSels = uint16GeNullableSels
	Uint16GeScalar = uint16GeScalar
	Uint16GeNullableScalar = uint16GeNullableScalar
	Uint16GeScalarSels = uint16GeScalarSels
	Uint16GeNullableScalarSels = uint16GeNullableScalarSels
	Uint32Ge = uint32Ge
	Uint32GeNullable = uint32GeNullable
	Uint32GeSels = uint32GeSels
	Uint32GeNullableSels = uint32GeNullableSels
	Uint32GeScalar = uint32GeScalar
	Uint32GeNullableScalar = uint32GeNullableScalar
	Uint32GeScalarSels = uint32GeScalarSels
	Uint32GeNullableScalarSels = uint32GeNullableScalarSels
	Uint64Ge = uint64Ge
	Uint64GeNullable = uint64GeNullable
	Uint64GeSels = uint64GeSels
	Uint64GeNullableSels = uint64GeNullableSels
	Uint64GeScalar = uint64GeScalar
	Uint64GeNullableScalar = uint64GeNullableScalar
	Uint64GeScalarSels = uint64GeScalarSels
	Uint64GeNullableScalarSels = uint64GeNullableScalarSels
	Float32Ge = float32Ge
	Float32GeNullable = float32GeNullable
	Float32GeSels = float32GeSels
	Float32GeNullableSels = float32GeNullableSels
	Float32GeScalar = float32GeScalar
	Float32GeNullableScalar = float32GeNullableScalar
	Float32GeScalarSels = float32GeScalarSels
	Float32GeNullableScalarSels = float32GeNullableScalarSels
	Float64Ge = float64Ge
	Float64GeNullable = float64GeNullable
	Float64GeSels = float64GeSels
	Float64GeNullableSels = float64GeNullableSels
	Float64GeScalar = float64GeScalar
	Float64GeNullableScalar = float64GeNullableScalar
	Float64GeScalarSels = float64GeScalarSels
	Float64GeNullableScalarSels = float64GeNullableScalarSels
	StrGe = strGe
	StrGeNullable = strGeNullable
	StrGeSels = strGeSels
	StrGeNullableSels = strGeNullableSels
	StrGeScalar = strGeScalar
	StrGeNullableScalar = strGeNullableScalar
	StrGeScalarSels = strGeScalarSels
	StrGeNullableScalarSels = strGeNullableScalarSels
}

func int8Ge(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeNullable(xs, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeSels(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeNullableSels(xs, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeScalar(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeNullableScalar(x int8, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8GeNullableScalarSels(x int8, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16Ge(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeNullable(xs, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeSels(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeNullableSels(xs, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeScalar(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeNullableScalar(x int16, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16GeNullableScalarSels(x int16, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32Ge(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeNullable(xs, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeSels(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeNullableSels(xs, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeScalar(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeNullableScalar(x int32, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32GeNullableScalarSels(x int32, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64Ge(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeNullable(xs, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeSels(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeNullableSels(xs, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeScalar(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeNullableScalar(x int64, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64GeNullableScalarSels(x int64, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8Ge(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeNullable(xs, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeNullableSels(xs, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeNullableScalar(x uint8, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8GeNullableScalarSels(x uint8, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16Ge(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeNullable(xs, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeNullableSels(xs, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeNullableScalar(x uint16, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16GeNullableScalarSels(x uint16, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32Ge(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeNullable(xs, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeNullableSels(xs, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeNullableScalar(x uint32, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32GeNullableScalarSels(x uint32, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64Ge(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeNullable(xs, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeNullableSels(xs, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeNullableScalar(x uint64, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64GeNullableScalarSels(x uint64, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32Ge(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeNullable(xs, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeSels(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeNullableSels(xs, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeScalar(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeNullableScalar(x float32, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32GeNullableScalarSels(x float32, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64Ge(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64GeNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGe(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if bytes.Compare(xs.Get(sel), ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGeNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	nullsIter := nulls.Iterator()
	nextNull := int64(0)

	if nullsIter.HasNext() {
		nextNull = int64(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for _, sel := range sels {
		if sel == nextNull {
			if nullsIter.HasNext() {
				nextNull = int64(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else if bytes.Compare(x, ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
