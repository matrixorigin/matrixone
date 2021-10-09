// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gt

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Gt                      func([]int8, []int8, []int64) []int64
	Int8GtNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8GtSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8GtNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8GtScalar                func(int8, []int8, []int64) []int64
	Int8GtNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8GtScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8GtNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Gt                     func([]int16, []int16, []int64) []int64
	Int16GtNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16GtSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16GtNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16GtScalar               func(int16, []int16, []int64) []int64
	Int16GtNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16GtScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16GtNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Gt                     func([]int32, []int32, []int64) []int64
	Int32GtNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32GtSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32GtNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32GtScalar               func(int32, []int32, []int64) []int64
	Int32GtNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32GtScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32GtNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Gt                     func([]int64, []int64, []int64) []int64
	Int64GtNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64GtSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64GtNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64GtScalar               func(int64, []int64, []int64) []int64
	Int64GtNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64GtScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64GtNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Gt                     func([]uint8, []uint8, []int64) []int64
	Uint8GtNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8GtSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8GtNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8GtScalar               func(uint8, []uint8, []int64) []int64
	Uint8GtNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8GtScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8GtNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Gt                    func([]uint16, []uint16, []int64) []int64
	Uint16GtNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16GtSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16GtNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16GtScalar              func(uint16, []uint16, []int64) []int64
	Uint16GtNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16GtScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16GtNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Gt                    func([]uint32, []uint32, []int64) []int64
	Uint32GtNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32GtSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32GtNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32GtScalar              func(uint32, []uint32, []int64) []int64
	Uint32GtNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32GtScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32GtNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Gt                    func([]uint64, []uint64, []int64) []int64
	Uint64GtNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64GtSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64GtNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64GtScalar              func(uint64, []uint64, []int64) []int64
	Uint64GtNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64GtScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64GtNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Gt                   func([]float32, []float32, []int64) []int64
	Float32GtNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32GtSels               func([]float32, []float32, []int64, []int64) []int64
	Float32GtNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32GtScalar             func(float32, []float32, []int64) []int64
	Float32GtNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32GtScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32GtNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Gt                   func([]float64, []float64, []int64) []int64
	Float64GtNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64GtSels               func([]float64, []float64, []int64, []int64) []int64
	Float64GtNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64GtScalar             func(float64, []float64, []int64) []int64
	Float64GtNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64GtScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64GtNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrGt                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrGtNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrGtSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrGtNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrGtScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrGtNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrGtScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrGtNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Gt = GtNumericGeneric[int8]
	Int8GtNullable = GtNumericNullableGeneric[int8]
	Int8GtSels = GtNumericSelsGeneric[int8]
	Int8GtNullableSels = GtNumericNullableSelsGeneric[int8]
	Int8GtScalar = GtNumericScalarGeneric[int8]
	Int8GtNullableScalar = GtNumericNullableScalarGeneric[int8]
	Int8GtScalarSels = GtNumericScalarSelsGeneric[int8]
	Int8GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[int8]

	Int16Gt = GtNumericGeneric[int16]
	Int16GtNullable = GtNumericNullableGeneric[int16]
	Int16GtSels = GtNumericSelsGeneric[int16]
	Int16GtNullableSels = GtNumericNullableSelsGeneric[int16]
	Int16GtScalar = GtNumericScalarGeneric[int16]
	Int16GtNullableScalar = GtNumericNullableScalarGeneric[int16]
	Int16GtScalarSels = GtNumericScalarSelsGeneric[int16]
	Int16GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[int16]

	Int32Gt = GtNumericGeneric[int32]
	Int32GtNullable = GtNumericNullableGeneric[int32]
	Int32GtSels = GtNumericSelsGeneric[int32]
	Int32GtNullableSels = GtNumericNullableSelsGeneric[int32]
	Int32GtScalar = GtNumericScalarGeneric[int32]
	Int32GtNullableScalar = GtNumericNullableScalarGeneric[int32]
	Int32GtScalarSels = GtNumericScalarSelsGeneric[int32]
	Int32GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[int32]

	Int64Gt = GtNumericGeneric[int64]
	Int64GtNullable = GtNumericNullableGeneric[int64]
	Int64GtSels = GtNumericSelsGeneric[int64]
	Int64GtNullableSels = GtNumericNullableSelsGeneric[int64]
	Int64GtScalar = GtNumericScalarGeneric[int64]
	Int64GtNullableScalar = GtNumericNullableScalarGeneric[int64]
	Int64GtScalarSels = GtNumericScalarSelsGeneric[int64]
	Int64GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[int64]

	Uint8Gt = GtNumericGeneric[uint8]
	Uint8GtNullable = GtNumericNullableGeneric[uint8]
	Uint8GtSels = GtNumericSelsGeneric[uint8]
	Uint8GtNullableSels = GtNumericNullableSelsGeneric[uint8]
	Uint8GtScalar = GtNumericScalarGeneric[uint8]
	Uint8GtNullableScalar = GtNumericNullableScalarGeneric[uint8]
	Uint8GtScalarSels = GtNumericScalarSelsGeneric[uint8]
	Uint8GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[uint8]

	Uint16Gt = GtNumericGeneric[uint16]
	Uint16GtNullable = GtNumericNullableGeneric[uint16]
	Uint16GtSels = GtNumericSelsGeneric[uint16]
	Uint16GtNullableSels = GtNumericNullableSelsGeneric[uint16]
	Uint16GtScalar = GtNumericScalarGeneric[uint16]
	Uint16GtNullableScalar = GtNumericNullableScalarGeneric[uint16]
	Uint16GtScalarSels = GtNumericScalarSelsGeneric[uint16]
	Uint16GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[uint16]

	Uint32Gt = GtNumericGeneric[uint32]
	Uint32GtNullable = GtNumericNullableGeneric[uint32]
	Uint32GtSels = GtNumericSelsGeneric[uint32]
	Uint32GtNullableSels = GtNumericNullableSelsGeneric[uint32]
	Uint32GtScalar = GtNumericScalarGeneric[uint32]
	Uint32GtNullableScalar = GtNumericNullableScalarGeneric[uint32]
	Uint32GtScalarSels = GtNumericScalarSelsGeneric[uint32]
	Uint32GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[uint32]

	Uint64Gt = GtNumericGeneric[uint64]
	Uint64GtNullable = GtNumericNullableGeneric[uint64]
	Uint64GtSels = GtNumericSelsGeneric[uint64]
	Uint64GtNullableSels = GtNumericNullableSelsGeneric[uint64]
	Uint64GtScalar = GtNumericScalarGeneric[uint64]
	Uint64GtNullableScalar = GtNumericNullableScalarGeneric[uint64]
	Uint64GtScalarSels = GtNumericScalarSelsGeneric[uint64]
	Uint64GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[uint64]

	Float32Gt = GtNumericGeneric[float32]
	Float32GtNullable = GtNumericNullableGeneric[float32]
	Float32GtSels = GtNumericSelsGeneric[float32]
	Float32GtNullableSels = GtNumericNullableSelsGeneric[float32]
	Float32GtScalar = GtNumericScalarGeneric[float32]
	Float32GtNullableScalar = GtNumericNullableScalarGeneric[float32]
	Float32GtScalarSels = GtNumericScalarSelsGeneric[float32]
	Float32GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[float32]

	Float64Gt = GtNumericGeneric[float64]
	Float64GtNullable = GtNumericNullableGeneric[float64]
	Float64GtSels = GtNumericSelsGeneric[float64]
	Float64GtNullableSels = GtNumericNullableSelsGeneric[float64]
	Float64GtScalar = GtNumericScalarGeneric[float64]
	Float64GtNullableScalar = GtNumericNullableScalarGeneric[float64]
	Float64GtScalarSels = GtNumericScalarSelsGeneric[float64]
	Float64GtNullableScalarSels = GtNumericNullableScalarSelsGeneric[float64]

	StrGt = GtStringGeneric
	StrGtNullable = GtStringNullableGeneric
	StrGtSels = GtStringSelsGeneric
	StrGtNullableSels = GtStringNullableSelsGeneric
	StrGtScalar = GtStringScalarGeneric
	StrGtNullableScalar = GtStringNullableScalarGeneric
	StrGtScalarSels = GtStringScalarSelsGeneric
	StrGtNullableScalarSels = GtStringNullableScalarSelsGeneric
}

func GtNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GtStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
