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

package le

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

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
	Int8Le = LeNumericGeneric[int8]
	Int8LeNullable = LeNumericNullableGeneric[int8]
	Int8LeSels = LeNumericSelsGeneric[int8]
	Int8LeNullableSels = LeNumericNullableSelsGeneric[int8]
	Int8LeScalar = LeNumericScalarGeneric[int8]
	Int8LeNullableScalar = LeNumericNullableScalarGeneric[int8]
	Int8LeScalarSels = LeNumericScalarSelsGeneric[int8]
	Int8LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[int8]

	Int16Le = LeNumericGeneric[int16]
	Int16LeNullable = LeNumericNullableGeneric[int16]
	Int16LeSels = LeNumericSelsGeneric[int16]
	Int16LeNullableSels = LeNumericNullableSelsGeneric[int16]
	Int16LeScalar = LeNumericScalarGeneric[int16]
	Int16LeNullableScalar = LeNumericNullableScalarGeneric[int16]
	Int16LeScalarSels = LeNumericScalarSelsGeneric[int16]
	Int16LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[int16]

	Int32Le = LeNumericGeneric[int32]
	Int32LeNullable = LeNumericNullableGeneric[int32]
	Int32LeSels = LeNumericSelsGeneric[int32]
	Int32LeNullableSels = LeNumericNullableSelsGeneric[int32]
	Int32LeScalar = LeNumericScalarGeneric[int32]
	Int32LeNullableScalar = LeNumericNullableScalarGeneric[int32]
	Int32LeScalarSels = LeNumericScalarSelsGeneric[int32]
	Int32LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[int32]

	Int64Le = LeNumericGeneric[int64]
	Int64LeNullable = LeNumericNullableGeneric[int64]
	Int64LeSels = LeNumericSelsGeneric[int64]
	Int64LeNullableSels = LeNumericNullableSelsGeneric[int64]
	Int64LeScalar = LeNumericScalarGeneric[int64]
	Int64LeNullableScalar = LeNumericNullableScalarGeneric[int64]
	Int64LeScalarSels = LeNumericScalarSelsGeneric[int64]
	Int64LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[int64]

	Uint8Le = LeNumericGeneric[uint8]
	Uint8LeNullable = LeNumericNullableGeneric[uint8]
	Uint8LeSels = LeNumericSelsGeneric[uint8]
	Uint8LeNullableSels = LeNumericNullableSelsGeneric[uint8]
	Uint8LeScalar = LeNumericScalarGeneric[uint8]
	Uint8LeNullableScalar = LeNumericNullableScalarGeneric[uint8]
	Uint8LeScalarSels = LeNumericScalarSelsGeneric[uint8]
	Uint8LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[uint8]

	Uint16Le = LeNumericGeneric[uint16]
	Uint16LeNullable = LeNumericNullableGeneric[uint16]
	Uint16LeSels = LeNumericSelsGeneric[uint16]
	Uint16LeNullableSels = LeNumericNullableSelsGeneric[uint16]
	Uint16LeScalar = LeNumericScalarGeneric[uint16]
	Uint16LeNullableScalar = LeNumericNullableScalarGeneric[uint16]
	Uint16LeScalarSels = LeNumericScalarSelsGeneric[uint16]
	Uint16LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[uint16]

	Uint32Le = LeNumericGeneric[uint32]
	Uint32LeNullable = LeNumericNullableGeneric[uint32]
	Uint32LeSels = LeNumericSelsGeneric[uint32]
	Uint32LeNullableSels = LeNumericNullableSelsGeneric[uint32]
	Uint32LeScalar = LeNumericScalarGeneric[uint32]
	Uint32LeNullableScalar = LeNumericNullableScalarGeneric[uint32]
	Uint32LeScalarSels = LeNumericScalarSelsGeneric[uint32]
	Uint32LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[uint32]

	Uint64Le = LeNumericGeneric[uint64]
	Uint64LeNullable = LeNumericNullableGeneric[uint64]
	Uint64LeSels = LeNumericSelsGeneric[uint64]
	Uint64LeNullableSels = LeNumericNullableSelsGeneric[uint64]
	Uint64LeScalar = LeNumericScalarGeneric[uint64]
	Uint64LeNullableScalar = LeNumericNullableScalarGeneric[uint64]
	Uint64LeScalarSels = LeNumericScalarSelsGeneric[uint64]
	Uint64LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[uint64]

	Float32Le = LeNumericGeneric[float32]
	Float32LeNullable = LeNumericNullableGeneric[float32]
	Float32LeSels = LeNumericSelsGeneric[float32]
	Float32LeNullableSels = LeNumericNullableSelsGeneric[float32]
	Float32LeScalar = LeNumericScalarGeneric[float32]
	Float32LeNullableScalar = LeNumericNullableScalarGeneric[float32]
	Float32LeScalarSels = LeNumericScalarSelsGeneric[float32]
	Float32LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[float32]

	Float64Le = LeNumericGeneric[float64]
	Float64LeNullable = LeNumericNullableGeneric[float64]
	Float64LeSels = LeNumericSelsGeneric[float64]
	Float64LeNullableSels = LeNumericNullableSelsGeneric[float64]
	Float64LeScalar = LeNumericScalarGeneric[float64]
	Float64LeNullableScalar = LeNumericNullableScalarGeneric[float64]
	Float64LeScalarSels = LeNumericScalarSelsGeneric[float64]
	Float64LeNullableScalarSels = LeNumericNullableScalarSelsGeneric[float64]

	StrLe = LeStringGeneric
	StrLeNullable = LeStringNullableGeneric
	StrLeSels = LeStringSelsGeneric
	StrLeNullableSels = LeStringNullableSelsGeneric
	StrLeScalar = LeStringScalarGeneric
	StrLeNullableScalar = LeStringNullableScalarGeneric
	StrLeScalarSels = LeStringScalarSelsGeneric
	StrLeNullableScalarSels = LeStringNullableScalarSelsGeneric
}

func LeNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LeNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LeNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LeNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LeNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LeStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LeStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LeStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
