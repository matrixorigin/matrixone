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

package lt

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

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
	Int8Lt = LtNumericGeneric[int8]
	Int8LtNullable = LtNumericNullableGeneric[int8]
	Int8LtSels = LtNumericSelsGeneric[int8]
	Int8LtNullableSels = LtNumericNullableSelsGeneric[int8]
	Int8LtScalar = LtNumericScalarGeneric[int8]
	Int8LtNullableScalar = LtNumericNullableScalarGeneric[int8]
	Int8LtScalarSels = LtNumericScalarSelsGeneric[int8]
	Int8LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[int8]

	Int16Lt = LtNumericGeneric[int16]
	Int16LtNullable = LtNumericNullableGeneric[int16]
	Int16LtSels = LtNumericSelsGeneric[int16]
	Int16LtNullableSels = LtNumericNullableSelsGeneric[int16]
	Int16LtScalar = LtNumericScalarGeneric[int16]
	Int16LtNullableScalar = LtNumericNullableScalarGeneric[int16]
	Int16LtScalarSels = LtNumericScalarSelsGeneric[int16]
	Int16LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[int16]

	Int32Lt = LtNumericGeneric[int32]
	Int32LtNullable = LtNumericNullableGeneric[int32]
	Int32LtSels = LtNumericSelsGeneric[int32]
	Int32LtNullableSels = LtNumericNullableSelsGeneric[int32]
	Int32LtScalar = LtNumericScalarGeneric[int32]
	Int32LtNullableScalar = LtNumericNullableScalarGeneric[int32]
	Int32LtScalarSels = LtNumericScalarSelsGeneric[int32]
	Int32LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[int32]

	Int64Lt = LtNumericGeneric[int64]
	Int64LtNullable = LtNumericNullableGeneric[int64]
	Int64LtSels = LtNumericSelsGeneric[int64]
	Int64LtNullableSels = LtNumericNullableSelsGeneric[int64]
	Int64LtScalar = LtNumericScalarGeneric[int64]
	Int64LtNullableScalar = LtNumericNullableScalarGeneric[int64]
	Int64LtScalarSels = LtNumericScalarSelsGeneric[int64]
	Int64LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[int64]

	Uint8Lt = LtNumericGeneric[uint8]
	Uint8LtNullable = LtNumericNullableGeneric[uint8]
	Uint8LtSels = LtNumericSelsGeneric[uint8]
	Uint8LtNullableSels = LtNumericNullableSelsGeneric[uint8]
	Uint8LtScalar = LtNumericScalarGeneric[uint8]
	Uint8LtNullableScalar = LtNumericNullableScalarGeneric[uint8]
	Uint8LtScalarSels = LtNumericScalarSelsGeneric[uint8]
	Uint8LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[uint8]

	Uint16Lt = LtNumericGeneric[uint16]
	Uint16LtNullable = LtNumericNullableGeneric[uint16]
	Uint16LtSels = LtNumericSelsGeneric[uint16]
	Uint16LtNullableSels = LtNumericNullableSelsGeneric[uint16]
	Uint16LtScalar = LtNumericScalarGeneric[uint16]
	Uint16LtNullableScalar = LtNumericNullableScalarGeneric[uint16]
	Uint16LtScalarSels = LtNumericScalarSelsGeneric[uint16]
	Uint16LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[uint16]

	Uint32Lt = LtNumericGeneric[uint32]
	Uint32LtNullable = LtNumericNullableGeneric[uint32]
	Uint32LtSels = LtNumericSelsGeneric[uint32]
	Uint32LtNullableSels = LtNumericNullableSelsGeneric[uint32]
	Uint32LtScalar = LtNumericScalarGeneric[uint32]
	Uint32LtNullableScalar = LtNumericNullableScalarGeneric[uint32]
	Uint32LtScalarSels = LtNumericScalarSelsGeneric[uint32]
	Uint32LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[uint32]

	Uint64Lt = LtNumericGeneric[uint64]
	Uint64LtNullable = LtNumericNullableGeneric[uint64]
	Uint64LtSels = LtNumericSelsGeneric[uint64]
	Uint64LtNullableSels = LtNumericNullableSelsGeneric[uint64]
	Uint64LtScalar = LtNumericScalarGeneric[uint64]
	Uint64LtNullableScalar = LtNumericNullableScalarGeneric[uint64]
	Uint64LtScalarSels = LtNumericScalarSelsGeneric[uint64]
	Uint64LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[uint64]

	Float32Lt = LtNumericGeneric[float32]
	Float32LtNullable = LtNumericNullableGeneric[float32]
	Float32LtSels = LtNumericSelsGeneric[float32]
	Float32LtNullableSels = LtNumericNullableSelsGeneric[float32]
	Float32LtScalar = LtNumericScalarGeneric[float32]
	Float32LtNullableScalar = LtNumericNullableScalarGeneric[float32]
	Float32LtScalarSels = LtNumericScalarSelsGeneric[float32]
	Float32LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[float32]

	Float64Lt = LtNumericGeneric[float64]
	Float64LtNullable = LtNumericNullableGeneric[float64]
	Float64LtSels = LtNumericSelsGeneric[float64]
	Float64LtNullableSels = LtNumericNullableSelsGeneric[float64]
	Float64LtScalar = LtNumericScalarGeneric[float64]
	Float64LtNullableScalar = LtNumericNullableScalarGeneric[float64]
	Float64LtScalarSels = LtNumericScalarSelsGeneric[float64]
	Float64LtNullableScalarSels = LtNumericNullableScalarSelsGeneric[float64]

	StrLt = LtStringGeneric
	StrLtNullable = LtStringNullableGeneric
	StrLtSels = LtStringSelsGeneric
	StrLtNullableSels = LtStringNullableSelsGeneric
	StrLtScalar = LtStringScalarGeneric
	StrLtNullableScalar = LtStringNullableScalarGeneric
	StrLtScalarSels = LtStringScalarSelsGeneric
	StrLtNullableScalarSels = LtStringNullableScalarSelsGeneric
}

func LtNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LtNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LtNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LtNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LtNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LtStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func LtStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func LtStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
