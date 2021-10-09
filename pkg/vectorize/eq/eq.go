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

package eq

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

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
	Int8Eq = EqNumericGeneric[int8]
	Int8EqNullable = EqNumericNullableGeneric[int8]
	Int8EqSels = EqNumericSelsGeneric[int8]
	Int8EqNullableSels = EqNumericNullableSelsGeneric[int8]
	Int8EqScalar = EqNumericScalarGeneric[int8]
	Int8EqNullableScalar = EqNumericNullableScalarGeneric[int8]
	Int8EqScalarSels = EqNumericScalarSelsGeneric[int8]
	Int8EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[int8]

	Int16Eq = EqNumericGeneric[int16]
	Int16EqNullable = EqNumericNullableGeneric[int16]
	Int16EqSels = EqNumericSelsGeneric[int16]
	Int16EqNullableSels = EqNumericNullableSelsGeneric[int16]
	Int16EqScalar = EqNumericScalarGeneric[int16]
	Int16EqNullableScalar = EqNumericNullableScalarGeneric[int16]
	Int16EqScalarSels = EqNumericScalarSelsGeneric[int16]
	Int16EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[int16]

	Int32Eq = EqNumericGeneric[int32]
	Int32EqNullable = EqNumericNullableGeneric[int32]
	Int32EqSels = EqNumericSelsGeneric[int32]
	Int32EqNullableSels = EqNumericNullableSelsGeneric[int32]
	Int32EqScalar = EqNumericScalarGeneric[int32]
	Int32EqNullableScalar = EqNumericNullableScalarGeneric[int32]
	Int32EqScalarSels = EqNumericScalarSelsGeneric[int32]
	Int32EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[int32]

	Int64Eq = EqNumericGeneric[int64]
	Int64EqNullable = EqNumericNullableGeneric[int64]
	Int64EqSels = EqNumericSelsGeneric[int64]
	Int64EqNullableSels = EqNumericNullableSelsGeneric[int64]
	Int64EqScalar = EqNumericScalarGeneric[int64]
	Int64EqNullableScalar = EqNumericNullableScalarGeneric[int64]
	Int64EqScalarSels = EqNumericScalarSelsGeneric[int64]
	Int64EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[int64]

	Uint8Eq = EqNumericGeneric[uint8]
	Uint8EqNullable = EqNumericNullableGeneric[uint8]
	Uint8EqSels = EqNumericSelsGeneric[uint8]
	Uint8EqNullableSels = EqNumericNullableSelsGeneric[uint8]
	Uint8EqScalar = EqNumericScalarGeneric[uint8]
	Uint8EqNullableScalar = EqNumericNullableScalarGeneric[uint8]
	Uint8EqScalarSels = EqNumericScalarSelsGeneric[uint8]
	Uint8EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[uint8]

	Uint16Eq = EqNumericGeneric[uint16]
	Uint16EqNullable = EqNumericNullableGeneric[uint16]
	Uint16EqSels = EqNumericSelsGeneric[uint16]
	Uint16EqNullableSels = EqNumericNullableSelsGeneric[uint16]
	Uint16EqScalar = EqNumericScalarGeneric[uint16]
	Uint16EqNullableScalar = EqNumericNullableScalarGeneric[uint16]
	Uint16EqScalarSels = EqNumericScalarSelsGeneric[uint16]
	Uint16EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[uint16]

	Uint32Eq = EqNumericGeneric[uint32]
	Uint32EqNullable = EqNumericNullableGeneric[uint32]
	Uint32EqSels = EqNumericSelsGeneric[uint32]
	Uint32EqNullableSels = EqNumericNullableSelsGeneric[uint32]
	Uint32EqScalar = EqNumericScalarGeneric[uint32]
	Uint32EqNullableScalar = EqNumericNullableScalarGeneric[uint32]
	Uint32EqScalarSels = EqNumericScalarSelsGeneric[uint32]
	Uint32EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[uint32]

	Uint64Eq = EqNumericGeneric[uint64]
	Uint64EqNullable = EqNumericNullableGeneric[uint64]
	Uint64EqSels = EqNumericSelsGeneric[uint64]
	Uint64EqNullableSels = EqNumericNullableSelsGeneric[uint64]
	Uint64EqScalar = EqNumericScalarGeneric[uint64]
	Uint64EqNullableScalar = EqNumericNullableScalarGeneric[uint64]
	Uint64EqScalarSels = EqNumericScalarSelsGeneric[uint64]
	Uint64EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[uint64]

	Float32Eq = EqNumericGeneric[float32]
	Float32EqNullable = EqNumericNullableGeneric[float32]
	Float32EqSels = EqNumericSelsGeneric[float32]
	Float32EqNullableSels = EqNumericNullableSelsGeneric[float32]
	Float32EqScalar = EqNumericScalarGeneric[float32]
	Float32EqNullableScalar = EqNumericNullableScalarGeneric[float32]
	Float32EqScalarSels = EqNumericScalarSelsGeneric[float32]
	Float32EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[float32]

	Float64Eq = EqNumericGeneric[float64]
	Float64EqNullable = EqNumericNullableGeneric[float64]
	Float64EqSels = EqNumericSelsGeneric[float64]
	Float64EqNullableSels = EqNumericNullableSelsGeneric[float64]
	Float64EqScalar = EqNumericScalarGeneric[float64]
	Float64EqNullableScalar = EqNumericNullableScalarGeneric[float64]
	Float64EqScalarSels = EqNumericScalarSelsGeneric[float64]
	Float64EqNullableScalarSels = EqNumericNullableScalarSelsGeneric[float64]

	StrEq = EqStringGeneric
	StrEqNullable = EqStringNullableGeneric
	StrEqSels = EqStringSelsGeneric
	StrEqNullableSels = EqStringNullableSelsGeneric
	StrEqScalar = EqStringScalarGeneric
	StrEqNullableScalar = EqStringNullableScalarGeneric
	StrEqScalarSels = EqStringScalarSelsGeneric
	StrEqNullableScalarSels = EqStringNullableScalarSelsGeneric
}

func EqNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func EqNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func EqNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func EqStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
