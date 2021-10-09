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

package ne

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Ne                      func([]int8, []int8, []int64) []int64
	Int8NeNullable              func([]int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8NeSels                  func([]int8, []int8, []int64, []int64) []int64
	Int8NeNullableSels          func([]int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int8NeScalar                func(int8, []int8, []int64) []int64
	Int8NeNullableScalar        func(int8, []int8, *roaring.Bitmap, []int64) []int64
	Int8NeScalarSels            func(int8, []int8, []int64, []int64) []int64
	Int8NeNullableScalarSels    func(int8, []int8, *roaring.Bitmap, []int64, []int64) []int64
	Int16Ne                     func([]int16, []int16, []int64) []int64
	Int16NeNullable             func([]int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16NeSels                 func([]int16, []int16, []int64, []int64) []int64
	Int16NeNullableSels         func([]int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int16NeScalar               func(int16, []int16, []int64) []int64
	Int16NeNullableScalar       func(int16, []int16, *roaring.Bitmap, []int64) []int64
	Int16NeScalarSels           func(int16, []int16, []int64, []int64) []int64
	Int16NeNullableScalarSels   func(int16, []int16, *roaring.Bitmap, []int64, []int64) []int64
	Int32Ne                     func([]int32, []int32, []int64) []int64
	Int32NeNullable             func([]int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32NeSels                 func([]int32, []int32, []int64, []int64) []int64
	Int32NeNullableSels         func([]int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int32NeScalar               func(int32, []int32, []int64) []int64
	Int32NeNullableScalar       func(int32, []int32, *roaring.Bitmap, []int64) []int64
	Int32NeScalarSels           func(int32, []int32, []int64, []int64) []int64
	Int32NeNullableScalarSels   func(int32, []int32, *roaring.Bitmap, []int64, []int64) []int64
	Int64Ne                     func([]int64, []int64, []int64) []int64
	Int64NeNullable             func([]int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64NeSels                 func([]int64, []int64, []int64, []int64) []int64
	Int64NeNullableSels         func([]int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Int64NeScalar               func(int64, []int64, []int64) []int64
	Int64NeNullableScalar       func(int64, []int64, *roaring.Bitmap, []int64) []int64
	Int64NeScalarSels           func(int64, []int64, []int64, []int64) []int64
	Int64NeNullableScalarSels   func(int64, []int64, *roaring.Bitmap, []int64, []int64) []int64
	Uint8Ne                     func([]uint8, []uint8, []int64) []int64
	Uint8NeNullable             func([]uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8NeSels                 func([]uint8, []uint8, []int64, []int64) []int64
	Uint8NeNullableSels         func([]uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint8NeScalar               func(uint8, []uint8, []int64) []int64
	Uint8NeNullableScalar       func(uint8, []uint8, *roaring.Bitmap, []int64) []int64
	Uint8NeScalarSels           func(uint8, []uint8, []int64, []int64) []int64
	Uint8NeNullableScalarSels   func(uint8, []uint8, *roaring.Bitmap, []int64, []int64) []int64
	Uint16Ne                    func([]uint16, []uint16, []int64) []int64
	Uint16NeNullable            func([]uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16NeSels                func([]uint16, []uint16, []int64, []int64) []int64
	Uint16NeNullableSels        func([]uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint16NeScalar              func(uint16, []uint16, []int64) []int64
	Uint16NeNullableScalar      func(uint16, []uint16, *roaring.Bitmap, []int64) []int64
	Uint16NeScalarSels          func(uint16, []uint16, []int64, []int64) []int64
	Uint16NeNullableScalarSels  func(uint16, []uint16, *roaring.Bitmap, []int64, []int64) []int64
	Uint32Ne                    func([]uint32, []uint32, []int64) []int64
	Uint32NeNullable            func([]uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32NeSels                func([]uint32, []uint32, []int64, []int64) []int64
	Uint32NeNullableSels        func([]uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint32NeScalar              func(uint32, []uint32, []int64) []int64
	Uint32NeNullableScalar      func(uint32, []uint32, *roaring.Bitmap, []int64) []int64
	Uint32NeScalarSels          func(uint32, []uint32, []int64, []int64) []int64
	Uint32NeNullableScalarSels  func(uint32, []uint32, *roaring.Bitmap, []int64, []int64) []int64
	Uint64Ne                    func([]uint64, []uint64, []int64) []int64
	Uint64NeNullable            func([]uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64NeSels                func([]uint64, []uint64, []int64, []int64) []int64
	Uint64NeNullableSels        func([]uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Uint64NeScalar              func(uint64, []uint64, []int64) []int64
	Uint64NeNullableScalar      func(uint64, []uint64, *roaring.Bitmap, []int64) []int64
	Uint64NeScalarSels          func(uint64, []uint64, []int64, []int64) []int64
	Uint64NeNullableScalarSels  func(uint64, []uint64, *roaring.Bitmap, []int64, []int64) []int64
	Float32Ne                   func([]float32, []float32, []int64) []int64
	Float32NeNullable           func([]float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32NeSels               func([]float32, []float32, []int64, []int64) []int64
	Float32NeNullableSels       func([]float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float32NeScalar             func(float32, []float32, []int64) []int64
	Float32NeNullableScalar     func(float32, []float32, *roaring.Bitmap, []int64) []int64
	Float32NeScalarSels         func(float32, []float32, []int64, []int64) []int64
	Float32NeNullableScalarSels func(float32, []float32, *roaring.Bitmap, []int64, []int64) []int64
	Float64Ne                   func([]float64, []float64, []int64) []int64
	Float64NeNullable           func([]float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64NeSels               func([]float64, []float64, []int64, []int64) []int64
	Float64NeNullableSels       func([]float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	Float64NeScalar             func(float64, []float64, []int64) []int64
	Float64NeNullableScalar     func(float64, []float64, *roaring.Bitmap, []int64) []int64
	Float64NeScalarSels         func(float64, []float64, []int64, []int64) []int64
	Float64NeNullableScalarSels func(float64, []float64, *roaring.Bitmap, []int64, []int64) []int64
	StrNe                       func(*types.Bytes, *types.Bytes, []int64) []int64
	StrNeNullable               func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrNeSels                   func(*types.Bytes, *types.Bytes, []int64, []int64) []int64
	StrNeNullableSels           func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
	StrNeScalar                 func([]byte, *types.Bytes, []int64) []int64
	StrNeNullableScalar         func([]byte, *types.Bytes, *roaring.Bitmap, []int64) []int64
	StrNeScalarSels             func([]byte, *types.Bytes, []int64, []int64) []int64
	StrNeNullableScalarSels     func([]byte, *types.Bytes, *roaring.Bitmap, []int64, []int64) []int64
)

func init() {
	Int8Ne = NeNumericGeneric[int8]
	Int8NeNullable = NeNumericNullableGeneric[int8]
	Int8NeSels = NeNumericSelsGeneric[int8]
	Int8NeNullableSels = NeNumericNullableSelsGeneric[int8]
	Int8NeScalar = NeNumericScalarGeneric[int8]
	Int8NeNullableScalar = NeNumericNullableScalarGeneric[int8]
	Int8NeScalarSels = NeNumericScalarSelsGeneric[int8]
	Int8NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[int8]

	Int16Ne = NeNumericGeneric[int16]
	Int16NeNullable = NeNumericNullableGeneric[int16]
	Int16NeSels = NeNumericSelsGeneric[int16]
	Int16NeNullableSels = NeNumericNullableSelsGeneric[int16]
	Int16NeScalar = NeNumericScalarGeneric[int16]
	Int16NeNullableScalar = NeNumericNullableScalarGeneric[int16]
	Int16NeScalarSels = NeNumericScalarSelsGeneric[int16]
	Int16NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[int16]

	Int32Ne = NeNumericGeneric[int32]
	Int32NeNullable = NeNumericNullableGeneric[int32]
	Int32NeSels = NeNumericSelsGeneric[int32]
	Int32NeNullableSels = NeNumericNullableSelsGeneric[int32]
	Int32NeScalar = NeNumericScalarGeneric[int32]
	Int32NeNullableScalar = NeNumericNullableScalarGeneric[int32]
	Int32NeScalarSels = NeNumericScalarSelsGeneric[int32]
	Int32NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[int32]

	Int64Ne = NeNumericGeneric[int64]
	Int64NeNullable = NeNumericNullableGeneric[int64]
	Int64NeSels = NeNumericSelsGeneric[int64]
	Int64NeNullableSels = NeNumericNullableSelsGeneric[int64]
	Int64NeScalar = NeNumericScalarGeneric[int64]
	Int64NeNullableScalar = NeNumericNullableScalarGeneric[int64]
	Int64NeScalarSels = NeNumericScalarSelsGeneric[int64]
	Int64NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[int64]

	Uint8Ne = NeNumericGeneric[uint8]
	Uint8NeNullable = NeNumericNullableGeneric[uint8]
	Uint8NeSels = NeNumericSelsGeneric[uint8]
	Uint8NeNullableSels = NeNumericNullableSelsGeneric[uint8]
	Uint8NeScalar = NeNumericScalarGeneric[uint8]
	Uint8NeNullableScalar = NeNumericNullableScalarGeneric[uint8]
	Uint8NeScalarSels = NeNumericScalarSelsGeneric[uint8]
	Uint8NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[uint8]

	Uint16Ne = NeNumericGeneric[uint16]
	Uint16NeNullable = NeNumericNullableGeneric[uint16]
	Uint16NeSels = NeNumericSelsGeneric[uint16]
	Uint16NeNullableSels = NeNumericNullableSelsGeneric[uint16]
	Uint16NeScalar = NeNumericScalarGeneric[uint16]
	Uint16NeNullableScalar = NeNumericNullableScalarGeneric[uint16]
	Uint16NeScalarSels = NeNumericScalarSelsGeneric[uint16]
	Uint16NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[uint16]

	Uint32Ne = NeNumericGeneric[uint32]
	Uint32NeNullable = NeNumericNullableGeneric[uint32]
	Uint32NeSels = NeNumericSelsGeneric[uint32]
	Uint32NeNullableSels = NeNumericNullableSelsGeneric[uint32]
	Uint32NeScalar = NeNumericScalarGeneric[uint32]
	Uint32NeNullableScalar = NeNumericNullableScalarGeneric[uint32]
	Uint32NeScalarSels = NeNumericScalarSelsGeneric[uint32]
	Uint32NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[uint32]

	Uint64Ne = NeNumericGeneric[uint64]
	Uint64NeNullable = NeNumericNullableGeneric[uint64]
	Uint64NeSels = NeNumericSelsGeneric[uint64]
	Uint64NeNullableSels = NeNumericNullableSelsGeneric[uint64]
	Uint64NeScalar = NeNumericScalarGeneric[uint64]
	Uint64NeNullableScalar = NeNumericNullableScalarGeneric[uint64]
	Uint64NeScalarSels = NeNumericScalarSelsGeneric[uint64]
	Uint64NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[uint64]

	Float32Ne = NeNumericGeneric[float32]
	Float32NeNullable = NeNumericNullableGeneric[float32]
	Float32NeSels = NeNumericSelsGeneric[float32]
	Float32NeNullableSels = NeNumericNullableSelsGeneric[float32]
	Float32NeScalar = NeNumericScalarGeneric[float32]
	Float32NeNullableScalar = NeNumericNullableScalarGeneric[float32]
	Float32NeScalarSels = NeNumericScalarSelsGeneric[float32]
	Float32NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[float32]

	Float64Ne = NeNumericGeneric[float64]
	Float64NeNullable = NeNumericNullableGeneric[float64]
	Float64NeSels = NeNumericSelsGeneric[float64]
	Float64NeNullableSels = NeNumericNullableSelsGeneric[float64]
	Float64NeScalar = NeNumericScalarGeneric[float64]
	Float64NeNullableScalar = NeNumericNullableScalarGeneric[float64]
	Float64NeScalarSels = NeNumericScalarSelsGeneric[float64]
	Float64NeNullableScalarSels = NeNumericNullableScalarSelsGeneric[float64]

	StrNe = NeStringGeneric
	StrNeNullable = NeStringNullableGeneric
	StrNeSels = NeStringSelsGeneric
	StrNeNullableSels = NeStringNullableSelsGeneric
	StrNeScalar = NeStringScalarGeneric
	StrNeNullableScalar = NeStringNullableScalarGeneric
	StrNeScalarSels = NeStringScalarSelsGeneric
	StrNeNullableScalarSels = NeStringNullableScalarSelsGeneric
}

func NeNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Compare(x, ys.Get(int64(i))) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NeStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
