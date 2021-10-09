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

package ge

import (
	"bytes"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"

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
	Int8Ge = GeNumericGeneric[int8]
	Int8GeNullable = GeNumericNullableGeneric[int8]
	Int8GeSels = GeNumericSelsGeneric[int8]
	Int8GeNullableSels = GeNumericNullableSelsGeneric[int8]
	Int8GeScalar = GeNumericScalarGeneric[int8]
	Int8GeNullableScalar = GeNumericNullableScalarGeneric[int8]
	Int8GeScalarSels = GeNumericScalarSelsGeneric[int8]
	Int8GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[int8]

	Int16Ge = GeNumericGeneric[int16]
	Int16GeNullable = GeNumericNullableGeneric[int16]
	Int16GeSels = GeNumericSelsGeneric[int16]
	Int16GeNullableSels = GeNumericNullableSelsGeneric[int16]
	Int16GeScalar = GeNumericScalarGeneric[int16]
	Int16GeNullableScalar = GeNumericNullableScalarGeneric[int16]
	Int16GeScalarSels = GeNumericScalarSelsGeneric[int16]
	Int16GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[int16]

	Int32Ge = GeNumericGeneric[int32]
	Int32GeNullable = GeNumericNullableGeneric[int32]
	Int32GeSels = GeNumericSelsGeneric[int32]
	Int32GeNullableSels = GeNumericNullableSelsGeneric[int32]
	Int32GeScalar = GeNumericScalarGeneric[int32]
	Int32GeNullableScalar = GeNumericNullableScalarGeneric[int32]
	Int32GeScalarSels = GeNumericScalarSelsGeneric[int32]
	Int32GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[int32]

	Int64Ge = GeNumericGeneric[int64]
	Int64GeNullable = GeNumericNullableGeneric[int64]
	Int64GeSels = GeNumericSelsGeneric[int64]
	Int64GeNullableSels = GeNumericNullableSelsGeneric[int64]
	Int64GeScalar = GeNumericScalarGeneric[int64]
	Int64GeNullableScalar = GeNumericNullableScalarGeneric[int64]
	Int64GeScalarSels = GeNumericScalarSelsGeneric[int64]
	Int64GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[int64]

	Uint8Ge = GeNumericGeneric[uint8]
	Uint8GeNullable = GeNumericNullableGeneric[uint8]
	Uint8GeSels = GeNumericSelsGeneric[uint8]
	Uint8GeNullableSels = GeNumericNullableSelsGeneric[uint8]
	Uint8GeScalar = GeNumericScalarGeneric[uint8]
	Uint8GeNullableScalar = GeNumericNullableScalarGeneric[uint8]
	Uint8GeScalarSels = GeNumericScalarSelsGeneric[uint8]
	Uint8GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[uint8]

	Uint16Ge = GeNumericGeneric[uint16]
	Uint16GeNullable = GeNumericNullableGeneric[uint16]
	Uint16GeSels = GeNumericSelsGeneric[uint16]
	Uint16GeNullableSels = GeNumericNullableSelsGeneric[uint16]
	Uint16GeScalar = GeNumericScalarGeneric[uint16]
	Uint16GeNullableScalar = GeNumericNullableScalarGeneric[uint16]
	Uint16GeScalarSels = GeNumericScalarSelsGeneric[uint16]
	Uint16GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[uint16]

	Uint32Ge = GeNumericGeneric[uint32]
	Uint32GeNullable = GeNumericNullableGeneric[uint32]
	Uint32GeSels = GeNumericSelsGeneric[uint32]
	Uint32GeNullableSels = GeNumericNullableSelsGeneric[uint32]
	Uint32GeScalar = GeNumericScalarGeneric[uint32]
	Uint32GeNullableScalar = GeNumericNullableScalarGeneric[uint32]
	Uint32GeScalarSels = GeNumericScalarSelsGeneric[uint32]
	Uint32GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[uint32]

	Uint64Ge = GeNumericGeneric[uint64]
	Uint64GeNullable = GeNumericNullableGeneric[uint64]
	Uint64GeSels = GeNumericSelsGeneric[uint64]
	Uint64GeNullableSels = GeNumericNullableSelsGeneric[uint64]
	Uint64GeScalar = GeNumericScalarGeneric[uint64]
	Uint64GeNullableScalar = GeNumericNullableScalarGeneric[uint64]
	Uint64GeScalarSels = GeNumericScalarSelsGeneric[uint64]
	Uint64GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[uint64]

	Float32Ge = GeNumericGeneric[float32]
	Float32GeNullable = GeNumericNullableGeneric[float32]
	Float32GeSels = GeNumericSelsGeneric[float32]
	Float32GeNullableSels = GeNumericNullableSelsGeneric[float32]
	Float32GeScalar = GeNumericScalarGeneric[float32]
	Float32GeNullableScalar = GeNumericNullableScalarGeneric[float32]
	Float32GeScalarSels = GeNumericScalarSelsGeneric[float32]
	Float32GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[float32]

	Float64Ge = GeNumericGeneric[float64]
	Float64GeNullable = GeNumericNullableGeneric[float64]
	Float64GeSels = GeNumericSelsGeneric[float64]
	Float64GeNullableSels = GeNumericNullableSelsGeneric[float64]
	Float64GeScalar = GeNumericScalarGeneric[float64]
	Float64GeNullableScalar = GeNumericNullableScalarGeneric[float64]
	Float64GeScalarSels = GeNumericScalarSelsGeneric[float64]
	Float64GeNullableScalarSels = GeNumericNullableScalarSelsGeneric[float64]

	StrGe = GeStringGeneric
	StrGeNullable = GeStringNullableGeneric
	StrGeSels = GeStringSelsGeneric
	StrGeNullableSels = GeStringNullableSelsGeneric
	StrGeScalar = GeStringScalarGeneric
	StrGeNullableScalar = GeStringNullableScalarGeneric
	StrGeScalarSels = GeStringScalarSelsGeneric
	StrGeNullableScalarSels = GeStringNullableScalarSelsGeneric
}

func GeNumericGeneric[T vectorize.Numeric](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x >= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GeNumericNullableGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func GeNumericSelsGeneric[T vectorize.Numeric](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeNumericNullableSelsGeneric[T vectorize.Numeric](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeNumericScalarGeneric[T vectorize.Numeric](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x >= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GeNumericNullableScalarGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func GeNumericScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeNumericNullableScalarSelsGeneric[T vectorize.Numeric](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x >= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringGeneric(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringNullableGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func GeStringSelsGeneric(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringNullableSelsGeneric(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringScalarGeneric(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) >= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringNullableScalarGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func GeStringScalarSelsGeneric(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func GeStringNullableScalarSelsGeneric(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) >= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
