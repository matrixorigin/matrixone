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
	"github.com/matrixorigin/matrixone/pkg/container/types"

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
	Int8Ne = int8Ne
	Int8NeNullable = int8NeNullable
	Int8NeSels = int8NeSels
	Int8NeNullableSels = int8NeNullableSels
	Int8NeScalar = int8NeScalar
	Int8NeNullableScalar = int8NeNullableScalar
	Int8NeScalarSels = int8NeScalarSels
	Int8NeNullableScalarSels = int8NeNullableScalarSels
	Int16Ne = int16Ne
	Int16NeNullable = int16NeNullable
	Int16NeSels = int16NeSels
	Int16NeNullableSels = int16NeNullableSels
	Int16NeScalar = int16NeScalar
	Int16NeNullableScalar = int16NeNullableScalar
	Int16NeScalarSels = int16NeScalarSels
	Int16NeNullableScalarSels = int16NeNullableScalarSels
	Int32Ne = int32Ne
	Int32NeNullable = int32NeNullable
	Int32NeSels = int32NeSels
	Int32NeNullableSels = int32NeNullableSels
	Int32NeScalar = int32NeScalar
	Int32NeNullableScalar = int32NeNullableScalar
	Int32NeScalarSels = int32NeScalarSels
	Int32NeNullableScalarSels = int32NeNullableScalarSels
	Int64Ne = int64Ne
	Int64NeNullable = int64NeNullable
	Int64NeSels = int64NeSels
	Int64NeNullableSels = int64NeNullableSels
	Int64NeScalar = int64NeScalar
	Int64NeNullableScalar = int64NeNullableScalar
	Int64NeScalarSels = int64NeScalarSels
	Int64NeNullableScalarSels = int64NeNullableScalarSels
	Uint8Ne = uint8Ne
	Uint8NeNullable = uint8NeNullable
	Uint8NeSels = uint8NeSels
	Uint8NeNullableSels = uint8NeNullableSels
	Uint8NeScalar = uint8NeScalar
	Uint8NeNullableScalar = uint8NeNullableScalar
	Uint8NeScalarSels = uint8NeScalarSels
	Uint8NeNullableScalarSels = uint8NeNullableScalarSels
	Uint16Ne = uint16Ne
	Uint16NeNullable = uint16NeNullable
	Uint16NeSels = uint16NeSels
	Uint16NeNullableSels = uint16NeNullableSels
	Uint16NeScalar = uint16NeScalar
	Uint16NeNullableScalar = uint16NeNullableScalar
	Uint16NeScalarSels = uint16NeScalarSels
	Uint16NeNullableScalarSels = uint16NeNullableScalarSels
	Uint32Ne = uint32Ne
	Uint32NeNullable = uint32NeNullable
	Uint32NeSels = uint32NeSels
	Uint32NeNullableSels = uint32NeNullableSels
	Uint32NeScalar = uint32NeScalar
	Uint32NeNullableScalar = uint32NeNullableScalar
	Uint32NeScalarSels = uint32NeScalarSels
	Uint32NeNullableScalarSels = uint32NeNullableScalarSels
	Uint64Ne = uint64Ne
	Uint64NeNullable = uint64NeNullable
	Uint64NeSels = uint64NeSels
	Uint64NeNullableSels = uint64NeNullableSels
	Uint64NeScalar = uint64NeScalar
	Uint64NeNullableScalar = uint64NeNullableScalar
	Uint64NeScalarSels = uint64NeScalarSels
	Uint64NeNullableScalarSels = uint64NeNullableScalarSels
	Float32Ne = float32Ne
	Float32NeNullable = float32NeNullable
	Float32NeSels = float32NeSels
	Float32NeNullableSels = float32NeNullableSels
	Float32NeScalar = float32NeScalar
	Float32NeNullableScalar = float32NeNullableScalar
	Float32NeScalarSels = float32NeScalarSels
	Float32NeNullableScalarSels = float32NeNullableScalarSels
	Float64Ne = float64Ne
	Float64NeNullable = float64NeNullable
	Float64NeSels = float64NeSels
	Float64NeNullableSels = float64NeNullableSels
	Float64NeScalar = float64NeScalar
	Float64NeNullableScalar = float64NeNullableScalar
	Float64NeScalarSels = float64NeScalarSels
	Float64NeNullableScalarSels = float64NeNullableScalarSels
	StrNe = strNe
	StrNeNullable = strNeNullable
	StrNeSels = strNeSels
	StrNeNullableSels = strNeNullableSels
	StrNeScalar = strNeScalar
	StrNeNullableScalar = strNeNullableScalar
	StrNeScalarSels = strNeScalarSels
	StrNeNullableScalarSels = strNeNullableScalarSels
}

func int8Ne(xs, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8NeNullable(xs, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int8NeSels(xs, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8NeNullableSels(xs, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8NeScalar(x int8, ys []int8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int8NeNullableScalar(x int8, ys []int8, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int8NeScalarSels(x int8, ys []int8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int8NeNullableScalarSels(x int8, ys []int8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16Ne(xs, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16NeNullable(xs, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int16NeSels(xs, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16NeNullableSels(xs, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16NeScalar(x int16, ys []int16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int16NeNullableScalar(x int16, ys []int16, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int16NeScalarSels(x int16, ys []int16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int16NeNullableScalarSels(x int16, ys []int16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32Ne(xs, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32NeNullable(xs, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int32NeSels(xs, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32NeNullableSels(xs, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32NeScalar(x int32, ys []int32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int32NeNullableScalar(x int32, ys []int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int32NeScalarSels(x int32, ys []int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int32NeNullableScalarSels(x int32, ys []int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64Ne(xs, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64NeNullable(xs, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int64NeSels(xs, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64NeNullableSels(xs, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64NeScalar(x int64, ys []int64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func int64NeNullableScalar(x int64, ys []int64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func int64NeScalarSels(x int64, ys []int64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func int64NeNullableScalarSels(x int64, ys []int64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8Ne(xs, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8NeNullable(xs, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint8NeSels(xs, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8NeNullableSels(xs, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8NeScalar(x uint8, ys []uint8, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8NeNullableScalar(x uint8, ys []uint8, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint8NeScalarSels(x uint8, ys []uint8, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint8NeNullableScalarSels(x uint8, ys []uint8, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16Ne(xs, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16NeNullable(xs, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint16NeSels(xs, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16NeNullableSels(xs, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16NeScalar(x uint16, ys []uint16, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16NeNullableScalar(x uint16, ys []uint16, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint16NeScalarSels(x uint16, ys []uint16, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint16NeNullableScalarSels(x uint16, ys []uint16, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32Ne(xs, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32NeNullable(xs, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint32NeSels(xs, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32NeNullableSels(xs, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32NeScalar(x uint32, ys []uint32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32NeNullableScalar(x uint32, ys []uint32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint32NeScalarSels(x uint32, ys []uint32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint32NeNullableScalarSels(x uint32, ys []uint32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64Ne(xs, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64NeNullable(xs, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint64NeSels(xs, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64NeNullableSels(xs, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64NeScalar(x uint64, ys []uint64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64NeNullableScalar(x uint64, ys []uint64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func uint64NeScalarSels(x uint64, ys []uint64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func uint64NeNullableScalarSels(x uint64, ys []uint64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32Ne(xs, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32NeNullable(xs, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func float32NeSels(xs, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32NeNullableSels(xs, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32NeScalar(x float32, ys []float32, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float32NeNullableScalar(x float32, ys []float32, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func float32NeScalarSels(x float32, ys []float32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float32NeNullableScalarSels(x float32, ys []float32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64Ne(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64NeNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func float64NeSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64NeNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64NeScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64NeNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func float64NeScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64NeNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strNe(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if !bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if !bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && !bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if !bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if !bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strNeNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && !bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
