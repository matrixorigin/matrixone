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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Eq                      = NumericEq[int8]
	Int8EqNullable              = NumericEqNullable[int8]
	Int8EqSels                  = NumericEqSels[int8]
	Int8EqNullableSels          = NumericEqNullableSels[int8]
	Int8EqScalar                = NumericEqScalar[int8]
	Int8EqNullableScalar        = NumericEqNullableScalar[int8]
	Int8EqScalarSels            = NumericEqScalarSels[int8]
	Int8EqNullableScalarSels    = NumericEqNullableScalarSels[int8]
	Int16Eq                     = NumericEq[int16]
	Int16EqNullable             = NumericEqNullable[int16]
	Int16EqSels                 = NumericEqSels[int16]
	Int16EqNullableSels         = NumericEqNullableSels[int16]
	Int16EqScalar               = NumericEqScalar[int16]
	Int16EqNullableScalar       = NumericEqNullableScalar[int16]
	Int16EqScalarSels           = NumericEqScalarSels[int16]
	Int16EqNullableScalarSels   = NumericEqNullableScalarSels[int16]
	Int32Eq                     = NumericEq[int32]
	Int32EqNullable             = NumericEqNullable[int32]
	Int32EqSels                 = NumericEqSels[int32]
	Int32EqNullableSels         = NumericEqNullableSels[int32]
	Int32EqScalar               = NumericEqScalar[int32]
	Int32EqNullableScalar       = NumericEqNullableScalar[int32]
	Int32EqScalarSels           = NumericEqScalarSels[int32]
	Int32EqNullableScalarSels   = NumericEqNullableScalarSels[int32]
	Int64Eq                     = NumericEq[int64]
	Int64EqNullable             = NumericEqNullable[int64]
	Int64EqSels                 = NumericEqSels[int64]
	Int64EqNullableSels         = NumericEqNullableSels[int64]
	Int64EqScalar               = NumericEqScalar[int64]
	Int64EqNullableScalar       = NumericEqNullableScalar[int64]
	Int64EqScalarSels           = NumericEqScalarSels[int64]
	Int64EqNullableScalarSels   = NumericEqNullableScalarSels[int64]
	Uint8Eq                     = NumericEq[uint8]
	Uint8EqNullable             = NumericEqNullable[uint8]
	Uint8EqSels                 = NumericEqSels[uint8]
	Uint8EqNullableSels         = NumericEqNullableSels[uint8]
	Uint8EqScalar               = NumericEqScalar[uint8]
	Uint8EqNullableScalar       = NumericEqNullableScalar[uint8]
	Uint8EqScalarSels           = NumericEqScalarSels[uint8]
	Uint8EqNullableScalarSels   = NumericEqNullableScalarSels[uint8]
	Uint16Eq                    = NumericEq[uint16]
	Uint16EqNullable            = NumericEqNullable[uint16]
	Uint16EqSels                = NumericEqSels[uint16]
	Uint16EqNullableSels        = NumericEqNullableSels[uint16]
	Uint16EqScalar              = NumericEqScalar[uint16]
	Uint16EqNullableScalar      = NumericEqNullableScalar[uint16]
	Uint16EqScalarSels          = NumericEqScalarSels[uint16]
	Uint16EqNullableScalarSels  = NumericEqNullableScalarSels[uint16]
	Uint32Eq                    = NumericEq[uint32]
	Uint32EqNullable            = NumericEqNullable[uint32]
	Uint32EqSels                = NumericEqSels[uint32]
	Uint32EqNullableSels        = NumericEqNullableSels[uint32]
	Uint32EqScalar              = NumericEqScalar[uint32]
	Uint32EqNullableScalar      = NumericEqNullableScalar[uint32]
	Uint32EqScalarSels          = NumericEqScalarSels[uint32]
	Uint32EqNullableScalarSels  = NumericEqNullableScalarSels[uint32]
	Uint64Eq                    = NumericEq[uint64]
	Uint64EqNullable            = NumericEqNullable[uint64]
	Uint64EqSels                = NumericEqSels[uint64]
	Uint64EqNullableSels        = NumericEqNullableSels[uint64]
	Uint64EqScalar              = NumericEqScalar[uint64]
	Uint64EqNullableScalar      = NumericEqNullableScalar[uint64]
	Uint64EqScalarSels          = NumericEqScalarSels[uint64]
	Uint64EqNullableScalarSels  = NumericEqNullableScalarSels[uint64]
	Float32Eq                   = NumericEq[float32]
	Float32EqNullable           = NumericEqNullable[float32]
	Float32EqSels               = NumericEqSels[float32]
	Float32EqNullableSels       = NumericEqNullableSels[float32]
	Float32EqScalar             = NumericEqScalar[float32]
	Float32EqNullableScalar     = NumericEqNullableScalar[float32]
	Float32EqScalarSels         = NumericEqScalarSels[float32]
	Float32EqNullableScalarSels = NumericEqNullableScalarSels[float32]

	Float64Eq                   = float64Eq
	Float64EqNullable           = float64EqNullable
	Float64EqSels               = float64EqSels
	Float64EqNullableSels       = float64EqNullableSels
	Float64EqScalar             = float64EqScalar
	Float64EqNullableScalar     = float64EqNullableScalar
	Float64EqScalarSels         = float64EqScalarSels
	Float64EqNullableScalarSels = float64EqNullableScalarSels

	StrEq                   = strEq
	StrEqNullable           = strEqNullable
	StrEqSels               = strEqSels
	StrEqNullableSels       = strEqNullableSels
	StrEqScalar             = strEqScalar
	StrEqNullableScalar     = strEqNullableScalar
	StrEqScalarSels         = strEqScalarSels
	StrEqNullableScalarSels = strEqNullableScalarSels

	Decimal64Eq                    = decimal64Eq
	Decimal64EqNullable            = decimal64EqNullable
	Decimal64EqSels                = decimal64EqSels
	Decimal64EqNullableSels        = decimal64EqNullableSels
	Decimal64EqScalar              = decimal64EqScalar
	Decimal64EqNullableScalar      = decimal64EqNullableScalar
	Decimal64EqScalarSels          = decimal64EqScalarSels
	Decimal64EqNullableScalarSels  = decimal64EqNullableScalarSels
	Decimal128Eq                   = decimal128Eq
	Decimal128EqNullable           = decimal128EqNullable
	Decimal128EqSels               = decimal128EqSels
	Decimal128EqNullableSels       = decimal128EqNullableSels
	Decimal128EqScalar             = decimal128EqScalar
	Decimal128EqNullableScalar     = decimal128EqNullableScalar
	Decimal128EqScalarSels         = decimal128EqScalarSels
	Decimal128EqNullableScalarSels = decimal128EqNullableScalarSels
)

func NumericEq[T constraints.Integer | constraints.Float](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x == ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NumericEqNullable[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func NumericEqSels[T constraints.Integer | constraints.Float](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NumericEqNullableSels[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NumericEqScalar[T constraints.Integer | constraints.Float](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x == y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func NumericEqNullableScalar[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func NumericEqScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func NumericEqNullableScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x == ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

const tolerance = .00001

func float64Equal(x, y float64) bool {
	diff := math.Abs(x - y)
	mean := math.Abs(x + y)
	if math.IsNaN(diff / mean) {
		return true
	}
	return (diff / mean) < tolerance
}

func float64Eq(xs, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if float64Equal(x, ys[i]) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullable(xs, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if float64Equal(x, ys[i]) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqSels(xs, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if float64Equal(xs[sel], ys[sel]) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableSels(xs, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && float64Equal(xs[sel], ys[sel]) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqScalar(x float64, ys []float64, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if float64Equal(x, y) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableScalar(x float64, ys []float64, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if float64Equal(x, y) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqScalarSels(x float64, ys []float64, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if float64Equal(x, ys[sel]) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func float64EqNullableScalarSels(x float64, ys []float64, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && float64Equal(x, ys[sel]) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEq(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Equal(xs.Get(int64(i)), ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Equal(xs.Get(sel), ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if bytes.Equal(x, ys.Get(int64(i))) {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strEqNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Equal(x, ys.Get(sel)) {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64Eq(xs, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	// to compare two decimal values, first we need to align them to the same scale
	// for example,    							Decimal(20, 3)  Decimal(20, 5)
	// 				value: 						12.3 			123.45
	// 				internal representation: 	12300  			12345000
	// align to the same scale by scale the smaller scale decimal to the same scale as the bigger one:
	// 											1230000         12345000
	// 				then do integer comparison

	if xScale > yScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xScale - yScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ysScaled[i]) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else if xScale < yScale {
		xsScaled := make([]types.Decimal64, len(xs))
		scaleDiff := yScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, x := range xs {
			xsScaled[i] = types.ScaleDecimal64(x, scale)
		}
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(xsScaled[i], y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ys[i]) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64EqNullable(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, ys[i], xScale, yScale) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64EqSels(xs, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64EqNullableSels(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64EqScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	if xScale > yScale {
		ysScaled := make([]types.Decimal64, len(ys))
		for i, y := range ys {
			scaleDiff := xScale - yScale
			scale := int64(math.Pow10(int(scaleDiff)))
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, yScaled := range ysScaled {
			if types.CompareDecimal64Decimal64Aligned(x, yScaled) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else if xScale < yScale {
		scaleDiff := yScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		xScaled := types.ScaleDecimal64(x, scale)
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(xScaled, y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(x, y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64EqNullableScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, y, xScale, yScale) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64EqScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64EqNullableScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128Eq(xs, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	// to compare two decimal values, first we need to align them to the same scale
	// for example,    							Decimal(20, 3)  Decimal(20, 5)
	// 				value: 						12.3 			123.45
	// 				internal representation: 	12300  			12345000
	// align to the same scale by scale the smaller scale decimal to the same scale as the bigger one:
	// 											1230000         12345000
	// 				then do integer comparison

	if xScale > yScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xScale - yScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ysScaled[i]) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else if xScale < yScale {
		xsScaled := make([]types.Decimal128, len(xs))
		scaleDiff := yScale - xScale
		for i, x := range xs {
			xsScaled[i] = x
			for i := 0; i < int(scaleDiff); i++ {
				xsScaled[i] = types.ScaleDecimal128By10(xsScaled[i])
			}
		}
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(xsScaled[i], y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ys[i]) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128EqNullable(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, ys[i], xScale, yScale) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128EqSels(xs, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128EqNullableSels(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128EqScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	if xScale > yScale {
		ysScaled := make([]types.Decimal128, len(ys))
		for i, y := range ys {
			scaleDiff := xScale - yScale
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}

		}
		for i, yScaled := range ysScaled {
			if types.CompareDecimal128Decimal128Aligned(x, yScaled) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else if xScale < yScale {
		xScaled := x
		scaleDiff := yScale - xScale
		for i := 0; i < int(scaleDiff); i++ {
			xScaled = types.ScaleDecimal128By10(xScaled)
		}
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(xScaled, y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(x, y) == 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128EqNullableScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, y, xScale, yScale) == 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128EqScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128EqNullableScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) == 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
