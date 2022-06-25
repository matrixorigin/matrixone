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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Ne                      = numericNe[int8]
	Int8NeNullable              = numericNeNullable[int8]
	Int8NeSels                  = numericNeSels[int8]
	Int8NeNullableSels          = numericNeNullableSels[int8]
	Int8NeScalar                = numericNeScalar[int8]
	Int8NeNullableScalar        = numericNeNullableScalar[int8]
	Int8NeScalarSels            = numericNeScalarSels[int8]
	Int8NeNullableScalarSels    = numericNeNullableScalarSels[int8]
	Int16Ne                     = numericNe[int16]
	Int16NeNullable             = numericNeNullable[int16]
	Int16NeSels                 = numericNeSels[int16]
	Int16NeNullableSels         = numericNeNullableSels[int16]
	Int16NeScalar               = numericNeScalar[int16]
	Int16NeNullableScalar       = numericNeNullableScalar[int16]
	Int16NeScalarSels           = numericNeScalarSels[int16]
	Int16NeNullableScalarSels   = numericNeNullableScalarSels[int16]
	Int32Ne                     = numericNe[int32]
	Int32NeNullable             = numericNeNullable[int32]
	Int32NeSels                 = numericNeSels[int32]
	Int32NeNullableSels         = numericNeNullableSels[int32]
	Int32NeScalar               = numericNeScalar[int32]
	Int32NeNullableScalar       = numericNeNullableScalar[int32]
	Int32NeScalarSels           = numericNeScalarSels[int32]
	Int32NeNullableScalarSels   = numericNeNullableScalarSels[int32]
	Int64Ne                     = numericNe[int64]
	Int64NeNullable             = numericNeNullable[int64]
	Int64NeSels                 = numericNeSels[int64]
	Int64NeNullableSels         = numericNeNullableSels[int64]
	Int64NeScalar               = numericNeScalar[int64]
	Int64NeNullableScalar       = numericNeNullableScalar[int64]
	Int64NeScalarSels           = numericNeScalarSels[int64]
	Int64NeNullableScalarSels   = numericNeNullableScalarSels[int64]
	Uint8Ne                     = numericNe[uint8]
	Uint8NeNullable             = numericNeNullable[uint8]
	Uint8NeSels                 = numericNeSels[uint8]
	Uint8NeNullableSels         = numericNeNullableSels[uint8]
	Uint8NeScalar               = numericNeScalar[uint8]
	Uint8NeNullableScalar       = numericNeNullableScalar[uint8]
	Uint8NeScalarSels           = numericNeScalarSels[uint8]
	Uint8NeNullableScalarSels   = numericNeNullableScalarSels[uint8]
	Uint16Ne                    = numericNe[uint16]
	Uint16NeNullable            = numericNeNullable[uint16]
	Uint16NeSels                = numericNeSels[uint16]
	Uint16NeNullableSels        = numericNeNullableSels[uint16]
	Uint16NeScalar              = numericNeScalar[uint16]
	Uint16NeNullableScalar      = numericNeNullableScalar[uint16]
	Uint16NeScalarSels          = numericNeScalarSels[uint16]
	Uint16NeNullableScalarSels  = numericNeNullableScalarSels[uint16]
	Uint32Ne                    = numericNe[uint32]
	Uint32NeNullable            = numericNeNullable[uint32]
	Uint32NeSels                = numericNeSels[uint32]
	Uint32NeNullableSels        = numericNeNullableSels[uint32]
	Uint32NeScalar              = numericNeScalar[uint32]
	Uint32NeNullableScalar      = numericNeNullableScalar[uint32]
	Uint32NeScalarSels          = numericNeScalarSels[uint32]
	Uint32NeNullableScalarSels  = numericNeNullableScalarSels[uint32]
	Uint64Ne                    = numericNe[uint64]
	Uint64NeNullable            = numericNeNullable[uint64]
	Uint64NeSels                = numericNeSels[uint64]
	Uint64NeNullableSels        = numericNeNullableSels[uint64]
	Uint64NeScalar              = numericNeScalar[uint64]
	Uint64NeNullableScalar      = numericNeNullableScalar[uint64]
	Uint64NeScalarSels          = numericNeScalarSels[uint64]
	Uint64NeNullableScalarSels  = numericNeNullableScalarSels[uint64]
	Float32Ne                   = numericNe[float32]
	Float32NeNullable           = numericNeNullable[float32]
	Float32NeSels               = numericNeSels[float32]
	Float32NeNullableSels       = numericNeNullableSels[float32]
	Float32NeScalar             = numericNeScalar[float32]
	Float32NeNullableScalar     = numericNeNullableScalar[float32]
	Float32NeScalarSels         = numericNeScalarSels[float32]
	Float32NeNullableScalarSels = numericNeNullableScalarSels[float32]
	Float64Ne                   = numericNe[float64]
	Float64NeNullable           = numericNeNullable[float64]
	Float64NeSels               = numericNeSels[float64]
	Float64NeNullableSels       = numericNeNullableSels[float64]
	Float64NeScalar             = numericNeScalar[float64]
	Float64NeNullableScalar     = numericNeNullableScalar[float64]
	Float64NeScalarSels         = numericNeScalarSels[float64]
	Float64NeNullableScalarSels = numericNeNullableScalarSels[float64]

	StrNe                   = strNe
	StrNeNullable           = strNeNullable
	StrNeSels               = strNeSels
	StrNeNullableSels       = strNeNullableSels
	StrNeScalar             = strNeScalar
	StrNeNullableScalar     = strNeNullableScalar
	StrNeScalarSels         = strNeScalarSels
	StrNeNullableScalarSels = strNeNullableScalarSels

	Decimal64Ne                    = decimal64Ne
	Decimal64NeNullable            = decimal64NeNullable
	Decimal64NeSels                = decimal64NeSels
	Decimal64NeNullableSels        = decimal64NeNullableSels
	Decimal64NeScalar              = decimal64NeScalar
	Decimal64NeNullableScalar      = decimal64NeNullableScalar
	Decimal64NeScalarSels          = decimal64NeScalarSels
	Decimal64NeNullableScalarSels  = decimal64NeNullableScalarSels
	Decimal128Ne                   = decimal128Ne
	Decimal128NeNullable           = decimal128NeNullable
	Decimal128NeSels               = decimal128NeSels
	Decimal128NeNullableSels       = decimal128NeNullableSels
	Decimal128NeScalar             = decimal128NeScalar
	Decimal128NeNullableScalar     = decimal128NeNullableScalar
	Decimal128NeScalarSels         = decimal128NeScalarSels
	Decimal128NeNullableScalarSels = decimal128NeNullableScalarSels
)

func numericNe[T constraints.Integer | constraints.Float](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x != ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericNeNullable[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericNeSels[T constraints.Integer | constraints.Float](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericNeNullableSels[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericNeScalar[T constraints.Integer | constraints.Float](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x != y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericNeNullableScalar[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericNeScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x != ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericNeNullableScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
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

func decimal64Ne(xs, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, ysScaled[i]) != 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xsScaled[i], y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ys[i]) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64NeNullable(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, ys[i], xScale, yScale) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64NeSels(xs, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64NeNullableSels(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64NeScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	if xScale > yScale {
		ysScaled := make([]types.Decimal64, len(ys))
		for i, y := range ys {
			scaleDiff := xScale - yScale
			scale := int64(math.Pow10(int(scaleDiff)))
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, yScaled := range ysScaled {
			if types.CompareDecimal64Decimal64Aligned(x, yScaled) != 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xScaled, y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(x, y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64NeNullableScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, y, xScale, yScale) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64NeScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64NeNullableScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128Ne(xs, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, ysScaled[i]) != 0 {
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
			for j := 0; j < int(scaleDiff); j++ {
				xsScaled[i] = types.ScaleDecimal128By10(xsScaled[i])
			}
		}
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(xsScaled[i], y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ys[i]) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128NeNullable(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, ys[i], xScale, yScale) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128NeSels(xs, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128NeNullableSels(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128NeScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, yScaled) != 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xScaled, y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(x, y) != 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128NeNullableScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, y, xScale, yScale) != 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128NeScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128NeNullableScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) != 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
