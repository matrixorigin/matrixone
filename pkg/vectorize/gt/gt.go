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
	"math"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Gt                      = numericGt[int8]
	Int8GtNullable              = numericGtNullable[int8]
	Int8GtSels                  = numericGtSels[int8]
	Int8GtNullableSels          = numericGtNullableSels[int8]
	Int8GtScalar                = numericGtScalar[int8]
	Int8GtNullableScalar        = numericGtNullableScalar[int8]
	Int8GtScalarSels            = numericGtScalarSels[int8]
	Int8GtNullableScalarSels    = numericGtNullableScalarSels[int8]
	Int16Gt                     = numericGt[int16]
	Int16GtNullable             = numericGtNullable[int16]
	Int16GtSels                 = numericGtSels[int16]
	Int16GtNullableSels         = numericGtNullableSels[int16]
	Int16GtScalar               = numericGtScalar[int16]
	Int16GtNullableScalar       = numericGtNullableScalar[int16]
	Int16GtScalarSels           = numericGtScalarSels[int16]
	Int16GtNullableScalarSels   = numericGtNullableScalarSels[int16]
	Int32Gt                     = numericGt[int32]
	Int32GtNullable             = numericGtNullable[int32]
	Int32GtSels                 = numericGtSels[int32]
	Int32GtNullableSels         = numericGtNullableSels[int32]
	Int32GtScalar               = numericGtScalar[int32]
	Int32GtNullableScalar       = numericGtNullableScalar[int32]
	Int32GtScalarSels           = numericGtScalarSels[int32]
	Int32GtNullableScalarSels   = numericGtNullableScalarSels[int32]
	Int64Gt                     = numericGt[int64]
	Int64GtNullable             = numericGtNullable[int64]
	Int64GtSels                 = numericGtSels[int64]
	Int64GtNullableSels         = numericGtNullableSels[int64]
	Int64GtScalar               = numericGtScalar[int64]
	Int64GtNullableScalar       = numericGtNullableScalar[int64]
	Int64GtScalarSels           = numericGtScalarSels[int64]
	Int64GtNullableScalarSels   = numericGtNullableScalarSels[int64]
	Uint8Gt                     = numericGt[uint8]
	Uint8GtNullable             = numericGtNullable[uint8]
	Uint8GtSels                 = numericGtSels[uint8]
	Uint8GtNullableSels         = numericGtNullableSels[uint8]
	Uint8GtScalar               = numericGtScalar[uint8]
	Uint8GtNullableScalar       = numericGtNullableScalar[uint8]
	Uint8GtScalarSels           = numericGtScalarSels[uint8]
	Uint8GtNullableScalarSels   = numericGtNullableScalarSels[uint8]
	Uint16Gt                    = numericGt[uint16]
	Uint16GtNullable            = numericGtNullable[uint16]
	Uint16GtSels                = numericGtSels[uint16]
	Uint16GtNullableSels        = numericGtNullableSels[uint16]
	Uint16GtScalar              = numericGtScalar[uint16]
	Uint16GtNullableScalar      = numericGtNullableScalar[uint16]
	Uint16GtScalarSels          = numericGtScalarSels[uint16]
	Uint16GtNullableScalarSels  = numericGtNullableScalarSels[uint16]
	Uint32Gt                    = numericGt[uint32]
	Uint32GtNullable            = numericGtNullable[uint32]
	Uint32GtSels                = numericGtSels[uint32]
	Uint32GtNullableSels        = numericGtNullableSels[uint32]
	Uint32GtScalar              = numericGtScalar[uint32]
	Uint32GtNullableScalar      = numericGtNullableScalar[uint32]
	Uint32GtScalarSels          = numericGtScalarSels[uint32]
	Uint32GtNullableScalarSels  = numericGtNullableScalarSels[uint32]
	Uint64Gt                    = numericGt[uint64]
	Uint64GtNullable            = numericGtNullable[uint64]
	Uint64GtSels                = numericGtSels[uint64]
	Uint64GtNullableSels        = numericGtNullableSels[uint64]
	Uint64GtScalar              = numericGtScalar[uint64]
	Uint64GtNullableScalar      = numericGtNullableScalar[uint64]
	Uint64GtScalarSels          = numericGtScalarSels[uint64]
	Uint64GtNullableScalarSels  = numericGtNullableScalarSels[uint64]
	Float32Gt                   = numericGt[float32]
	Float32GtNullable           = numericGtNullable[float32]
	Float32GtSels               = numericGtSels[float32]
	Float32GtNullableSels       = numericGtNullableSels[float32]
	Float32GtScalar             = numericGtScalar[float32]
	Float32GtNullableScalar     = numericGtNullableScalar[float32]
	Float32GtScalarSels         = numericGtScalarSels[float32]
	Float32GtNullableScalarSels = numericGtNullableScalarSels[float32]
	Float64Gt                   = numericGt[float64]
	Float64GtNullable           = numericGtNullable[float64]
	Float64GtSels               = numericGtSels[float64]
	Float64GtNullableSels       = numericGtNullableSels[float64]
	Float64GtScalar             = numericGtScalar[float64]
	Float64GtNullableScalar     = numericGtNullableScalar[float64]
	Float64GtScalarSels         = numericGtScalarSels[float64]
	Float64GtNullableScalarSels = numericGtNullableScalarSels[float64]

	StrGt                   = strGt
	StrGtNullable           = strGtNullable
	StrGtSels               = strGtSels
	StrGtNullableSels       = strGtNullableSels
	StrGtScalar             = strGtScalar
	StrGtNullableScalar     = strGtNullableScalar
	StrGtScalarSels         = strGtScalarSels
	StrGtNullableScalarSels = strGtNullableScalarSels

	Decimal64Gt                    = decimal64Gt
	Decimal64GtNullable            = decimal64GtNullable
	Decimal64GtSels                = decimal64GtSels
	Decimal64GtNullableSels        = decimal64GtNullableSels
	Decimal64GtScalar              = decimal64GtScalar
	Decimal64GtNullableScalar      = decimal64GtNullableScalar
	Decimal64GtScalarSels          = decimal64GtScalarSels
	Decimal64GtNullableScalarSels  = decimal64GtNullableScalarSels
	Decimal128Gt                   = decimal128Gt
	Decimal128GtNullable           = decimal128GtNullable
	Decimal128GtSels               = decimal128GtSels
	Decimal128GtNullableSels       = decimal128GtNullableSels
	Decimal128GtScalar             = decimal128GtScalar
	Decimal128GtNullableScalar     = decimal128GtNullableScalar
	Decimal128GtScalarSels         = decimal128GtScalarSels
	Decimal128GtNullableScalarSels = decimal128GtNullableScalarSels
)

func numericGt[T constraints.Integer | constraints.Float](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x > ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericGtNullable[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericGtSels[T constraints.Integer | constraints.Float](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericGtNullableSels[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericGtScalar[T constraints.Integer | constraints.Float](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x > y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericGtNullableScalar[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericGtScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericGtNullableScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x > ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGt(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGtNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strGtSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGtNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGtScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strGtNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strGtScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strGtNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64Gt(xs, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, ysScaled[i]) > 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xsScaled[i], y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ys[i]) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64GtNullable(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, ys[i], xScale, yScale) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64GtSels(xs, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64GtNullableSels(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64GtScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
	rsi := 0
	if xScale > yScale {
		ysScaled := make([]types.Decimal64, len(ys))
		for i, y := range ys {
			scaleDiff := xScale - yScale
			ysScaled[i] = y
			scale := int64(math.Pow10(int(scaleDiff)))
			for i, y := range ys {
				ysScaled[i] = types.ScaleDecimal64(y, scale)
			}
		}
		for i, yScaled := range ysScaled {
			if types.CompareDecimal64Decimal64Aligned(x, yScaled) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else if xScale < yScale {
		xScaled := x
		scaleDiff := yScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		xScaled = types.ScaleDecimal64(xScaled, scale)
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(xScaled, y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(x, y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64GtNullableScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, y, xScale, yScale) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64GtScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64GtNullableScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128Gt(xs, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, ysScaled[i]) > 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xsScaled[i], y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ys[i]) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128GtNullable(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, ys[i], xScale, yScale) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128GtSels(xs, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128GtNullableSels(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128GtScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, yScaled) > 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xScaled, y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(x, y) > 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128GtNullableScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, y, xScale, yScale) > 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128GtScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128GtNullableScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) > 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
