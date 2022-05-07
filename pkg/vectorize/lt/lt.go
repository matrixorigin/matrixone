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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Lt                      = numericLt[int8]
	Int8LtNullable              = numericLtNullable[int8]
	Int8LtSels                  = numericLtSels[int8]
	Int8LtNullableSels          = numericLtNullableSels[int8]
	Int8LtScalar                = numericLtScalar[int8]
	Int8LtNullableScalar        = numericLtNullableScalar[int8]
	Int8LtScalarSels            = numericLtScalarSels[int8]
	Int8LtNullableScalarSels    = numericLtNullableScalarSels[int8]
	Int16Lt                     = numericLt[int16]
	Int16LtNullable             = numericLtNullable[int16]
	Int16LtSels                 = numericLtSels[int16]
	Int16LtNullableSels         = numericLtNullableSels[int16]
	Int16LtScalar               = numericLtScalar[int16]
	Int16LtNullableScalar       = numericLtNullableScalar[int16]
	Int16LtScalarSels           = numericLtScalarSels[int16]
	Int16LtNullableScalarSels   = numericLtNullableScalarSels[int16]
	Int32Lt                     = numericLt[int32]
	Int32LtNullable             = numericLtNullable[int32]
	Int32LtSels                 = numericLtSels[int32]
	Int32LtNullableSels         = numericLtNullableSels[int32]
	Int32LtScalar               = numericLtScalar[int32]
	Int32LtNullableScalar       = numericLtNullableScalar[int32]
	Int32LtScalarSels           = numericLtScalarSels[int32]
	Int32LtNullableScalarSels   = numericLtNullableScalarSels[int32]
	Int64Lt                     = numericLt[int64]
	Int64LtNullable             = numericLtNullable[int64]
	Int64LtSels                 = numericLtSels[int64]
	Int64LtNullableSels         = numericLtNullableSels[int64]
	Int64LtScalar               = numericLtScalar[int64]
	Int64LtNullableScalar       = numericLtNullableScalar[int64]
	Int64LtScalarSels           = numericLtScalarSels[int64]
	Int64LtNullableScalarSels   = numericLtNullableScalarSels[int64]
	Uint8Lt                     = numericLt[uint8]
	Uint8LtNullable             = numericLtNullable[uint8]
	Uint8LtSels                 = numericLtSels[uint8]
	Uint8LtNullableSels         = numericLtNullableSels[uint8]
	Uint8LtScalar               = numericLtScalar[uint8]
	Uint8LtNullableScalar       = numericLtNullableScalar[uint8]
	Uint8LtScalarSels           = numericLtScalarSels[uint8]
	Uint8LtNullableScalarSels   = numericLtNullableScalarSels[uint8]
	Uint16Lt                    = numericLt[uint16]
	Uint16LtNullable            = numericLtNullable[uint16]
	Uint16LtSels                = numericLtSels[uint16]
	Uint16LtNullableSels        = numericLtNullableSels[uint16]
	Uint16LtScalar              = numericLtScalar[uint16]
	Uint16LtNullableScalar      = numericLtNullableScalar[uint16]
	Uint16LtScalarSels          = numericLtScalarSels[uint16]
	Uint16LtNullableScalarSels  = numericLtNullableScalarSels[uint16]
	Uint32Lt                    = numericLt[uint32]
	Uint32LtNullable            = numericLtNullable[uint32]
	Uint32LtSels                = numericLtSels[uint32]
	Uint32LtNullableSels        = numericLtNullableSels[uint32]
	Uint32LtScalar              = numericLtScalar[uint32]
	Uint32LtNullableScalar      = numericLtNullableScalar[uint32]
	Uint32LtScalarSels          = numericLtScalarSels[uint32]
	Uint32LtNullableScalarSels  = numericLtNullableScalarSels[uint32]
	Uint64Lt                    = numericLt[uint64]
	Uint64LtNullable            = numericLtNullable[uint64]
	Uint64LtSels                = numericLtSels[uint64]
	Uint64LtNullableSels        = numericLtNullableSels[uint64]
	Uint64LtScalar              = numericLtScalar[uint64]
	Uint64LtNullableScalar      = numericLtNullableScalar[uint64]
	Uint64LtScalarSels          = numericLtScalarSels[uint64]
	Uint64LtNullableScalarSels  = numericLtNullableScalarSels[uint64]
	Float32Lt                   = numericLt[float32]
	Float32LtNullable           = numericLtNullable[float32]
	Float32LtSels               = numericLtSels[float32]
	Float32LtNullableSels       = numericLtNullableSels[float32]
	Float32LtScalar             = numericLtScalar[float32]
	Float32LtNullableScalar     = numericLtNullableScalar[float32]
	Float32LtScalarSels         = numericLtScalarSels[float32]
	Float32LtNullableScalarSels = numericLtNullableScalarSels[float32]
	Float64Lt                   = numericLt[float64]
	Float64LtNullable           = numericLtNullable[float64]
	Float64LtSels               = numericLtSels[float64]
	Float64LtNullableSels       = numericLtNullableSels[float64]
	Float64LtScalar             = numericLtScalar[float64]
	Float64LtNullableScalar     = numericLtNullableScalar[float64]
	Float64LtScalarSels         = numericLtScalarSels[float64]
	Float64LtNullableScalarSels = numericLtNullableScalarSels[float64]

	StrLt                   = strLt
	StrLtNullable           = strLtNullable
	StrLtSels               = strLtSels
	StrLtNullableSels       = strLtNullableSels
	StrLtScalar             = strLtScalar
	StrLtNullableScalar     = strLtNullableScalar
	StrLtScalarSels         = strLtScalarSels
	StrLtNullableScalarSels = strLtNullableScalarSels

	Decimal64Lt                    = decimal64Lt
	Decimal64LtNullable            = decimal64LtNullable
	Decimal64LtSels                = decimal64LtSels
	Decimal64LtNullableSels        = decimal64LtNullableSels
	Decimal64LtScalar              = decimal64LtScalar
	Decimal64LtNullableScalar      = decimal64LtNullableScalar
	Decimal64LtScalarSels          = decimal64LtScalarSels
	Decimal64LtNullableScalarSels  = decimal64LtNullableScalarSels
	Decimal128Lt                   = decimal128Lt
	Decimal128LtNullable           = decimal128LtNullable
	Decimal128LtSels               = decimal128LtSels
	Decimal128LtNullableSels       = decimal128LtNullableSels
	Decimal128LtScalar             = decimal128LtScalar
	Decimal128LtNullableScalar     = decimal128LtNullableScalar
	Decimal128LtScalarSels         = decimal128LtScalarSels
	Decimal128LtNullableScalarSels = decimal128LtNullableScalarSels
)

func numericLt[T constraints.Integer | constraints.Float](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x < ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLtNullable[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericLtSels[T constraints.Integer | constraints.Float](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLtNullableSels[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLtScalar[T constraints.Integer | constraints.Float](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x < y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLtNullableScalar[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericLtScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLtNullableScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x < ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLt(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strLtSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strLtScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLtNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64Lt(xs, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, ysScaled[i]) < 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xsScaled[i], y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ys[i]) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64LtNullable(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, ys[i], xScale, yScale) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LtSels(xs, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LtNullableSels(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LtScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, yScaled) < 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xScaled, y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(x, y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64LtNullableScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, y, xScale, yScale) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LtScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LtNullableScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128Lt(xs, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, ysScaled[i]) < 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xsScaled[i], y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ys[i]) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128LtNullable(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, ys[i], xScale, yScale) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LtSels(xs, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LtNullableSels(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LtScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, yScaled) < 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xScaled, y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(x, y) < 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128LtNullableScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, y, xScale, yScale) < 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LtScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LtNullableScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) < 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
