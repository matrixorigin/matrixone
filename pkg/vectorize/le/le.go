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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	Int8Le                      = numericLe[int8]
	Int8LeNullable              = numericLeNullable[int8]
	Int8LeSels                  = numericLeSels[int8]
	Int8LeNullableSels          = numericLeNullableSels[int8]
	Int8LeScalar                = numericLeScalar[int8]
	Int8LeNullableScalar        = numericLeNullableScalar[int8]
	Int8LeScalarSels            = numericLeScalarSels[int8]
	Int8LeNullableScalarSels    = numericLeNullableScalarSels[int8]
	Int16Le                     = numericLe[int16]
	Int16LeNullable             = numericLeNullable[int16]
	Int16LeSels                 = numericLeSels[int16]
	Int16LeNullableSels         = numericLeNullableSels[int16]
	Int16LeScalar               = numericLeScalar[int16]
	Int16LeNullableScalar       = numericLeNullableScalar[int16]
	Int16LeScalarSels           = numericLeScalarSels[int16]
	Int16LeNullableScalarSels   = numericLeNullableScalarSels[int16]
	Int32Le                     = numericLe[int32]
	Int32LeNullable             = numericLeNullable[int32]
	Int32LeSels                 = numericLeSels[int32]
	Int32LeNullableSels         = numericLeNullableSels[int32]
	Int32LeScalar               = numericLeScalar[int32]
	Int32LeNullableScalar       = numericLeNullableScalar[int32]
	Int32LeScalarSels           = numericLeScalarSels[int32]
	Int32LeNullableScalarSels   = numericLeNullableScalarSels[int32]
	Int64Le                     = numericLe[int64]
	Int64LeNullable             = numericLeNullable[int64]
	Int64LeSels                 = numericLeSels[int64]
	Int64LeNullableSels         = numericLeNullableSels[int64]
	Int64LeScalar               = numericLeScalar[int64]
	Int64LeNullableScalar       = numericLeNullableScalar[int64]
	Int64LeScalarSels           = numericLeScalarSels[int64]
	Int64LeNullableScalarSels   = numericLeNullableScalarSels[int64]
	Uint8Le                     = numericLe[uint8]
	Uint8LeNullable             = numericLeNullable[uint8]
	Uint8LeSels                 = numericLeSels[uint8]
	Uint8LeNullableSels         = numericLeNullableSels[uint8]
	Uint8LeScalar               = numericLeScalar[uint8]
	Uint8LeNullableScalar       = numericLeNullableScalar[uint8]
	Uint8LeScalarSels           = numericLeScalarSels[uint8]
	Uint8LeNullableScalarSels   = numericLeNullableScalarSels[uint8]
	Uint16Le                    = numericLe[uint16]
	Uint16LeNullable            = numericLeNullable[uint16]
	Uint16LeSels                = numericLeSels[uint16]
	Uint16LeNullableSels        = numericLeNullableSels[uint16]
	Uint16LeScalar              = numericLeScalar[uint16]
	Uint16LeNullableScalar      = numericLeNullableScalar[uint16]
	Uint16LeScalarSels          = numericLeScalarSels[uint16]
	Uint16LeNullableScalarSels  = numericLeNullableScalarSels[uint16]
	Uint32Le                    = numericLe[uint32]
	Uint32LeNullable            = numericLeNullable[uint32]
	Uint32LeSels                = numericLeSels[uint32]
	Uint32LeNullableSels        = numericLeNullableSels[uint32]
	Uint32LeScalar              = numericLeScalar[uint32]
	Uint32LeNullableScalar      = numericLeNullableScalar[uint32]
	Uint32LeScalarSels          = numericLeScalarSels[uint32]
	Uint32LeNullableScalarSels  = numericLeNullableScalarSels[uint32]
	Uint64Le                    = numericLe[uint64]
	Uint64LeNullable            = numericLeNullable[uint64]
	Uint64LeSels                = numericLeSels[uint64]
	Uint64LeNullableSels        = numericLeNullableSels[uint64]
	Uint64LeScalar              = numericLeScalar[uint64]
	Uint64LeNullableScalar      = numericLeNullableScalar[uint64]
	Uint64LeScalarSels          = numericLeScalarSels[uint64]
	Uint64LeNullableScalarSels  = numericLeNullableScalarSels[uint64]
	Float32Le                   = numericLe[float32]
	Float32LeNullable           = numericLeNullable[float32]
	Float32LeSels               = numericLeSels[float32]
	Float32LeNullableSels       = numericLeNullableSels[float32]
	Float32LeScalar             = numericLeScalar[float32]
	Float32LeNullableScalar     = numericLeNullableScalar[float32]
	Float32LeScalarSels         = numericLeScalarSels[float32]
	Float32LeNullableScalarSels = numericLeNullableScalarSels[float32]
	Float64Le                   = numericLe[float64]
	Float64LeNullable           = numericLeNullable[float64]
	Float64LeSels               = numericLeSels[float64]
	Float64LeNullableSels       = numericLeNullableSels[float64]
	Float64LeScalar             = numericLeScalar[float64]
	Float64LeNullableScalar     = numericLeNullableScalar[float64]
	Float64LeScalarSels         = numericLeScalarSels[float64]
	Float64LeNullableScalarSels = numericLeNullableScalarSels[float64]

	StrLe                   = strLe
	StrLeNullable           = strLeNullable
	StrLeSels               = strLeSels
	StrLeNullableSels       = strLeNullableSels
	StrLeScalar             = strLeScalar
	StrLeNullableScalar     = strLeNullableScalar
	StrLeScalarSels         = strLeScalarSels
	StrLeNullableScalarSels = strLeNullableScalarSels

	Decimal64Le                    = decimal64Le
	Decimal64LeNullable            = decimal64LeNullable
	Decimal64LeSels                = decimal64LeSels
	Decimal64LeNullableSels        = decimal64LeNullableSels
	Decimal64LeScalar              = decimal64LeScalar
	Decimal64LeNullableScalar      = decimal64LeNullableScalar
	Decimal64LeScalarSels          = decimal64LeScalarSels
	Decimal64LeNullableScalarSels  = decimal64LeNullableScalarSels
	Decimal128Le                   = decimal128Le
	Decimal128LeNullable           = decimal128LeNullable
	Decimal128LeSels               = decimal128LeSels
	Decimal128LeNullableSels       = decimal128LeNullableSels
	Decimal128LeScalar             = decimal128LeScalar
	Decimal128LeNullableScalar     = decimal128LeNullableScalar
	Decimal128LeScalarSels         = decimal128LeScalarSels
	Decimal128LeNullableScalarSels = decimal128LeNullableScalarSels
)

func numericLe[T constraints.Integer | constraints.Float](xs, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, x := range xs {
		if x <= ys[i] {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLeNullable[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericLeSels[T constraints.Integer | constraints.Float](xs, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLeNullableSels[T constraints.Integer | constraints.Float](xs, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && xs[sel] <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLeScalar[T constraints.Integer | constraints.Float](x T, ys []T, rs []int64) []int64 {
	rsi := 0
	for i, y := range ys {
		if x <= y {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLeNullableScalar[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func numericLeScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func numericLeNullableScalarSels[T constraints.Integer | constraints.Float](x T, ys []T, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && x <= ys[sel] {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLe(xs, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(xs.Offsets); i < n; i++ {
		if bytes.Compare(xs.Get(int64(i)), ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullable(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strLeSels(xs, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableSels(xs, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(xs.Get(sel), ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeScalar(x []byte, ys *types.Bytes, rs []int64) []int64 {
	rsi := 0
	for i, n := 0, len(ys.Offsets); i < n; i++ {
		if bytes.Compare(x, ys.Get(int64(i))) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableScalar(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs []int64) []int64 {
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

func strLeScalarSels(x []byte, ys *types.Bytes, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func strLeNullableScalarSels(x []byte, ys *types.Bytes, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && bytes.Compare(x, ys.Get(sel)) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64Le(xs, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, ysScaled[i]) <= 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xsScaled[i], y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal64Decimal64Aligned(x, ys[i]) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64LeNullable(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, ys[i], xScale, yScale) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LeSels(xs, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LeNullableSels(xs, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(xs[sel], ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LeScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal64Decimal64Aligned(x, yScaled) <= 0 {
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
			if types.CompareDecimal64Decimal64Aligned(xScaled, y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal64Decimal64Aligned(x, y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal64LeNullableScalar(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal64Decimal64(x, y, xScale, yScale) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LeScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal64LeNullableScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal64Decimal64(x, ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128Le(xs, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, ysScaled[i]) <= 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xsScaled[i], y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, x := range xs {
			if types.CompareDecimal128Decimal128Aligned(x, ys[i]) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128LeNullable(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, ys[i], xScale, yScale) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LeSels(xs, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LeNullableSels(xs, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(xs[sel], ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LeScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs []int64) []int64 {
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
			if types.CompareDecimal128Decimal128Aligned(x, yScaled) <= 0 {
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
			if types.CompareDecimal128Decimal128Aligned(xScaled, y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	} else {
		for i, y := range ys {
			if types.CompareDecimal128Decimal128Aligned(x, y) <= 0 {
				rs[rsi] = int64(i)
				rsi++
			}
		}
		return rs[:rsi]
	}
}

func decimal128LeNullableScalar(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs []int64) []int64 {
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
		} else if types.CompareDecimal128Decimal128(x, y, xScale, yScale) <= 0 {
			rs[rsi] = int64(i)
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LeScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}

func decimal128LeNullableScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, yScale int32, nulls *roaring.Bitmap, rs, sels []int64) []int64 {
	rsi := 0
	for _, sel := range sels {
		if !nulls.Contains(uint64(sel)) && types.CompareDecimal128Decimal128(x, ys[sel], xScale, yScale) <= 0 {
			rs[rsi] = sel
			rsi++
		}
	}
	return rs[:rsi]
}
