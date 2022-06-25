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

package sub

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Sub                = Numeric[int8]
	Int8SubScalar          = NumericScalar[int8]
	Int8SubByScalar        = NumericByScalar[int8]
	Int16Sub               = Numeric[int16]
	Int16SubScalar         = NumericScalar[int16]
	Int16SubByScalar       = NumericByScalar[int16]
	Int32Sub               = Numeric[int32]
	Int32SubScalar         = NumericScalar[int32]
	Int32SubByScalar       = NumericByScalar[int32]
	Int64Sub               = Numeric[int64]
	Int64SubScalar         = NumericScalar[int64]
	Int64SubByScalar       = NumericByScalar[int64]
	Uint8Sub               = Numeric[uint8]
	Uint8SubScalar         = NumericScalar[uint8]
	Uint8SubByScalar       = NumericByScalar[uint8]
	Uint16Sub              = Numeric[uint16]
	Uint16SubScalar        = NumericScalar[uint16]
	Uint16SubByScalar      = NumericByScalar[uint16]
	Uint32Sub              = Numeric[uint32]
	Uint32SubScalar        = NumericScalar[uint32]
	Uint32SubByScalar      = NumericByScalar[uint32]
	Uint64Sub              = Numeric[uint64]
	Uint64SubScalar        = NumericScalar[uint64]
	Uint64SubByScalar      = NumericByScalar[uint64]
	Float32Sub             = Numeric[float32]
	Float32SubScalar       = NumericScalar[float32]
	Float32SubByScalar     = NumericByScalar[float32]
	Float64Sub             = Numeric[float64]
	Float64SubScalar       = NumericScalar[float64]
	Float64SubByScalar     = NumericByScalar[float64]
	Int8SubSels            = NumericSels[int8]
	Int8SubScalarSels      = NumericScalarSels[int8]
	Int8SubByScalarSels    = NumericByScalarSels[int8]
	Int16SubSels           = NumericSels[int16]
	Int16SubScalarSels     = NumericScalarSels[int16]
	Int16SubByScalarSels   = NumericByScalarSels[int16]
	Int32SubSels           = NumericSels[int32]
	Int32SubScalarSels     = NumericScalarSels[int32]
	Int32SubByScalarSels   = NumericByScalarSels[int32]
	Int64SubSels           = NumericSels[int64]
	Int64SubScalarSels     = NumericScalarSels[int64]
	Int64SubByScalarSels   = NumericByScalarSels[int64]
	Uint8SubSels           = NumericSels[uint8]
	Uint8SubScalarSels     = NumericScalarSels[uint8]
	Uint8SubByScalarSels   = NumericByScalarSels[uint8]
	Uint16SubSels          = NumericSels[uint16]
	Uint16SubScalarSels    = NumericScalarSels[uint16]
	Uint16SubByScalarSels  = NumericByScalarSels[uint16]
	Uint32SubSels          = NumericSels[uint32]
	Uint32SubScalarSels    = NumericScalarSels[uint32]
	Uint32SubByScalarSels  = NumericByScalarSels[uint32]
	Uint64SubSels          = NumericSels[uint64]
	Uint64SubScalarSels    = NumericScalarSels[uint64]
	Uint64SubByScalarSels  = NumericByScalarSels[uint64]
	Float32SubSels         = NumericSels[float32]
	Float32SubScalarSels   = NumericScalarSels[float32]
	Float32SubByScalarSels = NumericByScalarSels[float32]
	Float64SubSels         = NumericSels[float64]
	Float64SubScalarSels   = NumericScalarSels[float64]
	Float64SubByScalarSels = NumericByScalarSels[float64]

	Decimal64Sub              = decimal64Sub
	Decimal64SubSels          = decimal64SubSels
	Decimal64SubScalar        = decimal64SubScalar
	Decimal64SubScalarSels    = decimal64SubScalarSels
	Decimal64SubByScalar      = decimal64SubByScalar
	Decimal64SubByScalarSels  = decimal64SubByScalarSels
	Decimal128Sub             = decimal128Sub
	Decimal128SubSels         = decimal128SubSels
	Decimal128SubScalar       = decimal128SubScalar
	Decimal128SubScalarSels   = decimal128SubScalarSels
	Decimal128SubByScalar     = decimal128SubByScalar
	Decimal128SubByScalarSels = decimal128SubByScalarSels

	Int32Int64Sub         = NumericBigSmall[int64, int32]
	Int32Int64SubSels     = NumericSelsBigSmall[int64, int32]
	Int16Int64Sub         = NumericBigSmall[int64, int16]
	Int16Int64SubSels     = NumericSelsBigSmall[int64, int16]
	Int8Int64Sub          = NumericBigSmall[int64, int8]
	Int8Int64SubSels      = NumericSelsBigSmall[int64, int8]
	Int16Int32Sub         = NumericBigSmall[int32, int16]
	Int16Int32SubSels     = NumericSelsBigSmall[int32, int16]
	Int8Int32Sub          = NumericBigSmall[int32, int8]
	Int8Int32SubSels      = NumericSelsBigSmall[int32, int8]
	Int8Int16Sub          = NumericBigSmall[int16, int8]
	Int8Int16SubSels      = NumericSelsBigSmall[int16, int8]
	Uint32Uint64Sub       = NumericBigSmall[uint64, uint32]
	Uint32Uint64SubSels   = NumericSelsBigSmall[uint64, uint32]
	Uint16Uint64Sub       = NumericBigSmall[uint64, uint16]
	Uint16Uint64SubSels   = NumericSelsBigSmall[uint64, uint16]
	Uint8Uint64Sub        = NumericBigSmall[uint64, uint8]
	Uint8Uint64SubSels    = NumericSelsBigSmall[uint64, uint8]
	Uint16Uint32Sub       = NumericBigSmall[uint32, uint16]
	Uint16Uint32SubSels   = NumericSelsBigSmall[uint32, uint16]
	Uint8Uint32Sub        = NumericBigSmall[uint32, uint8]
	Uint8Uint32SubSels    = NumericSelsBigSmall[uint32, uint8]
	Uint8Uint16Sub        = NumericBigSmall[uint16, uint8]
	Uint8Uint16SubSels    = NumericSelsBigSmall[uint16, uint8]
	Float32Float64Sub     = NumericBigSmall[float64, float32]
	Float32Float64SubSels = NumericSelsBigSmall[float64, float32]
)

func Numeric[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func NumericSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func NumericScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func NumericScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func NumericByScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func NumericByScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func NumericBigSmall[TBig, TSmall constraints.Integer | constraints.Float](xs []TBig, ys []TSmall, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = x - TBig(ys[i])
	}
	return rs
}

func NumericSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](xs []TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = xs[sel] - TBig(ys[sel])
	}
	return rs
}

/*
func numericSubScalarBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = x - TBig(y)
	}
	return rs
}

func numericSubScalarSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = x - TBig(ys[sel])
	}
	return rs
}

func numericSubByScalarBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = y - TBig(x)
	}
	return rs
}

func numericSubByScalarSelsBigSmall[TBig, TSmall constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = ys[sel] - TBig(x)
	}
	return rs
}

func numericSubSmallBig[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) - ys[i]
	}
	return rs
}

func numericSubSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) - ys[sel]
	}
	return rs
}

func numericSubScalarSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) - y
	}
	return rs
}

func numericSubScalarSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(x) - ys[sel]
	}
	return rs
}

func numericSubByScalarSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(y) - x
	}
	return rs
}

func numericSubByScalarSelsSmallBig[TSmall, TBig constraints.Integer | constraints.Float](x TBig, ys []TSmall, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(ys[sel]) - x
	}
	return rs
}
*/

func decimal64Sub(xs []types.Decimal64, ys []types.Decimal64, xsScale int32, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	if xsScale > ysScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xsScale - ysScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, x := range xs {
			rs[i] = types.Decimal64SubAligned(x, ysScaled[i])
		}
		return rs
	} else if xsScale < ysScale {
		xsScaled := make([]types.Decimal64, len(xs))
		scaleDiff := ysScale - xsScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, x := range xs {
			xsScaled[i] = types.ScaleDecimal64(x, scale)
		}
		for i, y := range ys {
			rs[i] = types.Decimal64SubAligned(xsScaled[i], y)
		}
		return rs
	} else {
		for i, x := range xs {
			rs[i] = types.Decimal64SubAligned(x, ys[i])
		}
		return rs
	}
}

func decimal64SubSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Sub(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal64SubScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xScale - ysScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal64SubAligned(x, yScaled)
		}
		return rs
	} else if xScale < ysScale {
		scaleDiff := ysScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		xScaled := types.ScaleDecimal64(x, scale)
		for i, y := range ys {
			rs[i] = types.Decimal64SubAligned(xScaled, y)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal64SubAligned(x, y)
		}
		return rs
	}
}

func decimal64SubScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Sub(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal64SubByScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xScale - ysScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal64SubAligned(yScaled, x)
		}
		return rs
	} else if xScale < ysScale {
		scaleDiff := ysScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		xScaled := types.ScaleDecimal64(x, scale)
		for i, y := range ys {
			rs[i] = types.Decimal64SubAligned(y, xScaled)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal64SubAligned(y, x)
		}
		return rs
	}
}

func decimal64SubByScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Sub(ys[sel], x, ysScale, xScale)
	}
	return rs
}

func decimal128Sub(xs []types.Decimal128, ys []types.Decimal128, xsScale int32, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	/* to add two decimal128 value, first we need to align them to the same scale(the maximum of the two)
																	Decimal(20, 5), Decimal(20, 6)
	value																321.4			123.5
	representation														32,140,000		123,500,000
	align to the same scale	by scale 12340000 by 10 321400000			321,400,000		123,500,000
	add

	*/
	if xsScale > ysScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xsScale - ysScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, x := range xs {
			rs[i] = types.Decimal128SubAligned(x, ysScaled[i])
		}
		return rs
	} else if xsScale < ysScale {
		xsScaled := make([]types.Decimal128, len(xs))
		scaleDiff := ysScale - xsScale
		for i, x := range xs {
			xsScaled[i] = x
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				xsScaled[i] = types.ScaleDecimal128By10(xsScaled[i])
			}
		}
		for i, y := range ys {
			rs[i] = types.Decimal128SubAligned(xsScaled[i], y)
		}
		return rs
	} else {
		for i, x := range xs {
			rs[i] = types.Decimal128SubAligned(x, ys[i])
		}
		return rs
	}
}

func decimal128SubSels(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal128SubScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xScale - ysScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal128SubAligned(x, yScaled)
		}
		return rs
	} else if xScale < ysScale {
		xScaled := x
		scaleDiff := ysScale - xScale
		// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
		for i := 0; i < int(scaleDiff); i++ {
			xScaled = types.ScaleDecimal128By10(xScaled)
		}
		for i, y := range ys {
			rs[i] = types.Decimal128SubAligned(xScaled, y)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal128SubAligned(x, y)
		}
		return rs
	}
}

func decimal128SubScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128SubByScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xScale - ysScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal128SubAligned(yScaled, x)
		}
		return rs
	} else if xScale < ysScale {
		xScaled := x
		scaleDiff := ysScale - xScale
		// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
		for i := 0; i < int(scaleDiff); i++ {
			xScaled = types.ScaleDecimal128By10(xScaled)
		}
		for i, y := range ys {
			rs[i] = types.Decimal128SubAligned(y, xScaled)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal128SubAligned(y, x)
		}
		return rs
	}
}

func decimal128SubByScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(ys[sel], x, ysScale, xScale)
	}
	return rs
}
