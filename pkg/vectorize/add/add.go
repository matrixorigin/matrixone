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

package add

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Add              = NumericAddSigned[int8]
	Int8AddScalar        = NumericAddScalarSigned[int8]
	Int16Add             = NumericAddSigned[int16]
	Int16AddScalar       = NumericAddScalarSigned[int16]
	Int32Add             = NumericAddSigned[int32]
	Int32AddScalar       = NumericAddScalarSigned[int32]
	Int64Add             = NumericAddSigned[int64]
	Int64AddScalar       = NumericAddScalarSigned[int64]
	Uint8Add             = NumericAddUnsigned[uint8]
	Uint8AddScalar       = NumericAddScalarUnsigned[uint8]
	Uint16Add            = NumericAddUnsigned[uint16]
	Uint16AddScalar      = NumericAddScalarUnsigned[uint16]
	Uint32Add            = NumericAddUnsigned[uint32]
	Uint32AddScalar      = NumericAddScalarUnsigned[uint32]
	Uint64Add            = NumericAddUnsigned[uint64]
	Uint64AddScalar      = NumericAddScalarUnsigned[uint64]
	Float32Add           = NumericAdd[float32]
	Float32AddScalar     = NumericAddScalar[float32]
	Float64Add           = NumericAdd[float64]
	Float64AddScalar     = NumericAddScalar[float64]
	Int8AddSels          = NumericAddSelsSigned[int8]
	Int8AddScalarSels    = NumericAddScalarSelsSigned[int8]
	Int16AddSels         = NumericAddSelsSigned[int16]
	Int16AddScalarSels   = NumericAddScalarSelsSigned[int16]
	Int32AddSels         = NumericAddSelsSigned[int32]
	Int32AddScalarSels   = NumericAddScalarSelsSigned[int32]
	Int64AddSels         = NumericAddSelsSigned[int64]
	Int64AddScalarSels   = NumericAddScalarSelsSigned[int64]
	Uint8AddSels         = NumericAddSelsUnsigned[uint8]
	Uint8AddScalarSels   = NumericAddScalarSelsUnsigned[uint8]
	Uint16AddSels        = NumericAddSelsUnsigned[uint16]
	Uint16AddScalarSels  = NumericAddScalarSelsUnsigned[uint16]
	Uint32AddSels        = NumericAddSelsUnsigned[uint32]
	Uint32AddScalarSels  = NumericAddScalarSelsUnsigned[uint32]
	Uint64AddSels        = NumericAddSelsUnsigned[uint64]
	Uint64AddScalarSels  = NumericAddScalarSelsUnsigned[uint64]
	Float32AddSels       = NumericAddSels[float32]
	Float32AddScalarSels = NumericAddScalarSels[float32]
	Float64AddSels       = NumericAddSels[float64]
	Float64AddScalarSels = NumericAddScalarSels[float64]

	Int32Int64Add               = NumericAdd2[int32, int64]
	Int32Int64AddScalar         = NumericAddScalar2[int32, int64]
	Int32Int64AddSels           = NumericAddSels2[int32, int64]
	Int32Int64AddScalarSels     = NumericAddScalarSels2[int32, int64]
	Int16Int64Add               = NumericAdd2[int16, int64]
	Int16Int64AddScalar         = NumericAddScalar2[int16, int64]
	Int16Int64AddSels           = NumericAddSels2[int16, int64]
	Int16Int64AddScalarSels     = NumericAddScalarSels2[int16, int64]
	Int8Int64Add                = NumericAdd2[int8, int64]
	Int8Int64AddScalar          = NumericAddScalar2[int8, int64]
	Int8Int64AddSels            = NumericAddSels2[int8, int64]
	Int8Int64AddScalarSels      = NumericAddScalarSels2[int8, int64]
	Int16Int32Add               = NumericAdd2[int16, int32]
	Int16Int32AddScalar         = NumericAddScalar2[int16, int32]
	Int16Int32AddSels           = NumericAddSels2[int16, int32]
	Int16Int32AddScalarSels     = NumericAddScalarSels2[int16, int32]
	Int8Int32Add                = NumericAdd2[int8, int32]
	Int8Int32AddScalar          = NumericAddScalar2[int8, int32]
	Int8Int32AddSels            = NumericAddSels2[int8, int32]
	Int8Int32AddScalarSels      = NumericAddScalarSels2[int8, int32]
	Int8Int16Add                = NumericAdd2[int8, int16]
	Int8Int16AddScalar          = NumericAddScalar2[int8, int16]
	Int8Int16AddSels            = NumericAddSels2[int8, int16]
	Int8Int16AddScalarSels      = NumericAddScalarSels2[int8, int16]
	Float32Float64Add           = NumericAdd2[float32, float64]
	Float32Float64AddScalar     = NumericAddScalar2[float32, float64]
	Float32Float64AddSels       = NumericAddSels2[float32, float64]
	Float32Float64AddScalarSels = NumericAddScalarSels2[float32, float64]
	Uint32Uint64Add             = NumericAdd2[uint32, uint64]
	Uint32Uint64AddScalar       = NumericAddScalar2[uint32, uint64]
	Uint32Uint64AddSels         = NumericAddSels2[uint32, uint64]
	Uint32Uint64AddScalarSels   = NumericAddScalarSels2[uint32, uint64]
	Uint16Uint64Add             = NumericAdd2[uint16, uint64]
	Uint16Uint64AddScalar       = NumericAddScalar2[uint16, uint64]
	Uint16Uint64AddSels         = NumericAddSels2[uint16, uint64]
	Uint16Uint64AddScalarSels   = NumericAddScalarSels2[uint16, uint64]
	Uint8Uint64Add              = NumericAdd2[uint8, uint64]
	Uint8Uint64AddScalar        = NumericAddScalar2[uint8, uint64]
	Uint8Uint64AddSels          = NumericAddSels2[uint8, uint64]
	Uint8Uint64AddScalarSels    = NumericAddScalarSels2[uint8, uint64]
	Uint16Uint32Add             = NumericAdd2[uint16, uint32]
	Uint16Uint32AddScalar       = NumericAddScalar2[uint16, uint32]
	Uint16Uint32AddSels         = NumericAddSels2[uint16, uint32]
	Uint16Uint32AddScalarSels   = NumericAddScalarSels2[uint16, uint32]
	Uint8Uint32Add              = NumericAdd2[uint8, uint32]
	Uint8Uint32AddScalar        = NumericAddScalar2[uint8, uint32]
	Uint8Uint32AddSels          = NumericAddSels2[uint8, uint32]
	Uint8Uint32AddScalarSels    = NumericAddScalarSels2[uint8, uint32]
	Uint8Uint16Add              = NumericAdd2[uint8, uint16]
	Uint8Uint16AddScalar        = NumericAddScalar2[uint8, uint16]
	Uint8Uint16AddSels          = NumericAddSels2[uint8, uint16]
	Uint8Uint16AddScalarSels    = NumericAddScalarSels2[uint8, uint16]

	Decimal64Add            = decimal64Add
	Decimal64AddSels        = decimal64AddSels
	Decimal64AddScalar      = decimal64AddScalar
	Decimal64AddScalarSels  = decimal64AddScalarSels
	Decimal128Add           = decimal128Add
	Decimal128AddSels       = decimal128AddSels
	Decimal128AddScalar     = decimal128AddScalar
	Decimal128AddScalarSels = decimal128AddScalarSels
)

// this is the slowest overflow check
func NumericAddSigned[T constraints.Signed](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x + ys[i]
		if x > 0 && ys[i] > 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddSelsSigned[T constraints.Signed](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
		if xs[sel] > 0 && ys[i] > 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddScalarSigned[T constraints.Signed](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
		if x > 0 && y > 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
		if x < 0 && y < 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddScalarSelsSigned[T constraints.Signed](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
		if x > 0 && ys[sel] > 0 && rs[i] <= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
		if x < 0 && ys[sel] < 0 && rs[i] >= 0 {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddUnsigned[T constraints.Unsigned](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x + ys[i]
		if rs[i] < x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddSelsUnsigned[T constraints.Unsigned](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
		if rs[i] < xs[sel] {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddScalarUnsigned[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
		if rs[i] < x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAddScalarSelsUnsigned[T constraints.Unsigned](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
		if rs[i] < x {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
		}
	}
	return rs
}

func NumericAdd[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func NumericAddSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func NumericAddScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func NumericAddScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func NumericAdd2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) + ys[i]
	}
	return rs
}

func NumericAddSels2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) + ys[sel]
	}
	return rs
}

func NumericAddScalar2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) + y
	}
	return rs
}

func NumericAddScalarSels2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(x) + ys[sel]
	}
	return rs
}

func decimal64Add(xs []types.Decimal64, ys []types.Decimal64, xsScale int32, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	for i, x := range xs {
		rs[i] = types.Decimal64AddAligned(x, ys[i])
	}
	return rs
}

func decimal64AddSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Add(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal64AddScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	for i, y := range ys {
		rs[i] = types.Decimal64AddAligned(x, y)
	}
	return rs
}

func decimal64AddScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Add(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128Add(xs []types.Decimal128, ys []types.Decimal128, xsScale int32, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = types.Decimal128AddAligned(x, ys[i])
	}
	return rs
}

func decimal128AddSels(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Add(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal128AddScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128AddAligned(x, y)
	}
	return rs
}

func decimal128AddScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Add(x, ys[sel], xScale, ysScale)
	}
	return rs
}
