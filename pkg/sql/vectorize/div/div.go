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

package div

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Div                = NumericDiv[int8]
	Int8DivSels            = NumericDivSels[int8]
	Int8DivScalar          = NumericDivScalar[int8]
	Int8DivScalarSels      = NumericDivScalarSels[int8]
	Int8DivByScalar        = NumericDivByScalar[int8]
	Int8DivByScalarSels    = NumericDivByScalarSels[int8]
	Int16Div               = NumericDiv[int16]
	Int16DivSels           = NumericDivSels[int16]
	Int16DivScalar         = NumericDivScalar[int16]
	Int16DivScalarSels     = NumericDivScalarSels[int16]
	Int16DivByScalar       = NumericDivByScalar[int16]
	Int16DivByScalarSels   = NumericDivByScalarSels[int16]
	Int32Div               = NumericDiv[int32]
	Int32DivSels           = NumericDivSels[int32]
	Int32DivScalar         = NumericDivScalar[int32]
	Int32DivScalarSels     = NumericDivScalarSels[int32]
	Int32DivByScalar       = NumericDivByScalar[int32]
	Int32DivByScalarSels   = NumericDivByScalarSels[int32]
	Int64Div               = NumericDiv[int64]
	Int64DivSels           = NumericDivSels[int64]
	Int64DivScalar         = NumericDivScalar[int64]
	Int64DivScalarSels     = NumericDivScalarSels[int64]
	Int64DivByScalar       = NumericDivByScalar[int64]
	Int64DivByScalarSels   = NumericDivByScalarSels[int64]
	Uint8Div               = NumericDiv[uint8]
	Uint8DivSels           = NumericDivSels[uint8]
	Uint8DivScalar         = NumericDivScalar[uint8]
	Uint8DivScalarSels     = NumericDivScalarSels[uint8]
	Uint8DivByScalar       = NumericDivByScalar[uint8]
	Uint8DivByScalarSels   = NumericDivByScalarSels[uint8]
	Uint16Div              = NumericDiv[uint16]
	Uint16DivSels          = NumericDivSels[uint16]
	Uint16DivScalar        = NumericDivScalar[uint16]
	Uint16DivScalarSels    = NumericDivScalarSels[uint16]
	Uint16DivByScalar      = NumericDivByScalar[uint16]
	Uint16DivByScalarSels  = NumericDivByScalarSels[uint16]
	Uint32Div              = NumericDiv[uint32]
	Uint32DivSels          = NumericDivSels[uint32]
	Uint32DivScalar        = NumericDivScalar[uint32]
	Uint32DivScalarSels    = NumericDivScalarSels[uint32]
	Uint32DivByScalar      = NumericDivByScalar[uint32]
	Uint32DivByScalarSels  = NumericDivByScalarSels[uint32]
	Uint64Div              = NumericDiv[uint64]
	Uint64DivSels          = NumericDivSels[uint64]
	Uint64DivScalar        = NumericDivScalar[uint64]
	Uint64DivScalarSels    = NumericDivScalarSels[uint64]
	Uint64DivByScalar      = NumericDivByScalar[uint64]
	Uint64DivByScalarSels  = NumericDivByScalarSels[uint64]
	Float32Div             = NumericDiv[float32]
	Float32DivSels         = NumericDivSels[float32]
	Float32DivScalar       = NumericDivScalar[float32]
	Float32DivScalarSels   = NumericDivScalarSels[float32]
	Float32DivByScalar     = NumericDivByScalar[float32]
	Float32DivByScalarSels = NumericDivByScalarSels[float32]
	Float64Div             = NumericDiv[float64]
	Float64DivSels         = NumericDivSels[float64]
	Float64DivScalar       = NumericDivScalar[float64]
	Float64DivScalarSels   = NumericDivScalarSels[float64]
	Float64DivByScalar     = NumericDivByScalar[float64]
	Float64DivByScalarSels = NumericDivByScalarSels[float64]

	Decimal64Div              = decimal64Div
	Decimal64DivSels          = decimal64DivSels
	Decimal64DivScalar        = decimal64DivScalar
	Decimal64DivScalarSels    = decimal64DivScalarSels
	Decimal64DivByScalar      = decimal64DivByScalar
	Decimal64DivByScalarSels  = decimal64DivByScalarSels
	Decimal128Div             = decimal128Div
	Decimal128DivSels         = decimal128DivSels
	Decimal128DivScalar       = decimal128DivScalar
	Decimal128DivScalarSels   = decimal128DivScalarSels
	Decimal128DivByScalar     = decimal128DivByScalar
	Decimal128DivByScalarSels = decimal128DivByScalarSels

	Float32IntegerDiv             = FloatIntegerDiv[float32]
	Float32IntegerDivSels         = FloatIntegerDivSels[float32]
	Float32IntegerDivScalar       = FloatIntegerDivScalar[float32]
	Float32IntegerDivScalarSels   = FloatIntegerDivScalarSels[float32]
	Float32IntegerDivByScalar     = FloatIntegerDivByScalar[float32]
	Float32IntegerDivByScalarSels = FloatIntegerDivByScalarSels[float32]

	Float64IntegerDiv             = FloatIntegerDiv[float64]
	Float64IntegerDivSels         = FloatIntegerDivSels[float64]
	Float64IntegerDivScalar       = FloatIntegerDivScalar[float64]
	Float64IntegerDivScalarSels   = FloatIntegerDivScalarSels[float64]
	Float64IntegerDivByScalar     = FloatIntegerDivByScalar[float64]
	Float64IntegerDivByScalarSels = FloatIntegerDivByScalarSels[float64]
)

func NumericDiv[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func NumericDivSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func NumericDivScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func NumericDivScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func NumericDivByScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func NumericDivByScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func FloatIntegerDiv[T constraints.Float](xs, ys []T, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x / ys[i])
	}
	return rs
}

func FloatIntegerDivSels[T constraints.Float](xs, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(xs[sel] / ys[sel])
	}
	return rs
}

func FloatIntegerDivScalar[T constraints.Float](x T, ys []T, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(x / y)
	}
	return rs
}

func FloatIntegerDivScalarSels[T constraints.Float](x T, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(x / ys[sel])
	}
	return rs
}

func FloatIntegerDivByScalar[T constraints.Float](x T, ys []T, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(y / x)
	}
	return rs
}

func FloatIntegerDivByScalarSels[T constraints.Float](x T, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(ys[sel] / x)
	}
	return rs
}

func decimal64Div(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = types.Decimal64Decimal64Div(x, ys[i])
	}
	return rs
}

func decimal64DivSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(xs[sel], ys[sel])
	}
	return rs
}

func decimal64DivScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal64Decimal64Div(x, y)
	}
	return rs
}

func decimal64DivScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(x, ys[sel])
	}
	return rs
}

func decimal64DivByScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal64Decimal64Div(y, x)
	}
	return rs
}

func decimal64DivByScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(x, ys[sel])
	}
	return rs
}

func decimal128Div(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = types.Decimal128Decimal128Div(x, ys[i])
	}
	return rs
}

func decimal128DivSels(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(xs[sel], ys[sel])
	}
	return rs
}

func decimal128DivScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128Decimal128Div(x, y)
	}
	return rs
}

func decimal128DivScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(x, ys[sel])
	}
	return rs
}

func decimal128DivByScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128Decimal128Div(y, x)
	}
	return rs
}

func decimal128DivByScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(x, ys[sel])
	}
	return rs
}
