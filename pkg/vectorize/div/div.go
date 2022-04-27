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
	Int8Div                = numericDiv[int8]
	Int8DivSels            = numericDivSels[int8]
	Int8DivScalar          = numericDivScalar[int8]
	Int8DivScalarSels      = numericDivScalarSels[int8]
	Int8DivByScalar        = numericDivByScalar[int8]
	Int8DivByScalarSels    = numericDivByScalarSels[int8]
	Int16Div               = numericDiv[int16]
	Int16DivSels           = numericDivSels[int16]
	Int16DivScalar         = numericDivScalar[int16]
	Int16DivScalarSels     = numericDivScalarSels[int16]
	Int16DivByScalar       = numericDivByScalar[int16]
	Int16DivByScalarSels   = numericDivByScalarSels[int16]
	Int32Div               = numericDiv[int32]
	Int32DivSels           = numericDivSels[int32]
	Int32DivScalar         = numericDivScalar[int32]
	Int32DivScalarSels     = numericDivScalarSels[int32]
	Int32DivByScalar       = numericDivByScalar[int32]
	Int32DivByScalarSels   = numericDivByScalarSels[int32]
	Int64Div               = numericDiv[int64]
	Int64DivSels           = numericDivSels[int64]
	Int64DivScalar         = numericDivScalar[int64]
	Int64DivScalarSels     = numericDivScalarSels[int64]
	Int64DivByScalar       = numericDivByScalar[int64]
	Int64DivByScalarSels   = numericDivByScalarSels[int64]
	Uint8Div               = numericDiv[uint8]
	Uint8DivSels           = numericDivSels[uint8]
	Uint8DivScalar         = numericDivScalar[uint8]
	Uint8DivScalarSels     = numericDivScalarSels[uint8]
	Uint8DivByScalar       = numericDivByScalar[uint8]
	Uint8DivByScalarSels   = numericDivByScalarSels[uint8]
	Uint16Div              = numericDiv[uint16]
	Uint16DivSels          = numericDivSels[uint16]
	Uint16DivScalar        = numericDivScalar[uint16]
	Uint16DivScalarSels    = numericDivScalarSels[uint16]
	Uint16DivByScalar      = numericDivByScalar[uint16]
	Uint16DivByScalarSels  = numericDivByScalarSels[uint16]
	Uint32Div              = numericDiv[uint32]
	Uint32DivSels          = numericDivSels[uint32]
	Uint32DivScalar        = numericDivScalar[uint32]
	Uint32DivScalarSels    = numericDivScalarSels[uint32]
	Uint32DivByScalar      = numericDivByScalar[uint32]
	Uint32DivByScalarSels  = numericDivByScalarSels[uint32]
	Uint64Div              = numericDiv[uint64]
	Uint64DivSels          = numericDivSels[uint64]
	Uint64DivScalar        = numericDivScalar[uint64]
	Uint64DivScalarSels    = numericDivScalarSels[uint64]
	Uint64DivByScalar      = numericDivByScalar[uint64]
	Uint64DivByScalarSels  = numericDivByScalarSels[uint64]
	Float32Div             = numericDiv[float32]
	Float32DivSels         = numericDivSels[float32]
	Float32DivScalar       = numericDivScalar[float32]
	Float32DivScalarSels   = numericDivScalarSels[float32]
	Float32DivByScalar     = numericDivByScalar[float32]
	Float32DivByScalarSels = numericDivByScalarSels[float32]
	Float64Div             = numericDiv[float64]
	Float64DivSels         = numericDivSels[float64]
	Float64DivScalar       = numericDivScalar[float64]
	Float64DivScalarSels   = numericDivScalarSels[float64]
	Float64DivByScalar     = numericDivByScalar[float64]
	Float64DivByScalarSels = numericDivByScalarSels[float64]

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

	Float32IntegerDiv             = floatIntegerDiv[float32]
	Float32IntegerDivSels         = floatIntegerDivSels[float32]
	Float32IntegerDivScalar       = floatIntegerDivScalar[float32]
	Float32IntegerDivScalarSels   = floatIntegerDivScalarSels[float32]
	Float32IntegerDivByScalar     = floatIntegerDivByScalar[float32]
	Float32IntegerDivByScalarSels = floatIntegerDivByScalarSels[float32]

	Float64IntegerDiv             = floatIntegerDiv[float64]
	Float64IntegerDivSels         = floatIntegerDivSels[float64]
	Float64IntegerDivScalar       = floatIntegerDivScalar[float64]
	Float64IntegerDivScalarSels   = floatIntegerDivScalarSels[float64]
	Float64IntegerDivByScalar     = floatIntegerDivByScalar[float64]
	Float64IntegerDivByScalarSels = floatIntegerDivByScalarSels[float64]
)

func numericDiv[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func numericDivSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func numericDivScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func numericDivScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func numericDivByScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func numericDivByScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func floatIntegerDiv[T constraints.Float](xs, ys []T, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x / ys[i])
	}
	return rs
}

func floatIntegerDivSels[T constraints.Float](xs, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(xs[sel] / ys[sel])
	}
	return rs
}

func floatIntegerDivScalar[T constraints.Float](x T, ys []T, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(x / y)
	}
	return rs
}

func floatIntegerDivScalarSels[T constraints.Float](x T, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(x / ys[sel])
	}
	return rs
}

func floatIntegerDivByScalar[T constraints.Float](x T, ys []T, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(y / x)
	}
	return rs
}

func floatIntegerDivByScalarSels[T constraints.Float](x T, ys []T, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(ys[sel] / x)
	}
	return rs
}

func decimal64Div(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	// a / b
	// to divide two decimal value
	// 1. scale dividend using divisor's scale
	// 2. perform integer division
	// division result precision: 38(decimal64) division result scale: dividend's scale
	for i, x := range xs {
		rs[i] = types.Decimal64Decimal64Div(x, ys[i], xsScale, ysScale)
	}
	return rs
}

func decimal64DivSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal64DivScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal64Decimal64Div(x, y, xScale, ysScale)
	}
	return rs
}

func decimal64DivScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal64DivByScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal64Decimal64Div(y, x, ysScale, xScale)
	}
	return rs
}

func decimal64DivByScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal64Decimal64Div(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128Div(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	// a / b
	// to divide two decimal value
	// 1. scale dividend using divisor's scale
	// 2. perform integer division
	// division result precision: 38(decimal128) division result scale: dividend's scale
	for i, x := range xs {
		rs[i] = types.Decimal128Decimal128Div(x, ys[i], xsScale, ysScale)
	}
	return rs
}

func decimal128DivSels(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal128DivScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128Decimal128Div(x, y, xScale, ysScale)
	}
	return rs
}

func decimal128DivScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128DivByScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128Decimal128Div(y, x, ysScale, xScale)
	}
	return rs
}

func decimal128DivByScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for _, sel := range sels {
		rs[sel] = types.Decimal128Decimal128Div(x, ys[sel], xScale, ysScale)
	}
	return rs
}
