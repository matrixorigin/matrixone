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

package mul

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	Int8Mul              = NumericMul[int8]
	Int8MulSels          = NumericMulSels[int8]
	Int8MulScalar        = NumericMulScalar[int8]
	Int8MulScalarSels    = NumericMulScalarSels[int8]
	Int16Mul             = NumericMul[int16]
	Int16MulSels         = NumericMulSels[int16]
	Int16MulScalar       = NumericMulScalar[int16]
	Int16MulScalarSels   = NumericMulScalarSels[int16]
	Int32Mul             = NumericMul[int32]
	Int32MulSels         = NumericMulSels[int32]
	Int32MulScalar       = NumericMulScalar[int32]
	Int32MulScalarSels   = NumericMulScalarSels[int32]
	Int64Mul             = NumericMul[int64]
	Int64MulSels         = NumericMulSels[int64]
	Int64MulScalar       = NumericMulScalar[int64]
	Int64MulScalarSels   = NumericMulScalarSels[int64]
	Uint8Mul             = NumericMul[uint8]
	Uint8MulSels         = NumericMulSels[uint8]
	Uint8MulScalar       = NumericMulScalar[uint8]
	Uint8MulScalarSels   = NumericMulScalarSels[uint8]
	Uint16Mul            = NumericMul[uint16]
	Uint16MulSels        = NumericMulSels[uint16]
	Uint16MulScalar      = NumericMulScalar[uint16]
	Uint16MulScalarSels  = NumericMulScalarSels[uint16]
	Uint32Mul            = NumericMul[uint32]
	Uint32MulSels        = NumericMulSels[uint32]
	Uint32MulScalar      = NumericMulScalar[uint32]
	Uint32MulScalarSels  = NumericMulScalarSels[uint32]
	Uint64Mul            = NumericMul[uint64]
	Uint64MulSels        = NumericMulSels[uint64]
	Uint64MulScalar      = NumericMulScalar[uint64]
	Uint64MulScalarSels  = NumericMulScalarSels[uint64]
	Float32Mul           = NumericMul[float32]
	Float32MulSels       = NumericMulSels[float32]
	Float32MulScalar     = NumericMulScalar[float32]
	Float32MulScalarSels = NumericMulScalarSels[float32]
	Float64Mul           = NumericMul[float64]
	Float64MulSels       = NumericMulSels[float64]
	Float64MulScalar     = NumericMulScalar[float64]
	Float64MulScalarSels = NumericMulScalarSels[float64]

	Int32Int64Mul         = NumericMul2[int32, int64]
	Int32Int64MulSels     = NumericMulSels2[int32, int64]
	Int16Int64Mul         = NumericMul2[int16, int64]
	Int16Int64MulSels     = NumericMulSels2[int16, int64]
	Int8Int64Mul          = NumericMul2[int8, int64]
	Int8Int64MulSels      = NumericMulSels2[int8, int64]
	Int16Int32Mul         = NumericMul2[int16, int32]
	Int16Int32MulSels     = NumericMulSels2[int16, int32]
	Int8Int32Mul          = NumericMul2[int8, int32]
	Int8Int32MulSels      = NumericMulSels2[int8, int32]
	Int8Int16Mul          = NumericMul2[int8, int16]
	Int8Int16MulSels      = NumericMulSels2[int8, int16]
	Float32Float64Mul     = NumericMul2[float32, float64]
	Float32Float64MulSels = NumericMulSels2[float32, float64]
	Uint32Uint64Mul       = NumericMul2[uint32, uint64]
	Uint32Uint64MulSels   = NumericMulSels2[uint32, uint64]
	Uint16Uint64Mul       = NumericMul2[uint16, uint64]
	Uint16Uint64MulSels   = NumericMulSels2[uint16, uint64]
	Uint8Uint64Mul        = NumericMul2[uint8, uint64]
	Uint8Uint64MulSels    = NumericMulSels2[uint8, uint64]
	Uint16Uint32Mul       = NumericMul2[uint16, uint32]
	Uint16Uint32MulSels   = NumericMulSels2[uint16, uint32]
	Uint8Uint32Mul        = NumericMul2[uint8, uint32]
	Uint8Uint32MulSels    = NumericMulSels2[uint8, uint32]
	Uint8Uint16Mul        = NumericMul2[uint8, uint16]
	Uint8Uint16MulSels    = NumericMulSels2[uint8, uint16]

	Decimal64Mul            = decimal64Mul
	Decimal64MulSels        = decimal64MulSels
	Decimal64MulScalar      = decimal64MulScalar
	Decimal64MulScalarSels  = decimal64MulScalarSels
	Decimal128Mul           = decimal128Mul
	Decimal128MulSels       = decimal128MulSels
	Decimal128MulScalar     = decimal128MulScalar
	Decimal128MulScalarSels = decimal128MulScalarSels
)

func NumericMul[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func NumericMulSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func NumericMulScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func NumericMulScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func NumericMul2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) * ys[i]
	}
	return rs
}

func NumericMulSels2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) * ys[sel]
	}
	return rs
}

/*
func NumericMulScalar2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) * y
	}
	return rs
}

func NumericMulScalarSels2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(x) * ys[sel]
	}
	return rs
}
*/

func decimal64Mul(xs []types.Decimal64, ys []types.Decimal64, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = types.Decimal64Decimal64Mul(x, ys[i])
	}
	return rs
}

func decimal64MulSels(xs, ys []types.Decimal64, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Decimal64Mul(xs[sel], ys[sel])
	}
	return rs
}

func decimal64MulScalar(x types.Decimal64, ys []types.Decimal64, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal64Decimal64Mul(x, y)
	}
	return rs
}

func decimal64MulScalarSels(x types.Decimal64, ys []types.Decimal64, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Decimal64Mul(x, ys[sel])
	}
	return rs
}

func decimal128Mul(xs []types.Decimal128, ys []types.Decimal128, rs []types.Decimal128) []types.Decimal128 {
	for i, x := range xs {
		rs[i] = types.Decimal128Decimal128Mul(x, ys[i])
	}
	return rs
}

func decimal128MulSels(xs, ys []types.Decimal128, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Decimal128Mul(xs[sel], ys[sel])
	}
	return rs
}

func decimal128MulScalar(x types.Decimal128, ys []types.Decimal128, rs []types.Decimal128) []types.Decimal128 {
	for i, y := range ys {
		rs[i] = types.Decimal128Decimal128Mul(x, y)
	}
	return rs
}

func decimal128MulScalarSels(x types.Decimal128, ys []types.Decimal128, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Decimal128Mul(x, ys[sel])
	}
	return rs
}
