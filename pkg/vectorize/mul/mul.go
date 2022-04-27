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
	Int8Mul              = numericMul[int8]
	Int8MulSels          = numericMulSels[int8]
	Int8MulScalar        = numericMulScalar[int8]
	Int8MulScalarSels    = numericMulScalarSels[int8]
	Int16Mul             = numericMul[int16]
	Int16MulSels         = numericMulSels[int16]
	Int16MulScalar       = numericMulScalar[int16]
	Int16MulScalarSels   = numericMulScalarSels[int16]
	Int32Mul             = numericMul[int32]
	Int32MulSels         = numericMulSels[int32]
	Int32MulScalar       = numericMulScalar[int32]
	Int32MulScalarSels   = numericMulScalarSels[int32]
	Int64Mul             = numericMul[int64]
	Int64MulSels         = numericMulSels[int64]
	Int64MulScalar       = numericMulScalar[int64]
	Int64MulScalarSels   = numericMulScalarSels[int64]
	Uint8Mul             = numericMul[uint8]
	Uint8MulSels         = numericMulSels[uint8]
	Uint8MulScalar       = numericMulScalar[uint8]
	Uint8MulScalarSels   = numericMulScalarSels[uint8]
	Uint16Mul            = numericMul[uint16]
	Uint16MulSels        = numericMulSels[uint16]
	Uint16MulScalar      = numericMulScalar[uint16]
	Uint16MulScalarSels  = numericMulScalarSels[uint16]
	Uint32Mul            = numericMul[uint32]
	Uint32MulSels        = numericMulSels[uint32]
	Uint32MulScalar      = numericMulScalar[uint32]
	Uint32MulScalarSels  = numericMulScalarSels[uint32]
	Uint64Mul            = numericMul[uint64]
	Uint64MulSels        = numericMulSels[uint64]
	Uint64MulScalar      = numericMulScalar[uint64]
	Uint64MulScalarSels  = numericMulScalarSels[uint64]
	Float32Mul           = numericMul[float32]
	Float32MulSels       = numericMulSels[float32]
	Float32MulScalar     = numericMulScalar[float32]
	Float32MulScalarSels = numericMulScalarSels[float32]
	Float64Mul           = numericMul[float64]
	Float64MulSels       = numericMulSels[float64]
	Float64MulScalar     = numericMulScalar[float64]
	Float64MulScalarSels = numericMulScalarSels[float64]

	Int32Int64Mul         = numericMul2[int32, int64]
	Int32Int64MulSels     = numericMulSels2[int32, int64]
	Int16Int64Mul         = numericMul2[int16, int64]
	Int16Int64MulSels     = numericMulSels2[int16, int64]
	Int8Int64Mul          = numericMul2[int8, int64]
	Int8Int64MulSels      = numericMulSels2[int8, int64]
	Int16Int32Mul         = numericMul2[int16, int32]
	Int16Int32MulSels     = numericMulSels2[int16, int32]
	Int8Int32Mul          = numericMul2[int8, int32]
	Int8Int32MulSels      = numericMulSels2[int8, int32]
	Int8Int16Mul          = numericMul2[int8, int16]
	Int8Int16MulSels      = numericMulSels2[int8, int16]
	Float32Float64Mul     = numericMul2[float32, float64]
	Float32Float64MulSels = numericMulSels2[float32, float64]
	Uint32Uint64Mul       = numericMul2[uint32, uint64]
	Uint32Uint64MulSels   = numericMulSels2[uint32, uint64]
	Uint16Uint64Mul       = numericMul2[uint16, uint64]
	Uint16Uint64MulSels   = numericMulSels2[uint16, uint64]
	Uint8Uint64Mul        = numericMul2[uint8, uint64]
	Uint8Uint64MulSels    = numericMulSels2[uint8, uint64]
	Uint16Uint32Mul       = numericMul2[uint16, uint32]
	Uint16Uint32MulSels   = numericMulSels2[uint16, uint32]
	Uint8Uint32Mul        = numericMul2[uint8, uint32]
	Uint8Uint32MulSels    = numericMulSels2[uint8, uint32]
	Uint8Uint16Mul        = numericMul2[uint8, uint16]
	Uint8Uint16MulSels    = numericMulSels2[uint8, uint16]

	Decimal64Mul            = decimal64Mul
	Decimal64MulSels        = decimal64MulSels
	Decimal64MulScalar      = decimal64MulScalar
	Decimal64MulScalarSels  = decimal64MulScalarSels
	Decimal128Mul           = decimal128Mul
	Decimal128MulSels       = decimal128MulSels
	Decimal128MulScalar     = decimal128MulScalar
	Decimal128MulScalarSels = decimal128MulScalarSels
)

func numericMul[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func numericMulSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func numericMulScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func numericMulScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}

func numericMul2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) * ys[i]
	}
	return rs
}

func numericMulSels2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) * ys[sel]
	}
	return rs
}

/*
func numericMulScalar2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) * y
	}
	return rs
}

func numericMulScalarSels2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
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
