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

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	Int8Div                func([]int8, []int8, []int8) []int8
	Int8DivSels            func([]int8, []int8, []int8, []int64) []int8
	Int8DivScalar          func(int8, []int8, []int8) []int8
	Int8DivScalarSels      func(int8, []int8, []int8, []int64) []int8
	Int8DivByScalar        func(int8, []int8, []int8) []int8
	Int8DivByScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Div               func([]int16, []int16, []int16) []int16
	Int16DivSels           func([]int16, []int16, []int16, []int64) []int16
	Int16DivScalar         func(int16, []int16, []int16) []int16
	Int16DivScalarSels     func(int16, []int16, []int16, []int64) []int16
	Int16DivByScalar       func(int16, []int16, []int16) []int16
	Int16DivByScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Div               func([]int32, []int32, []int32) []int32
	Int32DivSels           func([]int32, []int32, []int32, []int64) []int32
	Int32DivScalar         func(int32, []int32, []int32) []int32
	Int32DivScalarSels     func(int32, []int32, []int32, []int64) []int32
	Int32DivByScalar       func(int32, []int32, []int32) []int32
	Int32DivByScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Div               func([]int64, []int64, []int64) []int64
	Int64DivSels           func([]int64, []int64, []int64, []int64) []int64
	Int64DivScalar         func(int64, []int64, []int64) []int64
	Int64DivScalarSels     func(int64, []int64, []int64, []int64) []int64
	Int64DivByScalar       func(int64, []int64, []int64) []int64
	Int64DivByScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Div               func([]uint8, []uint8, []uint8) []uint8
	Uint8DivSels           func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8DivScalar         func(uint8, []uint8, []uint8) []uint8
	Uint8DivScalarSels     func(uint8, []uint8, []uint8, []int64) []uint8
	Uint8DivByScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8DivByScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Div              func([]uint16, []uint16, []uint16) []uint16
	Uint16DivSels          func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16DivScalar        func(uint16, []uint16, []uint16) []uint16
	Uint16DivScalarSels    func(uint16, []uint16, []uint16, []int64) []uint16
	Uint16DivByScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16DivByScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Div              func([]uint32, []uint32, []uint32) []uint32
	Uint32DivSels          func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32DivScalar        func(uint32, []uint32, []uint32) []uint32
	Uint32DivScalarSels    func(uint32, []uint32, []uint32, []int64) []uint32
	Uint32DivByScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32DivByScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Div              func([]uint64, []uint64, []uint64) []uint64
	Uint64DivSels          func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64DivScalar        func(uint64, []uint64, []uint64) []uint64
	Uint64DivScalarSels    func(uint64, []uint64, []uint64, []int64) []uint64
	Uint64DivByScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64DivByScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Div             func([]float32, []float32, []float32) []float32
	Float32DivSels         func([]float32, []float32, []float32, []int64) []float32
	Float32DivScalar       func(float32, []float32, []float32) []float32
	Float32DivScalarSels   func(float32, []float32, []float32, []int64) []float32
	Float32DivByScalar     func(float32, []float32, []float32) []float32
	Float32DivByScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Div             func([]float64, []float64, []float64) []float64
	Float64DivSels         func([]float64, []float64, []float64, []int64) []float64
	Float64DivScalar       func(float64, []float64, []float64) []float64
	Float64DivScalarSels   func(float64, []float64, []float64, []int64) []float64
	Float64DivByScalar     func(float64, []float64, []float64) []float64
	Float64DivByScalarSels func(float64, []float64, []float64, []int64) []float64

	Float32IntegerDiv             func([]float32, []float32, []int64) []int64
	Float32IntegerDivSels         func([]float32, []float32, []int64, []int64) []int64
	Float32IntegerDivScalar       func(float32, []float32, []int64) []int64
	Float32IntegerDivScalarSels   func(float32, []float32, []int64, []int64) []int64
	Float32IntegerDivByScalar     func(float32, []float32, []int64) []int64
	Float32IntegerDivByScalarSels func(float32, []float32, []int64, []int64) []int64

	Float64IntegerDiv             func([]float64, []float64, []int64) []int64
	Float64IntegerDivSels         func([]float64, []float64, []int64, []int64) []int64
	Float64IntegerDivScalar       func(float64, []float64, []int64) []int64
	Float64IntegerDivScalarSels   func(float64, []float64, []int64, []int64) []int64
	Float64IntegerDivByScalar     func(float64, []float64, []int64) []int64
	Float64IntegerDivByScalarSels func(float64, []float64, []int64, []int64) []int64

	Decimal128Div             func([]types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128) []types.Decimal128
	Decimal128DivSels         func([]types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128, []int64) []types.Decimal128
	Decimal128DivScalar       func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128) []types.Decimal128
	Decimal128DivScalarSels   func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128, []int64) []types.Decimal128
	Decimal128DivByScalar     func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128) []types.Decimal128
	Decimal128DivByScalarSels func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128, []int64) []types.Decimal128
)

func init() {
	Int8Div = int8Div
	Int8DivSels = int8DivSels
	Int8DivScalar = int8DivScalar
	Int8DivScalarSels = int8DivScalarSels
	Int8DivByScalar = int8DivByScalar
	Int8DivByScalarSels = int8DivByScalarSels
	Int16Div = int16Div
	Int16DivSels = int16DivSels
	Int16DivScalar = int16DivScalar
	Int16DivScalarSels = int16DivScalarSels
	Int16DivByScalar = int16DivByScalar
	Int16DivByScalarSels = int16DivByScalarSels
	Int32Div = int32Div
	Int32DivSels = int32DivSels
	Int32DivScalar = int32DivScalar
	Int32DivScalarSels = int32DivScalarSels
	Int32DivByScalar = int32DivByScalar
	Int32DivByScalarSels = int32DivByScalarSels
	Int64Div = int64Div
	Int64DivSels = int64DivSels
	Int64DivScalar = int64DivScalar
	Int64DivScalarSels = int64DivScalarSels
	Int64DivByScalar = int64DivByScalar
	Int64DivByScalarSels = int64DivByScalarSels
	Uint8Div = uint8Div
	Uint8DivSels = uint8DivSels
	Uint8DivScalar = uint8DivScalar
	Uint8DivScalarSels = uint8DivScalarSels
	Uint8DivByScalar = uint8DivByScalar
	Uint8DivByScalarSels = uint8DivByScalarSels
	Uint16Div = uint16Div
	Uint16DivSels = uint16DivSels
	Uint16DivScalar = uint16DivScalar
	Uint16DivScalarSels = uint16DivScalarSels
	Uint16DivByScalar = uint16DivByScalar
	Uint16DivByScalarSels = uint16DivByScalarSels
	Uint32Div = uint32Div
	Uint32DivSels = uint32DivSels
	Uint32DivScalar = uint32DivScalar
	Uint32DivScalarSels = uint32DivScalarSels
	Uint32DivByScalar = uint32DivByScalar
	Uint32DivByScalarSels = uint32DivByScalarSels
	Uint64Div = uint64Div
	Uint64DivSels = uint64DivSels
	Uint64DivScalar = uint64DivScalar
	Uint64DivScalarSels = uint64DivScalarSels
	Uint64DivByScalar = uint64DivByScalar
	Uint64DivByScalarSels = uint64DivByScalarSels
	Float32Div = float32Div
	Float32DivSels = float32DivSels
	Float32DivScalar = float32DivScalar
	Float32DivScalarSels = float32DivScalarSels
	Float32DivByScalar = float32DivByScalar
	Float32DivByScalarSels = float32DivByScalarSels
	Float64Div = float64Div
	Float64DivSels = float64DivSels
	Float64DivScalar = float64DivScalar
	Float64DivScalarSels = float64DivScalarSels
	Float64DivByScalar = float64DivByScalar
	Float64DivByScalarSels = float64DivByScalarSels
	Decimal128Div = decimal128Div
	Decimal128DivSels = decimal128DivSels
	Decimal128DivScalar = decimal128DivScalar
	Decimal128DivScalarSels = decimal128DivScalarSels
	Decimal128DivByScalar = decimal128DivByScalar
	Decimal128DivByScalarSels = decimal128DivByScalarSels

	Float32IntegerDiv = float32IntegerDiv
	Float32IntegerDivSels = float32IntegerDivSels
	Float32IntegerDivScalar = float32IntegerDivScalar
	Float32IntegerDivScalarSels = float32IntegerDivScalarSels
	Float32IntegerDivByScalar = float32IntegerDivByScalar
	Float32IntegerDivByScalarSels = float32IntegerDivByScalarSels

	Float64IntegerDiv = float64IntegerDiv
	Float64IntegerDivSels = float64IntegerDivSels
	Float64IntegerDivScalar = float64IntegerDivScalar
	Float64IntegerDivScalarSels = float64IntegerDivScalarSels
	Float64IntegerDivByScalar = float64IntegerDivByScalar
	Float64IntegerDivByScalarSels = float64IntegerDivByScalarSels
}

func int8Div(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func int8DivSels(xs, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func int8DivScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func int8DivScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func int8DivByScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func int8DivByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func int16Div(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func int16DivSels(xs, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func int16DivScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func int16DivScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func int16DivByScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func int16DivByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func int32Div(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func int32DivSels(xs, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func int32DivScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func int32DivScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func int32DivByScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func int32DivByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func int64Div(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func int64DivSels(xs, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func int64DivScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func int64DivScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func int64DivByScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func int64DivByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func uint8Div(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func uint8DivSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func uint8DivScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func uint8DivScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func uint8DivByScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func uint8DivByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func uint16Div(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func uint16DivSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func uint16DivScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func uint16DivScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func uint16DivByScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func uint16DivByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func uint32Div(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func uint32DivSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func uint32DivScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func uint32DivScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func uint32DivByScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func uint32DivByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func uint64Div(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func uint64DivSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func uint64DivScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func uint64DivScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func uint64DivByScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func uint64DivByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func float32Div(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func float32DivSels(xs, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func float32DivScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func float32DivScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func float32DivByScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func float32DivByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func float64Div(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func float64DivSels(xs, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func float64DivScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func float64DivScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func float64DivByScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func float64DivByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}

func float32IntegerDiv(xs, ys []float32, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x / ys[i])
	}
	return rs
}

func float32IntegerDivSels(xs, ys []float32, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(xs[sel] / ys[sel])
	}
	return rs
}

func float32IntegerDivScalar(x float32, ys []float32, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(x / y)
	}
	return rs
}

func float32IntegerDivScalarSels(x float32, ys []float32, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(x / ys[sel])
	}
	return rs
}

func float32IntegerDivByScalar(x float32, ys []float32, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(y / x)
	}
	return rs
}

func float32IntegerDivByScalarSels(x float32, ys []float32, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(ys[sel] / x)
	}
	return rs
}

func float64IntegerDiv(xs, ys []float64, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = int64(x / ys[i])
	}
	return rs
}

func float64IntegerDivSels(xs, ys []float64, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(xs[sel] / ys[sel])
	}
	return rs
}

func float64IntegerDivScalar(x float64, ys []float64, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(x / y)
	}
	return rs
}

func float64IntegerDivScalarSels(x float64, ys []float64, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(x / ys[sel])
	}
	return rs
}

func float64IntegerDivByScalar(x float64, ys []float64, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = int64(y / x)
	}
	return rs
}

func float64IntegerDivByScalarSels(x float64, ys []float64, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = int64(ys[sel] / x)
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
