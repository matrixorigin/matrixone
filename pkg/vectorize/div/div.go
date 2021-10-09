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

import "matrixone/pkg/vectorize"

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
)

func init() {
	Int8Div = divGeneric[int8]
	Int8DivSels = divSelsGeneric[int8]
	Int8DivScalar = divScalarGeneric[int8]
	Int8DivScalarSels = divScalarSelsGeneric[int8]
	Int8DivByScalar = divByScalarGeneric[int8]
	Int8DivByScalarSels = divByScalarSelsGeneric[int8]
	Int16Div = divGeneric[int16]
	Int16DivSels = divSelsGeneric[int16]
	Int16DivScalar = divScalarGeneric[int16]
	Int16DivScalarSels = divScalarSelsGeneric[int16]
	Int16DivByScalar = divByScalarGeneric[int16]
	Int16DivByScalarSels = divByScalarSelsGeneric[int16]
	Int32Div = divGeneric[int32]
	Int32DivSels = divSelsGeneric[int32]
	Int32DivScalar = divScalarGeneric[int32]
	Int32DivScalarSels = divScalarSelsGeneric[int32]
	Int32DivByScalar = divByScalarGeneric[int32]
	Int32DivByScalarSels = divByScalarSelsGeneric[int32]
	Int64Div = divGeneric[int64]
	Int64DivSels = divSelsGeneric[int64]
	Int64DivScalar = divScalarGeneric[int64]
	Int64DivScalarSels = divScalarSelsGeneric[int64]
	Int64DivByScalar = divByScalarGeneric[int64]
	Int64DivByScalarSels = divByScalarSelsGeneric[int64]
	Uint8Div = divGeneric[uint8]
	Uint8DivSels = divSelsGeneric[uint8]
	Uint8DivScalar = divScalarGeneric[uint8]
	Uint8DivScalarSels = divScalarSelsGeneric[uint8]
	Uint8DivByScalar = divByScalarGeneric[uint8]
	Uint8DivByScalarSels = divByScalarSelsGeneric[uint8]
	Uint16Div = divGeneric[uint16]
	Uint16DivSels = divSelsGeneric[uint16]
	Uint16DivScalar = divScalarGeneric[uint16]
	Uint16DivScalarSels = divScalarSelsGeneric[uint16]
	Uint16DivByScalar = divByScalarGeneric[uint16]
	Uint16DivByScalarSels = divByScalarSelsGeneric[uint16]
	Uint32Div = divGeneric[uint32]
	Uint32DivSels = divSelsGeneric[uint32]
	Uint32DivScalar = divScalarGeneric[uint32]
	Uint32DivScalarSels = divScalarSelsGeneric[uint32]
	Uint32DivByScalar = divByScalarGeneric[uint32]
	Uint32DivByScalarSels = divByScalarSelsGeneric[uint32]
	Uint64Div = divGeneric[uint64]
	Uint64DivSels = divSelsGeneric[uint64]
	Uint64DivScalar = divScalarGeneric[uint64]
	Uint64DivScalarSels = divScalarSelsGeneric[uint64]
	Uint64DivByScalar = divByScalarGeneric[uint64]
	Uint64DivByScalarSels = divByScalarSelsGeneric[uint64]
	Float32Div = divGeneric[float32]
	Float32DivSels = divSelsGeneric[float32]
	Float32DivScalar = divScalarGeneric[float32]
	Float32DivScalarSels = divScalarSelsGeneric[float32]
	Float32DivByScalar = divByScalarGeneric[float32]
	Float32DivByScalarSels = divByScalarSelsGeneric[float32]
	Float64Div = divGeneric[float64]
	Float64DivSels = divSelsGeneric[float64]
	Float64DivScalar = divScalarGeneric[float64]
	Float64DivScalarSels = divScalarSelsGeneric[float64]
	Float64DivByScalar = divByScalarGeneric[float64]
	Float64DivByScalarSels = divByScalarSelsGeneric[float64]
}

func divGeneric[T vectorize.Numeric](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x / ys[i]
	}
	return rs
}

func divSelsGeneric[T vectorize.Numeric](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] / ys[sel]
	}
	return rs
}

func divScalarGeneric[T vectorize.Numeric](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x / y
	}
	return rs
}

func divScalarSelsGeneric[T vectorize.Numeric](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x / ys[sel]
	}
	return rs
}

func divByScalarGeneric[T vectorize.Numeric](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y / x
	}
	return rs
}

func divByScalarSelsGeneric[T vectorize.Numeric](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] / x
	}
	return rs
}
