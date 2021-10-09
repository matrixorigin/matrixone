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

import "matrixone/pkg/vectorize"

var (
	Int8Mul              func([]int8, []int8, []int8) []int8
	Int8MulSels          func([]int8, []int8, []int8, []int64) []int8
	Int8MulScalar        func(int8, []int8, []int8) []int8
	Int8MulScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Mul             func([]int16, []int16, []int16) []int16
	Int16MulSels         func([]int16, []int16, []int16, []int64) []int16
	Int16MulScalar       func(int16, []int16, []int16) []int16
	Int16MulScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Mul             func([]int32, []int32, []int32) []int32
	Int32MulSels         func([]int32, []int32, []int32, []int64) []int32
	Int32MulScalar       func(int32, []int32, []int32) []int32
	Int32MulScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Mul             func([]int64, []int64, []int64) []int64
	Int64MulSels         func([]int64, []int64, []int64, []int64) []int64
	Int64MulScalar       func(int64, []int64, []int64) []int64
	Int64MulScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Mul             func([]uint8, []uint8, []uint8) []uint8
	Uint8MulSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8MulScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8MulScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Mul            func([]uint16, []uint16, []uint16) []uint16
	Uint16MulSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16MulScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16MulScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Mul            func([]uint32, []uint32, []uint32) []uint32
	Uint32MulSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32MulScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32MulScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Mul            func([]uint64, []uint64, []uint64) []uint64
	Uint64MulSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64MulScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64MulScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Mul           func([]float32, []float32, []float32) []float32
	Float32MulSels       func([]float32, []float32, []float32, []int64) []float32
	Float32MulScalar     func(float32, []float32, []float32) []float32
	Float32MulScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Mul           func([]float64, []float64, []float64) []float64
	Float64MulSels       func([]float64, []float64, []float64, []int64) []float64
	Float64MulScalar     func(float64, []float64, []float64) []float64
	Float64MulScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	Int8Mul = mulGeneric[int8]
	Int8MulSels = mulSelsGeneric[int8]
	Int8MulScalar = mulScalarGeneric[int8]
	Int8MulScalarSels = mulScalarSelsGeneric[int8]
	Int16Mul = mulGeneric[int16]
	Int16MulSels = mulSelsGeneric[int16]
	Int16MulScalar = mulScalarGeneric[int16]
	Int16MulScalarSels = mulScalarSelsGeneric[int16]
	Int32Mul = mulGeneric[int32]
	Int32MulSels = mulSelsGeneric[int32]
	Int32MulScalar = mulScalarGeneric[int32]
	Int32MulScalarSels = mulScalarSelsGeneric[int32]
	Int64Mul = mulGeneric[int64]
	Int64MulSels = mulSelsGeneric[int64]
	Int64MulScalar = mulScalarGeneric[int64]
	Int64MulScalarSels = mulScalarSelsGeneric[int64]
	Uint8Mul = mulGeneric[uint8]
	Uint8MulSels = mulSelsGeneric[uint8]
	Uint8MulScalar = mulScalarGeneric[uint8]
	Uint8MulScalarSels = mulScalarSelsGeneric[uint8]
	Uint16Mul = mulGeneric[uint16]
	Uint16MulSels = mulSelsGeneric[uint16]
	Uint16MulScalar = mulScalarGeneric[uint16]
	Uint16MulScalarSels = mulScalarSelsGeneric[uint16]
	Uint32Mul = mulGeneric[uint32]
	Uint32MulSels = mulSelsGeneric[uint32]
	Uint32MulScalar = mulScalarGeneric[uint32]
	Uint32MulScalarSels = mulScalarSelsGeneric[uint32]
	Uint64Mul = mulGeneric[uint64]
	Uint64MulSels = mulSelsGeneric[uint64]
	Uint64MulScalar = mulScalarGeneric[uint64]
	Uint64MulScalarSels = mulScalarSelsGeneric[uint64]
	Float32Mul = mulGeneric[float32]
	Float32MulSels = mulSelsGeneric[float32]
	Float32MulScalar = mulScalarGeneric[float32]
	Float32MulScalarSels = mulScalarSelsGeneric[float32]
	Float64Mul = mulGeneric[float64]
	Float64MulSels = mulSelsGeneric[float64]
	Float64MulScalar = mulScalarGeneric[float64]
	Float64MulScalarSels = mulScalarSelsGeneric[float64]
}

func mulGeneric[T vectorize.Numeric](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x * ys[i]
	}
	return rs
}

func mulSelsGeneric[T vectorize.Numeric](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] * ys[sel]
	}
	return rs
}

func mulScalarGeneric[T vectorize.Numeric](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x * y
	}
	return rs
}

func mulScalarSelsGeneric[T vectorize.Numeric](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x * ys[sel]
	}
	return rs
}
