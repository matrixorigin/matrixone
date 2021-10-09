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

package mod

import "matrixone/pkg/vectorize"

var (
	Int8Mod                func([]int8, []int8, []int8) []int8
	Int8ModSels            func([]int8, []int8, []int8, []int64) []int8
	Int8ModScalar          func(int8, []int8, []int8) []int8
	Int8ModScalarSels      func(int8, []int8, []int8, []int64) []int8
	Int8ModByScalar        func(int8, []int8, []int8) []int8
	Int8ModByScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Mod               func([]int16, []int16, []int16) []int16
	Int16ModSels           func([]int16, []int16, []int16, []int64) []int16
	Int16ModScalar         func(int16, []int16, []int16) []int16
	Int16ModScalarSels     func(int16, []int16, []int16, []int64) []int16
	Int16ModByScalar       func(int16, []int16, []int16) []int16
	Int16ModByScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Mod               func([]int32, []int32, []int32) []int32
	Int32ModSels           func([]int32, []int32, []int32, []int64) []int32
	Int32ModScalar         func(int32, []int32, []int32) []int32
	Int32ModScalarSels     func(int32, []int32, []int32, []int64) []int32
	Int32ModByScalar       func(int32, []int32, []int32) []int32
	Int32ModByScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Mod               func([]int64, []int64, []int64) []int64
	Int64ModSels           func([]int64, []int64, []int64, []int64) []int64
	Int64ModScalar         func(int64, []int64, []int64) []int64
	Int64ModScalarSels     func(int64, []int64, []int64, []int64) []int64
	Int64ModByScalar       func(int64, []int64, []int64) []int64
	Int64ModByScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Mod               func([]uint8, []uint8, []uint8) []uint8
	Uint8ModSels           func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8ModScalar         func(uint8, []uint8, []uint8) []uint8
	Uint8ModScalarSels     func(uint8, []uint8, []uint8, []int64) []uint8
	Uint8ModByScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8ModByScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Mod              func([]uint16, []uint16, []uint16) []uint16
	Uint16ModSels          func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16ModScalar        func(uint16, []uint16, []uint16) []uint16
	Uint16ModScalarSels    func(uint16, []uint16, []uint16, []int64) []uint16
	Uint16ModByScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16ModByScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Mod              func([]uint32, []uint32, []uint32) []uint32
	Uint32ModSels          func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32ModScalar        func(uint32, []uint32, []uint32) []uint32
	Uint32ModScalarSels    func(uint32, []uint32, []uint32, []int64) []uint32
	Uint32ModByScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32ModByScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Mod              func([]uint64, []uint64, []uint64) []uint64
	Uint64ModSels          func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64ModScalar        func(uint64, []uint64, []uint64) []uint64
	Uint64ModScalarSels    func(uint64, []uint64, []uint64, []int64) []uint64
	Uint64ModByScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64ModByScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Mod             func([]float32, []float32, []float32) []float32
	Float32ModSels         func([]float32, []float32, []float32, []int64) []float32
	Float32ModScalar       func(float32, []float32, []float32) []float32
	Float32ModScalarSels   func(float32, []float32, []float32, []int64) []float32
	Float32ModByScalar     func(float32, []float32, []float32) []float32
	Float32ModByScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Mod             func([]float64, []float64, []float64) []float64
	Float64ModSels         func([]float64, []float64, []float64, []int64) []float64
	Float64ModScalar       func(float64, []float64, []float64) []float64
	Float64ModScalarSels   func(float64, []float64, []float64, []int64) []float64
	Float64ModByScalar     func(float64, []float64, []float64) []float64
	Float64ModByScalarSels func(float64, []float64, []float64, []int64) []float64
)

func init() {
	Int8Mod = modIntegerGeneric[int8]
	Int8ModSels = modIntegerSelsGeneric[int8]
	Int8ModScalar = modIntegerScalarGeneric[int8]
	Int8ModScalarSels = modIntegerScalarSelsGeneric[int8]
	Int8ModByScalar = modIntegerByScalarGeneric[int8]
	Int8ModByScalarSels = modIntegerByScalarSelsGeneric[int8]
	Int16Mod = modIntegerGeneric[int16]
	Int16ModSels = modIntegerSelsGeneric[int16]
	Int16ModScalar = modIntegerScalarGeneric[int16]
	Int16ModScalarSels = modIntegerScalarSelsGeneric[int16]
	Int16ModByScalar = modIntegerByScalarGeneric[int16]
	Int16ModByScalarSels = modIntegerByScalarSelsGeneric[int16]
	Int32Mod = modIntegerGeneric[int32]
	Int32ModSels = modIntegerSelsGeneric[int32]
	Int32ModScalar = modIntegerScalarGeneric[int32]
	Int32ModScalarSels = modIntegerScalarSelsGeneric[int32]
	Int32ModByScalar = modIntegerByScalarGeneric[int32]
	Int32ModByScalarSels = modIntegerByScalarSelsGeneric[int32]
	Int64Mod = modIntegerGeneric[int64]
	Int64ModSels = modIntegerSelsGeneric[int64]
	Int64ModScalar = modIntegerScalarGeneric[int64]
	Int64ModScalarSels = modIntegerScalarSelsGeneric[int64]
	Int64ModByScalar = modIntegerByScalarGeneric[int64]
	Int64ModByScalarSels = modIntegerByScalarSelsGeneric[int64]
	Uint8Mod = modIntegerGeneric[uint8]
	Uint8ModSels = modIntegerSelsGeneric[uint8]
	Uint8ModScalar = modIntegerScalarGeneric[uint8]
	Uint8ModScalarSels = modIntegerScalarSelsGeneric[uint8]
	Uint8ModByScalar = modIntegerByScalarGeneric[uint8]
	Uint8ModByScalarSels = modIntegerByScalarSelsGeneric[uint8]
	Uint16Mod = modIntegerGeneric[uint16]
	Uint16ModSels = modIntegerSelsGeneric[uint16]
	Uint16ModScalar = modIntegerScalarGeneric[uint16]
	Uint16ModScalarSels = modIntegerScalarSelsGeneric[uint16]
	Uint16ModByScalar = modIntegerByScalarGeneric[uint16]
	Uint16ModByScalarSels = modIntegerByScalarSelsGeneric[uint16]
	Uint32Mod = modIntegerGeneric[uint32]
	Uint32ModSels = modIntegerSelsGeneric[uint32]
	Uint32ModScalar = modIntegerScalarGeneric[uint32]
	Uint32ModScalarSels = modIntegerScalarSelsGeneric[uint32]
	Uint32ModByScalar = modIntegerByScalarGeneric[uint32]
	Uint32ModByScalarSels = modIntegerByScalarSelsGeneric[uint32]
	Uint64Mod = modIntegerGeneric[uint64]
	Uint64ModSels = modIntegerSelsGeneric[uint64]
	Uint64ModScalar = modIntegerScalarGeneric[uint64]
	Uint64ModScalarSels = modIntegerScalarSelsGeneric[uint64]
	Uint64ModByScalar = modIntegerByScalarGeneric[uint64]
	Uint64ModByScalarSels = modIntegerByScalarSelsGeneric[uint64]
	Float32Mod = modFloatGeneric[float32]
	Float32ModSels = modFloatSelsGeneric[float32]
	Float32ModScalar = modFloatScalarGeneric[float32]
	Float32ModScalarSels = modFloatScalarSelsGeneric[float32]
	Float32ModByScalar = modFloatByScalarGeneric[float32]
	Float32ModByScalarSels = modFloatByScalarSelsGeneric[float32]
	Float64Mod = modFloatGeneric[float64]
	Float64ModSels = modFloatSelsGeneric[float64]
	Float64ModScalar = modFloatScalarGeneric[float64]
	Float64ModScalarSels = modFloatScalarSelsGeneric[float64]
	Float64ModByScalar = modFloatByScalarGeneric[float64]
	Float64ModByScalarSels = modFloatByScalarSelsGeneric[float64]
}

func modIntegerGeneric[T vectorize.Integer](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func modIntegerSelsGeneric[T vectorize.Integer](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func modIntegerScalarGeneric[T vectorize.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func modIntegerScalarSelsGeneric[T vectorize.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func modIntegerByScalarGeneric[T vectorize.Integer](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func modIntegerByScalarSelsGeneric[T vectorize.Integer](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func modFloatGeneric[T vectorize.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x - x/ys[i]*ys[i]
	}
	return rs
}

func modFloatSelsGeneric[T vectorize.Float](xs, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = xs[sel] - xs[sel]/ys[sel]*ys[sel]
	}
	return rs
}

func modFloatScalarGeneric[T vectorize.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x - x/y*y
	}
	return rs
}

func modFloatScalarSelsGeneric[T vectorize.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = x - x/ys[sel]*ys[sel]
	}
	return rs
}

func modFloatByScalarGeneric[T vectorize.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = y - y/x*x
	}
	return rs
}

func modFloatByScalarSelsGeneric[T vectorize.Float](x T, ys, rs []T, sels []int64) []T {
	for _, sel := range sels {
		rs[sel] = ys[sel] - ys[sel]/x*x
	}
	return rs
}
