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

import "github.com/matrixorigin/matrixone/pkg/container/types"

var (
	Int8Sub                 func([]int8, []int8, []int8) []int8
	Int8SubSels             func([]int8, []int8, []int8, []int64) []int8
	Int8SubScalar           func(int8, []int8, []int8) []int8
	Int8SubScalarSels       func(int8, []int8, []int8, []int64) []int8
	Int8SubByScalar         func(int8, []int8, []int8) []int8
	Int8SubByScalarSels     func(int8, []int8, []int8, []int64) []int8
	Int16Sub                func([]int16, []int16, []int16) []int16
	Int16SubSels            func([]int16, []int16, []int16, []int64) []int16
	Int16SubScalar          func(int16, []int16, []int16) []int16
	Int16SubScalarSels      func(int16, []int16, []int16, []int64) []int16
	Int16SubByScalar        func(int16, []int16, []int16) []int16
	Int16SubByScalarSels    func(int16, []int16, []int16, []int64) []int16
	Int32Sub                func([]int32, []int32, []int32) []int32
	Int32SubSels            func([]int32, []int32, []int32, []int64) []int32
	Int32SubScalar          func(int32, []int32, []int32) []int32
	Int32SubScalarSels      func(int32, []int32, []int32, []int64) []int32
	Int32SubByScalar        func(int32, []int32, []int32) []int32
	Int32SubByScalarSels    func(int32, []int32, []int32, []int64) []int32
	Int64Sub                func([]int64, []int64, []int64) []int64
	Int64SubSels            func([]int64, []int64, []int64, []int64) []int64
	Int64SubScalar          func(int64, []int64, []int64) []int64
	Int64SubScalarSels      func(int64, []int64, []int64, []int64) []int64
	Int64SubByScalar        func(int64, []int64, []int64) []int64
	Int64SubByScalarSels    func(int64, []int64, []int64, []int64) []int64
	Uint8Sub                func([]uint8, []uint8, []uint8) []uint8
	Uint8SubSels            func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8SubScalar          func(uint8, []uint8, []uint8) []uint8
	Uint8SubScalarSels      func(uint8, []uint8, []uint8, []int64) []uint8
	Uint8SubByScalar        func(uint8, []uint8, []uint8) []uint8
	Uint8SubByScalarSels    func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Sub               func([]uint16, []uint16, []uint16) []uint16
	Uint16SubSels           func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16SubScalar         func(uint16, []uint16, []uint16) []uint16
	Uint16SubScalarSels     func(uint16, []uint16, []uint16, []int64) []uint16
	Uint16SubByScalar       func(uint16, []uint16, []uint16) []uint16
	Uint16SubByScalarSels   func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Sub               func([]uint32, []uint32, []uint32) []uint32
	Uint32SubSels           func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32SubScalar         func(uint32, []uint32, []uint32) []uint32
	Uint32SubScalarSels     func(uint32, []uint32, []uint32, []int64) []uint32
	Uint32SubByScalar       func(uint32, []uint32, []uint32) []uint32
	Uint32SubByScalarSels   func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Sub               func([]uint64, []uint64, []uint64) []uint64
	Uint64SubSels           func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64SubScalar         func(uint64, []uint64, []uint64) []uint64
	Uint64SubScalarSels     func(uint64, []uint64, []uint64, []int64) []uint64
	Uint64SubByScalar       func(uint64, []uint64, []uint64) []uint64
	Uint64SubByScalarSels   func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Sub              func([]float32, []float32, []float32) []float32
	Float32SubSels          func([]float32, []float32, []float32, []int64) []float32
	Float32SubScalar        func(float32, []float32, []float32) []float32
	Float32SubScalarSels    func(float32, []float32, []float32, []int64) []float32
	Float32SubByScalar      func(float32, []float32, []float32) []float32
	Float32SubByScalarSels  func(float32, []float32, []float32, []int64) []float32
	Float64Sub              func([]float64, []float64, []float64) []float64
	Float64SubSels          func([]float64, []float64, []float64, []int64) []float64
	Float64SubScalar        func(float64, []float64, []float64) []float64
	Float64SubScalarSels    func(float64, []float64, []float64, []int64) []float64
	Float64SubByScalar      func(float64, []float64, []float64) []float64
	Float64SubByScalarSels  func(float64, []float64, []float64, []int64) []float64
	Decimal128Sub           func([]types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128) []types.Decimal128
	Decimal128SubSels       func([]types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128, []int64) []types.Decimal128
	Decimal128SubScalar     func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128) []types.Decimal128
	Decimal128SubScalarSels func(types.Decimal128, []types.Decimal128, int32, int32, []types.Decimal128, []int64) []types.Decimal128

	Int32Int64Sub         func([]int64, []int32, []int64) []int64
	Int32Int64SubSels     func([]int64, []int32, []int64, []int64) []int64
	Int16Int64Sub         func([]int64, []int16, []int64) []int64
	Int16Int64SubSels     func([]int64, []int16, []int64, []int64) []int64
	Int8Int64Sub          func([]int64, []int8, []int64) []int64
	Int8Int64SubSels      func([]int64, []int8, []int64, []int64) []int64
	Int16Int32Sub         func([]int32, []int16, []int32) []int32
	Int16Int32SubSels     func([]int32, []int16, []int32, []int64) []int32
	Int8Int32Sub          func([]int32, []int8, []int32) []int32
	Int8Int32SubSels      func([]int32, []int8, []int32, []int64) []int32
	Int8Int16Sub          func([]int16, []int8, []int16) []int16
	Int8Int16SubSels      func([]int16, []int8, []int16, []int64) []int16
	Float32Float64Sub     func([]float64, []float32, []float64) []float64
	Float32Float64SubSels func([]float64, []float32, []float64, []int64) []float64
	Uint32Uint64Sub       func([]uint64, []uint32, []uint64) []uint64
	Uint32Uint64SubSels   func([]uint64, []uint32, []uint64, []int64) []uint64
	Uint16Uint64Sub       func([]uint64, []uint16, []uint64) []uint64
	Uint16Uint64SubSels   func([]uint64, []uint16, []uint64, []int64) []uint64
	Uint8Uint64Sub        func([]uint64, []uint8, []uint64) []uint64
	Uint8Uint64SubSels    func([]uint64, []uint8, []uint64, []int64) []uint64
	Uint16Uint32Sub       func([]uint32, []uint16, []uint32) []uint32
	Uint16Uint32SubSels   func([]uint32, []uint16, []uint32, []int64) []uint32
	Uint8Uint32Sub        func([]uint32, []uint8, []uint32) []uint32
	Uint8Uint32SubSels    func([]uint32, []uint8, []uint32, []int64) []uint32
	Uint8Uint16Sub        func([]uint16, []uint8, []uint16) []uint16
	Uint8Uint16SubSels    func([]uint16, []uint8, []uint16, []int64) []uint16
)

func init() {
	Decimal128Sub = decimal128Sub
	Decimal128SubSels = decimal128SubSels
	Decimal128SubScalar = decimal128SubScalar
	Decimal128SubScalarSels = decimal128SubScalarSels
}

func int8Sub(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int8SubSels(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int8SubScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int8SubScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int8SubByScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int8SubByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int16Sub(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int16SubSels(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int16SubScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int16SubScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int16SubByScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int16SubByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int32Sub(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int32SubSels(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int32SubScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int32SubScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int32SubByScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int32SubByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int64Sub(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func int64SubSels(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func int64SubScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func int64SubScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func int64SubByScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func int64SubByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint8Sub(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint8SubSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint8SubScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint8SubScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint8SubByScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint8SubByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint16Sub(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint16SubSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint16SubScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint16SubScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint16SubByScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint16SubByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint32Sub(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint32SubSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint32SubScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint32SubScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint32SubByScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint32SubByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func uint64Sub(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func uint64SubSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func uint64SubScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func uint64SubScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func uint64SubByScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func uint64SubByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func float32Sub(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float32SubSels(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func float32SubScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float32SubScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func float32SubByScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float32SubByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func float64Sub(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - ys[i]
	}
	return rs
}

func float64SubSels(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] - ys[sel]
	}
	return rs
}

func float64SubScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - y
	}
	return rs
}

func float64SubScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x - ys[sel]
	}
	return rs
}

func float64SubByScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - x
	}
	return rs
}

func float64SubByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = ys[sel] - x
	}
	return rs
}

func int32Int64Sub(xs []int64, ys []int32, rs []int64) []int64 {
	for i := range rs {
		rs[i] = xs[i] - int64(ys[i])
	}
	return rs
}

func int32Int64SubSels(xs []int64, ys []int32, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int64(ys[sel])
	}
	return rs
}

func int16Int64Sub(xs []int64, ys []int16, rs []int64) []int64 {
	for i := range rs {
		rs[i] = xs[i] - int64(ys[i])
	}
	return rs
}

func int16Int64SubSels(xs []int64, ys []int16, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int64(ys[sel])
	}
	return rs
}

func int8Int64Sub(xs []int64, ys []int8, rs []int64) []int64 {
	for i := range rs {
		rs[i] = xs[i] - int64(ys[i])
	}
	return rs
}

func int8Int64SubSels(xs []int64, ys []int8, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int64(ys[sel])
	}
	return rs
}

func int16Int32Sub(xs []int32, ys []int16, rs []int32) []int32 {
	for i := range rs {
		rs[i] = xs[i] - int32(ys[i])
	}
	return rs
}

func int16Int32SubSels(xs []int32, ys []int16, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int32(ys[sel])
	}
	return rs
}

func int8Int32Sub(xs []int32, ys []int8, rs []int32) []int32 {
	for i := range rs {
		rs[i] = xs[i] - int32(ys[i])
	}
	return rs
}

func int8Int32SubSels(xs []int32, ys []int8, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int32(ys[sel])
	}
	return rs
}

func int8Int16Sub(xs []int16, ys []int8, rs []int16) []int16 {
	for i := range rs {
		rs[i] = xs[i] - int16(ys[i])
	}
	return rs
}

func int8Int16SubSels(xs []int16, ys []int8, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - int16(ys[sel])
	}
	return rs
}

func float32Float64Sub(xs []float64, ys []float32, rs []float64) []float64 {
	for i := range rs {
		rs[i] = xs[i] - float64(ys[i])
	}
	return rs
}

func float32Float64SubSels(xs []float64, ys []float32, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - float64(ys[sel])
	}
	return rs
}

func uint32Uint64Sub(xs []uint64, ys []uint32, rs []uint64) []uint64 {
	for i := range rs {
		rs[i] = xs[i] - uint64(ys[i])
	}
	return rs
}

func uint32Uint64SubSels(xs []uint64, ys []uint32, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint64(ys[sel])
	}
	return rs
}

func uint16Uint64Sub(xs []uint64, ys []uint16, rs []uint64) []uint64 {
	for i := range rs {
		rs[i] = xs[i] - uint64(ys[i])
	}
	return rs
}

func uint16Uint64SubSels(xs []uint64, ys []uint16, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint64(ys[sel])
	}
	return rs
}

func uint8Uint64Sub(xs []uint64, ys []uint8, rs []uint64) []uint64 {
	for i := range rs {
		rs[i] = xs[i] - uint64(ys[i])
	}
	return rs
}

func uint8Uint64SubSels(xs []uint64, ys []uint8, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint64(ys[sel])
	}
	return rs
}

func uint16Uint32Sub(xs []uint32, ys []uint16, rs []uint32) []uint32 {
	for i := range rs {
		rs[i] = xs[i] - uint32(ys[i])
	}
	return rs
}

func uint16Uint32SubSels(xs []uint32, ys []uint16, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint32(ys[sel])
	}
	return rs
}

func uint8Uint32Sub(xs []uint32, ys []uint8, rs []uint32) []uint32 {
	for i := range rs {
		rs[i] = xs[i] - uint32(ys[i])
	}
	return rs
}

func uint8Uint32SubSels(xs []uint32, ys []uint8, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint32(ys[sel])
	}
	return rs
}

func uint8Uint16Sub(xs []uint16, ys []uint8, rs []uint16) []uint16 {
	for i := range rs {
		rs[i] = xs[i] - uint16(ys[i])
	}
	return rs
}

func uint8Uint16SubSels(xs []uint16, ys []uint8, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - uint16(ys[sel])
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
			for i := 0; i < int(scaleDiff); i++ {
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
			for i := 0; i < int(scaleDiff); i++ {
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
			for i := 0; i < int(scaleDiff); i++ {
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
	return rs
}

func decimal128SubScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Sub(x, ys[sel], xScale, ysScale)
	}
	return rs
}

/*


 */
