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
	Int8Mod = int8Mod
	Int8ModSels = int8ModSels
	Int8ModScalar = int8ModScalar
	Int8ModScalarSels = int8ModScalarSels
	Int8ModByScalar = int8ModByScalar
	Int8ModByScalarSels = int8ModByScalarSels
	Int16Mod = int16Mod
	Int16ModSels = int16ModSels
	Int16ModScalar = int16ModScalar
	Int16ModScalarSels = int16ModScalarSels
	Int16ModByScalar = int16ModByScalar
	Int16ModByScalarSels = int16ModByScalarSels
	Int32Mod = int32Mod
	Int32ModSels = int32ModSels
	Int32ModScalar = int32ModScalar
	Int32ModScalarSels = int32ModScalarSels
	Int32ModByScalar = int32ModByScalar
	Int32ModByScalarSels = int32ModByScalarSels
	Int64Mod = int64Mod
	Int64ModSels = int64ModSels
	Int64ModScalar = int64ModScalar
	Int64ModScalarSels = int64ModScalarSels
	Int64ModByScalar = int64ModByScalar
	Int64ModByScalarSels = int64ModByScalarSels
	Uint8Mod = uint8Mod
	Uint8ModSels = uint8ModSels
	Uint8ModScalar = uint8ModScalar
	Uint8ModScalarSels = uint8ModScalarSels
	Uint8ModByScalar = uint8ModByScalar
	Uint8ModByScalarSels = uint8ModByScalarSels
	Uint16Mod = uint16Mod
	Uint16ModSels = uint16ModSels
	Uint16ModScalar = uint16ModScalar
	Uint16ModScalarSels = uint16ModScalarSels
	Uint16ModByScalar = uint16ModByScalar
	Uint16ModByScalarSels = uint16ModByScalarSels
	Uint32Mod = uint32Mod
	Uint32ModSels = uint32ModSels
	Uint32ModScalar = uint32ModScalar
	Uint32ModScalarSels = uint32ModScalarSels
	Uint32ModByScalar = uint32ModByScalar
	Uint32ModByScalarSels = uint32ModByScalarSels
	Uint64Mod = uint64Mod
	Uint64ModSels = uint64ModSels
	Uint64ModScalar = uint64ModScalar
	Uint64ModScalarSels = uint64ModScalarSels
	Uint64ModByScalar = uint64ModByScalar
	Uint64ModByScalarSels = uint64ModByScalarSels
	Float32Mod = float32Mod
	Float32ModSels = float32ModSels
	Float32ModScalar = float32ModScalar
	Float32ModScalarSels = float32ModScalarSels
	Float32ModByScalar = float32ModByScalar
	Float32ModByScalarSels = float32ModByScalarSels
	Float64Mod = float64Mod
	Float64ModSels = float64ModSels
	Float64ModScalar = float64ModScalar
	Float64ModScalarSels = float64ModScalarSels
	Float64ModByScalar = float64ModByScalar
	Float64ModByScalarSels = float64ModByScalarSels
}

func int8Mod(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int8ModSels(xs, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func int8ModScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int8ModScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func int8ModByScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func int8ModByScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func int16Mod(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int16ModSels(xs, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func int16ModScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int16ModScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func int16ModByScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func int16ModByScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func int32Mod(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int32ModSels(xs, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func int32ModScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int32ModScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func int32ModByScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func int32ModByScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func int64Mod(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func int64ModSels(xs, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func int64ModScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func int64ModScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func int64ModByScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func int64ModByScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func uint8Mod(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint8ModSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func uint8ModScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint8ModScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func uint8ModByScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func uint8ModByScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func uint16Mod(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint16ModSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func uint16ModScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint16ModScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func uint16ModByScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func uint16ModByScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func uint32Mod(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint32ModSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func uint32ModScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint32ModScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func uint32ModByScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func uint32ModByScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func uint64Mod(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x % ys[i]
	}
	return rs
}

func uint64ModSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] % ys[sel]
	}
	return rs
}

func uint64ModScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x % y
	}
	return rs
}

func uint64ModScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = x % ys[sel]
	}
	return rs
}

func uint64ModByScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = y % x
	}
	return rs
}

func uint64ModByScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] % x
	}
	return rs
}

func float32Mod(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x - x/ys[i]*ys[i]
	}
	return rs
}

func float32ModSels(xs, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - xs[sel]/ys[sel]*ys[sel]
	}
	return rs
}

func float32ModScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x - x/y*y
	}
	return rs
}

func float32ModScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = x - x/ys[sel]*ys[sel]
	}
	return rs
}

func float32ModByScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = y - y/x*x
	}
	return rs
}

func float32ModByScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for _, sel := range sels {
		rs[sel] = ys[sel] - ys[sel]/x*x
	}
	return rs
}

func float64Mod(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x - x/ys[i]*ys[i]
	}
	return rs
}

func float64ModSels(xs, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = xs[sel] - xs[sel]/ys[sel]*ys[sel]
	}
	return rs
}

func float64ModScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x - x/y*y
	}
	return rs
}

func float64ModScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = x - x/ys[sel]*ys[sel]
	}
	return rs
}

func float64ModByScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = y - y/x*x
	}
	return rs
}

func float64ModByScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for _, sel := range sels {
		rs[sel] = ys[sel] - ys[sel]/x*x
	}
	return rs
}
