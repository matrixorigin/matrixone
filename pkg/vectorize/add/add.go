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

package add

var (
	Int8Add              func([]int8, []int8, []int8) []int8
	Int8AddSels          func([]int8, []int8, []int8, []int64) []int8
	Int8AddScalar        func(int8, []int8, []int8) []int8
	Int8AddScalarSels    func(int8, []int8, []int8, []int64) []int8
	Int16Add             func([]int16, []int16, []int16) []int16
	Int16AddSels         func([]int16, []int16, []int16, []int64) []int16
	Int16AddScalar       func(int16, []int16, []int16) []int16
	Int16AddScalarSels   func(int16, []int16, []int16, []int64) []int16
	Int32Add             func([]int32, []int32, []int32) []int32
	Int32AddSels         func([]int32, []int32, []int32, []int64) []int32
	Int32AddScalar       func(int32, []int32, []int32) []int32
	Int32AddScalarSels   func(int32, []int32, []int32, []int64) []int32
	Int64Add             func([]int64, []int64, []int64) []int64
	Int64AddSels         func([]int64, []int64, []int64, []int64) []int64
	Int64AddScalar       func(int64, []int64, []int64) []int64
	Int64AddScalarSels   func(int64, []int64, []int64, []int64) []int64
	Uint8Add             func([]uint8, []uint8, []uint8) []uint8
	Uint8AddSels         func([]uint8, []uint8, []uint8, []int64) []uint8
	Uint8AddScalar       func(uint8, []uint8, []uint8) []uint8
	Uint8AddScalarSels   func(uint8, []uint8, []uint8, []int64) []uint8
	Uint16Add            func([]uint16, []uint16, []uint16) []uint16
	Uint16AddSels        func([]uint16, []uint16, []uint16, []int64) []uint16
	Uint16AddScalar      func(uint16, []uint16, []uint16) []uint16
	Uint16AddScalarSels  func(uint16, []uint16, []uint16, []int64) []uint16
	Uint32Add            func([]uint32, []uint32, []uint32) []uint32
	Uint32AddSels        func([]uint32, []uint32, []uint32, []int64) []uint32
	Uint32AddScalar      func(uint32, []uint32, []uint32) []uint32
	Uint32AddScalarSels  func(uint32, []uint32, []uint32, []int64) []uint32
	Uint64Add            func([]uint64, []uint64, []uint64) []uint64
	Uint64AddSels        func([]uint64, []uint64, []uint64, []int64) []uint64
	Uint64AddScalar      func(uint64, []uint64, []uint64) []uint64
	Uint64AddScalarSels  func(uint64, []uint64, []uint64, []int64) []uint64
	Float32Add           func([]float32, []float32, []float32) []float32
	Float32AddSels       func([]float32, []float32, []float32, []int64) []float32
	Float32AddScalar     func(float32, []float32, []float32) []float32
	Float32AddScalarSels func(float32, []float32, []float32, []int64) []float32
	Float64Add           func([]float64, []float64, []float64) []float64
	Float64AddSels       func([]float64, []float64, []float64, []int64) []float64
	Float64AddScalar     func(float64, []float64, []float64) []float64
	Float64AddScalarSels func(float64, []float64, []float64, []int64) []float64
)

func int8Add(xs, ys, rs []int8) []int8 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int8AddSels(xs, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func int8AddScalar(x int8, ys, rs []int8) []int8 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int8AddScalarSels(x int8, ys, rs []int8, sels []int64) []int8 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func int16Add(xs, ys, rs []int16) []int16 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int16AddSels(xs, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func int16AddScalar(x int16, ys, rs []int16) []int16 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int16AddScalarSels(x int16, ys, rs []int16, sels []int64) []int16 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func int32Add(xs, ys, rs []int32) []int32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int32AddSels(xs, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func int32AddScalar(x int32, ys, rs []int32) []int32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int32AddScalarSels(x int32, ys, rs []int32, sels []int64) []int32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func int64Add(xs, ys, rs []int64) []int64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func int64AddSels(xs, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func int64AddScalar(x int64, ys, rs []int64) []int64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func int64AddScalarSels(x int64, ys, rs []int64, sels []int64) []int64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func uint8Add(xs, ys, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint8AddSels(xs, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func uint8AddScalar(x uint8, ys, rs []uint8) []uint8 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint8AddScalarSels(x uint8, ys, rs []uint8, sels []int64) []uint8 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func uint16Add(xs, ys, rs []uint16) []uint16 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint16AddSels(xs, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func uint16AddScalar(x uint16, ys, rs []uint16) []uint16 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint16AddScalarSels(x uint16, ys, rs []uint16, sels []int64) []uint16 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func uint32Add(xs, ys, rs []uint32) []uint32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint32AddSels(xs, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func uint32AddScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint32AddScalarSels(x uint32, ys, rs []uint32, sels []int64) []uint32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func uint64Add(xs, ys, rs []uint64) []uint64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func uint64AddSels(xs, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func uint64AddScalar(x uint64, ys, rs []uint64) []uint64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func uint64AddScalarSels(x uint64, ys, rs []uint64, sels []int64) []uint64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func float32Add(xs, ys, rs []float32) []float32 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func float32AddSels(xs, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func float32AddScalar(x float32, ys, rs []float32) []float32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func float32AddScalarSels(x float32, ys, rs []float32, sels []int64) []float32 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func float64Add(xs, ys, rs []float64) []float64 {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func float64AddSels(xs, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func float64AddScalar(x float64, ys, rs []float64) []float64 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func float64AddScalarSels(x float64, ys, rs []float64, sels []int64) []float64 {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}
