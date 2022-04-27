// Copyright 2022 Matrix Origin
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

package tan

import (
	"math"
)

var (
	TanUint8   func([]uint8, []float64) []float64
	TanUint16  func([]uint16, []float64) []float64
	TanUint32  func([]uint32, []float64) []float64
	TanUint64  func([]uint64, []float64) []float64
	TanInt8    func([]int8, []float64) []float64
	TanInt16   func([]int16, []float64) []float64
	TanInt32   func([]int32, []float64) []float64
	TanInt64   func([]int64, []float64) []float64
	TanFloat32 func([]float32, []float64) []float64
	TanFloat64 func([]float64, []float64) []float64
)

func init() {
	TanUint8 = tanUint8
	TanUint16 = tanUint16
	TanUint32 = tanUint32
	TanUint64 = tanUint64
	TanInt8 = tanInt8
	TanInt16 = tanInt16
	TanInt32 = tanInt32
	TanInt64 = tanInt64
	TanFloat32 = tanFloat32
	TanFloat64 = tanFloat64
}

func tanUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(float64(n))
	}
	return rs
}

func tanFloat64(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Tan(n)
	}
	return rs
}
