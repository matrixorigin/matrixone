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

package sinh

import "math"

var (
	SinhUint8   func([]uint8, []float64) []float64
	SinhUint16  func([]uint16, []float64) []float64
	SinhUint32  func([]uint32, []float64) []float64
	SinhUint64  func([]uint64, []float64) []float64
	SinhInt8    func([]int8, []float64) []float64
	SinhInt16   func([]int16, []float64) []float64
	SinhInt32   func([]int32, []float64) []float64
	SinhInt64   func([]int64, []float64) []float64
	SinhFloat32 func([]float32, []float64) []float64
	SinhFloat64 func([]float64, []float64) []float64
)

func init() {
	SinhUint8 = sinhUint8
	SinhUint16 = sinhUint16
	SinhUint32 = sinhUint32
	SinhUint64 = sinhUint64
	SinhInt8 = sinhInt8
	SinhInt16 = sinhInt16
	SinhInt32 = sinhInt32
	SinhInt64 = sinhInt64
	SinhFloat32 = sinhFloat32
	SinhFloat64 = sinhFloat64
}

func sinhFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhFloat64(xs, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(n)
	}
	return rs
}

func sinhUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func sinhInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}
