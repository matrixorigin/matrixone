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
	sinhUint8   func([]uint8, []float64) []float64
	sinhUint16  func([]uint16, []float64) []float64
	sinhUint32  func([]uint32, []float64) []float64
	sinhUint64  func([]uint64, []float64) []float64
	sinhInt8    func([]int8, []float64) []float64
	sinhInt16   func([]int16, []float64) []float64
	sinhInt32   func([]int32, []float64) []float64
	sinhInt64   func([]int64, []float64) []float64
	sinhFloat32 func([]float32, []float64) []float64
	sinhFloat64 func([]float64, []float64) []float64
)

func init() {
	sinhUint8 = sinhUint8Pure
	sinhUint16 = sinhUint16Pure
	sinhUint32 = sinhUint32Pure
	sinhUint64 = sinhUint64Pure
	sinhInt8 = sinhInt8Pure
	sinhInt16 = sinhInt16Pure
	sinhInt32 = sinhInt32Pure
	sinhInt64 = sinhInt64Pure
	sinhFloat32 = sinhFloat32Pure
	sinhFloat64 = sinhFloat64Pure
}

func SinhFloat32(xs []float32, rs []float64) []float64 {
	return sinhFloat32(xs, rs)
}

func sinhFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhFloat64(xs, rs []float64) []float64 {
	return sinhFloat64(xs, rs)
}

func sinhFloat64Pure(xs, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(n)
	}
	return rs
}

func SinhUint8(xs []uint8, rs []float64) []float64 {
	return sinhUint8(xs, rs)
}

func sinhUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhUint16(xs []uint16, rs []float64) []float64 {
	return sinhUint16(xs, rs)
}

func sinhUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhUint32(xs []uint32, rs []float64) []float64 {
	return sinhUint32(xs, rs)
}

func sinhUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhUint64(xs []uint64, rs []float64) []float64 {
	return sinhUint64(xs, rs)
}

func sinhUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhInt8(xs []int8, rs []float64) []float64 {
	return sinhInt8(xs, rs)
}

func sinhInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhInt16(xs []int16, rs []float64) []float64 {
	return sinhInt16(xs, rs)
}

func sinhInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhInt32(xs []int32, rs []float64) []float64 {
	return sinhInt32(xs, rs)
}

func sinhInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}

func SinhInt64(xs []int64, rs []float64) []float64 {
	return sinhInt64(xs, rs)
}

func sinhInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sinh(float64(n))
	}
	return rs
}
