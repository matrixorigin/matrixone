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

package sin

import "math"

var (
	SinUint8   func([]uint8, []float64) []float64
	SinUint16  func([]uint16, []float64) []float64
	SinUint32  func([]uint32, []float64) []float64
	SinUint64  func([]uint64, []float64) []float64
	SinInt8    func([]int8, []float64) []float64
	SinInt16   func([]int16, []float64) []float64
	SinInt32   func([]int32, []float64) []float64
	SinInt64   func([]int64, []float64) []float64
	SinFloat32 func([]float32, []float64) []float64
	SinFloat64 func([]float64, []float64) []float64
)

func init() {
	SinUint8 = sinUint8
	SinUint16 = sinUint16
	SinUint32 = sinUint32
	SinUint64 = sinUint64
	SinInt8 = sinInt8
	SinInt16 = sinInt16
	SinInt32 = sinInt32
	SinInt64 = sinInt64
	SinFloat32 = sinFloat32
	SinFloat64 = sinFloat64
}

func sinFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinFloat64(xs, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(n)
	}
	return rs
}

func sinUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}

func sinInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Sin(float64(n))
	}
	return rs
}
