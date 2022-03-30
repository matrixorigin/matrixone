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

package atan

import "math"

var (
	atanUint8   func([]uint8, []float64) []float64
	atanUint16  func([]uint16, []float64) []float64
	atanUint32  func([]uint32, []float64) []float64
	atanUint64  func([]uint64, []float64) []float64
	atanInt8    func([]int8, []float64) []float64
	atanInt16   func([]int16, []float64) []float64
	atanInt32   func([]int32, []float64) []float64
	atanInt64   func([]int64, []float64) []float64
	atanFloat32 func([]float32, []float64) []float64
	atanFloat64 func([]float64, []float64) []float64
)

func init() {
	atanUint8 = atanUint8Pure
	atanUint16 = atanUint16Pure
	atanUint32 = atanUint32Pure
	atanUint64 = atanUint64Pure
	atanInt8 = atanInt8Pure
	atanInt16 = atanInt16Pure
	atanInt32 = atanInt32Pure
	atanInt64 = atanInt64Pure
	atanFloat32 = atanFloat32Pure
	atanFloat64 = atanFloat64Pure
}

func AtanUint8(xs []uint8, rs []float64) []float64 {
	return atanUint8(xs, rs)
}

func atanUint8Pure(xs []uint8, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanUint16(xs []uint16, rs []float64) []float64 {
	return atanUint16(xs, rs)
}

func atanUint16Pure(xs []uint16, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanUint32(xs []uint32, rs []float64) []float64 {
	return atanUint32(xs, rs)
}

func atanUint32Pure(xs []uint32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanUint64(xs []uint64, rs []float64) []float64 {
	return atanUint64(xs, rs)
}

func atanUint64Pure(xs []uint64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanInt8(xs []int8, rs []float64) []float64 {
	return atanInt8(xs, rs)
}

func atanInt8Pure(xs []int8, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanInt16(xs []int16, rs []float64) []float64 {
	return atanInt16(xs, rs)
}

func atanInt16Pure(xs []int16, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanInt32(xs []int32, rs []float64) []float64 {
	return atanInt32(xs, rs)
}

func atanInt32Pure(xs []int32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanInt64(xs []int64, rs []float64) []float64 {
	return atanInt64(xs, rs)
}

func atanInt64Pure(xs []int64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanFloat32(xs []float32, rs []float64) []float64 {
	return atanFloat32(xs, rs)
}

func atanFloat32Pure(xs []float32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func AtanFloat64(xs []float64, rs []float64) []float64 {
	return atanFloat64(xs, rs)
}

func atanFloat64Pure(xs []float64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(xs[i])
	}
	return rs
}
