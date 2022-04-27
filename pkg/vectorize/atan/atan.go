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
	AtanUint8   func([]uint8, []float64) []float64
	AtanUint16  func([]uint16, []float64) []float64
	AtanUint32  func([]uint32, []float64) []float64
	AtanUint64  func([]uint64, []float64) []float64
	AtanInt8    func([]int8, []float64) []float64
	AtanInt16   func([]int16, []float64) []float64
	AtanInt32   func([]int32, []float64) []float64
	AtanInt64   func([]int64, []float64) []float64
	AtanFloat32 func([]float32, []float64) []float64
	AtanFloat64 func([]float64, []float64) []float64
)

func init() {
	AtanUint8 = atanUint8
	AtanUint16 = atanUint16
	AtanUint32 = atanUint32
	AtanUint64 = atanUint64
	AtanInt8 = atanInt8
	AtanInt16 = atanInt16
	AtanInt32 = atanInt32
	AtanInt64 = atanInt64
	AtanFloat32 = atanFloat32
	AtanFloat64 = atanFloat64
}

func atanUint8(xs []uint8, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanUint16(xs []uint16, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanUint32(xs []uint32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanUint64(xs []uint64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanInt8(xs []int8, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanInt16(xs []int16, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanInt32(xs []int32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanInt64(xs []int64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanFloat32(xs []float32, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(float64(xs[i]))
	}
	return rs
}

func atanFloat64(xs []float64, rs []float64) []float64 {
	for i := range xs {
		rs[i] = math.Atan(xs[i])
	}
	return rs
}
