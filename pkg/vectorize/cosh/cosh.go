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

package cosh

import "math"

var (
	coshUint8   func([]uint8, []float64) []float64
	coshUint16  func([]uint16, []float64) []float64
	coshUint32  func([]uint32, []float64) []float64
	coshUint64  func([]uint64, []float64) []float64
	coshInt8    func([]int8, []float64) []float64
	coshInt16   func([]int16, []float64) []float64
	coshInt32   func([]int32, []float64) []float64
	coshInt64   func([]int64, []float64) []float64
	coshFloat32 func([]float32, []float64) []float64
	coshFloat64 func([]float64, []float64) []float64
)

func init() {
	coshUint8 = coshUint8Pure
	coshUint16 = coshUint16Pure
	coshUint32 = coshUint32Pure
	coshUint64 = coshUint64Pure
	coshInt8 = coshInt8Pure
	coshInt16 = coshInt16Pure
	coshInt32 = coshInt32Pure
	coshInt64 = coshInt64Pure
	coshFloat32 = coshFloat32Pure
	coshFloat64 = coshFloat64Pure
}

func CoshFloat32(xs []float32, rs []float64) []float64 {
	return coshFloat32(xs, rs)
}

func coshFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshFloat64(xs, rs []float64) []float64 {
	return coshFloat64(xs, rs)
}

func coshFloat64Pure(xs, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(n)
	}
	return rs
}

func CoshUint8(xs []uint8, rs []float64) []float64 {
	return coshUint8(xs, rs)
}

func coshUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshUint16(xs []uint16, rs []float64) []float64 {
	return coshUint16(xs, rs)
}

func coshUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshUint32(xs []uint32, rs []float64) []float64 {
	return coshUint32(xs, rs)
}

func coshUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshUint64(xs []uint64, rs []float64) []float64 {
	return coshUint64(xs, rs)
}

func coshUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshInt8(xs []int8, rs []float64) []float64 {
	return coshInt8(xs, rs)
}

func coshInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshInt16(xs []int16, rs []float64) []float64 {
	return coshInt16(xs, rs)
}

func coshInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshInt32(xs []int32, rs []float64) []float64 {
	return coshInt32(xs, rs)
}

func coshInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}

func CoshInt64(xs []int64, rs []float64) []float64 {
	return coshInt64(xs, rs)
}

func coshInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cosh(float64(n))
	}
	return rs
}
