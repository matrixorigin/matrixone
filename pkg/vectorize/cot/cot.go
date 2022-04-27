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

package cot

import (
	"math"
)

var (
	CotUint8   func([]uint8, []float64) []float64
	CotUint16  func([]uint16, []float64) []float64
	CotUint32  func([]uint32, []float64) []float64
	CotUint64  func([]uint64, []float64) []float64
	CotInt8    func([]int8, []float64) []float64
	CotInt16   func([]int16, []float64) []float64
	CotInt32   func([]int32, []float64) []float64
	CotInt64   func([]int64, []float64) []float64
	CotFloat32 func([]float32, []float64) []float64
	CotFloat64 func([]float64, []float64) []float64
)

func init() {
	CotUint8 = cotUint8
	CotUint16 = cotUint16
	CotUint32 = cotUint32
	CotUint64 = cotUint64
	CotInt8 = cotInt8
	CotInt16 = cotInt16
	CotInt32 = cotInt32
	CotInt64 = cotInt64
	CotFloat32 = cotFloat32
	CotFloat64 = cotFloat64
}

func cotUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func cotFloat64(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(n)
	}
	return rs
}
