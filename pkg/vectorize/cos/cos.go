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

package cos

import (
	"math"
)

var (
	CosUint8   func([]uint8, []float64) []float64
	CosUint16  func([]uint16, []float64) []float64
	CosUint32  func([]uint32, []float64) []float64
	CosUint64  func([]uint64, []float64) []float64
	CosInt8    func([]int8, []float64) []float64
	CosInt16   func([]int16, []float64) []float64
	CosInt32   func([]int32, []float64) []float64
	CosInt64   func([]int64, []float64) []float64
	CosFloat32 func([]float32, []float64) []float64
	CosFloat64 func([]float64, []float64) []float64
)

func init() {
	CosUint8 = cosUint8
	CosUint16 = cosUint16
	CosUint32 = cosUint32
	CosUint64 = cosUint64
	CosInt8 = cosInt8
	CosInt16 = cosInt16
	CosInt32 = cosInt32
	CosInt64 = cosInt64
	CosFloat32 = cosFloat32
	CosFloat64 = cosFloat64
}

func cosUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func cosFloat64(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(n)
	}
	return rs
}
