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

package exp

import (
	"math"
)

var (
	ExpUint8   func([]uint8, []float64) []float64
	ExpUint16  func([]uint16, []float64) []float64
	ExpUint32  func([]uint32, []float64) []float64
	ExpUint64  func([]uint64, []float64) []float64
	ExpInt8    func([]int8, []float64) []float64
	ExpInt16   func([]int16, []float64) []float64
	ExpInt32   func([]int32, []float64) []float64
	ExpInt64   func([]int64, []float64) []float64
	ExpFloat32 func([]float32, []float64) []float64
	ExpFloat64 func([]float64, []float64) []float64
)

func init() {
	ExpUint8 = expUint8
	ExpUint16 = expUint16
	ExpUint32 = expUint32
	ExpUint64 = expUint64
	ExpInt8 = expInt8
	ExpInt16 = expInt16
	ExpInt32 = expInt32
	ExpInt64 = expInt64
	ExpFloat32 = expFloat32
	ExpFloat64 = expFloat64
}

func expUint8(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expUint16(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expUint32(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expUint64(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expInt8(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expInt16(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expInt32(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expInt64(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expFloat32(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(float64(n))
	}
	return rs
}

func expFloat64(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Exp(n)
	}
	return rs
}
