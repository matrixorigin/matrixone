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
	cotUint8   func([]uint8, []float64) []float64
	cotUint16  func([]uint16, []float64) []float64
	cotUint32  func([]uint32, []float64) []float64
	cotUint64  func([]uint64, []float64) []float64
	cotInt8    func([]int8, []float64) []float64
	cotInt16   func([]int16, []float64) []float64
	cotInt32   func([]int32, []float64) []float64
	cotInt64   func([]int64, []float64) []float64
	cotFloat32 func([]float32, []float64) []float64
	cotFloat64 func([]float64, []float64) []float64
)

func init() {
	cotUint8 = CotUint8Pure
	cotUint16 = CotUint16Pure
	cotUint32 = CotUint32Pure
	cotUint64 = CotUint64Pure
	cotInt8 = CotInt8Pure
	cotInt16 = CotInt16Pure
	cotInt32 = CotInt32Pure
	cotInt64 = CotInt64Pure
	cotFloat32 = CotFloat32Pure
	cotFloat64 = CotFloat64Pure
}
func CotUint8(xs []uint8, rs []float64) []float64 {
	return cotUint8(xs, rs)
}

func CotUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotUint16(xs []uint16, rs []float64) []float64 {
	return cotUint16(xs, rs)
}

func CotUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotUint32(xs []uint32, rs []float64) []float64 {
	return cotUint32(xs, rs)
}

func CotUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotUint64(xs []uint64, rs []float64) []float64 {
	return cotUint64(xs, rs)
}

func CotUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotInt8(xs []int8, rs []float64) []float64 {
	return cotInt8(xs, rs)
}

func CotInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] =1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotInt16(xs []int16, rs []float64) []float64 {
	return cotInt16(xs, rs)
}

func CotInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotInt32(xs []int32, rs []float64) []float64 {
	return cotInt32(xs, rs)
}

func CotInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotInt64(xs []int64, rs []float64) []float64 {
	return cotInt64(xs, rs)
}

func CotInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotFloat32(xs []float32, rs []float64) []float64 {
	return cotFloat32(xs, rs)
}

func CotFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
}

func CotFloat64(xs []float64, rs []float64) []float64 {
	return cotFloat64(xs, rs)
}

func CotFloat64Pure(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = 1.0 - math.Tan(float64(n))
	}
	return rs
} 