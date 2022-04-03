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
	cosUint8   func([]uint8, []float64) []float64
	cosUint16  func([]uint16, []float64) []float64
	cosUint32  func([]uint32, []float64) []float64
	cosUint64  func([]uint64, []float64) []float64
	cosInt8    func([]int8, []float64) []float64
	cosInt16   func([]int16, []float64) []float64
	cosInt32   func([]int32, []float64) []float64
	cosInt64   func([]int64, []float64) []float64
	cosFloat32 func([]float32, []float64) []float64
	cosFloat64 func([]float64, []float64) []float64
)

func init() {
	cosUint8 = cosUint8Pure
	cosUint16 = cosUint16Pure
	cosUint32 = cosUint32Pure
	cosUint64 = cosUint64Pure
	cosInt8 = cosInt8Pure
	cosInt16 = cosInt16Pure
	cosInt32 = cosInt32Pure
	cosInt64 = cosInt64Pure
	cosFloat32 = cosFloat32Pure
	cosFloat64 = cosFloat64Pure
}

func CosUint8(xs []uint8, rs []float64) []float64 {
	return cosUint8(xs, rs)
}

func cosUint8Pure(xs []uint8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosUint16(xs []uint16, rs []float64) []float64 {
	return cosUint16(xs, rs)
}

func cosUint16Pure(xs []uint16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosUint32(xs []uint32, rs []float64) []float64 {
	return cosUint32(xs, rs)
}

func cosUint32Pure(xs []uint32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosUint64(xs []uint64, rs []float64) []float64 {
	return cosUint64(xs, rs)
}

func cosUint64Pure(xs []uint64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosInt8(xs []int8, rs []float64) []float64 {
	return cosInt8(xs, rs)
}

func cosInt8Pure(xs []int8, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosInt16(xs []int16, rs []float64) []float64 {
	return cosInt16(xs, rs)
}

func cosInt16Pure(xs []int16, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosInt32(xs []int32, rs []float64) []float64 {
	return cosInt32(xs, rs)
}

func cosInt32Pure(xs []int32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosInt64(xs []int64, rs []float64) []float64 {
	return cosInt64(xs, rs)
}

func cosInt64Pure(xs []int64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosFloat32(xs []float32, rs []float64) []float64 {
	return cosFloat32(xs, rs)
}

func cosFloat32Pure(xs []float32, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(float64(n))
	}
	return rs
}

func CosFloat64(xs []float64, rs []float64) []float64 {
	return cosFloat64(xs, rs)
}

func cosFloat64Pure(xs []float64, rs []float64) []float64 {
	for i, n := range xs {
		rs[i] = math.Cos(n)
	}
	return rs
}
