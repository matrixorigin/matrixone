// Copyright 2021 Matrix Origin
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

package ceil

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"math"
	"strconv"
)

var (
	ceilUint8   func([]uint8, []uint8) []uint8
	ceilUint16  func([]uint16, []uint16) []uint16
	ceilUint32  func([]uint32, []uint32) []uint32
	ceilUint64  func([]uint64, []uint64) []uint64
	ceilInt8    func([]int8, []int8) []int8
	ceilInt16   func([]int16, []int16) []int16
	ceilInt32   func([]int32, []int32) []int32
	ceilInt64   func([]int64, []int64) []int64
	ceilFloat32 func([]float32, []float32) []float32
	ceilFloat64 func([]float64, []float64) []float64
	ceilString  func(*types.Bytes, []float64) []float64
)

func init() {
	ceilUint8 = ceilUint8Pure
	ceilUint16 = ceilUint16Pure
	ceilUint32 = ceilUint32Pure
	ceilUint64 = ceilUint64Pure
	ceilInt8 = ceilInt8Pure
	ceilInt16 = ceilInt16Pure
	ceilInt32 = ceilInt32Pure
	ceilInt64 = ceilInt64Pure
	ceilFloat32 = ceilFloat32Pure
	ceilFloat64 = ceilFloat64Pure
	ceilString = ceilStringPure
}

func CeilUint8(xs []uint8, rs []uint8) []uint8 {
	return ceilUint8(xs, rs)
}
func ceilUint8Pure(xs []uint8, rs []uint8) []uint8 {
	for i, r := range xs {
		rs[i] = uint8(math.Ceil(float64(r)))
	}
	return rs
}
func CeilUint16(xs []uint16, rs []uint16) []uint16 {
	return ceilUint16(xs, rs)
}
func ceilUint16Pure(xs []uint16, rs []uint16) []uint16 {
	for i, r := range xs {
		rs[i] = uint16(math.Ceil(float64(r)))
	}
	return rs
}
func CeilUint32(xs []uint32, rs []uint32) []uint32 {
	return ceilUint32(xs, rs)
}
func ceilUint32Pure(xs []uint32, rs []uint32) []uint32 {
	for i, r := range xs {
		rs[i] = uint32(math.Ceil(float64(r)))
	}
	return rs
}
func CeilUint64(xs []uint64, rs []uint64) []uint64 {
	return ceilUint64(xs, rs)
}
func ceilUint64Pure(xs []uint64, rs []uint64) []uint64 {
	for i, r := range xs {
		rs[i] = uint64(math.Ceil(float64(r)))
	}
	return rs
}
func CeilInt8(xs []int8, rs []int8) []int8 {
	return ceilInt8(xs, rs)
}
func ceilInt8Pure(xs []int8, rs []int8) []int8 {
	for i, r := range xs {
		rs[i] = int8(math.Ceil(float64(r)))
	}
	return rs
}
func CeilInt16(xs []int16, rs []int16) []int16 {
	return ceilInt16(xs, rs)
}
func ceilInt16Pure(xs []int16, rs []int16) []int16 {
	for i, r := range xs {
		rs[i] = int16(math.Ceil(float64(r)))
	}
	return rs
}
func CeilInt32(xs []int32, rs []int32) []int32 {
	return ceilInt32(xs, rs)
}
func ceilInt32Pure(xs []int32, rs []int32) []int32 {
	for i, r := range xs {
		rs[i] = int32(math.Ceil(float64(r)))
	}
	return rs
}
func CeilInt64(xs []int64, rs []int64) []int64 {
	return ceilInt64(xs, rs)
}
func ceilInt64Pure(xs, rs []int64) []int64 {
	for i, r := range xs {
		rs[i] = int64(math.Ceil(float64(r)))
	}
	return rs
}
func CeilFloat32(xs []float32, rs []float32) []float32 {
	return ceilFloat32(xs, rs)
}
func ceilFloat32Pure(xs, rs []float32) []float32 {
	for i, r := range xs {
		rs[i] = float32(math.Ceil(float64(r)))
	}
	return rs
}
func CeilFloat64(xs []float64, rs []float64) []float64 {
	return ceilFloat64(xs, rs)
}
func ceilFloat64Pure(xs, rs []float64) []float64 {
	for i, r := range xs {
		rs[i] = float64(math.Ceil(float64(r)))
	}
	return rs
}
func CeilString(xs *types.Bytes, rs []float64) []float64 {
	return ceilString(xs, rs)
}
func ceilStringPure(xs *types.Bytes, rs []float64) []float64 {
	var tt uint32
	tt = 0
	for i, r := range xs.Lengths {
		t := xs.Data[tt : tt+r]
		tt += r
		ss, er := strconv.ParseFloat(string(t), 64)
		if er == nil {
			rs[i] = float64(math.Ceil(float64(ss)))
		}
	}
	return rs
}
