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

package abs

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	AbsUint8   func([]uint8, []uint8) []uint8
	AbsUint16  func([]uint16, []uint16) []uint16
	AbsUint32  func([]uint32, []uint32) []uint32
	AbsUint64  func([]uint64, []uint64) []uint64
	AbsInt8    func([]int8, []int8) []int8
	AbsInt16   func([]int16, []int16) []int16
	AbsInt32   func([]int32, []int32) []int32
	AbsInt64   func([]int64, []int64) []int64
	AbsFloat32 func([]float32, []float32) []float32
	AbsFloat64 func([]float64, []float64) []float64
)

func init() {
	AbsUint8 = absUint8
	AbsUint16 = absUint16
	AbsUint32 = absUint32
	AbsUint64 = absUint64
	AbsInt8 = absInt8
	AbsInt16 = absInt16
	AbsInt32 = absInt32
	AbsInt64 = absInt64
	AbsFloat32 = absFloat32
	AbsFloat64 = absFloat64
}

// uint8 simply return the input values.
func absUint8(xs, rs []uint8) []uint8 {
	return xs
}

// uint16 simply return the input values.
func absUint16(xs, rs []uint16) []uint16 {
	return xs
}

// uint32 simply return the input values.
func absUint32(xs, rs []uint32) []uint32 {
	return xs
}

// uint64 simply return the input values.
func absUint64(xs, rs []uint64) []uint64 {
	return xs
}

func absInt8(xs, rs []int8) []int8 {
	for i := range xs {
		if xs[i] == int8(math.MinInt8) {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int8 value out of range"))
		}
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func absInt16(xs, rs []int16) []int16 {
	for i := range xs {
		if xs[i] == int16(math.MinInt16) {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int16 value out of range"))
		}

		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func absInt32(xs, rs []int32) []int32 {
	for i := range xs {
		if xs[i] == int32(math.MinInt32) {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int32 value out of range"))
		}

		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func absInt64(xs, rs []int64) []int64 {
	for i := range xs {
		if xs[i] == int64(math.MinInt64) {
			panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int64 value out of range"))
		}
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func absFloat32(xs, rs []float32) []float32 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}

func absFloat64(xs, rs []float64) []float64 {
	for i := range xs {
		if xs[i] < 0 {
			rs[i] = -xs[i]
		} else {
			rs[i] = xs[i]
		}
	}
	return rs
}
