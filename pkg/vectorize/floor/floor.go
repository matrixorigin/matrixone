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

package floor

/* floor package provides floor function for all numeric types(uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64).
Floor returns the largest round number that is less than or equal to x.
example:
	floor(12, -1) ----> 10
	floor(12) ----> 12
	floor(-12, -1) ----> -20
	floor(-12, 1) ----> -12
	floor(12.345) ----> 12
	floor(12.345, 1) ----> 12.3
	floor(-12.345, 1) ----> -12.4
	floor(-12.345, -1) ----> -20
	floor(-12.345) ----> -13
floor function takes one or two parameters as its argument, and the second argument must be a constant.
floor(x, N)
floor(x) == floor(x, 0)
N < 0, N zeroes in front of decimal point
N >= 0, floor to the Nth placeholder after decimal point
*/

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	FloorUint8      func([]uint8, []uint8, int64) []uint8
	FloorUint16     func([]uint16, []uint16, int64) []uint16
	FloorUint32     func([]uint32, []uint32, int64) []uint32
	FloorUint64     func([]uint64, []uint64, int64) []uint64
	FloorInt8       func([]int8, []int8, int64) []int8
	FloorInt16      func([]int16, []int16, int64) []int16
	FloorInt32      func([]int32, []int32, int64) []int32
	FloorInt64      func([]int64, []int64, int64) []int64
	FloorFloat32    func([]float32, []float32, int64) []float32
	FloorFloat64    func([]float64, []float64, int64) []float64
	FloorDecimal64  func([]types.Decimal64, []types.Decimal64, int64, int32) []types.Decimal64
	FloorDecimal128 func([]types.Decimal128, []types.Decimal128, int64, int32) []types.Decimal128
)

var MaxUint8digits = numOfDigits(math.MaxUint8)
var MaxUint16digits = numOfDigits(math.MaxUint16)
var MaxUint32digits = numOfDigits(math.MaxUint32)
var MaxUint64digits = numOfDigits(math.MaxUint64) // 20
var MaxInt8digits = numOfDigits(math.MaxInt8)
var MaxInt16digits = numOfDigits(math.MaxInt16)
var MaxInt32digits = numOfDigits(math.MaxInt32)
var MaxInt64digits = numOfDigits(math.MaxInt64) // 19

func numOfDigits(value uint64) int64 {
	digits := int64(0)
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

// ScaleTable is a lookup array for digits
var ScaleTable = [...]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
	10000000000000000000, // 1 followed by 19 zeros, maxUint64 number has 20 digits, so the max scale is 1 followed by 19 zeroes
}

func init() {
	FloorUint8 = floorUint8
	FloorUint16 = floorUint16
	FloorUint32 = floorUint32
	FloorUint64 = floorUint64
	FloorInt8 = floorInt8
	FloorInt16 = floorInt16
	FloorInt32 = floorInt32
	FloorInt64 = floorInt64
	FloorFloat32 = floorFloat32
	FloorFloat64 = floorFloat64
	FloorDecimal64 = floorDecimal64
	FloorDecimal128 = floorDecimal128
}

func floorUint8(xs, rs []uint8, digits int64) []uint8 {
	// maximum uint8 number is 255, so we only need to worry about a few digit cases,
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := uint8(ScaleTable[-digits])
		for i := range xs {
			rs[i] = xs[i] / scale * scale
		}
	case digits <= -MaxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorUint16(xs, rs []uint16, digits int64) []uint16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxUint16digits:
		scale := uint16(ScaleTable[-digits])
		for i := range xs {
			rs[i] = xs[i] / scale * scale
		}
	case digits <= -MaxUint16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorUint32(xs, rs []uint32, digits int64) []uint32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxUint32digits:
		scale := uint32(ScaleTable[-digits])
		for i := range xs {
			rs[i] = xs[i] / scale * scale
		}
	case digits <= MaxUint32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorUint64(xs, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxUint64digits:
		scale := ScaleTable[-digits]
		for i := range xs {
			rs[i] = xs[i] / scale * scale
		}
	case digits <= -MaxUint64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorInt8(xs, rs []int8, digits int64) []int8 {
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := int8(ScaleTable[-digits])
		for i := range xs {
			value := xs[i]
			if value < 0 {
				value -= scale - 1
			}
			rs[i] = value / scale * scale
		}
	case digits <= -MaxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorInt16(xs, rs []int16, digits int64) []int16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxInt16digits:
		scale := int16(ScaleTable[-digits])
		for i := range xs {
			value := xs[i]
			if value < 0 {
				value -= scale - 1
			}
			rs[i] = value / scale * scale
		}
	case digits <= -MaxInt16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorInt32(xs, rs []int32, digits int64) []int32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxInt32digits:
		scale := int32(ScaleTable[-digits])
		for i := range xs {
			value := xs[i]
			if value < 0 {
				value -= scale - 1
			}
			rs[i] = value / scale * scale
		}
	case digits <= -MaxInt32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorInt64(xs, rs []int64, digits int64) []int64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxInt64digits:
		scale := int64(ScaleTable[-digits])
		for i := range xs {
			value := xs[i]
			if value < 0 {
				value -= scale - 1
			}
			rs[i] = value / scale * scale
		}
	case digits <= -MaxInt64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func floorFloat32(xs, rs []float32, digits int64) []float32 {
	if digits == 0 {
		for i := range xs {
			rs[i] = float32(math.Floor(float64(xs[i])))
		}
	} else {
		scale := float32(math.Pow10(int(digits)))
		for i := range xs {
			value := xs[i] * scale
			rs[i] = float32(math.Floor(float64(value))) / scale
		}
	}
	return rs
}

func floorFloat64(xs, rs []float64, digits int64) []float64 {
	if digits == 0 {
		for i := range xs {
			rs[i] = math.Floor(xs[i])
		}
	} else {
		scale := math.Pow10(int(digits))
		for i := range xs {
			value := xs[i] * scale
			rs[i] = math.Floor(value) / scale
		}
	}
	return rs
}

func floorDecimal64(xs, rs []types.Decimal64, digits int64, scale int32) []types.Decimal64 {
	if digits > 19 {
		digits = 19
	}
	if digits < -18 {
		digits = -18
	}
	for i := range xs {
		rs[i] = xs[i].Floor(scale, int32(digits))
	}
	return rs
}

func floorDecimal128(xs, rs []types.Decimal128, digits int64, scale int32) []types.Decimal128 {
	if digits > 39 {
		digits = 39
	}
	if digits < -38 {
		digits = -38
	}
	for i := range xs {
		rs[i] = xs[i].Floor(scale, int32(digits))
	}
	return rs
}
