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
)

var (
	floorUint8   func([]uint8, []uint8, int64) []uint8
	floorUint16  func([]uint16, []uint16, int64) []uint16
	floorUint32  func([]uint32, []uint32, int64) []uint32
	floorUint64  func([]uint64, []uint64, int64) []uint64
	floorInt8    func([]int8, []int8, int64) []int8
	floorInt16   func([]int16, []int16, int64) []int16
	floorInt32   func([]int32, []int32, int64) []int32
	floorInt64   func([]int64, []int64, int64) []int64
	floorFloat32 func([]float32, []float32, int64) []float32
	floorFloat64 func([]float64, []float64, int64) []float64
)

var MaxUint8digits = digits(math.MaxUint8)
var MaxUint16digits = digits(math.MaxUint16)
var MaxUint32digits = digits(math.MaxUint32)
var MaxUint64digits = digits(math.MaxUint64) // 20
var MaxInt8digits = digits(math.MaxInt8)
var MaxInt16digits = digits(math.MaxInt16)
var MaxInt32digits = digits(math.MaxInt32)
var MaxInt64digits = digits(math.MaxInt64) // 19

func digits(value uint64) int64 {
	digits := int64(0)
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

// scaleTable is a lookup array for digits
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
	floorUint8 = floorUint8Pure
	floorUint16 = floorUint16Pure
	floorUint32 = floorUint32Pure
	floorUint64 = floorUint64Pure
	floorInt8 = floorInt8Pure
	floorInt16 = floorInt16Pure
	floorInt32 = floorInt32Pure
	floorInt64 = floorInt64Pure
	floorFloat32 = floorFloat32Pure
	floorFloat64 = floorFloat64Pure
}

func FloorUint8(xs, rs []uint8, digits int64) []uint8 {
	return floorUint8(xs, rs, digits)
}

func floorUint8Pure(xs, rs []uint8, digits int64) []uint8 {
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

func FloorUint16(xs, rs []uint16, digits int64) []uint16 {
	return floorUint16(xs, rs, digits)
}

func floorUint16Pure(xs, rs []uint16, digits int64) []uint16 {
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

func FloorUint32(xs, rs []uint32, digits int64) []uint32 {
	return floorUint32(xs, rs, digits)
}

func floorUint32Pure(xs, rs []uint32, digits int64) []uint32 {
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

func FloorUint64(xs, rs []uint64, digits int64) []uint64 {
	return floorUint64(xs, rs, digits)
}

func floorUint64Pure(xs, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -MaxUint64digits:
		scale := uint64(ScaleTable[-digits])
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

func FloorInt8(xs, rs []int8, digits int64) []int8 {
	return floorInt8(xs, rs, digits)
}

func floorInt8Pure(xs, rs []int8, digits int64) []int8 {
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

func FloorInt16(xs, rs []int16, digits int64) []int16 {
	return floorInt16(xs, rs, digits)
}

func floorInt16Pure(xs, rs []int16, digits int64) []int16 {
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

func FloorInt32(xs, rs []int32, digits int64) []int32 {
	return floorInt32(xs, rs, digits)
}

func floorInt32Pure(xs, rs []int32, digits int64) []int32 {
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

func FloorInt64(xs, rs []int64, digits int64) []int64 {
	return floorInt64(xs, rs, digits)
}

func floorInt64Pure(xs, rs []int64, digits int64) []int64 {
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

func FloorFloat32(xs, rs []float32, digits int64) []float32 {
	return floorFloat32(xs, rs, digits)
}

func floorFloat32Pure(xs, rs []float32, digits int64) []float32 {
	if digits == 0 {
		for i := range xs {
			rs[i] = float32(math.Floor(float64(xs[i])))
		}
	} else if digits > 0 {
		scale := float32(ScaleTable[digits])
		for i := range xs {
			value := xs[i] * scale
			rs[i] = float32(math.Floor(float64(value))) / scale
		}
	} else {
		scale := float32(ScaleTable[-digits])
		for i := range xs {
			value := xs[i] / scale
			rs[i] = float32(math.Floor(float64(value))) * scale
		}
	}
	return rs
}

func FloorFloat64(xs, rs []float64, digits int64) []float64 {
	return floorFloat64(xs, rs, digits)
}

func floorFloat64Pure(xs, rs []float64, digits int64) []float64 {
	if digits == 0 {
		for i := range xs {
			rs[i] = math.Floor(xs[i])
		}
	} else if digits > 0 {
		scale := float64(ScaleTable[digits])
		for i := range xs {
			value := xs[i] * scale
			rs[i] = math.Floor(value) / scale
		}
	} else {
		scale := float64(ScaleTable[-digits])
		for i := range xs {
			value := xs[i] / scale
			rs[i] = math.Floor(value) * scale
		}
	}
	return rs
}
