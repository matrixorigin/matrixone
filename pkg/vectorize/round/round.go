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

package round

import (
	"math"
)

/* round package provides rounding function for all numeric types(uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64).
   the round functions here round numbers using the banker's rule, that is, round to the nearest even number in the situation of .5,
	1.5 ----> 2
	2.5 ----> 2
	3.5 ----> 4 and so on.
	caveat: for integer numbers, overflow can happen.
*/

var (
	roundUint8   func([]uint8, []uint8, int64) []uint8
	roundUint16  func([]uint16, []uint16, int64) []uint16
	roundUint32  func([]uint32, []uint32, int64) []uint32
	roundUint64  func([]uint64, []uint64, int64) []uint64
	roundInt8    func([]int8, []int8, int64) []int8
	roundInt16   func([]int16, []int16, int64) []int16
	roundInt32   func([]int32, []int32, int64) []int32
	roundInt64   func([]int64, []int64, int64) []int64
	roundFloat32 func([]float32, []float32, int64) []float32
	roundFloat64 func([]float64, []float64, int64) []float64
)

func init() {
	roundUint8 = roundUint8Pure
	roundUint16 = roundUint16Pure
	roundUint32 = roundUint32Pure
	roundUint64 = roundUint64Pure
	roundInt8 = roundInt8Pure
	roundInt16 = roundInt16Pure
	roundInt32 = roundInt32Pure
	roundInt64 = roundInt64Pure
	roundFloat32 = roundFloat32Pure
	roundFloat64 = roundFloat64Pure
}

var maxUint8digits = digits(math.MaxUint8)
var maxUint16digits = digits(math.MaxUint16)
var maxUint32digits = digits(math.MaxUint32)
var maxUint64digits = digits(math.MaxUint64)
var maxInt8digits = digits(math.MaxInt8)
var maxInt16digits = digits(math.MaxInt16)
var maxInt32digits = digits(math.MaxInt32)
var maxInt64digits = digits(math.MaxInt64)

func digits(value uint64) int64 {
	digits := int64(0)
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

func RoundUint8(xs []uint8, rs []uint8, digits int64) []uint8 {
	return roundUint8(xs, rs, digits)
}

func roundUint8Pure(xs []uint8, rs []uint8, digits int64) []uint8 {
	// maximum uint8 number is 255, so we only need to worry about a few digit cases,
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := uint8(10)
		if digits == -2 {
			scale = 100
		}
		for i := range xs {
			quotient := (xs[i] + scale/2) / scale
			if quotient*scale == xs[i]+scale/2 {
				// round half(.5) to the nearest even number
				flag := uint8(0)
				if xs[i] < 0 {
					flag = 1
				}
				rs[i] = ((quotient + flag) & (0xFE)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= -maxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundUint16(xs []uint16, rs []uint16, digits int64) []uint16 {
	return roundUint16(xs, rs, digits)
}

func roundUint16Pure(xs []uint16, rs []uint16, digits int64) []uint16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint16digits:
		scale := uint16(math.Pow10(int(-digits)))
		for i := range xs {
			quotient := (xs[i] + scale/2) / scale
			if quotient*scale == xs[i]+scale/2 {
				// round .5 to the nearest even number
				flag := uint16(0)
				if xs[i] < 0 {
					flag = 1
				}
				rs[i] = ((quotient + flag) & (0xFFFE)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= -maxUint16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundUint32(xs []uint32, rs []uint32, digits int64) []uint32 {
	return roundUint32(xs, rs, digits)
}

func roundUint32Pure(xs []uint32, rs []uint32, digits int64) []uint32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint32digits:
		scale := uint32(math.Pow10(int(-digits)))
		for i := range xs {
			quotient := (xs[i] + scale/2) / scale
			if quotient*scale == xs[i]+scale/2 {
				// round .5 to the nearest even number
				flag := uint32(0)
				if xs[i] < 0 {
					flag = 1
				}
				rs[i] = ((quotient + flag) & (0xFFFFFFFE)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= maxUint32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundUint64(xs []uint64, rs []uint64, digits int64) []uint64 {
	return roundUint64(xs, rs, digits)
}

func roundUint64Pure(xs []uint64, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint64digits:
		scale := uint64(math.Pow10(int(-digits)))
		for i := range xs {
			quotient := (xs[i] + scale/2) / scale
			if quotient*scale == xs[i]+scale/2 {
				// round .5 to the nearest even number
				flag := uint64(0)
				if xs[i] < 0 {
					flag = 1
				}
				rs[i] = ((quotient + flag) & (0xFFFFFFFFFFFFFFFE)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= -maxUint64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundInt8(xs []int8, rs []int8, digits int64) []int8 {
	return roundInt8Pure(xs, rs, digits)
}

func roundInt8Pure(xs []int8, rs []int8, digits int64) []int8 {
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := int8(10)
		if digits == -2 {
			scale = 100
		}
		for i := range xs {
			value := xs[i]
			flag := int8(0)
			if value < 0 {
				value -= scale
				flag = 1
			}
			quotient := (value + scale/2) / scale
			if quotient*scale == value+scale/2 {
				// round half(.5) to the nearest even number
				rs[i] = ((quotient + flag) & (^1)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= -maxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundInt16(xs []int16, rs []int16, digits int64) []int16 {
	return roundInt16Pure(xs, rs, digits)
}

func roundInt16Pure(xs []int16, rs []int16, digits int64) []int16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt16digits:
		scale := int16(math.Pow10(int(-digits)))
		for i := range xs {
			value := xs[i]
			flag := int16(0)
			if value < 0 {
				value -= scale
				flag = 1
			}
			quotient := (value + scale/2) / scale
			if quotient*scale == value+scale/2 {
				// round .5 to the nearest even number
				rs[i] = ((quotient + flag) & (^1)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= -maxInt16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundInt32(xs []int32, rs []int32, digits int64) []int32 {
	return roundInt32Pure(xs, rs, digits)
}

func roundInt32Pure(xs []int32, rs []int32, digits int64) []int32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt32digits:
		scale := int32(math.Pow10(int(-digits)))
		for i := range xs {
			value := xs[i]
			flag := int32(0)
			if value < 0 {
				value -= scale
				flag = 1
			}
			quotient := (value + scale/2) / scale
			if quotient*scale == value+scale/2 {
				// round .5 to the nearest even number
				rs[i] = ((quotient + flag) & (^1)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= maxInt32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundInt64(xs []int64, rs []int64, digits int64) []int64 {
	return roundInt64Pure(xs, rs, digits)
}

func roundInt64Pure(xs []int64, rs []int64, digits int64) []int64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt32digits:
		scale := int64(math.Pow10(int(-digits)))
		for i := range xs {
			value := xs[i]
			flag := int64(0)
			if value < 0 {
				value -= scale
				flag = 1
			}
			quotient := (value + scale/2) / scale
			if quotient*scale == value+scale/2 {
				// round .5 to the nearest even number
				rs[i] = ((quotient + flag) & (^1)) * scale
			} else {
				// round others
				rs[i] = quotient * scale
			}
		}
	case digits <= maxInt32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func RoundFloat32(xs []float32, rs []float32, digits int64) []float32 {
	return roundFloat32Pure(xs, rs, digits)
}

func roundFloat32Pure(xs []float32, rs []float32, digits int64) []float32 {
	if digits < 0 {
		scale := math.Pow10(int(-digits))
		for i, _ := range xs {
			value := float64(xs[i]) / scale
			roundResult := math.RoundToEven(value)
			rs[i] = float32(roundResult * scale)
		}
	} else {
		scale := math.Pow10(int(digits))
		for i, _ := range xs {
			value := float64(xs[i]) * scale
			roundResult := math.RoundToEven(value)
			rs[i] = float32(roundResult / scale)
		}
	}
	return rs
}

func RoundFloat64(xs []float64, rs []float64, digits int64) []float64 {
	return roundFloat64Pure(xs, rs, digits)
}

func roundFloat64Pure(xs []float64, rs []float64, digits int64) []float64 {
	if digits < 0 {
		scale := math.Pow10(int(-digits))
		for i, _ := range xs {
			value := xs[i] / scale
			roundResult := math.RoundToEven(value) // the go standard library RoundToEven implementation is quite neat
			rs[i] = roundResult * scale
		}
	} else {
		scale := math.Pow10(int(digits))
		for i, _ := range xs {
			value := xs[i] * scale
			roundResult := math.RoundToEven(value)
			rs[i] = roundResult / scale
		}
	}
	return rs
}
