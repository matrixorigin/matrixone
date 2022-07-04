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

/* round package provides rounding function for all numeric types(uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64).
   the round functions here round numbers using the banker's rule, that is, round to the nearest even number in the situation of .5,
	1.5 ----> 2
	2.5 ----> 2
	3.5 ----> 4 and so on.
	caveat: for integer numbers, overflow can happen and it's behavior is undefined.
	round function takes one or two parameters as its argument, and the second argument must be a constant.
	round(x, N)
	round(x) == round(x, 0)
	N < 0, N zeroes in front of decimal point
	N >= 0, round to the Nth placeholder after decimal point
*/

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
)

var (
	RoundUint8   func([]uint8, []uint8, int64) []uint8
	RoundUint16  func([]uint16, []uint16, int64) []uint16
	RoundUint32  func([]uint32, []uint32, int64) []uint32
	RoundUint64  func([]uint64, []uint64, int64) []uint64
	RoundInt8    func([]int8, []int8, int64) []int8
	RoundInt16   func([]int16, []int16, int64) []int16
	RoundInt32   func([]int32, []int32, int64) []int32
	RoundInt64   func([]int64, []int64, int64) []int64
	RoundFloat32 func([]float32, []float32, int64) []float32
	RoundFloat64 func([]float64, []float64, int64) []float64
)

func init() {
	RoundUint8 = roundUint8
	RoundUint16 = roundUint16
	RoundUint32 = roundUint32
	RoundUint64 = roundUint64
	RoundInt8 = roundInt8
	RoundInt16 = roundInt16
	RoundInt32 = roundInt32
	RoundInt64 = roundInt64
	RoundFloat32 = roundFloat32
	RoundFloat64 = roundFloat64
}

var maxUint8digits = floor.MaxUint8digits
var maxUint16digits = floor.MaxUint16digits
var maxUint32digits = floor.MaxUint32digits
var maxUint64digits = floor.MaxUint64digits
var maxInt8digits = floor.MaxInt8digits
var maxInt16digits = floor.MaxInt16digits
var maxInt32digits = floor.MaxInt32digits
var maxInt64digits = floor.MaxInt64digits

var scaleTable = floor.ScaleTable

func roundUint8(xs []uint8, rs []uint8, digits int64) []uint8 {
	// maximum uint8 number is 255, so we only need to worry about a few digit cases,
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := float64(scaleTable[-digits])
		/////

		/////
		for i := range xs {
			value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
			rs[i] = uint8(value)
		}
	case digits <= -maxUint8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundUint16(xs []uint16, rs []uint16, digits int64) []uint16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint16digits:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
			rs[i] = uint16(value)
		}
	case digits <= -maxUint16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundUint32(xs []uint32, rs []uint32, digits int64) []uint32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint32digits:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
			rs[i] = uint32(value)
		}
	case digits <= maxUint32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundUint64(xs []uint64, rs []uint64, digits int64) []uint64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxUint64digits:
		scale := scaleTable[-digits]
		for i := range xs {
			value := (xs[i] + scale/2) / scale * scale //todo(broccoli): please find a better way to round away from zero
			rs[i] = uint64(value)
		}
	case digits <= -maxUint64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundInt8(xs []int8, rs []int8, digits int64) []int8 {
	switch {
	case digits >= 0:
		return xs
	case digits == -1 || digits == -2:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			if xs[i] > 0 {
				value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int8(value)
			} else if xs[i] < 0 {
				value := int((float64(xs[i])-0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int8(value)
			} else {
				rs[i] = 0
			}
		}
	case digits <= -maxInt8digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundInt16(xs []int16, rs []int16, digits int64) []int16 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt16digits:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			if xs[i] > 0 {
				value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int16(value)
			} else if xs[i] < 0 {
				value := int((float64(xs[i])-0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int16(value)
			} else {
				rs[i] = 0
			}
		}
	case digits <= -maxInt16digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundInt32(xs []int32, rs []int32, digits int64) []int32 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt32digits:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			if xs[i] > 0 {
				value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int32(value)
			} else if xs[i] < 0 {
				value := int((float64(xs[i])-0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int32(value)
			} else {
				rs[i] = 0
			}
		}
	case digits <= maxInt32digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundInt64(xs []int64, rs []int64, digits int64) []int64 {
	switch {
	case digits >= 0:
		return xs
	case digits > -maxInt64digits:
		scale := float64(scaleTable[-digits])
		for i := range xs {
			if xs[i] > 0 {
				value := int((float64(xs[i])+0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int64(value)
			} else if xs[i] < 0 {
				value := int((float64(xs[i])-0.5*scale)/scale) * int(scale) //todo(broccoli): please find a better way to round away from zero
				rs[i] = int64(value)
			} else {
				rs[i] = 0
			}
		}
	case digits <= maxInt64digits:
		for i := range xs {
			rs[i] = 0
		}
	}
	return rs
}

func roundFloat32(xs []float32, rs []float32, digits int64) []float32 {
	if digits == 0 {
		for i := range xs {
			rs[i] = float32(math.RoundToEven(float64(xs[i])))
		}
	} else {
		scale := math.Pow10(int(digits))
		for i := range xs {
			value := float64(xs[i]) * scale
			roundResult := math.RoundToEven(value)
			rs[i] = float32(roundResult / scale)
		}
	}
	return rs
}

func roundFloat64(xs []float64, rs []float64, digits int64) []float64 {
	if digits == 0 {
		for i := range xs {
			rs[i] = math.RoundToEven(xs[i])
		}
	} else {
		scale := math.Pow10(int(digits))
		for i := range xs {
			value := xs[i] * scale
			roundResult := math.RoundToEven(value)
			rs[i] = roundResult / scale
		}
	}
	return rs
}
