// Copyright 2021 - 2024 Matrix Origin
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

package types

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// This file defines the narrow element types used by the vecbf16 / vecf16 /
// veci8 vector column types. They participate ONLY in the storage /
// serialization / accessor / display / cast-plumbing layer (the ArrayElement
// constraint). All arithmetic (distance, normalize, ...) is performed by
// upcasting to []float32, running the existing float32 kernels, and (for
// vector-returning ops) converting back. uint16/int8 arithmetic kernels are
// never written.

// BF16 is the bfloat16 floating-point format: the top 16 bits of an IEEE
// float32 (1 sign bit, 8 exponent bits, 7 mantissa bits). Conversion to
// float32 is a left shift; conversion from float32 truncates the low 16 bits
// with round-to-nearest-even.
type BF16 uint16

// Float16 is the IEEE 754 binary16 (half precision) format: 1 sign bit, 5
// exponent bits, 10 mantissa bits.
type Float16 uint16

// ----------------------------------------------------------------------------
// BF16
// ----------------------------------------------------------------------------

// ToFloat32 widens a bfloat16 to float32 by placing its bits in the high half
// of the float32 representation.
func (b BF16) ToFloat32() float32 {
	return math.Float32frombits(uint32(b) << 16)
}

// BF16FromFloat32 narrows a float32 to bfloat16 using round-to-nearest-even.
// NaN inputs are preserved as a (quiet) NaN.
func BF16FromFloat32(f float32) BF16 {
	x := math.Float32bits(f)
	if (x>>23)&0xff == 0xff && x&0x7fffff != 0 {
		// NaN: truncating the low 16 bits could zero the mantissa and turn it
		// into an Inf, so force a non-zero mantissa bit.
		return BF16(x>>16 | 0x0040)
	}
	// Round to nearest even: add 0x7fff plus the LSB of the surviving mantissa.
	rounding := uint32(0x7fff) + ((x >> 16) & 1)
	return BF16((x + rounding) >> 16)
}

// ----------------------------------------------------------------------------
// Float16 (IEEE binary16)
//
// The conversion routines below follow the vetted scalar algorithm used by
// github.com/x448/float16 (Apache-2.0), with full subnormal / Inf / NaN
// handling and round-to-nearest-even.
// ----------------------------------------------------------------------------

// ToFloat32 widens an IEEE half to float32.
func (h Float16) ToFloat32() float32 {
	return math.Float32frombits(f16bitsToF32bits(uint16(h)))
}

// Float16FromFloat32 narrows a float32 to an IEEE half using
// round-to-nearest-even, with overflow to Inf and subnormal handling.
func Float16FromFloat32(f float32) Float16 {
	return Float16(f32bitsToF16bits(math.Float32bits(f)))
}

func f16bitsToF32bits(in uint16) uint32 {
	sign := uint32(in&0x8000) << 16 // sign bit, shifted to float32 position
	exp := uint32(in&0x7c00) >> 10  // 5-bit exponent
	coef := uint32(in&0x03ff) << 13 // 10-bit mantissa, shifted to float32 position

	if exp == 0x1f {
		if coef == 0 {
			// Infinity
			return sign | 0x7f800000
		}
		// NaN
		return sign | 0x7fc00000 | coef
	}

	if exp == 0 {
		if coef == 0 {
			// signed zero
			return sign
		}
		// normalize the subnormal
		exp++
		for coef&0x7f800000 == 0 {
			coef <<= 1
			exp--
		}
		coef &= 0x007fffff
	}

	return sign | ((exp + (0x7f - 0xf)) << 23) | coef
}

func f32bitsToF16bits(u32 uint32) uint16 {
	sign := u32 & 0x80000000
	exp := u32 & 0x7f800000
	coef := u32 & 0x007fffff

	if exp == 0x7f800000 {
		// NaN or Infinity
		nanBit := uint32(0)
		if coef != 0 {
			nanBit = uint32(0x0200)
		}
		return uint16((sign >> 16) | uint32(0x7c00) | nanBit | (coef >> 13))
	}

	halfSign := sign >> 16

	unbiasedExp := int32(exp>>23) - 127
	halfExp := unbiasedExp + 15

	if halfExp >= 0x1f {
		// overflow -> Inf
		return uint16(halfSign | uint32(0x7c00))
	}

	if halfExp <= 0 {
		if 14-halfExp > 24 {
			// too small -> signed zero
			return uint16(halfSign)
		}
		coef := coef | uint32(0x00800000)
		halfCoef := coef >> uint32(14-halfExp)
		roundBit := uint32(1) << uint32(13-halfExp)
		if (coef&roundBit) != 0 && (coef&(3*roundBit-1)) != 0 {
			halfCoef++
		}
		return uint16(halfSign | halfCoef)
	}

	halfExp2 := uint32(halfExp) << 10
	halfCoef := coef >> 13
	roundBit := uint32(0x00001000)
	if (coef&roundBit) != 0 && (coef&(3*roundBit-1)) != 0 {
		return uint16((halfSign | halfExp2 | halfCoef) + 1)
	}
	return uint16(halfSign | halfExp2 | halfCoef)
}

// ----------------------------------------------------------------------------
// Batch converters (hot path). These power the float32 bridge used by the
// distance / cast wrappers; keep them allocation-light.
// ----------------------------------------------------------------------------

func BF16ToFloat32Slice(src []BF16) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = v.ToFloat32()
	}
	return dst
}

func Float16ToFloat32Slice(src []Float16) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = v.ToFloat32()
	}
	return dst
}

func Int8ToFloat32Slice(src []int8) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = float32(v)
	}
	return dst
}

func Uint8ToFloat32Slice(src []uint8) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = float32(v)
	}
	return dst
}

func Float32ToBF16Slice(src []float32) []BF16 {
	dst := make([]BF16, len(src))
	for i, v := range src {
		dst[i] = BF16FromFloat32(v)
	}
	return dst
}

func Float32ToFloat16Slice(src []float32) []Float16 {
	dst := make([]Float16, len(src))
	for i, v := range src {
		dst[i] = Float16FromFloat32(v)
	}
	return dst
}

// Float32ToInt8Slice rounds to nearest and clamps to the int8 range
// [-128, 127]. NaN maps to 0.
func Float32ToInt8Slice(src []float32) []int8 {
	dst := make([]int8, len(src))
	for i, v := range src {
		dst[i] = Float32ToInt8(v)
	}
	return dst
}

// Float32ToInt8 rounds to nearest (ties away from zero, via math.Round) and
// clamps to [-128, 127]. NaN maps to 0.
func Float32ToInt8(v float32) int8 {
	if v != v { // NaN
		return 0
	}
	r := math.Round(float64(v))
	if r > 127 {
		return 127
	}
	if r < -128 {
		return -128
	}
	return int8(r)
}

// Float32ToUint8Slice rounds to nearest and clamps to the uint8 range
// [0, 255]. NaN maps to 0.
func Float32ToUint8Slice(src []float32) []uint8 {
	dst := make([]uint8, len(src))
	for i, v := range src {
		dst[i] = Float32ToUint8(v)
	}
	return dst
}

// Float32ToUint8 rounds to nearest (ties away from zero, via math.Round) and
// clamps to [0, 255]. NaN maps to 0.
func Float32ToUint8(v float32) uint8 {
	if v != v { // NaN
		return 0
	}
	r := math.Round(float64(v))
	if r > 255 {
		return 255
	}
	if r < 0 {
		return 0
	}
	return uint8(r)
}

// ----------------------------------------------------------------------------
// Generic float32 bridge. This is THE boundary between the storage tier
// (ArrayElement) and the compute tier (RealNumbers). Any math on a narrow type
// upcasts here, runs the float32 kernel, and (for vector results) converts back.
// ----------------------------------------------------------------------------

// ToFloat32Array upcasts any ArrayElement slice to []float32. For []float32 it
// returns the input unchanged (no copy); callers must not mutate the result in
// place when T is float32 unless they own the input.
func ToFloat32Array[T ArrayElement](in []T) []float32 {
	switch v := any(in).(type) {
	case []float32:
		return v
	case []float64:
		out := make([]float32, len(v))
		for i, x := range v {
			out[i] = float32(x)
		}
		return out
	case []BF16:
		return BF16ToFloat32Slice(v)
	case []Float16:
		return Float16ToFloat32Slice(v)
	case []int8:
		return Int8ToFloat32Slice(v)
	case []uint8:
		return Uint8ToFloat32Slice(v)
	default:
		panic(moerr.NewInternalErrorNoCtx("ToFloat32Array: unsupported element type"))
	}
}

// FromFloat32Array narrows a []float32 back to the target ArrayElement type.
// int8 rounds-to-nearest and clamps to [-128,127]; bf16/f16 round-to-nearest-even.
func FromFloat32Array[T ArrayElement](in []float32) []T {
	var zero T
	switch any(zero).(type) {
	case float32:
		out := make([]float32, len(in))
		copy(out, in)
		return any(out).([]T)
	case float64:
		out := make([]float64, len(in))
		for i, x := range in {
			out[i] = float64(x)
		}
		return any(out).([]T)
	case BF16:
		return any(Float32ToBF16Slice(in)).([]T)
	case Float16:
		return any(Float32ToFloat16Slice(in)).([]T)
	case int8:
		return any(Float32ToInt8Slice(in)).([]T)
	case uint8:
		return any(Float32ToUint8Slice(in)).([]T)
	default:
		panic(moerr.NewInternalErrorNoCtx("FromFloat32Array: unsupported element type"))
	}
}
