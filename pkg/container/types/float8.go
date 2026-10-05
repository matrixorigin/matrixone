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

// RejectNonFiniteNarrowFloat returns an error if v (widened to float32) is NaN, Inf,
// or outside the finite range of the low-precision float type oid. Strict SQL string
// parsing and LOAD use it so an out-of-range value errors -- MySQL-style -- instead of
// silently saturating (bf16/float16 have Inf, so overflow is detected as Inf after
// narrowing; float8/float4 saturate, so their input magnitude is range-checked). This
// mirrors the narrow-vector element parser's rejectNonFiniteArrayElem. oid must be one
// of T_bf16/T_float16/T_float8/T_float4.
// Float32RoundToOdd rounds v to float32 toward zero and sets the last significand bit when
// the result is inexact. Rounding the result to bf16, float16, float8 or float4 rounds v
// once: the float32 keeps at least two bits beyond each of their significands.
func Float32RoundToOdd(v float64) float32 {
	f := float32(v)
	if float64(f) == v || v != v || math.IsInf(float64(f), 0) {
		return f
	}
	if math.Abs(float64(f)) > math.Abs(v) {
		f = math.Nextafter32(f, 0)
	}
	return math.Float32frombits(math.Float32bits(f) | 1)
}

func RejectNonFiniteNarrowFloat(v float32, oid T) error {
	f := float64(v)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return moerr.NewInvalidInputNoCtxf("value %v is not a finite %s", v, oid.String())
	}
	switch oid {
	case T_bf16:
		if math.IsInf(float64(BF16FromFloat32(v).ToFloat32()), 0) {
			return moerr.NewOutOfRangeNoCtxf(oid.String(), "value %v", v)
		}
	case T_float16:
		if math.IsInf(float64(Float16FromFloat32(v).ToFloat32()), 0) {
			return moerr.NewOutOfRangeNoCtxf(oid.String(), "value %v", v)
		}
	case T_float8:
		if v > f8e4m3MaxNorm || v < -f8e4m3MaxNorm {
			return moerr.NewOutOfRangeNoCtxf(oid.String(), "value %v", v)
		}
	case T_float4:
		if v > f4e2m1Mags[7] || v < -f4e2m1Mags[7] {
			return moerr.NewOutOfRangeNoCtxf(oid.String(), "value %v", v)
		}
	}
	return nil
}

// Float8 is the OCP FP8 E4M3 format: 1 sign bit, 4 exponent bits (bias 7), 3
// mantissa bits. Unlike IEEE formats it has NO infinity: the all-ones exponent
// still encodes finite values, and only S.1111.111 is NaN. The largest finite
// magnitude is 448. This matches NVIDIA's E4M3 (Hopper Transformer Engine) and
// the OCP FP8 spec, so the bits interoperate with GPU FP8 when kernels land.
//
// Like BF16/Float16 this is a storage/plumbing element only: all arithmetic
// upcasts to float32, runs the float32 kernel, and rounds back. Conversion from
// float32 is round-to-nearest-even and saturates overflow to the max finite ±448.
type Float8 uint8

const (
	f8e4m3Bias    = 7
	f8e4m3MaxNorm = 448.0 // 0.1111.110
	f8e4m3NaN     = 0x7f  // x.1111.111
	f8e4m3MaxBits = 0x7e  // x.1111.110 = 448
	f8e4m3MantW   = 3     // mantissa bits
)

// ToFloat32 widens an E4M3 value to float32 exactly (every E4M3 value is
// representable in float32).
func (f Float8) ToFloat32() float32 {
	u := uint8(f)
	sign := float32(1)
	if u&0x80 != 0 {
		sign = -1
	}
	exp := int(u>>f8e4m3MantW) & 0x0f
	mant := int(u & 0x07)

	if exp == 0x0f && mant == 0x07 {
		return float32(math.NaN())
	}

	var mag float32
	if exp == 0 {
		// subnormal: (mant / 2^3) * 2^(1-bias)
		mag = float32(mant) / 8.0 * float32(math.Exp2(float64(1-f8e4m3Bias)))
	} else {
		// normal: (1 + mant/2^3) * 2^(exp-bias)
		mag = (1.0 + float32(mant)/8.0) * float32(math.Exp2(float64(exp-f8e4m3Bias)))
	}
	return sign * mag
}

// Float8FromFloat32 narrows a float32 to E4M3 with round-to-nearest-even.
// Overflow (including +/-Inf) saturates to the max finite +/-448; NaN maps to the
// canonical E4M3 NaN preserving the sign.
func Float8FromFloat32(v float32) Float8 {
	if v != v { // NaN
		var s uint8
		if math.Signbit(float64(v)) {
			s = 0x80
		}
		return Float8(s | f8e4m3NaN)
	}

	var sign uint8
	if math.Signbit(float64(v)) {
		sign = 0x80
		v = -v
	}
	if v == 0 {
		return Float8(sign)
	}
	if math.IsInf(float64(v), 1) {
		return Float8(sign | f8e4m3MaxBits)
	}

	bits := math.Float32bits(v)
	e32 := int(bits>>23) & 0xff
	m32 := int(bits & 0x7fffff)
	unbiased := e32 - 127

	// Target biased exponent in E4M3.
	te := unbiased + f8e4m3Bias

	if te >= 0x0f {
		// te == 15 can still hold normals with mantissa 0..6; a value whose
		// rounded mantissa would exceed 6 (i.e. reaches the NaN slot) or whose
		// exponent is larger saturates to the max finite 448.
		if te > 0x0f {
			return Float8(sign | f8e4m3MaxBits)
		}
		// te == 15: round the 23-bit mantissa down to 3 bits, RNE.
		mant, carry := roundMantissaRNE(m32, 23, f8e4m3MantW)
		exp := te
		if carry != 0 {
			mant = 0
			exp++
		}
		if exp > 0x0f || (exp == 0x0f && mant > 6) {
			return Float8(sign | f8e4m3MaxBits)
		}
		return Float8(sign | uint8(exp<<f8e4m3MantW) | uint8(mant))
	}

	if te <= 0 {
		// Subnormal or underflow. The subnormal step is 2^(1-bias-mantW) = 2^-9.
		// Represent v as an integer count of 2^-9, RNE.
		// v = 2^unbiased * (1 + m32/2^23); express its mantissa incl. the implicit
		// bit, then shift right by (1 - te + mantW) with RNE.
		full := m32 | 0x800000 // 24-bit significand with implicit leading 1
		shift := (1 - te) + (23 - f8e4m3MantW)
		if shift > 24 {
			// far below the smallest subnormal -> signed zero (RNE)
			return Float8(sign)
		}
		mant, carry := roundMantissaRNE(full, shift+f8e4m3MantW, f8e4m3MantW)
		// A carry (roundMantissaRNE masks the result, so mant itself never reaches 8)
		// means the value rounded up across the subnormal->normal boundary into the
		// smallest normal (exponent field 1, mantissa 0 = 0x08). Honoring the carry is
		// required: dropping it returned signed zero for the (largest-subnormal, smallest
		// -normal) input band -- silent narrowing-to-zero data loss.
		if carry != 0 || mant >= 8 {
			return Float8(sign | uint8(1<<f8e4m3MantW))
		}
		return Float8(sign | uint8(mant))
	}

	// Normal range 1 <= te <= 14.
	mant, carry := roundMantissaRNE(m32, 23, f8e4m3MantW)
	exp := te
	if carry != 0 {
		mant = 0
		exp++
	}
	if exp >= 0x0f && mant > 6 {
		return Float8(sign | f8e4m3MaxBits)
	}
	return Float8(sign | uint8(exp<<f8e4m3MantW) | uint8(mant))
}

// roundMantissaRNE rounds an srcW-bit mantissa (value in the low srcW bits of m)
// down to dstW bits with round-to-nearest-even. It returns the dstW-bit result
// and a carry (1 if rounding overflowed the dstW field, i.e. the result is 2^dstW).
func roundMantissaRNE(m, srcW, dstW int) (int, int) {
	drop := srcW - dstW
	if drop <= 0 {
		return m << (-drop), 0
	}
	kept := m >> drop
	roundBit := (m >> (drop - 1)) & 1
	sticky := 0
	if m&((1<<(drop-1))-1) != 0 {
		sticky = 1
	}
	if roundBit == 1 && (sticky == 1 || kept&1 == 1) {
		kept++
	}
	if kept>>dstW != 0 {
		return kept & ((1 << dstW) - 1), 1
	}
	return kept, 0
}

// ----------------------------------------------------------------------------
// Batch converters (float32 bridge).
// ----------------------------------------------------------------------------

func Float8ToFloat32Slice(src []Float8) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = v.ToFloat32()
	}
	return dst
}

func Float32ToFloat8Slice(src []float32) []Float8 {
	dst := make([]Float8, len(src))
	for i, v := range src {
		dst[i] = Float8FromFloat32(v)
	}
	return dst
}
