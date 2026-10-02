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

import "math"

// Float4 is the OCP MXFP4 E2M1 format: 1 sign bit, 2 exponent bits (bias 1), 1
// mantissa bit, stored in one byte (the high nibble is unused). It has no
// infinity and no NaN -- all codes are finite. The eight magnitudes are
// {0, 0.5, 1, 1.5, 2, 3, 4, 6}; the largest finite magnitude is 6. This layout
// is bit-compatible with NVIDIA FP4 (Blackwell MXFP4), so the encoding is
// GPU-ready when FP4 kernels land.
//
// Like the other narrow types this is storage/plumbing only: arithmetic upcasts
// to float32. Conversion from float32 is round-to-nearest-even and saturates
// overflow to the max finite +/-6; NaN maps to 0 (FP4 has no NaN slot).
type Float4 uint8

// f4e2m1Mags maps the 3-bit magnitude code to its value.
//
//	code: 0    1    2    3    4    5    6    7
//	exp:  00   00   01   01   10   10   11   11
//	mant: 0    1    0    1    0    1    0    1
var f4e2m1Mags = [8]float32{0, 0.5, 1, 1.5, 2, 3, 4, 6}

// ToFloat32 widens an E2M1 value to float32 exactly.
func (f Float4) ToFloat32() float32 {
	u := uint8(f) & 0x0f
	mag := f4e2m1Mags[u&0x07]
	if u&0x08 != 0 {
		return -mag
	}
	return mag
}

// Float4FromFloat32 narrows a float32 to E2M1 with round-to-nearest-even, tie to
// the even code. Overflow (including +/-Inf) saturates to +/-6; NaN maps to +0.
func Float4FromFloat32(v float32) Float4 {
	if v != v { // NaN -> +0 (E2M1 has no NaN)
		return Float4(0)
	}
	var sign uint8
	if math.Signbit(float64(v)) {
		sign = 0x08
		v = -v
	}
	if math.IsInf(float64(v), 1) || v >= f4e2m1Mags[7] {
		if v == f4e2m1Mags[7] {
			return Float4(sign | 7)
		}
		// > 6 (or Inf) saturates to 6
		if math.IsInf(float64(v), 1) || v > f4e2m1Mags[7] {
			return Float4(sign | 7)
		}
	}

	// v is in [0, 6). Find the bracketing codes and pick the nearer (even on tie).
	hi := 0
	for hi < 8 && f4e2m1Mags[hi] < v {
		hi++
	}
	if hi == 0 {
		return Float4(sign) // v == 0
	}
	lo := hi - 1
	dLo := v - f4e2m1Mags[lo]
	dHi := f4e2m1Mags[hi] - v
	var code int
	switch {
	case dLo < dHi:
		code = lo
	case dHi < dLo:
		code = hi
	default: // tie -> even code
		if lo&1 == 0 {
			code = lo
		} else {
			code = hi
		}
	}
	return Float4(sign | uint8(code))
}

// ----------------------------------------------------------------------------
// Batch converters (float32 bridge).
// ----------------------------------------------------------------------------

func Float4ToFloat32Slice(src []Float4) []float32 {
	dst := make([]float32, len(src))
	for i, v := range src {
		dst[i] = v.ToFloat32()
	}
	return dst
}

func Float32ToFloat4Slice(src []float32) []Float4 {
	dst := make([]Float4, len(src))
	for i, v := range src {
		dst[i] = Float4FromFloat32(v)
	}
	return dst
}
