// Copyright 2021 - 2025 Matrix Origin
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

package onnx

import "math"

// IEEE 754 half-precision (float16) conversion helpers. onnxruntime_go does not
// expose a Go float16 type; float16 tensors are handled as raw little-endian
// bytes via CustomDataTensor, so we convert to/from float32 ourselves.

// float32ToFloat16 converts a float32 to its IEEE 754 half-precision bit
// pattern, with round-to-nearest-even and correct handling of subnormals,
// infinities and NaN.
func float32ToFloat16(f float32) uint16 {
	b := math.Float32bits(f)
	sign := uint16((b >> 16) & 0x8000)
	exp := int32((b>>23)&0xff) - 127 + 15 // rebias 127 -> 15
	mant := b & 0x7fffff

	if (b>>23)&0xff == 0xff {
		// Inf or NaN.
		if mant != 0 {
			return sign | 0x7e00 // NaN (quiet)
		}
		return sign | 0x7c00 // Inf
	}

	if exp >= 0x1f {
		// Overflow -> Inf.
		return sign | 0x7c00
	}

	if exp <= 0 {
		// Subnormal or zero in half precision.
		if exp < -10 {
			return sign // too small -> signed zero
		}
		// Add the implicit leading 1 and shift into subnormal range with
		// round-to-nearest-even.
		mant |= 0x800000
		shift := uint32(14 - exp)
		half := mant >> shift
		rem := mant & ((1 << shift) - 1)
		halfway := uint32(1) << (shift - 1)
		if rem > halfway || (rem == halfway && (half&1) == 1) {
			half++
		}
		return sign | uint16(half)
	}

	// Normalized. Round mantissa from 23 to 10 bits (round-to-nearest-even).
	half := mant >> 13
	rem := mant & 0x1fff
	if rem > 0x1000 || (rem == 0x1000 && (half&1) == 1) {
		half++
		if half == 0x400 { // mantissa overflow -> bump exponent
			half = 0
			exp++
			if exp >= 0x1f {
				return sign | 0x7c00
			}
		}
	}
	return sign | uint16(exp<<10) | uint16(half)
}

// float16ToFloat32 converts an IEEE 754 half-precision bit pattern to float32.
func float16ToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := uint32(h>>10) & 0x1f
	mant := uint32(h & 0x3ff)

	switch exp {
	case 0:
		if mant == 0 {
			return math.Float32frombits(sign) // signed zero
		}
		// Subnormal: normalize.
		e := int32(-1)
		for mant&0x400 == 0 {
			mant <<= 1
			e++
		}
		mant &= 0x3ff
		exp32 := uint32(127-15-e) << 23
		return math.Float32frombits(sign | exp32 | (mant << 13))
	case 0x1f:
		if mant == 0 {
			return math.Float32frombits(sign | 0x7f800000) // Inf
		}
		return math.Float32frombits(sign | 0x7f800000 | (mant << 13)) // NaN
	default:
		exp32 := (exp - 15 + 127) << 23
		return math.Float32frombits(sign | exp32 | (mant << 13))
	}
}
