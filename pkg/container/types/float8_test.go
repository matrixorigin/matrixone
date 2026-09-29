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
	"testing"

	"github.com/stretchr/testify/require"
)

// FP8 E4M3 reference values (OCP / NVIDIA layout).
func TestFloat8E4M3ReferenceValues(t *testing.T) {
	cases := []struct {
		bits Float8
		f    float32
	}{
		{0x00, 0},           // +0
		{0x80, 0},           // -0 (value 0, sign preserved separately)
		{0x08, 0.015625},    // min normal 2^-6
		{0x01, 0.001953125}, // min subnormal 2^-9
		{0x07, 0.013671875}, // max subnormal 7/8 * 2^-6
		{0x38, 1.0},         // 1.0 (exp=7, mant=0)
		{0x3f, 1.875},       // exp=7, mant=7
		{0x40, 2.0},         // exp=8
		{0x7e, 448.0},       // max finite
		{0xfe, -448.0},      // -max finite
	}
	for _, c := range cases {
		got := c.bits.ToFloat32()
		require.Equalf(t, c.f, got, "ToFloat32(0x%02x)", uint8(c.bits))
	}
	// NaN slot
	require.True(t, math.IsNaN(float64(Float8(0x7f).ToFloat32())))
	require.True(t, math.IsNaN(float64(Float8(0xff).ToFloat32())))
}

func TestFloat8E4M3FromFloat32(t *testing.T) {
	// exact representable round-trips
	for _, c := range []struct {
		f    float32
		bits Float8
	}{
		{0, 0x00},
		{0.015625, 0x08},
		{0.001953125, 0x01},
		{1.0, 0x38},
		{1.875, 0x3f},
		{2.0, 0x40},
		{448.0, 0x7e},
		{-1.0, 0xb8},
		{-448.0, 0xfe},
	} {
		require.Equalf(t, c.bits, Float8FromFloat32(c.f), "FromFloat32(%v)", c.f)
	}

	// saturation: overflow and +/-Inf clamp to the max finite, not NaN
	require.Equal(t, Float8(0x7e), Float8FromFloat32(1000))
	require.Equal(t, Float8(0x7e), Float8FromFloat32(float32(math.Inf(1))))
	require.Equal(t, Float8(0xfe), Float8FromFloat32(float32(math.Inf(-1))))

	// NaN -> canonical NaN
	require.Equal(t, Float8(0x7f), Float8FromFloat32(float32(math.NaN())))

	// round-to-nearest-even: 1.0 (0x38) and 1.25 (0x3a) bracket 1.125; midpoint
	// 1.0625 rounds to even 1.0.
	require.Equal(t, Float8(0x38), Float8FromFloat32(1.0625))

	// every E4M3 value round-trips through float32 and back to itself.
	for u := 0; u < 256; u++ {
		b := Float8(u)
		f := b.ToFloat32()
		if math.IsNaN(float64(f)) {
			continue
		}
		require.Equalf(t, b, Float8FromFloat32(f), "roundtrip 0x%02x (%v)", u, f)
	}
}
