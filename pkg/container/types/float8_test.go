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

func TestRejectNonFiniteNarrowFloat(t *testing.T) {
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))

	// NaN/Inf are rejected for every low-precision float type.
	for _, oid := range []T{T_bf16, T_float16, T_float8, T_float4} {
		require.Error(t, RejectNonFiniteNarrowFloat(nan, oid), "nan %s", oid)
		require.Error(t, RejectNonFiniteNarrowFloat(inf, oid), "inf %s", oid)
		require.Error(t, RejectNonFiniteNarrowFloat(-inf, oid), "-inf %s", oid)
		// In-range finite values pass.
		require.NoError(t, RejectNonFiniteNarrowFloat(1.5, oid), "1.5 %s", oid)
		require.NoError(t, RejectNonFiniteNarrowFloat(-2.0, oid), "-2.0 %s", oid)
		require.NoError(t, RejectNonFiniteNarrowFloat(0, oid), "0 %s", oid)
	}

	// Overflow beyond each type's finite range is rejected (would otherwise saturate).
	require.Error(t, RejectNonFiniteNarrowFloat(70000, T_float16), "float16 max is 65504")
	require.NoError(t, RejectNonFiniteNarrowFloat(60000, T_float16))
	require.Error(t, RejectNonFiniteNarrowFloat(1000, T_float8), "float8 max is 448")
	require.NoError(t, RejectNonFiniteNarrowFloat(448, T_float8))
	require.Error(t, RejectNonFiniteNarrowFloat(7, T_float4), "float4 max is 6")
	require.NoError(t, RejectNonFiniteNarrowFloat(6, T_float4))
	require.Error(t, RejectNonFiniteNarrowFloat(-7, T_float4))
	// bf16 shares float32's exponent range, so a normal float32 never overflows it.
	require.NoError(t, RejectNonFiniteNarrowFloat(3.0e38, T_bf16))
}

// TestLowPrecInfConversion covers +/-Inf conversion for all four low-precision float
// types: bf16/float16 preserve Inf (they have an Inf encoding); float8/float4 have no
// Inf and saturate to their max finite magnitude.
func TestLowPrecInfConversion(t *testing.T) {
	pinf := float32(math.Inf(1))
	ninf := float32(math.Inf(-1))

	// bf16 preserves Inf: float32 +Inf (0x7f800000) truncates to 0x7f80.
	require.Equal(t, BF16(0x7f80), BF16FromFloat32(pinf))
	require.Equal(t, BF16(0xff80), BF16FromFloat32(ninf))
	require.True(t, math.IsInf(float64(BF16(0x7f80).ToFloat32()), 1))
	require.True(t, math.IsInf(float64(BF16(0xff80).ToFloat32()), -1))

	// float16 preserves Inf.
	require.Equal(t, Float16(0x7c00), Float16FromFloat32(pinf))
	require.Equal(t, Float16(0xfc00), Float16FromFloat32(ninf))
	require.True(t, math.IsInf(float64(Float16(0x7c00).ToFloat32()), 1))

	// float8 has no Inf: +/-Inf saturate to +/-448 (max finite), never NaN.
	require.Equal(t, Float8(0x7e), Float8FromFloat32(pinf))
	require.Equal(t, Float8(0xfe), Float8FromFloat32(ninf))
	require.InDelta(t, 448.0, float64(Float8(0x7e).ToFloat32()), 1e-3)
	require.False(t, math.IsInf(float64(Float8FromFloat32(pinf).ToFloat32()), 0))

	// float4 has no Inf: +/-Inf saturate to +/-6.
	require.Equal(t, Float4(0x7), Float4FromFloat32(pinf))
	require.Equal(t, Float4(0xf), Float4FromFloat32(ninf))
	require.Equal(t, float32(6), Float4(0x7).ToFloat32())
	require.False(t, math.IsInf(float64(Float4FromFloat32(pinf).ToFloat32()), 0))
}

// TestLowPrecSubnormal covers subnormal (denormal) values for all four types: values
// below the smallest normal that are still representable with reduced mantissa precision.
func TestLowPrecSubnormal(t *testing.T) {
	// bf16 shares float32's exponent range; its smallest subnormal is 2^-133
	// (bits 0x0001), the largest 2^-126*(127/128) (bits 0x007f).
	require.Equal(t, float32(math.Ldexp(1, -133)), BF16(0x0001).ToFloat32())
	require.Equal(t, BF16(0x0001), BF16FromFloat32(float32(math.Ldexp(1, -133))))
	require.InEpsilon(t, math.Ldexp(127, -133), float64(BF16(0x007f).ToFloat32()), 1e-6)

	// float16 smallest positive subnormal is 2^-24 (bits 0x0001); largest 2^-14*(1023/1024).
	require.Equal(t, float32(math.Ldexp(1, -24)), Float16(0x0001).ToFloat32())
	require.Equal(t, Float16(0x0001), Float16FromFloat32(float32(math.Ldexp(1, -24))))

	// float8 E4M3 subnormals are m/8 * 2^-6 for m=1..7 (step 2^-9), bits 0x01..0x07.
	for m := 1; m <= 7; m++ {
		want := float32(float64(m) / 8.0 * math.Ldexp(1, -6))
		require.Equal(t, want, Float8(uint8(m)).ToFloat32(), "float8 subnormal m=%d", m)
		require.Equal(t, Float8(uint8(m)), Float8FromFloat32(want), "float8 subnormal roundtrip m=%d", m)
	}
	// Smallest float8 subnormal is 2^-9; half of it rounds to zero (RNE).
	require.Equal(t, float32(math.Ldexp(1, -9)), Float8(0x01).ToFloat32())
	require.Equal(t, Float8(0), Float8FromFloat32(float32(math.Ldexp(1, -10))))

	// float4 E2M1: the only subnormal is 0.5 (code 0x1); 0.25 rounds to even (0).
	require.Equal(t, float32(0.5), Float4(0x1).ToFloat32())
	require.Equal(t, Float4(0x1), Float4FromFloat32(0.5))
	require.Equal(t, Float4(0x9), Float4FromFloat32(-0.5))
	require.Equal(t, Float4(0), Float4FromFloat32(0.25))
}

// TestLowPrecTypeRegistration covers the type-system switches for the four scalar
// low-precision float types: sizes, names, IsFloat, ToType, and the value codec /
// slice converters (#20567).
func TestLowPrecTypeRegistration(t *testing.T) {
	for _, tc := range []struct {
		oid     T
		sqlName string
		sz      int
	}{
		{T_bf16, "BF16", 2},
		{T_float16, "FLOAT16", 2},
		{T_float8, "FLOAT8", 1},
		{T_float4, "FLOAT4", 1},
	} {
		require.True(t, tc.oid.IsFloat(), "%s T.IsFloat", tc.oid)
		require.True(t, tc.oid.ToType().IsFloat(), "%s Type.IsFloat", tc.oid)
		require.Equal(t, tc.sz, tc.oid.TypeLen(), "%s TypeLen", tc.oid)
		require.Equal(t, tc.sz, tc.oid.FixedLength(), "%s FixedLength", tc.oid)
		require.Equal(t, tc.sqlName, tc.oid.String(), "%s String", tc.oid)
		require.NotEmpty(t, tc.oid.OidString(), "%s OidString", tc.oid)
		require.Equal(t, tc.oid, tc.oid.ToType().Oid)
	}

	// EncodeValue / DecodeValue round-trip for each type.
	bf := BF16FromFloat32(1.5)
	require.Equal(t, bf, DecodeValue(EncodeValue(bf, T_bf16), T_bf16))
	h := Float16FromFloat32(1.5)
	require.Equal(t, h, DecodeValue(EncodeValue(h, T_float16), T_float16))
	f8 := Float8FromFloat32(1.5)
	require.Equal(t, f8, DecodeValue(EncodeValue(f8, T_float8), T_float8))
	f4 := Float4FromFloat32(1.5)
	require.Equal(t, f4, DecodeValue(EncodeValue(f4, T_float4), T_float4))

	// Slice converters.
	require.Equal(t, []float32{1.5, -2.0}, Float8ToFloat32Slice([]Float8{Float8FromFloat32(1.5), Float8FromFloat32(-2.0)}))
	require.Equal(t, []Float8{Float8FromFloat32(1.5)}, Float32ToFloat8Slice([]float32{1.5}))
	require.Equal(t, []float32{1.5, -2.0}, Float4ToFloat32Slice([]Float4{Float4FromFloat32(1.5), Float4FromFloat32(-2.0)}))
	require.Equal(t, []Float4{Float4FromFloat32(1.5)}, Float32ToFloat4Slice([]float32{1.5}))
	require.Equal(t, []float32{1.5}, BF16ToFloat32Slice([]BF16{BF16FromFloat32(1.5)}))
	require.Equal(t, []float32{1.5}, Float16ToFloat32Slice([]Float16{Float16FromFloat32(1.5)}))
}

// TestFloat8SubnormalRoundUpToNormal is the regression for the subnormal->normal round-up
// that silently returned zero: a value in (largest subnormal 0.013671875, smallest normal
// 0.015625) must round to the smallest normal (0x08), not 0. The bug dropped roundMantissaRNE's
// carry, and the mant>=8 guard was dead because the result is masked to 3 bits.
func TestFloat8SubnormalRoundUpToNormal(t *testing.T) {
	smallestNormal := float32(math.Ldexp(1, -6)) // 0.015625, bits 0x08
	for _, in := range []float32{0.015, 0.0151, 0.01546, 0.0155} {
		got := Float8FromFloat32(in)
		require.NotEqualf(t, Float8(0), got, "%v must not narrow to zero", in)
		require.Equalf(t, Float8(0x08), got, "%v must round to the smallest normal 0x08", in)
		require.Equal(t, smallestNormal, got.ToFloat32())
	}
	// Just below the midpoint to the smallest normal still resolves to the largest
	// subnormal (0x07), and a value near zero underflows to 0 (unchanged behavior).
	require.Equal(t, Float8(0x07), Float8FromFloat32(0.0138))
	require.Equal(t, Float8(0), Float8FromFloat32(0.0001))
}
