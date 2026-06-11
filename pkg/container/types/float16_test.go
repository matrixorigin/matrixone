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
)

func TestFloat16ReferenceValues(t *testing.T) {
	// (float32 input, expected IEEE half bits, expected float32 after round-trip)
	cases := []struct {
		in   float32
		bits uint16
		out  float32
	}{
		{0.0, 0x0000, 0.0},
		{1.0, 0x3c00, 1.0},
		{-1.0, 0xbc00, -1.0},
		{2.0, 0x4000, 2.0},
		{0.5, 0x3800, 0.5},
		{-0.5, 0xb800, -0.5},
		{65504.0, 0x7bff, 65504.0},      // largest normal half
		{0.00006103515625, 0x0400, 0.00006103515625}, // smallest normal half (2^-14)
		{0.00006097555, 0x03ff, 0.000060975552},       // largest subnormal half
		{5.9604645e-08, 0x0001, 5.9604645e-08},         // smallest positive subnormal (2^-24)
	}
	for _, c := range cases {
		got := Float16FromFloat32(c.in)
		if uint16(got) != c.bits {
			t.Errorf("Float16FromFloat32(%v) bits = 0x%04x, want 0x%04x", c.in, uint16(got), c.bits)
		}
		back := Float16(c.bits).ToFloat32()
		if math.Abs(float64(back-c.out)) > 1e-9 {
			t.Errorf("Float16(0x%04x).ToFloat32() = %v, want %v", c.bits, back, c.out)
		}
	}
}

func TestFloat16InfNaN(t *testing.T) {
	// +Inf
	if got := Float16FromFloat32(float32(math.Inf(1))); uint16(got) != 0x7c00 {
		t.Errorf("+Inf -> 0x%04x, want 0x7c00", uint16(got))
	}
	if got := Float16FromFloat32(float32(math.Inf(-1))); uint16(got) != 0xfc00 {
		t.Errorf("-Inf -> 0x%04x, want 0xfc00", uint16(got))
	}
	// overflow to +Inf
	if got := Float16FromFloat32(70000.0); uint16(got) != 0x7c00 {
		t.Errorf("70000 -> 0x%04x, want 0x7c00 (overflow to Inf)", uint16(got))
	}
	// NaN stays NaN
	nan := Float16FromFloat32(float32(math.NaN()))
	if !math.IsNaN(float64(nan.ToFloat32())) {
		t.Errorf("NaN did not survive round-trip: got %v", nan.ToFloat32())
	}
	// Inf round-trip
	if v := Float16(0x7c00).ToFloat32(); !math.IsInf(float64(v), 1) {
		t.Errorf("0x7c00 -> %v, want +Inf", v)
	}
}

func TestBF16ReferenceValues(t *testing.T) {
	cases := []struct {
		in   float32
		bits uint16
	}{
		{0.0, 0x0000},
		{1.0, 0x3f80},
		{-1.0, 0xbf80},
		{2.0, 0x4000},
		{0.5, 0x3f00},
		{3.14159265, 0x4049}, // pi truncated/rounded to bf16
	}
	for _, c := range cases {
		got := BF16FromFloat32(c.in)
		if uint16(got) != c.bits {
			t.Errorf("BF16FromFloat32(%v) = 0x%04x, want 0x%04x", c.in, uint16(got), c.bits)
		}
	}
	// bf16 keeps full float32 exponent range: round-trip is close
	for _, v := range []float32{1.0, -2.5, 100.0, 0.001, 12345.0} {
		back := BF16FromFloat32(v).ToFloat32()
		rel := math.Abs(float64((back - v) / v))
		if rel > 0.01 { // bf16 has ~7 mantissa bits -> ~0.4% worst case
			t.Errorf("BF16 round-trip %v -> %v, rel err %v too high", v, back, rel)
		}
	}
	// NaN survives
	if !math.IsNaN(float64(BF16FromFloat32(float32(math.NaN())).ToFloat32())) {
		t.Errorf("BF16 NaN did not survive round-trip")
	}
	// Inf survives
	if !math.IsInf(float64(BF16FromFloat32(float32(math.Inf(1))).ToFloat32()), 1) {
		t.Errorf("BF16 +Inf did not survive round-trip")
	}
}

func TestInt8Clamp(t *testing.T) {
	cases := []struct {
		in  float32
		out int8
	}{
		{0.0, 0},
		{1.4, 1},
		{1.6, 2},
		{-1.6, -2},
		{127.0, 127},
		{128.0, 127},  // clamp high
		{200.0, 127},  // clamp high
		{-128.0, -128},
		{-129.0, -128}, // clamp low
		{-500.0, -128}, // clamp low
	}
	for _, c := range cases {
		if got := Float32ToInt8(c.in); got != c.out {
			t.Errorf("Float32ToInt8(%v) = %d, want %d", c.in, got, c.out)
		}
	}
	if Float32ToInt8(float32(math.NaN())) != 0 {
		t.Errorf("NaN -> int8 should be 0")
	}
}

func TestFloat32Bridge(t *testing.T) {
	// ToFloat32Array for each element type
	if got := ToFloat32Array([]float32{1, 2, 3}); got[0] != 1 || got[2] != 3 {
		t.Errorf("f32 bridge = %v", got)
	}
	if got := ToFloat32Array([]float64{1, 2, 3}); got[1] != 2 {
		t.Errorf("f64 bridge = %v", got)
	}
	if got := ToFloat32Array([]BF16{BF16FromFloat32(1.5)}); got[0] != 1.5 {
		t.Errorf("bf16 bridge = %v", got)
	}
	if got := ToFloat32Array([]Float16{Float16FromFloat32(2.5)}); got[0] != 2.5 {
		t.Errorf("f16 bridge = %v", got)
	}
	if got := ToFloat32Array([]int8{-5, 7}); got[0] != -5 || got[1] != 7 {
		t.Errorf("int8 bridge = %v", got)
	}
	// FromFloat32Array narrows correctly
	src := []float32{1.0, 2.0, -3.0}
	if out := FromFloat32Array[float32](src); out[2] != -3.0 {
		t.Errorf("from f32 = %v", out)
	}
	if out := FromFloat32Array[float64](src); out[0] != 1.0 {
		t.Errorf("from f64 = %v", out)
	}
	if out := FromFloat32Array[BF16](src); out[0].ToFloat32() != 1.0 {
		t.Errorf("from bf16 = %v", out)
	}
	if out := FromFloat32Array[Float16](src); out[1].ToFloat32() != 2.0 {
		t.Errorf("from f16 = %v", out)
	}
	if out := FromFloat32Array[int8]([]float32{1.4, 130, -200}); out[0] != 1 || out[1] != 127 || out[2] != -128 {
		t.Errorf("from int8 = %v", out)
	}
}

func TestArrayElementCompare(t *testing.T) {
	// bf16/f16 must order by value, not raw bits (negative has high bit set)
	neg := []BF16{BF16FromFloat32(-1.0)}
	pos := []BF16{BF16FromFloat32(1.0)}
	if ArrayElementCompare(neg, pos) >= 0 {
		t.Errorf("bf16 compare: -1 should be < 1")
	}
	negh := []Float16{Float16FromFloat32(-2.0)}
	posh := []Float16{Float16FromFloat32(0.5)}
	if ArrayElementCompare(negh, posh) >= 0 {
		t.Errorf("f16 compare: -2 should be < 0.5")
	}
	if ArrayElementCompare([]int8{-5}, []int8{3}) >= 0 {
		t.Errorf("int8 compare: -5 should be < 3")
	}
}

func TestStringToArrayNarrow(t *testing.T) {
	// int8: strict integer parse. Valid integers in range round-trip exactly.
	i8, err := StringToArray[int8]("[1, -2, 127, -128, 0]")
	if err != nil {
		t.Fatalf("int8 parse: %v", err)
	}
	want := []int8{1, -2, 127, -128, 0}
	for i := range want {
		if i8[i] != want[i] {
			t.Errorf("int8[%d] = %d, want %d", i, i8[i], want[i])
		}
	}
	// int8: non-integer and out-of-range literals error (no silent round/clamp).
	if _, err := StringToArray[int8]("[1.4]"); err == nil {
		t.Errorf("int8 parse of non-integer should error")
	}
	if _, err := StringToArray[int8]("[200]"); err == nil {
		t.Errorf("int8 parse of out-of-range should error")
	}
	if _, err := StringToArray[int8]("[-129]"); err == nil {
		t.Errorf("int8 parse of out-of-range (low) should error")
	}
	// bf16 / f16: small integers round-trip exactly.
	bf, err := StringToArray[BF16]("[1, 2, 3]")
	if err != nil || bf[0].ToFloat32() != 1 || bf[2].ToFloat32() != 3 {
		t.Errorf("bf16 parse: %v %v", bf, err)
	}
	h, err := StringToArray[Float16]("[0.5, -2, 4]")
	if err != nil || h[0].ToFloat32() != 0.5 || h[1].ToFloat32() != -2 {
		t.Errorf("f16 parse: %v %v", h, err)
	}
	// ArrayToString round-trips the narrow types.
	if s := ArrayToString[int8]([]int8{1, -2, 127}); s != "[1, -2, 127]" {
		t.Errorf("int8 ArrayToString = %q", s)
	}
	if s := ArrayToString[BF16](Float32ToBF16Slice([]float32{1, 2, 3})); s != "[1, 2, 3]" {
		t.Errorf("bf16 ArrayToString = %q", s)
	}
}

func TestFloat16SliceRoundTrip(t *testing.T) {
	src := []float32{1.0, 2.0, 0.5, -3.0, 0.0}
	f16 := Float32ToFloat16Slice(src)
	back := Float16ToFloat32Slice(f16)
	for i := range src {
		if back[i] != src[i] {
			t.Errorf("f16 slice round-trip[%d]: %v != %v", i, back[i], src[i])
		}
	}
	bf := Float32ToBF16Slice(src)
	bback := BF16ToFloat32Slice(bf)
	for i := range src {
		if math.Abs(float64(bback[i]-src[i])) > math.Abs(float64(src[i]))*0.01+1e-6 {
			t.Errorf("bf16 slice round-trip[%d]: %v vs %v", i, bback[i], src[i])
		}
	}
	i8 := Float32ToInt8Slice([]float32{1.2, 130.0, -200.0})
	if i8[0] != 1 || i8[1] != 127 || i8[2] != -128 {
		t.Errorf("int8 slice = %v", i8)
	}
}
