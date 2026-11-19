// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package aggexec

import (
	"math"
	"testing"
)

// TestEwmConfigRoundTrip pins theconfiguration channel's encode/decode
// byte contract: every field survives exactly once (the row and the
// event-time forms, both policy shapes, negative-zero-free magnitudes
// and the boundary values ANTI-regression-wise).
func TestEwmConfigRoundTrip(t *testing.T) {
	configs := []EwmConfig{
		{Mode: EwmModeRow, Alpha: 0.5, Adjust: true, MinPeriods: 0},
		{Mode: EwmModeRow, Alpha: math.Nextafter(1.0, 2.0), Adjust: false, IgnoreNA: true, MinPeriods: 7},
		{Mode: EwmModeRow, Alpha: math.SmallestNonzeroFloat64, Adjust: true},
		{Mode: EwmModeTime, HalfLife: 3600, Adjust: true, MinPeriods: 2},
		{Mode: EwmModeTime, HalfLife: math.MaxFloat64, Adjust: false, IgnoreNA: true, MinPeriods: math.MaxInt32},
		// the eleventh review round's boundary rows: §4 declares no
		// int32 limit, so the full int64 threshold axis round-trips
		{Mode: EwmModeRow, Alpha: 0.5, Adjust: true, IgnoreNA: false, MinPeriods: 2147483648},
		{Mode: EwmModeRow, Alpha: 0.5, Adjust: false, IgnoreNA: true, MinPeriods: math.MaxInt64},
		{Mode: EwmModeTime, HalfLife: 1e-300, Adjust: true},
	}
	for i, want := range configs {
		got, err := DecodeEwmConfig(EncodeEwmConfig(want))
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if got.Mode != want.Mode || got.Adjust != want.Adjust || got.IgnoreNA != want.IgnoreNA ||
			got.MinPeriods != want.MinPeriods {
			t.Fatalf("case %d: flag mismatch: %+v vs %+v", i, got, want)
		}
		if math.Float64bits(got.Alpha) != math.Float64bits(want.Alpha) {
			t.Fatalf("case %d: alpha bits %x vs %x", i, math.Float64bits(got.Alpha), math.Float64bits(want.Alpha))
		}
		if math.Float64bits(got.HalfLife) != math.Float64bits(want.HalfLife) {
			t.Fatalf("case %d: halflife bits %x vs %x", i, math.Float64bits(got.HalfLife), math.Float64bits(want.HalfLife))
		}
	}
}

// TestEwmConfigDecode rejects truncated, longer, bad-version and
// bad-mode bytes: anything not produced by EncodeEwmConfig fails closed.
func TestEwmConfigDecode(t *testing.T) {
	valid := EncodeEwmConfig(EwmConfig{Mode: EwmModeRow, Alpha: 0.5, Adjust: true})
	if _, err := DecodeEwmConfig(valid); err != nil {
		t.Fatalf("valid decode: %v", err)
	}
	if _, err := DecodeEwmConfig(nil); err == nil {
		t.Fatal("nil bytes decoded")
	}
	if _, err := DecodeEwmConfig(valid[:10]); err == nil {
		t.Fatal("truncated bytes decoded")
	}
	if _, err := DecodeEwmConfig(append(append([]byte(nil), valid...), 1)); err == nil {
		t.Fatal("overlong bytes decoded")
	}
	badVersion := append([]byte(nil), valid...)
	badVersion[0] = 0x7f
	if _, err := DecodeEwmConfig(badVersion); err == nil {
		t.Fatal("bad version decoded")
	}
	badMode := append([]byte(nil), valid...)
	badMode[1] = 0x42
	if _, err := DecodeEwmConfig(badMode); err == nil {
		t.Fatal("bad mode decoded")
	}
}
