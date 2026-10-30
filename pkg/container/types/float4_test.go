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

// FP4 E2M1 reference values: magnitudes {0,0.5,1,1.5,2,3,4,6}.
func TestFloat4E2M1ReferenceValues(t *testing.T) {
	mags := []float32{0, 0.5, 1, 1.5, 2, 3, 4, 6}
	for code, want := range mags {
		require.Equalf(t, want, Float4(code).ToFloat32(), "ToFloat32(code %d)", code)
		require.Equalf(t, -want, Float4(uint8(code)|0x08).ToFloat32(), "ToFloat32(neg code %d)", code)
	}
}

func TestFloat4E2M1FromFloat32(t *testing.T) {
	// exact round-trips
	for _, c := range []struct {
		f    float32
		bits Float4
	}{
		{0, 0x0},
		{0.5, 0x1},
		{1, 0x2},
		{1.5, 0x3},
		{2, 0x4},
		{3, 0x5},
		{4, 0x6},
		{6, 0x7},
		{-6, 0xf},
		{-1, 0xa},
	} {
		require.Equalf(t, c.bits, Float4FromFloat32(c.f), "FromFloat32(%v)", c.f)
	}

	// saturation: > 6 and +/-Inf clamp to +/-6
	require.Equal(t, Float4(0x7), Float4FromFloat32(100))
	require.Equal(t, Float4(0x7), Float4FromFloat32(float32(math.Inf(1))))
	require.Equal(t, Float4(0xf), Float4FromFloat32(float32(math.Inf(-1))))

	// NaN -> +0
	require.Equal(t, Float4(0x0), Float4FromFloat32(float32(math.NaN())))

	// round-to-nearest-even ties pick the even code:
	require.Equal(t, Float4(0x0), Float4FromFloat32(0.25)) // 0 vs 0.5 -> 0 (even)
	require.Equal(t, Float4(0x4), Float4FromFloat32(2.5))  // 2(code4) vs 3(code5) -> 2
	require.Equal(t, Float4(0x6), Float4FromFloat32(5.0))  // 4(code6) vs 6(code7) -> 4
	// non-tie nearest
	require.Equal(t, Float4(0x2), Float4FromFloat32(1.1)) // nearest 1.0
	require.Equal(t, Float4(0x5), Float4FromFloat32(2.9)) // nearest 3.0

	// every E2M1 code round-trips.
	for u := 0; u < 16; u++ {
		b := Float4(u)
		require.Equalf(t, b, Float4FromFloat32(b.ToFloat32()), "roundtrip 0x%x", u)
	}
}
