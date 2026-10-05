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
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// Float8/Float4 are compared with CUDA's cuda_fp8.h/cuda_fp4.h conversions
// recorded in narrow_float_cuda_golden_test.go. NaN encoding is excluded: the
// Go codecs keep the FP8 NaN sign and map FP4 NaN to +0.

func TestFloat8DecodeMatchesCUDA(t *testing.T) {
	for c := 0; c < 256; c++ {
		got := Float8(c).ToFloat32()
		want := math.Float32frombits(cudaFloat8Decode[c])
		if math.IsNaN(float64(want)) {
			if !math.IsNaN(float64(got)) {
				t.Fatalf("code 0x%02x: got %v, want NaN", c, got)
			}
			continue
		}
		if math.Float32bits(got) != cudaFloat8Decode[c] {
			t.Fatalf("code 0x%02x: got %v (0x%08x), want %v (0x%08x)",
				c, got, math.Float32bits(got), want, cudaFloat8Decode[c])
		}
	}
}

func TestFloat4DecodeMatchesCUDA(t *testing.T) {
	for c := 0; c < 16; c++ {
		got := Float4(c).ToFloat32()
		if math.Float32bits(got) != cudaFloat4Decode[c] {
			t.Fatalf("code 0x%x: got %v (0x%08x), want %v (0x%08x)",
				c, got, math.Float32bits(got), math.Float32frombits(cudaFloat4Decode[c]), cudaFloat4Decode[c])
		}
	}
}

func TestNarrowFloatEncodeMatchesCUDA(t *testing.T) {
	if len(cudaNarrowEncode) == 0 {
		t.Fatal("empty golden table")
	}
	for _, tc := range cudaNarrowEncode {
		in := math.Float32frombits(tc.in)
		if got := uint8(Float8FromFloat32(in)); got != tc.f8 {
			t.Errorf("Float8FromFloat32(%v 0x%08x) = 0x%02x, CUDA 0x%02x", in, tc.in, got, tc.f8)
		}
		if got := uint8(Float4FromFloat32(in)); got != tc.f4 {
			t.Errorf("Float4FromFloat32(%v 0x%08x) = 0x%x, CUDA 0x%x", in, tc.in, got, tc.f4)
		}
	}
}

// TestNarrowFloatFromFloat64MatchesCUDA checks that the float64 encoders the block
// quantizer uses round every float32 input as CUDA does: the golden inputs and a strided
// sweep of finite float32 bit patterns, compared with the CUDA-pinned float32 encoders.
func TestNarrowFloatFromFloat64MatchesCUDA(t *testing.T) {
	for _, tc := range cudaNarrowEncode {
		in := math.Float32frombits(tc.in)
		if in != in {
			continue
		}
		require.Equal(t, tc.f8, uint8(float8FromFloat64(float64(in))), "0x%08x", tc.in)
		require.Equal(t, tc.f4, uint8(float4FromFloat64(float64(in))), "0x%08x", tc.in)
	}
	for bits := uint64(0); bits <= math.MaxUint32; bits += 997 {
		in := math.Float32frombits(uint32(bits))
		if in != in {
			continue
		}
		if Float8FromFloat32(in) != float8FromFloat64(float64(in)) ||
			Float4FromFloat32(in) != float4FromFloat64(float64(in)) {
			t.Fatalf("0x%08x: float32 %x/%x, float64 %x/%x", bits, Float8FromFloat32(in), Float4FromFloat32(in),
				float8FromFloat64(float64(in)), float4FromFloat64(float64(in)))
		}
	}
}

// TestNarrowFloatFromFloat64RoundsOnce checks values just above a midpoint, which a
// float32 intermediate would round onto the midpoint and then to the even code.
func TestNarrowFloatFromFloat64RoundsOnce(t *testing.T) {
	above := 2.5 + math.Ldexp(1, -30) // between E2M1 2 and 3
	require.Equal(t, float32(2.5), float32(above))
	require.Equal(t, float32(2), Float4FromFloat32(float32(above)).ToFloat32())
	require.Equal(t, float32(3), float4FromFloat64(above).ToFloat32())
	require.Equal(t, float32(-3), float4FromFloat64(-above).ToFloat32())

	above = 1.0625 + math.Ldexp(1, -40) // between E4M3 1 and 1.125
	require.Equal(t, float32(1), Float8FromFloat32(float32(above)).ToFloat32())
	require.Equal(t, float32(1.125), float8FromFloat64(above).ToFloat32())

	// exact midpoints still tie to the even code; overflow saturates
	require.Equal(t, float32(2), float4FromFloat64(2.5).ToFloat32())
	require.Equal(t, float32(4), float4FromFloat64(3.5).ToFloat32())
	require.Equal(t, float32(6), float4FromFloat64(1e9).ToFloat32())
	require.Equal(t, float32(448), float8FromFloat64(1e9).ToFloat32())
	require.Equal(t, float32(math.Ldexp(1, -9)), float8FromFloat64(math.Ldexp(1, -9)).ToFloat32())
	require.Equal(t, uint8(0x80), uint8(float8FromFloat64(math.Copysign(0, -1))))
}
