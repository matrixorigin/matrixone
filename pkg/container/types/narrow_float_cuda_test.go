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
