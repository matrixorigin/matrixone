//go:build amd64 && go1.26 && goexperiment.simd

// Copyright 2023 Matrix Origin
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

// SIMD-build-only tests: the narrow archsimd kernels (bf16/f16/int8) coexist with
// their pure-Go twins here, so we can (a) prove they agree and (b) benchmark them
// head to head in one binary. Only built under `GOEXPERIMENT=simd GOAMD64=v3`.

package metric

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// dims exercise the 16-lane main loop (bf16/f16: 32/iter, int8: 64/iter) plus
// every tail remainder, including odd final elements.
var narrowSIMDDims = []int{1, 2, 3, 4, 7, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 1000, 1024, 1025}

func randF32(dim int, r *rand.Rand) []float32 {
	f := make([]float32, dim)
	for i := range f {
		f[i] = float32(r.Float64()*16 - 8) // [-8, 8)
	}
	return f
}
func randBF16(dim int, r *rand.Rand) []types.BF16 { return types.Float32ToBF16Slice(randF32(dim, r)) }
func randF16(dim int, r *rand.Rand) []types.Float16 {
	return types.Float32ToFloat16Slice(randF32(dim, r))
}
func randI8(dim int, r *rand.Rand) []int8 {
	v := make([]int8, dim)
	for i := range v {
		v[i] = int8(r.Intn(255) - 127)
	}
	return v
}
func randU8(dim int, r *rand.Rand) []uint8 {
	v := make([]uint8, dim)
	for i := range v {
		v[i] = uint8(r.Intn(256))
	}
	return v
}

// checkPair asserts a SIMD kernel matches its scalar oracle. exact=true requires
// bit-equality (integer int8 L2sq/IP/L1); otherwise a magnitude-scaled tolerance
// (float reductions reorder).
func checkPair(t *testing.T, name string, dim int, got, want float64, exact bool) {
	t.Helper()
	if exact {
		require.Equal(t, want, got, "%s dim=%d", name, dim)
		return
	}
	require.InDelta(t, want, got, 1e-4*(1+math.Abs(want)), "%s dim=%d", name, dim)
}

func TestBF16SIMDMatchesScalar(t *testing.T) {
	if !hasAVX512 {
		t.Skip("AVX-512 not available")
	}
	r := rand.New(rand.NewSource(42))
	type k struct {
		name         string
		simd, scalar func(a, b []types.BF16) (float64, error)
	}
	for _, kn := range []k{
		{"l2sq", l2sqBF16SIMD, l2sqBF16},
		{"innerproduct", innerProductBF16SIMD, innerProductBF16},
		{"l1", l1DistanceBF16SIMD, l1DistanceBF16},
		{"cosine", cosineDistanceBF16SIMD, cosineDistanceBF16},
	} {
		for _, dim := range narrowSIMDDims {
			a, b := randBF16(dim, r), randBF16(dim, r)
			got, err := kn.simd(a, b)
			require.NoError(t, err)
			want, err := kn.scalar(a, b)
			require.NoError(t, err)
			checkPair(t, "bf16/"+kn.name, dim, got, want, false)
		}
	}
}

func TestF16SIMDMatchesScalar(t *testing.T) {
	if !hasAVX512 {
		t.Skip("AVX-512 not available")
	}
	r := rand.New(rand.NewSource(7))
	type k struct {
		name         string
		simd, scalar func(a, b []types.Float16) (float64, error)
	}
	for _, kn := range []k{
		{"l2sq", l2sqF16SIMD, l2sqF16},
		{"innerproduct", innerProductF16SIMD, innerProductF16},
		{"l1", l1DistanceF16SIMD, l1DistanceF16},
		{"cosine", cosineDistanceF16SIMD, cosineDistanceF16},
	} {
		for _, dim := range narrowSIMDDims {
			a, b := randF16(dim, r), randF16(dim, r)
			got, err := kn.simd(a, b)
			require.NoError(t, err)
			want, err := kn.scalar(a, b)
			require.NoError(t, err)
			checkPair(t, "f16/"+kn.name, dim, got, want, false)
		}
	}
}

func TestInt8SIMDMatchesScalar(t *testing.T) {
	if !hasAVX512 {
		t.Skip("AVX-512 not available")
	}
	r := rand.New(rand.NewSource(9))
	type k struct {
		name         string
		simd, scalar func(a, b []int8) (float64, error)
		exact        bool // integer kernels are bit-exact; cosine goes through float
	}
	for _, kn := range []k{
		{"l2sq", l2sqInt8SIMD, l2sqInt8, true},
		{"innerproduct", innerProductInt8SIMD, innerProductInt8, true},
		{"l1", l1DistanceInt8SIMD, l1DistanceInt8, true},
		{"cosine", cosineDistanceInt8SIMD, cosineDistanceInt8, false},
	} {
		for _, dim := range narrowSIMDDims {
			a, b := randI8(dim, r), randI8(dim, r)
			got, err := kn.simd(a, b)
			require.NoError(t, err)
			want, err := kn.scalar(a, b)
			require.NoError(t, err)
			checkPair(t, "int8/"+kn.name, dim, got, want, kn.exact)
		}
	}
}

func TestUint8SIMDMatchesScalar(t *testing.T) {
	if !hasAVX2 {
		t.Skip("AVX2 not available")
	}
	r := rand.New(rand.NewSource(11))
	type k struct {
		name         string
		simd, scalar func(a, b []uint8) (float64, error)
		exact        bool // integer kernels are bit-exact; cosine goes through float
	}
	simdSet := func() []k {
		if hasAVX512 {
			return []k{
				{"l2sq", l2sqUint8SIMD, l2sqUint8, true},
				{"innerproduct", innerProductUint8SIMD, innerProductUint8, true},
				{"l1", l1DistanceUint8SIMD, l1DistanceUint8, true},
				{"cosine", cosineDistanceUint8SIMD, cosineDistanceUint8, false},
			}
		}
		return []k{
			{"l2sq", l2sqUint8AVX2, l2sqUint8, true},
			{"innerproduct", innerProductUint8AVX2, innerProductUint8, true},
			{"l1", l1DistanceUint8AVX2, l1DistanceUint8, true},
			{"cosine", cosineDistanceUint8AVX2, cosineDistanceUint8, false},
		}
	}
	for _, kn := range simdSet() {
		for _, dim := range narrowSIMDDims {
			a, b := randU8(dim, r), randU8(dim, r)
			got, err := kn.simd(a, b)
			require.NoError(t, err)
			want, err := kn.scalar(a, b)
			require.NoError(t, err)
			checkPair(t, "uint8/"+kn.name, dim, got, want, kn.exact)
		}
	}
}

// ---- head-to-head benchmarks (dim=1024, same binary) ----
//
//	GOEXPERIMENT=simd GOAMD64=v3 go test ./pkg/vectorindex/metric/ \
//	    -run x -bench Benchmark_Narrow_SIMDvsScalar -benchmem

func Benchmark_Narrow_SIMDvsScalar(b *testing.B) {
	const dim = 1024
	r := rand.New(rand.NewSource(1))
	bf16a, bf16b := randBF16(dim, r), randBF16(dim, r)
	f16a, f16b := randF16(dim, r), randF16(dim, r)
	i8a, i8b := randI8(dim, r), randI8(dim, r)
	u8a, u8b := randU8(dim, r), randU8(dim, r)
	f32a, f32b := randF32(dim, r), randF32(dim, r)
	f64a := make([]float64, dim)
	f64b := make([]float64, dim)
	for i := range f64a {
		f64a[i] = float64(f32a[i])
		f64b[i] = float64(f32b[i])
	}

	runF32 := func(b *testing.B, fn func(a, c []float32) (float32, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(f32a, f32b)
		}
	}
	runF64 := func(b *testing.B, fn func(a, c []float64) (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(f64a, f64b)
		}
	}
	runBF16 := func(b *testing.B, fn func(a, c []types.BF16) (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(bf16a, bf16b)
		}
	}
	runF16 := func(b *testing.B, fn func(a, c []types.Float16) (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(f16a, f16b)
		}
	}
	runI8 := func(b *testing.B, fn func(a, c []int8) (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(i8a, i8b)
		}
	}
	runU8 := func(b *testing.B, fn func(a, c []uint8) (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn(u8a, u8b)
		}
	}

	// avx512 sub-benchmarks call the x16 kernels directly, so they only run when
	// the CPU has AVX-512 (calling them otherwise would fault). avx2 always runs
	// (AVX2 is implied by the GOAMD64=v3 build). This bench bypasses the function-
	// pointer selection on purpose, so a single run shows all three tiers
	// side-by-side regardless of the MO_METRIC_NO_AVX512/AVX2 overrides.
	// f32/f64 native baselines (no decode). These auto-dispatch to AVX-512
	// internally via hasAVX512; with MO_METRIC_NO_AVX512=1 they fall to scalar Go.
	b.Run("f32", func(b *testing.B) {
		b.Run("l2sq", func(b *testing.B) { runF32(b, L2DistanceSq[float32]) })
		b.Run("innerproduct", func(b *testing.B) { runF32(b, InnerProduct[float32]) })
		b.Run("l1", func(b *testing.B) { runF32(b, L1Distance[float32]) })
		b.Run("cosine", func(b *testing.B) { runF32(b, CosineDistance[float32]) })
	})
	b.Run("f64", func(b *testing.B) {
		b.Run("l2sq", func(b *testing.B) { runF64(b, L2DistanceSq[float64]) })
		b.Run("innerproduct", func(b *testing.B) { runF64(b, InnerProduct[float64]) })
		b.Run("l1", func(b *testing.B) { runF64(b, L1Distance[float64]) })
		b.Run("cosine", func(b *testing.B) { runF64(b, CosineDistance[float64]) })
	})
	b.Run("bf16", func(b *testing.B) {
		b.Run("l2sq/scalar", func(b *testing.B) { runBF16(b, l2sqBF16) })
		b.Run("l2sq/avx2", func(b *testing.B) { runBF16(b, l2sqBF16AVX2) })
		if hasAVX512 {
			b.Run("l2sq/avx512", func(b *testing.B) { runBF16(b, l2sqBF16SIMD) })
		}
		b.Run("innerproduct/scalar", func(b *testing.B) { runBF16(b, innerProductBF16) })
		b.Run("innerproduct/avx2", func(b *testing.B) { runBF16(b, innerProductBF16AVX2) })
		if hasAVX512 {
			b.Run("innerproduct/avx512", func(b *testing.B) { runBF16(b, innerProductBF16SIMD) })
		}
		b.Run("l1/scalar", func(b *testing.B) { runBF16(b, l1DistanceBF16) })
		b.Run("l1/avx2", func(b *testing.B) { runBF16(b, l1DistanceBF16AVX2) })
		if hasAVX512 {
			b.Run("l1/avx512", func(b *testing.B) { runBF16(b, l1DistanceBF16SIMD) })
		}
		b.Run("cosine/scalar", func(b *testing.B) { runBF16(b, cosineDistanceBF16) })
		b.Run("cosine/avx2", func(b *testing.B) { runBF16(b, cosineDistanceBF16AVX2) })
		if hasAVX512 {
			b.Run("cosine/avx512", func(b *testing.B) { runBF16(b, cosineDistanceBF16SIMD) })
		}
	})
	b.Run("f16", func(b *testing.B) {
		b.Run("l2sq/scalar", func(b *testing.B) { runF16(b, l2sqF16) })
		b.Run("l2sq/avx2", func(b *testing.B) { runF16(b, l2sqF16AVX2) })
		if hasAVX512 {
			b.Run("l2sq/avx512", func(b *testing.B) { runF16(b, l2sqF16SIMD) })
		}
		b.Run("innerproduct/scalar", func(b *testing.B) { runF16(b, innerProductF16) })
		b.Run("innerproduct/avx2", func(b *testing.B) { runF16(b, innerProductF16AVX2) })
		if hasAVX512 {
			b.Run("innerproduct/avx512", func(b *testing.B) { runF16(b, innerProductF16SIMD) })
		}
		b.Run("l1/scalar", func(b *testing.B) { runF16(b, l1DistanceF16) })
		b.Run("l1/avx2", func(b *testing.B) { runF16(b, l1DistanceF16AVX2) })
		if hasAVX512 {
			b.Run("l1/avx512", func(b *testing.B) { runF16(b, l1DistanceF16SIMD) })
		}
		b.Run("cosine/scalar", func(b *testing.B) { runF16(b, cosineDistanceF16) })
		b.Run("cosine/avx2", func(b *testing.B) { runF16(b, cosineDistanceF16AVX2) })
		if hasAVX512 {
			b.Run("cosine/avx512", func(b *testing.B) { runF16(b, cosineDistanceF16SIMD) })
		}
	})
	b.Run("int8", func(b *testing.B) {
		b.Run("l2sq/scalar", func(b *testing.B) { runI8(b, l2sqInt8) })
		b.Run("l2sq/avx2", func(b *testing.B) { runI8(b, l2sqInt8AVX2) })
		if hasAVX512 {
			b.Run("l2sq/avx512", func(b *testing.B) { runI8(b, l2sqInt8SIMD) })
		}
		b.Run("innerproduct/scalar", func(b *testing.B) { runI8(b, innerProductInt8) })
		b.Run("innerproduct/avx2", func(b *testing.B) { runI8(b, innerProductInt8AVX2) })
		if hasAVX512 {
			b.Run("innerproduct/avx512", func(b *testing.B) { runI8(b, innerProductInt8SIMD) })
		}
		b.Run("l1/scalar", func(b *testing.B) { runI8(b, l1DistanceInt8) })
		b.Run("l1/avx2", func(b *testing.B) { runI8(b, l1DistanceInt8AVX2) })
		if hasAVX512 {
			b.Run("l1/avx512", func(b *testing.B) { runI8(b, l1DistanceInt8SIMD) })
		}
		b.Run("cosine/scalar", func(b *testing.B) { runI8(b, cosineDistanceInt8) })
		b.Run("cosine/avx2", func(b *testing.B) { runI8(b, cosineDistanceInt8AVX2) })
		if hasAVX512 {
			b.Run("cosine/avx512", func(b *testing.B) { runI8(b, cosineDistanceInt8SIMD) })
		}
	})
	b.Run("uint8", func(b *testing.B) {
		b.Run("l2sq/scalar", func(b *testing.B) { runU8(b, l2sqUint8) })
		b.Run("l2sq/avx2", func(b *testing.B) { runU8(b, l2sqUint8AVX2) })
		if hasAVX512 {
			b.Run("l2sq/avx512", func(b *testing.B) { runU8(b, l2sqUint8SIMD) })
		}
		b.Run("innerproduct/scalar", func(b *testing.B) { runU8(b, innerProductUint8) })
		b.Run("innerproduct/avx2", func(b *testing.B) { runU8(b, innerProductUint8AVX2) })
		if hasAVX512 {
			b.Run("innerproduct/avx512", func(b *testing.B) { runU8(b, innerProductUint8SIMD) })
		}
		b.Run("l1/scalar", func(b *testing.B) { runU8(b, l1DistanceUint8) })
		b.Run("l1/avx2", func(b *testing.B) { runU8(b, l1DistanceUint8AVX2) })
		if hasAVX512 {
			b.Run("l1/avx512", func(b *testing.B) { runU8(b, l1DistanceUint8SIMD) })
		}
		b.Run("cosine/scalar", func(b *testing.B) { runU8(b, cosineDistanceUint8) })
		b.Run("cosine/avx2", func(b *testing.B) { runU8(b, cosineDistanceUint8AVX2) })
		if hasAVX512 {
			b.Run("cosine/avx512", func(b *testing.B) { runU8(b, cosineDistanceUint8SIMD) })
		}
	})
}
