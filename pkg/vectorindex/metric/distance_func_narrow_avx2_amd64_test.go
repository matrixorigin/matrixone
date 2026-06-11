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

// Tests + benchmark for the AVX2 (256-bit) narrow fallback tier. The AVX2
// kernels live in distance_func_narrow_avx2_amd64.go (production); here we prove
// they match the scalar oracle and benchmark scalar / AVX2 / AVX-512 side by side
// in one binary. Only built under `GOEXPERIMENT=simd GOAMD64=v3`.

package metric

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestAVX2NarrowMatchesScalar checks all four metrics of each AVX2 narrow kernel
// against the scalar oracle across dims covering the 8-lane loop + every tail.
func TestAVX2NarrowMatchesScalar(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	chk := func(name string, dim int, got, want float64, exact bool) {
		t.Helper()
		if exact {
			require.Equal(t, want, got, "%s dim=%d", name, dim)
			return
		}
		require.InDelta(t, want, got, 1e-4*(1+math.Abs(want)), "%s dim=%d", name, dim)
	}
	for _, dim := range narrowSIMDDims {
		bfa, bfb := randBF16(dim, r), randBF16(dim, r)
		for _, k := range []struct {
			name         string
			avx2, scalar func(a, b []types.BF16) (float64, error)
		}{
			{"bf16/l2sq", l2sqBF16AVX2, l2sqBF16},
			{"bf16/ip", innerProductBF16AVX2, innerProductBF16},
			{"bf16/l1", l1DistanceBF16AVX2, l1DistanceBF16},
			{"bf16/cosine", cosineDistanceBF16AVX2, cosineDistanceBF16},
		} {
			g, _ := k.avx2(bfa, bfb)
			w, _ := k.scalar(bfa, bfb)
			chk(k.name, dim, g, w, false)
		}

		fa, fb := randF16(dim, r), randF16(dim, r)
		for _, k := range []struct {
			name         string
			avx2, scalar func(a, b []types.Float16) (float64, error)
		}{
			{"f16/l2sq", l2sqF16AVX2, l2sqF16},
			{"f16/ip", innerProductF16AVX2, innerProductF16},
			{"f16/l1", l1DistanceF16AVX2, l1DistanceF16},
			{"f16/cosine", cosineDistanceF16AVX2, cosineDistanceF16},
		} {
			g, _ := k.avx2(fa, fb)
			w, _ := k.scalar(fa, fb)
			chk(k.name, dim, g, w, false)
		}

		i8a, i8b := randI8(dim, r), randI8(dim, r)
		for _, k := range []struct {
			name         string
			avx2, scalar func(a, b []int8) (float64, error)
			exact        bool
		}{
			{"int8/l2sq", l2sqInt8AVX2, l2sqInt8, true},
			{"int8/ip", innerProductInt8AVX2, innerProductInt8, true},
			{"int8/l1", l1DistanceInt8AVX2, l1DistanceInt8, true},
			{"int8/cosine", cosineDistanceInt8AVX2, cosineDistanceInt8, false},
		} {
			g, _ := k.avx2(i8a, i8b)
			w, _ := k.scalar(i8a, i8b)
			chk(k.name, dim, g, w, k.exact)
		}
	}
}

// Benchmark_Narrow_AVX2vsAVX512 compares scalar / AVX2 (x8) / AVX-512 (x16) for
// the narrow L2sq kernels in one binary.
//
//	GOEXPERIMENT=simd GOAMD64=v3 go test ./pkg/vectorindex/metric/ \
//	    -run x -bench Benchmark_Narrow_AVX2vsAVX512
func Benchmark_Narrow_AVX2vsAVX512(b *testing.B) {
	const dim = 1024
	r := rand.New(rand.NewSource(1))
	bfa, bfb := randBF16(dim, r), randBF16(dim, r)
	fa, fb := randF16(dim, r), randF16(dim, r)
	i8a, i8b := randI8(dim, r), randI8(dim, r)

	run := func(b *testing.B, fn func() (float64, error)) {
		for i := 0; i < b.N; i++ {
			_, _ = fn()
		}
	}
	b.Run("bf16/scalar", func(b *testing.B) { run(b, func() (float64, error) { return l2sqBF16(bfa, bfb) }) })
	b.Run("bf16/avx2", func(b *testing.B) { run(b, func() (float64, error) { return l2sqBF16AVX2(bfa, bfb) }) })
	b.Run("bf16/avx512", func(b *testing.B) { run(b, func() (float64, error) { return l2sqBF16SIMD(bfa, bfb) }) })
	b.Run("f16/scalar", func(b *testing.B) { run(b, func() (float64, error) { return l2sqF16(fa, fb) }) })
	b.Run("f16/avx2", func(b *testing.B) { run(b, func() (float64, error) { return l2sqF16AVX2(fa, fb) }) })
	b.Run("f16/avx512", func(b *testing.B) { run(b, func() (float64, error) { return l2sqF16SIMD(fa, fb) }) })
	b.Run("int8/scalar", func(b *testing.B) { run(b, func() (float64, error) { return l2sqInt8(i8a, i8b) }) })
	b.Run("int8/avx2", func(b *testing.B) { run(b, func() (float64, error) { return l2sqInt8AVX2(i8a, i8b) }) })
	b.Run("int8/avx512", func(b *testing.B) { run(b, func() (float64, error) { return l2sqInt8SIMD(i8a, i8b) }) })
}
