//go:build arm64 && go1.27 && goexperiment.simd

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

// SIMD-build-only tests: the narrow NEON kernels (bf16/f16/int8/uint8) coexist
// with their pure-Go twins here, so we can (a) prove they agree and (b) benchmark
// them head to head in one binary. Only built under `GOEXPERIMENT=simd` on arm64.
//
// Fixtures (narrowSIMDDims, rand*, checkPair) come from the untagged
// distance_func_narrow_simd_shared_test.go, shared with the amd64 tests.

package metric

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBF16NeonMatchesScalar(t *testing.T) {
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
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

func TestF16NeonMatchesScalar(t *testing.T) {
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
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

func TestInt8NeonMatchesScalar(t *testing.T) {
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
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

func TestUint8NeonMatchesScalar(t *testing.T) {
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
	}
	r := rand.New(rand.NewSource(11))
	type k struct {
		name         string
		simd, scalar func(a, b []uint8) (float64, error)
		exact        bool // integer kernels are bit-exact; cosine goes through float
	}
	for _, kn := range []k{
		{"l2sq", l2sqUint8SIMD, l2sqUint8, true},
		{"innerproduct", innerProductUint8SIMD, innerProductUint8, true},
		{"l1", l1DistanceUint8SIMD, l1DistanceUint8, true},
		{"cosine", cosineDistanceUint8SIMD, cosineDistanceUint8, false},
	} {
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

// TestNarrowNeonExtremes pushes the kernels at the ends of each type's range,
// where a wrong widening shows up as a large error rather than a rounding
// difference. Random [-8,8) inputs would not catch, for example, an int8 unpack
// that zero-extends (UXTL) instead of sign-extending (SXTL) — every value would
// still be positive and small, so the two agree.
func TestNarrowNeonExtremes(t *testing.T) {
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
	}
	for _, dim := range narrowSIMDDims {
		// int8: alternate the extremes so every byte lane sees both signs.
		ai8, bi8 := make([]int8, dim), make([]int8, dim)
		for i := range ai8 {
			switch i % 4 {
			case 0:
				ai8[i], bi8[i] = -128, 127
			case 1:
				ai8[i], bi8[i] = 127, -128
			case 2:
				ai8[i], bi8[i] = -1, 0
			case 3:
				ai8[i], bi8[i] = 0, -1
			}
		}
		for name, pair := range map[string][2]func(a, b []int8) (float64, error){
			"l2sq":         {l2sqInt8SIMD, l2sqInt8},
			"innerproduct": {innerProductInt8SIMD, innerProductInt8},
			"l1":           {l1DistanceInt8SIMD, l1DistanceInt8},
		} {
			got, err := pair[0](ai8, bi8)
			require.NoError(t, err)
			want, err := pair[1](ai8, bi8)
			require.NoError(t, err)
			checkPair(t, "int8-extreme/"+name, dim, got, want, true)
		}

		// uint8: 0 and 255 alternating puts the sign bit of every byte set, which
		// is exactly where a zero-extend and a sign-extend diverge (+255 vs -1).
		au8, bu8 := make([]uint8, dim), make([]uint8, dim)
		for i := range au8 {
			if i%2 == 0 {
				au8[i], bu8[i] = 255, 0
			} else {
				au8[i], bu8[i] = 0, 255
			}
		}
		for name, pair := range map[string][2]func(a, b []uint8) (float64, error){
			"l2sq":         {l2sqUint8SIMD, l2sqUint8},
			"innerproduct": {innerProductUint8SIMD, innerProductUint8},
			"l1":           {l1DistanceUint8SIMD, l1DistanceUint8},
		} {
			got, err := pair[0](au8, bu8)
			require.NoError(t, err)
			want, err := pair[1](au8, bu8)
			require.NoError(t, err)
			checkPair(t, "uint8-extreme/"+name, dim, got, want, true)
		}

		// bf16/f16: negative values exercise the sign bit that the decode
		// re-attaches, and large magnitudes exercise the exponent path.
		f := make([]float32, dim)
		g := make([]float32, dim)
		for i := range f {
			switch i % 4 {
			case 0:
				f[i], g[i] = -65504, 1
			case 1:
				f[i], g[i] = 65504, -1
			case 2:
				f[i], g[i] = -0.0001, 0.0001
			case 3:
				f[i], g[i] = 0, -0
			}
		}
		abf, bbf := types.Float32ToBF16Slice(f), types.Float32ToBF16Slice(g)
		for name, pair := range map[string][2]func(a, b []types.BF16) (float64, error){
			"l2sq":         {l2sqBF16SIMD, l2sqBF16},
			"innerproduct": {innerProductBF16SIMD, innerProductBF16},
			"l1":           {l1DistanceBF16SIMD, l1DistanceBF16},
			"cosine":       {cosineDistanceBF16SIMD, cosineDistanceBF16},
		} {
			got, err := pair[0](abf, bbf)
			require.NoError(t, err)
			want, err := pair[1](abf, bbf)
			require.NoError(t, err)
			checkPair(t, "bf16-extreme/"+name, dim, got, want, false)
		}

		af16, bf16v := types.Float32ToFloat16Slice(f), types.Float32ToFloat16Slice(g)
		for name, pair := range map[string][2]func(a, b []types.Float16) (float64, error){
			"l2sq":         {l2sqF16SIMD, l2sqF16},
			"innerproduct": {innerProductF16SIMD, innerProductF16},
			"l1":           {l1DistanceF16SIMD, l1DistanceF16},
			"cosine":       {cosineDistanceF16SIMD, cosineDistanceF16},
		} {
			got, err := pair[0](af16, bf16v)
			require.NoError(t, err)
			want, err := pair[1](af16, bf16v)
			require.NoError(t, err)
			checkPair(t, "f16-extreme/"+name, dim, got, want, false)
		}
	}
}

// TestNarrowNeonDimensionMismatch covers the length-guard error branch of every
// narrow NEON kernel, and the empty-input early return on the cosine kernels.
func TestNarrowNeonDimensionMismatch(t *testing.T) {
	for name, fn := range map[string]func() error{
		"bf16/l2sq":   func() error { _, e := l2sqBF16SIMD(make([]types.BF16, 8), make([]types.BF16, 7)); return e },
		"bf16/ip":     func() error { _, e := innerProductBF16SIMD(make([]types.BF16, 8), make([]types.BF16, 7)); return e },
		"bf16/l1":     func() error { _, e := l1DistanceBF16SIMD(make([]types.BF16, 8), make([]types.BF16, 7)); return e },
		"bf16/cosine": func() error { _, e := cosineDistanceBF16SIMD(make([]types.BF16, 8), make([]types.BF16, 7)); return e },
		"f16/l2sq":    func() error { _, e := l2sqF16SIMD(make([]types.Float16, 8), make([]types.Float16, 7)); return e },
		"f16/ip": func() error {
			_, e := innerProductF16SIMD(make([]types.Float16, 8), make([]types.Float16, 7))
			return e
		},
		"f16/l1": func() error { _, e := l1DistanceF16SIMD(make([]types.Float16, 8), make([]types.Float16, 7)); return e },
		"f16/cosine": func() error {
			_, e := cosineDistanceF16SIMD(make([]types.Float16, 8), make([]types.Float16, 7))
			return e
		},
		"int8/l2sq":    func() error { _, e := l2sqInt8SIMD(make([]int8, 8), make([]int8, 7)); return e },
		"int8/ip":      func() error { _, e := innerProductInt8SIMD(make([]int8, 8), make([]int8, 7)); return e },
		"int8/l1":      func() error { _, e := l1DistanceInt8SIMD(make([]int8, 8), make([]int8, 7)); return e },
		"int8/cosine":  func() error { _, e := cosineDistanceInt8SIMD(make([]int8, 8), make([]int8, 7)); return e },
		"uint8/l2sq":   func() error { _, e := l2sqUint8SIMD(make([]uint8, 8), make([]uint8, 7)); return e },
		"uint8/ip":     func() error { _, e := innerProductUint8SIMD(make([]uint8, 8), make([]uint8, 7)); return e },
		"uint8/l1":     func() error { _, e := l1DistanceUint8SIMD(make([]uint8, 8), make([]uint8, 7)); return e },
		"uint8/cosine": func() error { _, e := cosineDistanceUint8SIMD(make([]uint8, 8), make([]uint8, 7)); return e },
	} {
		require.Error(t, fn(), name)
	}

	// Empty input returns (0, nil) before the length check on the cosine kernels.
	for name, fn := range map[string]func() (float64, error){
		"bf16":  func() (float64, error) { return cosineDistanceBF16SIMD(nil, nil) },
		"f16":   func() (float64, error) { return cosineDistanceF16SIMD(nil, nil) },
		"int8":  func() (float64, error) { return cosineDistanceInt8SIMD(nil, nil) },
		"uint8": func() (float64, error) { return cosineDistanceUint8SIMD(nil, nil) },
	} {
		v, err := fn()
		require.NoError(t, err, name)
		require.Equal(t, 0.0, v, name)
	}

	// Zero vectors -> zero denominator -> cosine distance exactly 1.0.
	for name, fn := range map[string]func() (float64, error){
		"bf16": func() (float64, error) { return cosineDistanceBF16SIMD(make([]types.BF16, 16), make([]types.BF16, 16)) },
		"f16": func() (float64, error) {
			return cosineDistanceF16SIMD(make([]types.Float16, 16), make([]types.Float16, 16))
		},
		"int8":  func() (float64, error) { return cosineDistanceInt8SIMD(make([]int8, 16), make([]int8, 16)) },
		"uint8": func() (float64, error) { return cosineDistanceUint8SIMD(make([]uint8, 16), make([]uint8, 16)) },
	} {
		v, err := fn()
		require.NoError(t, err, name)
		require.Equal(t, 1.0, v, name)
	}
}

// ---- head-to-head benchmarks (dim=1024, same binary) ----
//
//	GOEXPERIMENT=simd go test ./pkg/vectorindex/metric/ \
//	    -run x -bench Benchmark_Narrow_NeonVsScalar -benchmem

func Benchmark_Narrow_NeonVsScalar(b *testing.B) {
	const dim = 1024
	r := rand.New(rand.NewSource(1))
	abf, bbf := randBF16(dim, r), randBF16(dim, r)
	af16, bf16v := randF16(dim, r), randF16(dim, r)
	ai8, bi8 := randI8(dim, r), randI8(dim, r)
	au8, bu8 := randU8(dim, r), randU8(dim, r)

	b.Run("bf16/l2sq/neon", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqBF16SIMD(abf, bbf)
		}
	})
	b.Run("bf16/l2sq/scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqBF16(abf, bbf)
		}
	})
	b.Run("f16/l2sq/neon", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqF16SIMD(af16, bf16v)
		}
	})
	b.Run("f16/l2sq/scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqF16(af16, bf16v)
		}
	})
	b.Run("int8/l2sq/neon", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqInt8SIMD(ai8, bi8)
		}
	})
	b.Run("int8/l2sq/scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqInt8(ai8, bi8)
		}
	})
	b.Run("uint8/l2sq/neon", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqUint8SIMD(au8, bu8)
		}
	})
	b.Run("uint8/l2sq/scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = l2sqUint8(au8, bu8)
		}
	})
}
