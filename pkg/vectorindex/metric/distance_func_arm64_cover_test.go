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

// Branch-coverage tests for the f32/f64 arm64 NEON distance kernels, mirroring
// distance_func_amd64_cover_test.go: dimension mismatch guards, the NEON block +
// unrolled loop + scalar tail across many dims, cosine/spherical clamp +
// zero-denominator edges, and the NormalizeL2 / ScaleInPlace helpers.
//
// NEON blocks are narrower than AVX-512, so the dim sweep targets *this* file's
// boundaries: 16 elems/iter for the f32 kernels, 8 for f64, 8 for cosine-f32 and
// 4 for cosine-f64, each followed by an 8- or 4-wide unrolled scalar loop and a
// 1-wide remainder.

package metric

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func clampUnit(x float64) float64 {
	if x > 1 {
		return 1
	}
	if x < -1 {
		return -1
	}
	return x
}

// arm64Dims straddles every NEON block/tail boundary in this file (4, 8, 16) as
// well as n-1 / n / n+1 around them. Tail handling is the most likely source of
// a lane-width porting bug, so the sweep is deliberately dense at the bottom.
var arm64Dims = []int{1, 2, 3, 4, 5, 7, 8, 9, 11, 15, 16, 17, 23, 24, 31, 32, 33, 63, 64, 65, 100, 105, 1024, 1025}

// TestARM64KernelsAcrossDims drives every f32/f64 kernel over dims that hit the
// NEON block, the unrolled loop and the scalar remainder, checking each against a
// plain float64 oracle.
func TestARM64KernelsAcrossDims(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for _, n := range arm64Dims {
		a32, b32 := make([]float32, n), make([]float32, n)
		a64, b64 := make([]float64, n), make([]float64, n)
		var l2, dot, l1, na, nb float64
		for i := 0; i < n; i++ {
			a32[i], b32[i] = float32(r.NormFloat64()), float32(r.NormFloat64())
			a64[i], b64[i] = float64(a32[i]), float64(b32[i])
			d := a64[i] - b64[i]
			l2 += d * d
			dot += a64[i] * b64[i]
			if d < 0 {
				l1 -= d
			} else {
				l1 += d
			}
			na += a64[i] * a64[i]
			nb += b64[i] * b64[i]
		}
		den := math.Sqrt(na) * math.Sqrt(nb)
		rel := func(want float64) float64 { return 1e-2 * (1 + math.Abs(want)) }
		relTight := func(want float64) float64 { return 1e-6 * (1 + math.Abs(want)) }

		g32, err := L2DistanceSqFloat32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, l2, float64(g32), rel(l2), "L2sqF32 n=%d", n)
		g64, err := L2DistanceSqFloat64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, l2, g64, relTight(l2), "L2sqF64 n=%d", n)

		// L2Distance = sqrt(L2sq) via the generic dispatcher (both type arms).
		gd32, err := L2Distance(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, math.Sqrt(l2), float64(gd32), rel(math.Sqrt(l2)), "L2F32 n=%d", n)
		gd64, err := L2Distance(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, math.Sqrt(l2), gd64, relTight(math.Sqrt(l2)), "L2F64 n=%d", n)

		// InnerProduct returns the negated dot product.
		ip32, err := InnerProductFloat32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, -dot, float64(ip32), rel(dot), "IPF32 n=%d", n)
		ip64, err := InnerProductFloat64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, -dot, ip64, relTight(dot), "IPF64 n=%d", n)

		l1a, err := L1DistanceFloat32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, l1, float64(l1a), rel(l1), "L1F32 n=%d", n)
		l1b, err := L1DistanceFloat64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, l1, l1b, relTight(l1), "L1F64 n=%d", n)

		cd32, err := CosineDistanceF32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, 1.0-clampUnit(dot/den), float64(cd32), rel(1), "CosDistF32 n=%d", n)
		cd64, err := CosineDistanceF64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, 1.0-clampUnit(dot/den), cd64, relTight(1), "CosDistF64 n=%d", n)

		cs32, err := CosineSimilarityF32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, dot/den, float64(cs32), rel(1), "CosSimF32 n=%d", n)
		cs64, err := CosineSimilarityF64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, dot/den, cs64, relTight(1), "CosSimF64 n=%d", n)

		sp32, err := SphericalDistanceFloat32(a32, b32)
		require.NoError(t, err)
		require.InDelta(t, math.Acos(clampUnit(dot))/math.Pi, float64(sp32), rel(1), "SphF32 n=%d", n)
		sp64, err := SphericalDistanceFloat64(a64, b64)
		require.NoError(t, err)
		require.InDelta(t, math.Acos(clampUnit(dot))/math.Pi, sp64, rel(1), "SphF64 n=%d", n)
	}
}

// TestARM64NeonMatchesScalarTail is the strongest check in this file: it runs each
// kernel twice over identical input, once with the NEON block enabled and once with
// hasNeon forced off so the very same function takes only its unrolled scalar tail,
// then requires the two to agree closely.
//
// The float64 oracle in TestARM64KernelsAcrossDims accumulates in a different order
// than either path, so it needs a loose 1e-2 relative tolerance for f32 — wide enough
// to hide a genuine lane-handling bug. This test compares the two paths of the *same*
// kernel against each other instead, where the only legitimate difference is
// summation order, so it can demand a far tighter bound.
func TestARM64NeonMatchesScalarTail(t *testing.T) {
	// With MO_METRIC_NO_NEON=1 both sides of the comparison would take the
	// scalar path and the test would pass vacuously, so skip instead.
	if !hasNeon {
		t.Skip("NEON disabled via MO_METRIC_NO_NEON")
	}

	withNeon := func(on bool, fn func()) {
		saved := hasNeon
		hasNeon = on
		defer func() { hasNeon = saved }()
		fn()
	}

	r := rand.New(rand.NewSource(20260821))
	for _, n := range arm64Dims {
		a32, b32 := make([]float32, n), make([]float32, n)
		a64, b64 := make([]float64, n), make([]float64, n)
		for i := 0; i < n; i++ {
			a32[i], b32[i] = float32(r.NormFloat64()), float32(r.NormFloat64())
			a64[i], b64[i] = float64(a32[i]), float64(b32[i])
		}

		f32Kernels := map[string]func([]float32, []float32) (float32, error){
			"L2sqF32":    L2DistanceSqFloat32,
			"IPF32":      InnerProductFloat32,
			"L1F32":      L1DistanceFloat32,
			"CosDistF32": CosineDistanceF32,
			"CosSimF32":  CosineSimilarityF32,
			"SphF32":     SphericalDistanceFloat32,
		}
		for name, fn := range f32Kernels {
			var simd, scalar float32
			var errS, errV error
			withNeon(true, func() { simd, errV = fn(a32, b32) })
			withNeon(false, func() { scalar, errS = fn(a32, b32) })
			require.NoError(t, errV, "%s n=%d", name, n)
			require.NoError(t, errS, "%s n=%d", name, n)
			require.InDelta(t, float64(scalar), float64(simd),
				1e-4*(1+math.Abs(float64(scalar))), "%s neon vs scalar n=%d", name, n)
		}

		f64Kernels := map[string]func([]float64, []float64) (float64, error){
			"L2sqF64":    L2DistanceSqFloat64,
			"IPF64":      InnerProductFloat64,
			"L1F64":      L1DistanceFloat64,
			"CosDistF64": CosineDistanceF64,
			"CosSimF64":  CosineSimilarityF64,
			"SphF64":     SphericalDistanceFloat64,
		}
		for name, fn := range f64Kernels {
			var simd, scalar float64
			var errS, errV error
			withNeon(true, func() { simd, errV = fn(a64, b64) })
			withNeon(false, func() { scalar, errS = fn(a64, b64) })
			require.NoError(t, errV, "%s n=%d", name, n)
			require.NoError(t, errS, "%s n=%d", name, n)
			require.InDelta(t, scalar, simd,
				1e-9*(1+math.Abs(scalar)), "%s neon vs scalar n=%d", name, n)
		}
	}
}

// TestARM64DimensionMismatch covers the length-guard error branch of every kernel.
func TestARM64DimensionMismatch(t *testing.T) {
	x32, y32 := make([]float32, 8), make([]float32, 7)
	x64, y64 := make([]float64, 8), make([]float64, 7)
	for name, fn := range map[string]func() error{
		"L2sqF32":    func() error { _, e := L2DistanceSqFloat32(x32, y32); return e },
		"L2sqF64":    func() error { _, e := L2DistanceSqFloat64(x64, y64); return e },
		"IPF32":      func() error { _, e := InnerProductFloat32(x32, y32); return e },
		"IPF64":      func() error { _, e := InnerProductFloat64(x64, y64); return e },
		"L1F32":      func() error { _, e := L1DistanceFloat32(x32, y32); return e },
		"L1F64":      func() error { _, e := L1DistanceFloat64(x64, y64); return e },
		"CosDistF32": func() error { _, e := CosineDistanceF32(x32, y32); return e },
		"CosDistF64": func() error { _, e := CosineDistanceF64(x64, y64); return e },
		"CosSimF32":  func() error { _, e := CosineSimilarityF32(x32, y32); return e },
		"CosSimF64":  func() error { _, e := CosineSimilarityF64(x64, y64); return e },
		"SphF32":     func() error { _, e := SphericalDistanceFloat32(x32, y32); return e },
		"SphF64":     func() error { _, e := SphericalDistanceFloat64(x64, y64); return e },
	} {
		require.Error(t, fn(), name)
	}
}

// TestARM64ClampAndZeroEdges covers the spherical <-1 clamp, cosine zero-denom
// (distance 1.0), cosine-similarity zero-denom (error) and empty-input branches.
func TestARM64ClampAndZeroEdges(t *testing.T) {
	// Anti-correlated unit-ish vectors -> dot < -1 -> spherical low clamp.
	anti32 := []float32{1, 1, 1, 1}
	negs32 := []float32{-1, -1, -1, -1}
	sp, err := SphericalDistanceFloat32(anti32, negs32)
	require.NoError(t, err)
	require.InDelta(t, 1.0, float64(sp), 1e-6) // acos(-1)/pi == 1
	anti64 := []float64{1, 1, 1, 1}
	negs64 := []float64{-1, -1, -1, -1}
	sp64, err := SphericalDistanceFloat64(anti64, negs64)
	require.NoError(t, err)
	require.InDelta(t, 1.0, sp64, 1e-9)

	// Zero vector -> zero denominator. Sized 16/8 so the NEON block runs and
	// still drives the norms to zero, and 4 so only the tail runs.
	for _, n := range []int{4, 8, 16, 17} {
		z32, nz32 := make([]float32, n), make([]float32, n)
		z64, nz64 := make([]float64, n), make([]float64, n)
		for i := 0; i < n; i++ {
			nz32[i], nz64[i] = float32(i+1), float64(i+1)
		}

		d32, err := CosineDistanceF32(z32, nz32)
		require.NoError(t, err)
		require.Equal(t, float32(1.0), d32, "n=%d", n)
		d64, err := CosineDistanceF64(z64, nz64)
		require.NoError(t, err)
		require.Equal(t, 1.0, d64, "n=%d", n)

		_, err = CosineSimilarityF32(z32, nz32)
		require.Error(t, err, "n=%d", n)
		_, err = CosineSimilarityF64(z64, nz64)
		require.Error(t, err, "n=%d", n)
	}

	// Empty input -> cosine similarity returns (0, nil) before the length check.
	e32, err := CosineSimilarityF32(nil, nil)
	require.NoError(t, err)
	require.Equal(t, float32(0), e32)
	e64, err := CosineSimilarityF64(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 0.0, e64)
}

// TestARM64CosineSimilarityUpperClamp pins the [-1,1] clamp on the NEON cosine
// similarity kernels. float32 accumulation can push the raw quotient a hair above
// 1.0; the amd64 kernel returned 0x3f800001 (1.000000119) for these operands before
// the clamp existed. NEON reduces in a different order than AVX-512, so this guard
// matters at least as much here. The result must never exceed 1.0 (nor drop below -1.0).
func TestARM64CosineSimilarityUpperClamp(t *testing.T) {
	bits32 := func(bs ...uint32) []float32 {
		out := make([]float32, len(bs))
		for i, b := range bs {
			out[i] = math.Float32frombits(b)
		}
		return out
	}
	a := bits32(0xbb667df6, 0x3ea790ea, 0xc31cb60c, 0x3d8855cd)
	b := bits32(0xbe9dc4d1, 0x41e564b9, 0xc6568888, 0x40baa3a1)

	cs32, err := CosineSimilarityF32(a, b)
	require.NoError(t, err)
	require.LessOrEqual(t, float64(cs32), 1.0, "F32 cosine similarity must be clamped to <= 1.0")
	require.GreaterOrEqual(t, float64(cs32), -1.0, "F32 cosine similarity must be clamped to >= -1.0")

	// Self-similarity must land exactly on the clamp for every dim class, since
	// a vector against itself has dot == normA == normB up to rounding.
	for _, n := range []int{4, 7, 8, 15, 16, 17, 64, 1024} {
		v32 := make([]float32, n)
		v64 := make([]float64, n)
		for i := range v32 {
			v32[i], v64[i] = 1, 1
		}
		s32, err := CosineSimilarityF32(v32, v32)
		require.NoError(t, err)
		require.LessOrEqual(t, float64(s32), 1.0, "F32 self-similarity clamp n=%d", n)
		s64, err := CosineSimilarityF64(v64, v64)
		require.NoError(t, err)
		require.LessOrEqual(t, s64, 1.0, "F64 self-similarity clamp n=%d", n)
	}
}

// TestARM64GenericDispatchers covers the generic RealNumbers wrappers (both the
// float32 and float64 type arms) and L2Distance's error-propagation branches.
// The trailing "type not supported" returns are unreachable: RealNumbers is
// constrained to float32|float64.
func TestARM64GenericDispatchers(t *testing.T) {
	a32, b32 := []float32{1, 2, 3, 4}, []float32{4, 3, 2, 1}
	a64, b64 := []float64{1, 2, 3, 4}, []float64{4, 3, 2, 1}
	for name, fn := range map[string]func() error{
		"L2sq/32":   func() error { _, e := L2DistanceSq(a32, b32); return e },
		"L2sq/64":   func() error { _, e := L2DistanceSq(a64, b64); return e },
		"L2/32":     func() error { _, e := L2Distance(a32, b32); return e },
		"L2/64":     func() error { _, e := L2Distance(a64, b64); return e },
		"IP/32":     func() error { _, e := InnerProduct(a32, b32); return e },
		"IP/64":     func() error { _, e := InnerProduct(a64, b64); return e },
		"L1/32":     func() error { _, e := L1Distance(a32, b32); return e },
		"L1/64":     func() error { _, e := L1Distance(a64, b64); return e },
		"Cos/32":    func() error { _, e := CosineDistance(a32, b32); return e },
		"Cos/64":    func() error { _, e := CosineDistance(a64, b64); return e },
		"CosSim/32": func() error { _, e := CosineSimilarity(a32, b32); return e },
		"CosSim/64": func() error { _, e := CosineSimilarity(a64, b64); return e },
		"Sph/32":    func() error { _, e := SphericalDistance(a32, b32); return e },
		"Sph/64":    func() error { _, e := SphericalDistance(a64, b64); return e },
	} {
		require.NoError(t, fn(), name)
	}

	// L2Distance propagates the underlying mismatch error on both type arms.
	_, err := L2Distance([]float32{1, 2}, []float32{1})
	require.Error(t, err)
	_, err = L2Distance([]float64{1, 2}, []float64{1})
	require.Error(t, err)
}

// TestNormalizeL2AndScaleInPlace covers both helpers.
func TestNormalizeL2AndScaleInPlace(t *testing.T) {
	// Empty -> error.
	require.Error(t, NormalizeL2([]float32{}, []float32{}))

	// Zero vector -> copy through, norm stays zero.
	zin := []float32{0, 0, 0}
	zout := make([]float32, 3)
	require.NoError(t, NormalizeL2(zin, zout))
	require.Equal(t, zin, zout)

	// Normal vector -> unit L2 norm.
	in := []float64{3, 4}
	out := make([]float64, 2)
	require.NoError(t, NormalizeL2(in, out))
	require.InDelta(t, 0.6, out[0], 1e-12)
	require.InDelta(t, 0.8, out[1], 1e-12)
	var norm float64
	for _, v := range out {
		norm += v * v
	}
	require.InDelta(t, 1.0, norm, 1e-12)

	// ScaleInPlace mutates in place.
	v := []float32{1, 2, 3}
	ScaleInPlace(v, 2)
	require.Equal(t, []float32{2, 4, 6}, v)
}
