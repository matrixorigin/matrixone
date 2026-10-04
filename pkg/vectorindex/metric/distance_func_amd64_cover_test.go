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

// Branch-coverage tests for the f32/f64 amd64 distance kernels: dimension
// mismatch guards, the AVX-512 block + unrolled loop + scalar tail across many
// dims, cosine/spherical clamp + zero-denominator edges, and the
// NormalizeL2 / ScaleInPlace helpers (previously 0%).

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

// TestAMD64KernelsAcrossDims drives every f32/f64 kernel over dims that hit the
// AVX-512 block (>=64 f32 / >=32 f64), the 8/4-lane unrolled loop, and the scalar
// remainder, checking each against a plain float64 oracle.
func TestAMD64KernelsAcrossDims(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	dims := []int{1, 3, 4, 7, 8, 9, 15, 16, 17, 32, 33, 64, 65, 100, 105}
	for _, n := range dims {
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

// TestAMD64DimensionMismatch covers the length-guard error branch of every kernel.
func TestAMD64DimensionMismatch(t *testing.T) {
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

// TestAMD64ClampAndZeroEdges covers the spherical <-1 clamp, cosine zero-denom
// (distance 1.0), cosine-similarity zero-denom (error) and empty-input branches.
func TestAMD64ClampAndZeroEdges(t *testing.T) {
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

	// Zero vector -> zero denominator.
	z32, nz32 := make([]float32, 8), []float32{1, 2, 3, 4, 5, 6, 7, 8}
	z64, nz64 := make([]float64, 8), []float64{1, 2, 3, 4, 5, 6, 7, 8}

	d32, err := CosineDistanceF32(z32, nz32)
	require.NoError(t, err)
	require.Equal(t, float32(1.0), d32)
	d64, err := CosineDistanceF64(z64, nz64)
	require.NoError(t, err)
	require.Equal(t, 1.0, d64)

	_, err = CosineSimilarityF32(z32, nz32)
	require.Error(t, err)
	_, err = CosineSimilarityF64(z64, nz64)
	require.Error(t, err)

	// Empty input -> cosine similarity returns (0, nil) before the length check.
	e32, err := CosineSimilarityF32(nil, nil)
	require.NoError(t, err)
	require.Equal(t, float32(0), e32)
	e64, err := CosineSimilarityF64(nil, nil)
	require.NoError(t, err)
	require.Equal(t, 0.0, e64)
}

// TestAMD64CosineSimilarityUpperClamp pins the [-1,1] clamp on the SIMD cosine
// similarity kernels. float32 accumulation can push the raw quotient a hair above
// 1.0; without the clamp CosineSimilarityF32 returned 0x3f800001 (1.000000119) for
// these operands. The result must never exceed 1.0 (nor drop below -1.0).
func TestAMD64CosineSimilarityUpperClamp(t *testing.T) {
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

	// Same guard for the F64 kernel: a near-parallel pair must not exceed 1.0.
	p64 := []float64{1, 1, 1, 1, 1, 1, 1, 1}
	q64 := []float64{1, 1, 1, 1, 1, 1, 1, 1}
	cs64, err := CosineSimilarityF64(p64, q64)
	require.NoError(t, err)
	require.LessOrEqual(t, cs64, 1.0, "F64 cosine similarity must be clamped to <= 1.0")
	require.GreaterOrEqual(t, cs64, -1.0, "F64 cosine similarity must be clamped to >= -1.0")
}

// TestAMD64GenericDispatchers covers the generic RealNumbers wrappers (both the
// float32 and float64 type arms) and L2Distance's error-propagation branches.
// The trailing "type not supported" returns are unreachable: RealNumbers is
// constrained to float32|float64.
func TestAMD64GenericDispatchers(t *testing.T) {
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

// TestNormalizeL2AndScaleInPlace covers both helpers (previously 0%).
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
