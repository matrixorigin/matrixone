// Copyright 2026 Matrix Origin
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

package metric

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Lengths chosen to exercise both the unrolled chunk loop (>=8 for by-8, >=4 for by-4) and the
// scalar remainder tail.
var unrolledLens = []int{0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17}

// seqVecs builds two vectors of small integer-valued elements (exact in float32/float64), so a
// grouped (unrolled) sum and a sequential sum are bit-identical and can be compared with Equal.
func seqVecs[T types.RealNumbers](n int) (p, q []T) {
	p = make([]T, n)
	q = make([]T, n)
	for i := 0; i < n; i++ {
		p[i] = T(i%7 - 3)
		q[i] = T(i%5 - 2)
	}
	return p, q
}

func checkInnerProductUnrolled[T types.RealNumbers](t *testing.T) {
	for _, n := range unrolledLens {
		p, q := seqVecs[T](n)
		got, err := InnerProductUnrolled(p, q)
		require.NoErrorf(t, err, "n=%d", n)
		var dot T
		for i := 0; i < n; i++ {
			dot += p[i] * q[i]
		}
		require.Equalf(t, -dot, got, "n=%d", n)
	}
}

func checkL2DistanceSqUnrolled[T types.RealNumbers](t *testing.T) {
	for _, n := range unrolledLens {
		p, q := seqVecs[T](n)
		got, err := L2DistanceSqUnrolled(p, q)
		require.NoErrorf(t, err, "n=%d", n)
		var sum T
		for i := 0; i < n; i++ {
			d := p[i] - q[i]
			sum += d * d
		}
		require.Equalf(t, sum, got, "n=%d", n)
	}
}

func checkL1DistanceUnrolled[T types.RealNumbers](t *testing.T) {
	for _, n := range unrolledLens {
		p, q := seqVecs[T](n)
		got, err := L1DistanceUnrolled(p, q)
		require.NoErrorf(t, err, "n=%d", n)
		var sum T
		for i := 0; i < n; i++ {
			d := p[i] - q[i]
			if d < 0 {
				d = -d
			}
			sum += d
		}
		require.Equalf(t, sum, got, "n=%d", n)
	}
}

func checkSphericalDistanceUnrolled[T types.RealNumbers](t *testing.T) {
	for _, n := range unrolledLens {
		p, q := seqVecs[T](n)
		got, err := SphericalDistanceUnrolled(p, q)
		require.NoErrorf(t, err, "n=%d", n)
		var dot T
		for i := 0; i < n; i++ {
			dot += p[i] * q[i]
		}
		if dot > 1.0 {
			dot = 1.0
		} else if dot < -1.0 {
			dot = -1.0
		}
		want := T(math.Acos(float64(dot)) / math.Pi)
		require.InDeltaf(t, float64(want), float64(got), 1e-6, "n=%d", n)
	}
}

func checkCosineDistanceUnrolled[T types.RealNumbers](t *testing.T) {
	for _, n := range unrolledLens {
		if n == 0 {
			d, err := CosineDistanceUnrolled(seqVecs[T](0))
			require.NoError(t, err)
			require.Equal(t, T(0), d)
			continue
		}
		p, q := seqVecs[T](n)
		dist, errd := CosineDistanceUnrolled(p, q)
		sim, errs := CosineSimilarityUnrolled(p, q)
		require.NoErrorf(t, errd, "n=%d", n)
		require.NoErrorf(t, errs, "n=%d", n)

		var dot, np, nq float64
		for i := 0; i < n; i++ {
			a, b := float64(p[i]), float64(q[i])
			dot += a * b
			np += a * a
			nq += b * b
		}
		wantSim := dot / (math.Sqrt(np) * math.Sqrt(nq))
		if wantSim > 1.0 {
			wantSim = 1.0
		} else if wantSim < -1.0 {
			wantSim = -1.0
		}
		require.InDeltaf(t, 1.0-wantSim, float64(dist), 1e-6, "cosine distance n=%d", n)
		require.InDeltaf(t, wantSim, float64(sim), 1e-6, "cosine similarity n=%d", n)
	}
}

func TestUnrolledMatchesNaive(t *testing.T) {
	t.Run("inner_product", func(t *testing.T) {
		checkInnerProductUnrolled[float32](t)
		checkInnerProductUnrolled[float64](t)
	})
	t.Run("l2sq", func(t *testing.T) {
		checkL2DistanceSqUnrolled[float32](t)
		checkL2DistanceSqUnrolled[float64](t)
	})
	t.Run("l1", func(t *testing.T) {
		checkL1DistanceUnrolled[float32](t)
		checkL1DistanceUnrolled[float64](t)
	})
	t.Run("spherical", func(t *testing.T) {
		checkSphericalDistanceUnrolled[float32](t)
		checkSphericalDistanceUnrolled[float64](t)
	})
	t.Run("cosine", func(t *testing.T) {
		checkCosineDistanceUnrolled[float32](t)
		checkCosineDistanceUnrolled[float64](t)
	})
}

// TestUnrolledKnownValues pins hand-computed results for a non-chunk-aligned length (3).
func TestUnrolledKnownValues(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 6, 3}

	ip, err := InnerProductUnrolled(a, b)
	require.NoError(t, err)
	require.Equal(t, float32(-25), ip) // -(4+12+9)

	l2sq, err := L2DistanceSqUnrolled(a, b)
	require.NoError(t, err)
	require.Equal(t, float32(25), l2sq) // 9+16+0

	l2, err := L2DistanceUnrolled(a, b)
	require.NoError(t, err)
	require.Equal(t, float32(5), l2)

	l1, err := L1DistanceUnrolled(a, b)
	require.NoError(t, err)
	require.Equal(t, float32(7), l1) // 3+4+0
}

// TestUnrolledDimensionMismatch: every kernel reports a dimension mismatch rather than panicking.
func TestUnrolledDimensionMismatch(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{1, 2}

	_, err := InnerProductUnrolled(a, b)
	require.Error(t, err)
	_, err = L2DistanceSqUnrolled(a, b)
	require.Error(t, err)
	_, err = L2DistanceUnrolled(a, b)
	require.Error(t, err)
	_, err = L1DistanceUnrolled(a, b)
	require.Error(t, err)
	_, err = SphericalDistanceUnrolled(a, b)
	require.Error(t, err)
	_, err = CosineDistanceUnrolled(a, b)
	require.Error(t, err)
	_, err = CosineSimilarityUnrolled(a, b)
	require.Error(t, err)
}
