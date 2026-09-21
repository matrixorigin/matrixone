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

func TestStableNormalizeL2ExtremeFiniteValues(t *testing.T) {
	for _, input := range [][]float64{
		{1e300, 1e300},
		{1e-300, 1e-300},
	} {
		out := make([]float64, len(input))
		require.NoError(t, StableNormalizeL2(input, out))
		require.InDelta(t, 1/math.Sqrt(2), out[0], 1e-15)
		require.InDelta(t, 1/math.Sqrt(2), out[1], 1e-15)
	}
}

func TestStableFloat64ResultsForFloat32Input(t *testing.T) {
	input := []float32{3e38, 3e38}
	zero := []float32{0, 0}

	l1, err := StableL1Distance(input, zero)
	require.NoError(t, err)
	require.InEpsilon(t, float64(input[0])*2, l1, 1e-14)

	l2, err := StableL2Distance(input, zero)
	require.NoError(t, err)
	require.InEpsilon(t, float64(input[0])*math.Sqrt2, l2, 1e-14)

	sq, err := StableL2DistanceSq(input, zero)
	require.NoError(t, err)
	require.InEpsilon(t, 2*float64(input[0])*float64(input[0]), sq, 1e-14)

	ip, err := StableInnerProduct(input, []float32{1, 1})
	require.NoError(t, err)
	require.InEpsilon(t, -float64(input[0])*2, ip, 1e-14)

	wideFn, err := ResolveDistanceFn[float32, float64](Metric_L2Distance)
	require.NoError(t, err)
	wide, err := wideFn(input, zero)
	require.NoError(t, err)
	require.InEpsilon(t, sq, wide, 1e-14)
}

func TestStableDistancesPreserveNonFiniteValues(t *testing.T) {
	assertStableDistanceNonFinite[float32](t)
	assertStableDistanceNonFinite[float64](t)

	wide, err := ResolveDistanceFn[float32, float64](Metric_L2sqDistance)
	require.NoError(t, err)
	distance, err := wide([]float32{float32(math.NaN()), 0}, []float32{0, 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(distance))
}

func assertStableDistanceNonFinite[T types.RealNumbers](t *testing.T) {
	t.Helper()
	zero := []T{0, 0}
	functions := []struct {
		name string
		fn   func([]T, []T) (float64, error)
	}{
		{"l1", StableL1Distance[T]},
		{"l2", StableL2Distance[T]},
		{"l2 squared", StableL2DistanceSq[T]},
	}
	cases := []struct {
		name    string
		left    []T
		right   []T
		wantNaN bool
	}{
		{"nan", []T{T(math.NaN()), 0}, zero, true},
		{"inf", []T{T(math.Inf(1)), 0}, zero, false},
		{"nan after inf", []T{T(math.Inf(1)), T(math.NaN())}, zero, true},
		{"inf minus inf", []T{T(math.Inf(1)), 0}, []T{T(math.Inf(1)), 0}, true},
	}
	for _, tc := range cases {
		for _, distanceFn := range functions {
			t.Run(tc.name+"/"+distanceFn.name, func(t *testing.T) {
				got, err := distanceFn.fn(tc.left, tc.right)
				require.NoError(t, err)
				if tc.wantNaN {
					require.True(t, math.IsNaN(got))
				} else {
					require.True(t, math.IsInf(got, 1))
				}
			})
		}
	}
}

func TestStableReductionsPreserveNonFiniteValues(t *testing.T) {
	sum, err := StableSummation([]float64{math.NaN(), 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(sum))

	sum, err = StableSummation([]float64{math.Inf(1), math.Inf(-1)})
	require.NoError(t, err)
	require.True(t, math.IsNaN(sum))

	sum, err = StableSummation([]float64{math.Inf(1), 1})
	require.NoError(t, err)
	require.True(t, math.IsInf(sum, 1))

	mean, err := StableMean([]float64{math.NaN(), 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(mean))
	mean, err = StableMean([]float64{math.Inf(-1), 1})
	require.NoError(t, err)
	require.True(t, math.IsInf(mean, -1))

	l1Norm, err := StableL1Norm([]float64{math.NaN(), 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(l1Norm))
	l1Norm, err = StableL1Norm([]float64{math.Inf(1), 0})
	require.NoError(t, err)
	require.True(t, math.IsInf(l1Norm, 1))

	l2Norm, err := StableL2Norm([]float64{math.NaN(), 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(l2Norm))
	l2Norm, err = StableL2Norm([]float64{math.Inf(-1), 0})
	require.NoError(t, err)
	require.True(t, math.IsInf(l2Norm, 1))

	inner, err := StableInnerProduct([]float64{math.Inf(1), 0}, []float64{0, 1})
	require.NoError(t, err)
	require.True(t, math.IsNaN(inner))
	inner, err = StableInnerProduct([]float64{math.Inf(1), 1}, []float64{1, 0})
	require.NoError(t, err)
	require.True(t, math.IsInf(inner, -1))

	cosine, err := StableCosineDistance([]float64{math.NaN(), 0}, []float64{0, 0})
	require.NoError(t, err)
	require.True(t, math.IsNaN(cosine))
	_, err = StableCosineSimilarity([]float64{math.Inf(1), 0}, []float64{0, 0})
	require.NoError(t, err)

	normalized := make([]float64, 2)
	require.NoError(t, StableNormalizeL2([]float64{math.NaN(), 0}, normalized))
	require.True(t, math.IsNaN(normalized[0]))
	require.True(t, math.IsNaN(normalized[1]))
	require.NoError(t, StableNormalizeL2([]float64{math.Inf(1), 1}, normalized))
	require.True(t, math.IsNaN(normalized[0]))
	require.Equal(t, float64(0), normalized[1])
}

func TestStableReductionsPreserveCancellation(t *testing.T) {
	sum, err := StableSummation([]float64{1e308, 1, -1e308})
	require.NoError(t, err)
	require.Equal(t, float64(1), sum)

	sum, err = StableSummation([]float64{1e308, 1e308, -1e308, -1e308})
	require.NoError(t, err)
	require.Equal(t, float64(0), sum)

	ordered, err := StableInnerProduct(
		[]float64{1e308, 1e308, -1e308, -1e308},
		[]float64{1, 1, 1, 1},
	)
	require.NoError(t, err)
	require.Equal(t, float64(0), ordered)

	reordered, err := StableInnerProduct(
		[]float64{1e308, -1e308, 1e308, -1e308},
		[]float64{1, 1, 1, 1},
	)
	require.NoError(t, err)
	require.Equal(t, float64(0), reordered)

	residual, err := StableInnerProduct(
		[]float64{1e308, 1e308, 1},
		[]float64{1e308, -1e308, 1},
	)
	require.NoError(t, err)
	require.Equal(t, float64(-1), residual)

	underflowing, err := StableInnerProduct(
		[]float64{1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162},
		[]float64{1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162, 1e-162},
	)
	require.NoError(t, err)
	require.Equal(t, -1e-323, underflowing)

	subnormal, err := StableInnerProduct(
		[]float64{2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162},
		[]float64{2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162, 2e-162},
	)
	require.NoError(t, err)
	require.Equal(t, -3.9525251667299724e-323, subnormal)
}

func TestStableMeanWideAccumulator(t *testing.T) {
	mean, err := StableMean([]float64{1e308, 1e308})
	require.NoError(t, err)
	require.Equal(t, 1e308, mean)

	mean, err = StableMean([]float64{1e308, -1e308, 1e-300})
	require.NoError(t, err)
	require.InEpsilon(t, 1e-300/3, mean, 1e-12)
}

func TestStableReductionsRecoverFromBoundaryAccumulatorOverflow(t *testing.T) {
	value := math.MaxFloat64 / 11
	values := make([]float64, 11)
	for i := range values {
		values[i] = value
	}

	sum, err := StableSummation(values)
	require.NoError(t, err)
	require.Equal(t, math.MaxFloat64, sum)

	mean, err := StableMean(values)
	require.NoError(t, err)
	require.Equal(t, value, mean)

	inner, err := StableInnerProduct(values, make([]float64, len(values)))
	require.NoError(t, err)
	require.Equal(t, float64(0), inner)

	ones := make([]float64, len(values))
	for i := range ones {
		ones[i] = 1
	}
	inner, err = StableInnerProduct(values, ones)
	require.NoError(t, err)
	require.Equal(t, -math.MaxFloat64, inner)
}

func TestStableCosineScaleInvariance(t *testing.T) {
	t.Run("float32", func(t *testing.T) { testCosineScaleInvariance[float32](t, 1e-20, 1e20) })
	t.Run("float64", func(t *testing.T) { testCosineScaleInvariance[float64](t, 1e-300, 1e300) })
}

func testCosineScaleInvariance[T types.RealNumbers](t *testing.T, tiny, large T) {
	t.Helper()
	// Directions (1,1,1) and (2,1,1) have dot=4 and squared norms 3 and 6.
	// Derive the oracle independently of both numerical implementations.
	want := 1 - 4/math.Sqrt(18)
	control, err := StableCosineDistance([]T{1, 1, 1}, []T{2, 1, 1})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		a, b T
	}{{"ordinary", 1, 1}, {"tiny_left", tiny, 1}, {"tiny_right", 1, tiny}, {"mixed_extremes", tiny, large}} {
		t.Run(tc.name, func(t *testing.T) {
			p, q := []T{tc.a, tc.a, tc.a}, []T{2 * tc.b, tc.b, tc.b}
			d, err := StableCosineDistance(p, q)
			require.NoError(t, err)
			require.False(t, math.IsNaN(d) || math.IsInf(d, 0))
			require.GreaterOrEqual(t, d, float64(0))
			require.LessOrEqual(t, d, float64(2))
			require.InDelta(t, want, d, 1e-14)
			require.InDelta(t, control, d, 1e-14)
			negative, err := StableCosineDistance([]T{-tc.a, -tc.a, -tc.a}, q)
			require.NoError(t, err)
			require.InDelta(t, 1+4/math.Sqrt(18), negative, 1e-14)
			zero, err := StableCosineDistance([]T{0, 0, 0}, q)
			require.NoError(t, err)
			require.Equal(t, float64(1), zero)
		})
	}
}

func TestStableCosineExtremeFiniteValues(t *testing.T) {
	large := []float64{1e300, 1e300}
	tiny := []float64{1e-300, 1e-300}
	opposite := []float64{-1e300, -1e300}
	orthogonal := []float64{1e300, 0}
	otherOrthogonal := []float64{0, 1e300}

	distance, err := StableCosineDistance(large, large)
	require.NoError(t, err)
	require.Equal(t, float64(0), distance)
	distance, err = StableCosineDistance(tiny, tiny)
	require.NoError(t, err)
	require.Equal(t, float64(0), distance)
	distance, err = StableCosineDistance(large, opposite)
	require.NoError(t, err)
	require.Equal(t, float64(2), distance)
	distance, err = StableCosineDistance(orthogonal, otherOrthogonal)
	require.NoError(t, err)
	require.Equal(t, float64(1), distance)

	similarity, err := StableCosineSimilarity(large, large)
	require.NoError(t, err)
	require.Equal(t, float64(1), similarity)
	_, err = StableCosineSimilarity([]float64{0, 0}, large)
	require.Error(t, err)
}
