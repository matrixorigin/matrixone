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
