// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

// A squared L2 distance that leaves the element domain is rejected on both paths, with the same
// error: the scalar and batch kernels both accumulate the square in float32, so neither can answer
// this pair, and neither returns the +Inf that accumulation produced.
func TestPairwiseL2RejectsSquaredOverflow(t *testing.T) {
	x := [][]float32{{0, 0}}
	y := [][]float32{{2e19, 2e19}}

	_, err := L2Distance[float32](x[0], y[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows the element domain")

	_, err = GoPairWiseDistance(x, y, Metric_L2Distance)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows the element domain")

	// An ordinary pair is unaffected.
	ok, err := GoPairWiseDistance([][]float32{{0, 0}}, [][]float32{{3, 4}}, Metric_L2Distance)
	require.NoError(t, err)
	require.EqualValues(t, 5, ok[0])
}

// CheckFiniteDists / AllFiniteF32 accept every finite result and reject both non-finite values a
// batch kernel can produce: +Inf from the CPU loop, NaN from cuVS's expanded form.
func TestCheckFiniteDists(t *testing.T) {
	require.NoError(t, CheckFiniteDists(nil, l2What))
	require.NoError(t, CheckFiniteDists([]float32{0, 1.5, 7, -3, math.MaxFloat32}, l2What))
	require.True(t, AllFiniteF32([]float32{0, 1.5, 7}))

	for _, bad := range []float32{
		float32(math.Inf(1)),
		float32(math.Inf(-1)),
		float32(math.NaN()),
	} {
		dist := []float32{1, 2, bad, 4}
		require.False(t, AllFiniteF32(dist))
		err := CheckFiniteDists(dist, l2What)
		require.Error(t, err)
		require.Contains(t, err.Error(), "overflows the element domain")
	}
}

// A vector whose squared norm leaves the float64 domain has no computable norm: it is rejected
// instead of being returned as all zeros (overflow) or unnormalized (underflow). An all-zero vector
// keeps its documented copy-unchanged behaviour.
func TestNormalizeL2RejectsUnrepresentableNorm(t *testing.T) {
	out := make([]float64, 2)

	err := NormalizeL2([]float64{1e308, 1e308}, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows")

	err = NormalizeL2([]float64{1e-300, 1e-300}, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "underflows")

	require.NoError(t, NormalizeL2([]float64{0, 0}, out))
	require.Equal(t, []float64{0, 0}, out)

	require.NoError(t, NormalizeL2([]float64{3, 4}, out))
	require.InDelta(t, 0.6, out[0], 1e-12)
	require.InDelta(t, 0.8, out[1], 1e-12)
}

// The cosine kernels accumulate in the element type. A float32 pair whose products overflow is
// recomputed in float64, so identical vectors still score distance 0; a float64 pair that overflows
// float64 itself has no representable cosine and is rejected rather than returning Inf/Inf = NaN.
func TestCosineFiniteExtremes(t *testing.T) {
	d32, err := CosineDistance[float32]([]float32{1e20, 1e20}, []float32{1e20, 1e20})
	require.NoError(t, err)
	require.False(t, math.IsNaN(float64(d32)))
	require.InDelta(t, 0.0, float64(d32), 1e-6)

	s32, err := CosineSimilarity[float32]([]float32{1e20, 1e20}, []float32{1e20, 1e20})
	require.NoError(t, err)
	require.InDelta(t, 1.0, float64(s32), 1e-6)

	_, err = CosineDistance[float64]([]float64{1e200, 1e200}, []float64{1e200, 1e200})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows")

	_, err = CosineSimilarity[float64]([]float64{1e200, 1e200}, []float64{1e200, 1e200})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows")

	// A zero vector keeps its conventions: distance 1, similarity rejected.
	d, err := CosineDistance[float32]([]float32{0, 0}, []float32{0, 0})
	require.NoError(t, err)
	require.EqualValues(t, 1, d)
	_, err = CosineSimilarity[float32]([]float32{0, 0}, []float32{0, 0})
	require.Error(t, err)
}

// The GPU pairwise path computes the squared distance in float32 on the device, so it overflows
// exactly as the CPU kernel does and is rejected the same way. 2048x2048 single-element vectors put
// the workload over GPUThresholdSync so the GPU branch is the one exercised.
func TestPairWiseDistanceGPURejectsOverflow(t *testing.T) {
	const n = 2048
	x := make([][]float32, n)
	y := make([][]float32, n)
	for i := 0; i < n; i++ {
		x[i] = []float32{0}
		y[i] = []float32{2e19}
	}
	require.GreaterOrEqual(t, uint64(n)*uint64(n)*1, GPUThresholdSync)

	_, err := PairWiseDistance(x, y, Metric_L2Distance, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows the element domain")

	// The same shape with ordinary magnitudes still returns distances.
	for i := 0; i < n; i++ {
		y[i] = []float32{3}
	}
	got, err := PairWiseDistance(x, y, Metric_L2Distance, true)
	require.NoError(t, err)
	require.Len(t, got, n*n)
	require.EqualValues(t, 3, got[0])
}

// A zero denominator is either a genuinely zero vector, which keeps the documented convention, or
// two non-zero vectors whose norms underflowed -- which has no computable cosine and is rejected
// rather than reported as maximally dissimilar.
func TestCosineUnderflowVsZeroVector(t *testing.T) {
	// float64 elements near 1e-200 square below the float64 range; identical vectors must not
	// report distance 1.
	_, err := CosineDistance[float64]([]float64{1e-200, 0}, []float64{1e-200, 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "underflows")

	_, err = CosineSimilarity[float64]([]float64{1e-200, 0}, []float64{1e-200, 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "underflows")

	// float32 squares are recovered in float64, so this one answers.
	d, err := CosineDistance[float32]([]float32{1e-30, 0}, []float32{1e-30, 0})
	require.NoError(t, err)
	require.EqualValues(t, 0, d)

	// A genuinely zero vector keeps its convention on both functions.
	d64, err := CosineDistance[float64]([]float64{0, 0}, []float64{1, 1})
	require.NoError(t, err)
	require.EqualValues(t, 1, d64)
	_, err = CosineSimilarity[float64]([]float64{0, 0}, []float64{1, 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "one of the vector is zero")
}

// A squared norm that lands in the subnormals is rejected too. The square is still positive and
// finite there, so the zero/Inf/NaN tests above miss it, but it carries far fewer than 53
// significant bits and the norm derived from it is already wrong: float64 [2e-162] squares to the
// smallest subnormal and normalized to 0.899783 instead of 1.
func TestNormalizeL2RejectsSubnormalSquaredNorm(t *testing.T) {
	out := make([]float64, 1)

	err := NormalizeL2([]float64{2e-162}, out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "underflows")

	// the first element magnitude whose square is a normal float64 is accepted and normalizes to 1
	require.NoError(t, NormalizeL2([]float64{1.5e-154}, out))
	require.InDelta(t, 1.0, out[0], 1e-12)

	// a float32 base squares into float64, so an extreme float32 element stays in the normal range
	out32 := make([]float32, 2)
	require.NoError(t, NormalizeL2([]float32{1e-30, 0}, out32))
	require.EqualValues(t, 1, out32[0])
}

// The cosine kernels test their own squared norms before using them. A norm must be a NORMAL
// number of the accumulation type, not merely positive and finite: float32 [3e-23,3e-23] squares
// each element into the subnormals, so the accumulated norm keeps only a couple of bits. The
// denominator built from it is still positive and finite, so a zero/Inf/NaN test accepts it and
// the kernel answered 0.433316 where the cosine distance is 0.292893.
func TestCosineRejectsSubnormalNorms(t *testing.T) {
	d32, err := CosineDistance[float32]([]float32{3e-23, 3e-23}, []float32{3e-23, 0})
	require.NoError(t, err)
	require.InDelta(t, 0.2928932, float64(d32), 1e-6)

	// the float64 recompute still answers 0 for a vector against itself, whatever its magnitude
	d32, err = CosineDistance[float32]([]float32{1e-30, 0}, []float32{1e-30, 0})
	require.NoError(t, err)
	require.EqualValues(t, 0, d32)

	// ordinary vectors are untouched by the guard
	d32, err = CosineDistance[float32]([]float32{3, 4}, []float32{4, 3})
	require.NoError(t, err)
	require.InDelta(t, 0.04, float64(d32), 1e-6)
	d64, err := CosineDistance[float64]([]float64{3, 4}, []float64{4, 3})
	require.NoError(t, err)
	require.InDelta(t, 0.04, d64, 1e-12)

	// float64 has no wider accumulator to fall back on, so a subnormal float64 norm is rejected
	_, err = CosineDistance[float64]([]float64{1e-200, 0}, []float64{1e-200, 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "underflows the element domain")
}
