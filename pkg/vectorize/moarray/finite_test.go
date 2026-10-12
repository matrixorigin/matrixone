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

package moarray

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// The SQL distance functions reject a result that left the element domain. Finite inputs can still
// produce +Inf (a float32 dot product of 1e20-magnitude vectors) or NaN (that +Inf cancelling a
// -Inf); neither is a distance, and here each distance IS the value the query returns, so it fails
// rather than surfacing the Inf.
//
// The kernels themselves pass such a value through: the scan paths read one distance per candidate,
// where an out-of-domain value can never win a min-comparison and so cannot change a ranking. The
// contract belongs at the boundaries that hand a distance back.
func TestSQLDistancesRejectNonFiniteResults(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func() (float64, error)
	}{
		{"inner_product f32", func() (float64, error) {
			return InnerProduct[float32]([]float32{1e20, 1e20}, []float32{1e20, -1e20})
		}},
		{"inner_product f64", func() (float64, error) {
			return InnerProduct[float64]([]float64{1e200, 1e200}, []float64{1e200, -1e200})
		}},
		{"l2_distance_sq f32", func() (float64, error) {
			return L2DistanceSq[float32]([]float32{0, 0}, []float32{3e19, 3e19})
		}},
		{"l2_distance f32", func() (float64, error) {
			return L2Distance[float32]([]float32{0, 0}, []float32{3e19, 3e19})
		}},
		{"l1_distance f32", func() (float64, error) {
			big := []float32{math.MaxFloat32, math.MaxFloat32, math.MaxFloat32, math.MaxFloat32}
			return L1Distance[float32](big, make([]float32, 4))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.fn()
			require.Error(t, err)
			require.Contains(t, err.Error(), "overflows the element domain")
		})
	}

	// Ordinary vectors are unaffected on every function.
	a := []float32{1, 2, 3}
	b := []float32{4, 6, 3}
	for _, tc := range []struct {
		name string
		fn   func() (float64, error)
		want float64
	}{
		{"l2sq", func() (float64, error) { return L2DistanceSq[float32](a, b) }, 25},
		{"l2", func() (float64, error) { return L2Distance[float32](a, b) }, 5},
		{"l1", func() (float64, error) { return L1Distance[float32](a, b) }, 7},
		{"ip", func() (float64, error) { return InnerProduct[float32](a, b) }, -25},
	} {
		t.Run("ordinary "+tc.name, func(t *testing.T) {
			got, err := tc.fn()
			require.NoError(t, err)
			require.EqualValues(t, tc.want, got)
		})
	}
}

// Summation adds in float64 and rejects a total that left that domain: finite elements can still
// sum past it, and +Inf is not a sum.
func TestSummationRejectsOverflow(t *testing.T) {
	_, err := Summation[float64]([]float64{1e308, 1e308, -1e308, -1e308})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows the element domain")

	// the mathematical total of that input is 0, which is exactly why the +Inf must not be returned
	got, err := Summation[float64]([]float64{1, 2, 3, -4})
	require.NoError(t, err)
	require.EqualValues(t, 2, got)

	got, err = Summation[float32]([]float32{0.5, 0.25})
	require.NoError(t, err)
	require.EqualValues(t, 0.75, got)
}

// #29083: l1_norm/l2_norm are DOUBLE functions. A finite VECF32 whose norm exceeds the float32
// range but fits float64 must return that representable double, not overflow to +Inf and not error --
// the norm is accumulated in float64. VECF64 norms keep their existing (float64-domain) behavior.
func TestF32NormAccumulatesInDouble(t *testing.T) {
	// Norms past the float32 range (max ~3.4e38) but well within float64: finite double, no error.
	got, err := L1Norm[float32]([]float32{2e38, 2e38}) // ~4e38 > float32 max
	require.NoError(t, err)
	require.False(t, math.IsInf(got, 0))
	require.InEpsilon(t, 4e38, got, 1e-6)

	got, err = L2Norm[float32]([]float32{2.5e38, 2.5e38}) // ~3.54e38 > float32 max
	require.NoError(t, err)
	require.False(t, math.IsInf(got, 0))
	require.InEpsilon(t, 2.5e38*math.Sqrt2, got, 1e-6)

	// The reviewer's control: a single 1e20 element has a finite L2 norm (l2_distance to origin
	// separately errors on the float32-squared intermediate; the norm does not share that contract).
	got, err = L2Norm[float32]([]float32{1e20, 0})
	require.NoError(t, err)
	require.InEpsilon(t, 1e20, got, 1e-6)

	// Ordinary VECF32 norms are exact.
	got, err = L2Norm[float32]([]float32{3, 4})
	require.NoError(t, err)
	require.EqualValues(t, 5, got)
	got, err = L1Norm[float32]([]float32{3, -4})
	require.NoError(t, err)
	require.EqualValues(t, 7, got)

	// VECF64 norms are computed in the float64 domain and stay finite here (f64 is unchanged).
	got, err = L2Norm[float64]([]float64{1e300, 1e300})
	require.NoError(t, err)
	require.False(t, math.IsInf(got, 0))
	got, err = L1Norm[float64]([]float64{1e300, 1e300})
	require.NoError(t, err)
	require.False(t, math.IsInf(got, 0))
}

// BenchmarkF32Norm covers the VECF32 norm path the #29083 fix rewrote from a float32 blas
// reduction to a float64 accumulation. The loop is faster at every width measured (largest at
// small dims, where blas32.Nrm2's per-element scaling branch dominates) and never overflows.
func BenchmarkF32Norm(b *testing.B) {
	for _, n := range []int{2, 128, 1024} {
		v := make([]float32, n)
		for i := range v {
			v[i] = float32(i%13) - 6.5
		}
		var sink float64
		b.Run("L1/"+strconv.Itoa(n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sink, _ = L1Norm[float32](v)
			}
		})
		b.Run("L2/"+strconv.Itoa(n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sink, _ = L2Norm[float32](v)
			}
		})
		_ = sink
	}
}
