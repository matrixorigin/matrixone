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
