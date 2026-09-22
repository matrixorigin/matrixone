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

// The rule is metric-independent: usearch and cuVS accumulate in float32, so a stored vector whose
// squared norm leaves that domain is mis-scored on every metric, not just cosine.
func TestCheckIndexableVector(t *testing.T) {
	t.Run("rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			v    []float32
		}{
			{"squared norm underflows", []float32{1e-30, 0, 0}},
			{"squared norm overflows", []float32{1e20, 0, 0}},
			{"overflow across elements", []float32{2e19, 2e19, 2e19}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := CheckIndexableVector(tc.v)
				require.Error(t, err)
				require.Contains(t, err.Error(), "leaves the float32 domain")
			})
		}
	})

	t.Run("accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			v    []float32
		}{
			{"ordinary", []float32{1, 2, 3}},
			{"unit", []float32{1, 0, 0}},
			// An all-zero vector is legal data: l2sq to it is the other vector's squared norm and
			// cosine_distance returns 1 by convention.
			{"all zero", []float32{0, 0, 0}},
			{"smallest usable", []float32{float32(math.Sqrt(smallestNormalFloat32)), 0, 0}},
			{"largest usable", []float32{1e19, 0, 0}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				require.NoError(t, CheckIndexableVector(tc.v))
			})
		}
	})

	// float32 elements only. A float64 base is not tested here, and a narrower quantization has a
	// narrower bound this does not cover: under F16 a squared norm of 4.9e9 is an ordinary float32
	// yet stores as +Inf. Neither must be assumed safe because this returned nil.
	require.NoError(t, CheckIndexableVector([]float64{1e200, 0, 0}))
	require.NoError(t, CheckIndexableVector([]float64{3, 4}))

	// the unrolled accumulation agrees with a plain sum at every length across the 4-wide body
	for n := 1; n <= 13; n++ {
		v := make([]float32, n)
		for i := range v {
			v[i] = float32(i + 1)
		}
		require.NoError(t, CheckIndexableVector(v), "n=%d", n)

		v[n-1] = 1e20 // tail element alone pushes the squared norm out of range
		require.Error(t, CheckIndexableVector(v), "n=%d", n)

		zeros := make([]float32, n)
		require.NoError(t, CheckIndexableVector(zeros), "n=%d all zero", n)

		// a single non-zero subnormal-squared element anywhere is still rejected
		for pos := 0; pos < n; pos++ {
			tiny := make([]float32, n)
			tiny[pos] = 1e-30
			require.Error(t, CheckIndexableVector(tiny), "n=%d pos=%d", n, pos)
		}
	}
}
