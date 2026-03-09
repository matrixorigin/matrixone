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

package aggexec

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClampVarianceNearZero(t *testing.T) {
	tests := []struct {
		name     string
		variance float64
		part1    float64
		part2    float64
		expect   float64
		isNaN    bool
	}{
		{
			name:     "positive tiny cancellation noise",
			variance: 2.4781112219608194e-17,
			part1:    0.46170282708744254,
			part2:    0.4617028270874425,
			expect:   0,
		},
		{
			name:     "negative tiny cancellation noise",
			variance: -1.9990165263333845e-18,
			part1:    0.056094182825484756,
			part2:    0.05609418282548476,
			expect:   0,
		},
		{
			name:     "keep non-trivial variance",
			variance: 1e-8,
			part1:    1.00000001,
			part2:    1.0,
			expect:   1e-8,
		},
		{
			name:     "nan should pass through",
			variance: math.NaN(),
			part1:    1,
			part2:    1,
			isNaN:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := clampVarianceNearZero(tc.variance, tc.part1, tc.part2)
			if tc.isNaN {
				require.True(t, math.IsNaN(got))
				return
			}
			require.Equal(t, tc.expect, got)
		})
	}
}

func TestGetResultClampsTinyVarianceToZero(t *testing.T) {
	exec := &varStdDevExec[float64, float64]{
		isVar: false,
		isPop: true,
		f2t:   simpleF2T,
	}

	tests := []struct {
		name string
		s    float64
		s2   float64
		cnt  int64
	}{
		{
			name: "positive epsilon variance",
			s:    4.0,
			s2:   4.0 * (1.0 + 1e-16),
			cnt:  4,
		},
		{
			name: "negative epsilon variance",
			s:    4.0,
			s2:   4.0 * (1.0 - 1e-16),
			cnt:  4,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := exec.getResult(tc.s, tc.s2, tc.cnt)
			require.NoError(t, err)
			require.False(t, math.IsNaN(got))
			require.Equal(t, 0.0, got)
		})
	}
}

func TestGetResultKeepsNonTrivialVariance(t *testing.T) {
	exec := &varStdDevExec[float64, float64]{
		isVar: false,
		isPop: true,
		f2t:   simpleF2T,
	}

	got, err := exec.getResult(4.0, 4.0*(1.0+1e-8), 4)
	require.NoError(t, err)
	require.InEpsilon(t, 1e-4, got, 1e-8)
}

func TestGetResultVarClampsTinyVarianceToZero(t *testing.T) {
	exec := &varStdDevExec[float64, float64]{
		isVar: true,
		isPop: true,
		f2t:   simpleF2T,
	}

	got, err := exec.getResult(4.0, 4.0*(1.0-1e-16), 4)
	require.NoError(t, err)
	require.False(t, math.IsNaN(got))
	require.Equal(t, 0.0, got)
}
