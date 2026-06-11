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

package table_function

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrainInt8MinMax(t *testing.T) {
	// empty -> (-1, 1)
	lo, hi := trainInt8MinMax([][]float32{})
	require.Equal(t, -1.0, lo)
	require.Equal(t, 1.0, hi)

	// uniform 0..999: P0.1 near 0, P99.9 near 999
	d := make([][]float32, 1)
	d[0] = make([]float32, 1000)
	for i := range d[0] {
		d[0][i] = float32(i)
	}
	lo, hi = trainInt8MinMax(d)
	require.InDelta(t, 0.0, lo, 2)
	require.InDelta(t, 999.0, hi, 2)

	// degenerate (all equal) -> (v, v+1) so the range is never zero.
	lo, hi = trainInt8MinMax([][]float32{{5, 5, 5, 5}})
	require.Equal(t, 5.0, lo)
	require.Equal(t, 6.0, hi)

	// a single extreme outlier is clipped by the P99.9 percentile.
	o := make([][]float32, 1)
	o[0] = make([]float32, 1000)
	for i := 0; i < 999; i++ {
		o[0][i] = 1.0
	}
	o[0][999] = 1e6
	_, hi = trainInt8MinMax(o)
	require.Less(t, hi, 1e6)

	// works on float64 too (f64-base quantization): bounds are sane and ordered
	// (exact percentiles of a 4-element array are not the raw min/max).
	lo64, hi64 := trainInt8MinMax([][]float64{{-3, -1, 1, 3}})
	require.GreaterOrEqual(t, lo64, -3.0)
	require.LessOrEqual(t, hi64, 3.0)
	require.Less(t, lo64, hi64)
}
