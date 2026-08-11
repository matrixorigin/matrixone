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

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFloatComparatorsOrderNaNDeterministically(t *testing.T) {
	float32Values := []float32{
		math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002),
		float32(math.Inf(-1)), -1, math.Float32frombits(0x80000000), 1, float32(math.Inf(1)),
	}
	assertFloat32ComparatorOrder(t, float32Values)
	require.Zero(t, Float32AscCompare(math.Float32frombits(0x80000000), 0))

	float64Values := []float64{
		math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002),
		math.Inf(-1), -1, math.Float64frombits(0x8000000000000000), 1, math.Inf(1),
	}
	assertFloat64ComparatorOrder(t, float64Values)
	require.Zero(t, Float64AscCompare(math.Float64frombits(0x8000000000000000), 0))
}

func assertFloat32ComparatorOrder(t *testing.T, values []float32) {
	t.Helper()
	for i, x := range values {
		for j, y := range values {
			asc := Float32AscCompare(x, y)
			desc := Float32DescCompare(x, y)
			require.Equal(t, -asc, Float32AscCompare(y, x), "asc pair %d,%d", i, j)
			require.Equal(t, -desc, Float32DescCompare(y, x), "desc pair %d,%d", i, j)
			require.Equal(t, -asc, desc, "direction pair %d,%d", i, j)
			if i < j {
				require.Negative(t, asc, "asc pair %d,%d", i, j)
				require.Positive(t, desc, "desc pair %d,%d", i, j)
			}
		}
	}
}

func assertFloat64ComparatorOrder(t *testing.T, values []float64) {
	t.Helper()
	for i, x := range values {
		for j, y := range values {
			asc := Float64AscCompare(x, y)
			desc := Float64DescCompare(x, y)
			require.Equal(t, -asc, Float64AscCompare(y, x), "asc pair %d,%d", i, j)
			require.Equal(t, -desc, Float64DescCompare(y, x), "desc pair %d,%d", i, j)
			require.Equal(t, -asc, desc, "direction pair %d,%d", i, j)
			if i < j {
				require.Negative(t, asc, "asc pair %d,%d", i, j)
				require.Positive(t, desc, "desc pair %d,%d", i, j)
			}
		}
	}
}
