// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestD256DivBoundedIntermediateThroughD128(t *testing.T) {
	coefficient, err := types.ParseDecimal256("1e37", 65, 0)
	require.NoError(t, err)
	want, err := types.ParseDecimal256("1e37", 65, 6)
	require.NoError(t, err)
	for _, negativeX := range []bool{false, true} {
		for _, negativeY := range []bool{false, true} {
			x, y, expected := coefficient, coefficient, want
			if negativeX {
				x = x.Minus()
			}
			if negativeY {
				y = y.Minus()
			}
			if negativeX != negativeY {
				expected = expected.Minus()
			}
			for _, lengths := range [][2]int{{3, 3}, {1, 3}, {3, 1}} {
				xs, ys := make([]types.Decimal256, lengths[0]), make([]types.Decimal256, lengths[1])
				for i := range xs {
					xs[i] = x
				}
				for i := range ys {
					ys[i] = y
				}
				for _, masked := range []bool{false, true} {
					result := make([]types.Decimal256, 3)
					ns := new(nulls.Nulls)
					if masked {
						ns.Add(2)
					}
					require.NoError(t, d256Div(xs, ys, result, 0, 37, ns, true))
					require.Equal(t, expected, result[0])
					require.Equal(t, expected, result[1])
					if masked {
						require.True(t, ns.Contains(2))
					} else {
						require.Equal(t, expected, result[2])
					}
				}
			}
		}
	}
	// A genuinely unrepresentable result still fails after the wider fallback.
	var dst types.Decimal256
	require.Error(t, d128DivOneToD256(types.Decimal128{B0_63: 1}, types.Decimal128{B0_63: 1}, &dst, 82, new(nulls.Nulls), 0, true, 0, 76))
}
