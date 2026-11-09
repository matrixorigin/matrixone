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

package index

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/stretchr/testify/require"
)

// A known exclusion may prune a block. Unknown comparisons may not prune,
// and may not manufacture a positive hit for a physical-cost lower bound.
func TestZoneMapMatchThreeValuedLogic(t *testing.T) {
	unknown := ZoneMapMatch{}
	no := ZoneMapMatch{Known: true}
	yes := ZoneMapMatch{Match: true, Known: true}
	cases := []struct {
		left, right, and, or ZoneMapMatch
	}{
		{unknown, unknown, unknown, unknown},
		{unknown, no, no, unknown},
		{unknown, yes, unknown, yes},
		{no, unknown, no, unknown},
		{no, no, no, no},
		{no, yes, no, yes},
		{yes, unknown, unknown, yes},
		{yes, no, no, yes},
		{yes, yes, yes, yes},
	}
	for _, tc := range cases {
		require.Equal(t, tc.and, tc.left.And(tc.right))
		require.Equal(t, tc.or, tc.left.Or(tc.right))
	}
	require.True(t, unknown.MayMatch())
	require.False(t, unknown.Excludes())
	require.False(t, no.MayMatch())
	require.True(t, no.Excludes())
	require.True(t, yes.MayMatch())
	require.False(t, yes.Excludes())
}

// Independent scalar predicates establish exact block existence in the ordinary
// domain; uninitialized or differently typed metadata must remain unknown.
func TestScalarZoneMapMatcherOrdinary(t *testing.T) {
	values := []int64{-5, 0, 9}
	for _, decimal := range []bool{false, true} {
		kind, scale := types.T_int64, int32(0)
		if decimal {
			kind, scale = types.T_decimal64, 2
		}
		zm := NewZM(kind, scale)
		for _, value := range values {
			encoded := value
			if decimal {
				encoded *= 100
			}
			UpdateZM(zm, types.EncodeInt64(&encoded))
		}
		for _, bound := range []int64{-6, -5, 0, 9, 10} {
			for op := types.ComparisonEqual; op <= types.ComparisonGreaterEqual; op++ {
				// Decimal bounds deliberately use a different scale.
				value, boundScale := bound, int32(0)
				if decimal {
					value *= 10
					boundScale = 1
				}
				match, _, ok := MakeScalarZoneMapMatcher(types.New(kind, 0, scale), types.EncodeInt64(&value), types.New(kind, 0, boundScale), 0, nil, op)
				require.True(t, ok)
				want := false
				for _, row := range values {
					switch op {
					case types.ComparisonEqual:
						want = want || row == bound
					case types.ComparisonLess:
						want = want || row < bound
					case types.ComparisonLessEqual:
						want = want || row <= bound
					case types.ComparisonGreater:
						want = want || row > bound
					case types.ComparisonGreaterEqual:
						want = want || row >= bound
					}
				}
				got := match(zm)
				require.True(t, got.Known)
				require.Equal(t, want, got.Match, "decimal=%v bound=%d op=%s", decimal, bound, op)
				require.False(t, match(NewZM(kind, scale)).Known)
				require.False(t, match(NewZM(types.T_varchar, 0)).Known)
			}
		}
	}
}
