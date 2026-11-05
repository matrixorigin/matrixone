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
