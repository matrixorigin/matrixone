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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestPaginationCutoffPlacementAdmission(t *testing.T) {
	// Admission requires equal effective NULL placement between the sort and
	// scan specs, mirroring the DESC-bit check. The shared helper resolves a
	// both-bits flag as NULLS FIRST, exactly like the Order comparator, so
	// the matrix below includes that combination on both sides.
	for _, tc := range []struct {
		name      string
		sortFlag  plan.OrderBySpec_OrderByFlag
		scanFlag  plan.OrderBySpec_OrderByFlag
		wantLeadA bool
		wantLeadB bool
	}{
		{name: "default asc equals nulls first", sortFlag: plan.OrderBySpec_NULLS_FIRST, scanFlag: plan.OrderBySpec_ASC, wantLeadA: true, wantLeadB: true},
		{name: "nulls last differs from asc default", sortFlag: plan.OrderBySpec_NULLS_LAST, scanFlag: plan.OrderBySpec_ASC, wantLeadA: false, wantLeadB: true},
		{name: "nulls last under desc redundant", sortFlag: plan.OrderBySpec_NULLS_LAST | plan.OrderBySpec_DESC, scanFlag: plan.OrderBySpec_DESC, wantLeadA: false, wantLeadB: false},
		{name: "nulls first under desc differs", sortFlag: plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_DESC, scanFlag: plan.OrderBySpec_DESC, wantLeadA: true, wantLeadB: false},
		{
			name:      "both bits resolve as nulls first",
			sortFlag:  plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_NULLS_LAST,
			scanFlag:  plan.OrderBySpec_ASC,
			wantLeadA: true, wantLeadB: true,
		},
		{
			name:      "both bits with desc resolve as nulls first",
			sortFlag:  plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_NULLS_LAST | plan.OrderBySpec_DESC,
			scanFlag:  plan.OrderBySpec_NULLS_FIRST | plan.OrderBySpec_DESC,
			wantLeadA: true, wantLeadB: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantLeadA, readutil.OrderHeadNullsLead(tc.sortFlag))
			require.Equal(t, tc.wantLeadB, readutil.OrderHeadNullsLead(tc.scanFlag))
			c, scan, sort := scanCutoffTestPlan()
			sort.OrderBy[0].Flag = tc.sortFlag
			scan.OrderBy[0].Flag = tc.scanFlag
			got, tag := c.paginationCutoffScan(sort)
			if tc.wantLeadA == tc.wantLeadB {
				require.Same(t, scan, got, "placement-matched specs admit the cutoff")
				require.Equal(t, int32(42), tag)
			} else {
				require.Nil(t, got, "placement-mismatched specs fail closed")
				require.Zero(t, tag)
			}
		})
	}
}
