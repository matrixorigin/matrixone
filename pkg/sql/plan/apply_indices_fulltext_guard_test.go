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

package plan

import (
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// buildWrappedMatchGuardPlan builds `select <match-expr> from ft where base_id = 'b1'`
// over a table carrying BOTH a fulltext index (title, body) and an ordinary index on the
// filtered column. That second index is the whole point: it gives applyIndicesForFilters
// something to rewrite the scan into, so an unprotected scan actually gets taken away.
//
// bare selects a plain MATCH; wrapped selects `round(MATCH(...), <lit>)` -- the same query
// in every other respect.
func buildWrappedMatchGuardPlan(t *testing.T, bare bool) (*QueryBuilder, int32, int32) {
	t.Helper()

	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	projTag := builder.genNewBindTag()

	tableDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	tableDef.Indexes = append(tableDef.Indexes, &planpb.IndexDef{
		IndexName:      "idx_base_id",
		IndexTableName: "__mo_idx_base_id",
		Parts:          []string{"base_id", "id"},
		TableExist:     true,
	})
	registerFullTextJoinRegularIndexTable(builder, "__mo_idx_base_id")

	// Deliberately NO fulltext filter. scanHasMatchedFullTextFilter therefore does not
	// cover this scan, which leaves the project guard as the only thing that can reserve it.
	filters := []*planpb.Expr{
		ftjMakeEqExpr(t, ftjColExpr(tableDef, scanTag, 1), makePlan2StringConstExprWithType("b1", false)),
	}
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, filters), ctx)

	projExpr := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	if !bare {
		projExpr = scoreFn("round", projExpr, scoreLit())
	}
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{scanID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{projExpr},
	}, ctx)

	return builder, scanID, projID
}

// TestFullTextGuardCoversWrappedProjectionMatch pins the guard predicate to the rewrite
// predicate. applyIndicesForProject drives off wrapped MATCHes, so detectFullTextGuard has
// to reserve their scan too -- otherwise the regular-index rule consumes the scan first and
// the MATCH reaches execution unserved (20105).
//
// The bare subtest is the control: it passed before the guard was widened, and the only
// difference between the two is the round() wrapper. Without the widening, `wrapped` builds
// 0 fulltext scans and leaves 1 live MATCH in the plan.
func TestFullTextGuardCoversWrappedProjectionMatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		bare bool
	}{
		{"bare match in projection", true},
		{"wrapped match in projection", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, scanID, projID := buildWrappedMatchGuardPlan(t, tc.bare)
			projNode := builder.qry.Nodes[projID]
			scanNode := builder.qry.Nodes[scanID]

			require.Equal(t, []int32{scanID}, builder.detectFullTextGuard(projNode),
				"the scan the fulltext rewrite will consume must be reserved")

			// Nothing else reserves it: there is no fulltext filter on this scan.
			require.False(t, builder.scanHasMatchedFullTextFilter(scanNode))

			// Same arguments detectFullTextGuard uses: projids marks the bare positions as
			// already served, so only a genuinely wrapped MATCH is reported here.
			projids, _ := builder.getFullTextMatchFromProject(projNode, scanNode)
			wrapped, _ := builder.getWrappedFullTextMatches(projNode, scanNode, nil, projids)
			if tc.bare {
				require.Len(t, projids, 1, "the bare MATCH is served through projids")
				require.Empty(t, wrapped, "and is therefore not reported as wrapped")
			} else {
				require.Empty(t, projids, "a wrapped MATCH is invisible to the bare-projection scan")
				require.Len(t, wrapped, 1, "the rewrite drives off this wrapped MATCH instead")
			}

			builder.prepareSpecialIndexGuards(projID)
			require.True(t, builder.isScanProtected(scanID),
				"an unprotected scan is rewritten by the regular-index rule before the fulltext pass runs")

			newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			builder.qry.Steps = []int32{newID}

			require.Equal(t, 1, countReachableFullTextScans(builder.qry),
				"the MATCH must be served by a fulltext index scan")
			require.Zero(t, countReachableFullTextMatches(builder.qry),
				"a fulltext_match surviving into the executed plan throws 20105")
		})
	}
}
