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

// #27962: a MATCH inside a scalar subquery decorrelates to a plan shape the
// project-anchored fulltext rewrite never reaches, leaving the MATCH un-served
// (error 20105). These tests pin both flattened shapes to the rewrite.

// matchScanWithFulltextIndex builds a TABLE_SCAN over a fulltext-indexed table
// whose FilterList carries a bare MATCH -- i.e. `... where match(title,body)
// against('hello')` on the subquery's own scan.
func matchScanWithFulltextIndex(builder *QueryBuilder, ctx *BindContext) (int32, *planpb.Node) {
	tableDef := makeFullTextJoinTestTableDef("ft", true)
	// The rewrite resolves the fulltext index table from catalog metadata, so it
	// must be registered in the mock context (mirrors the guard tests).
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	scanTag := builder.genNewBindTag()
	filters := []*planpb.Expr{makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})}
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, filters), ctx)
	return scanID, builder.qry.Nodes[scanID]
}

// TestFullTextUncorrelatedScalarSubqueryAggAnchor pins the Node_AGG anchor:
// an uncorrelated scalar subquery flattens to JOIN(LEFT) -> AGG -> SCAN(match).
// The AGG sits under a 2-input JOIN, so the project-anchored path stops short of
// it; only the standalone AGG anchor serves the MATCH.
func TestFullTextUncorrelatedScalarSubqueryAggAnchor(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)
	aggID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG,
		Children: []int32{matchScanID},
	}, ctx)

	// Outer side of the scalar-subquery join (the `*VALUES*` row in the real plan).
	outerDef := makeFullTextJoinTestTableDef("outer", false)
	outerTag := builder.genNewBindTag()
	outerScanID := builder.appendNode(makeJoinIndexTestScan(outerDef, outerTag), ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_LEFT,
		Children: []int32{outerScanID, aggID},
	}, ctx)
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{joinID},
		BindingTags: []int32{projTag},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a fulltext_match surviving into the plan throws 20105 (#27962 uncorrelated)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the AGG anchor must serve the MATCH via a fulltext index scan")
}

// TestFullTextCorrelatedScalarSubqueryJoinChild pins the LEFT/SINGLE join-child
// relaxation: a correlated scalar subquery flattens to AGG -> JOIN(LEFT) ->
// [outer, SCAN(match)]. The MATCH scan is the inner child of a LEFT join, which
// applyFullTextFiltersForJoinChildren previously skipped (INNER/SEMI only).
func TestFullTextCorrelatedScalarSubqueryJoinChild(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	outerDef := makeFullTextJoinTestTableDef("outer", false)
	outerTag := builder.genNewBindTag()
	outerScanID := builder.appendNode(makeJoinIndexTestScan(outerDef, outerTag), ctx)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_LEFT, // outer(left) preserved; match scan is the inner(right) child
		Children: []int32{outerScanID, matchScanID},
	}, ctx)
	aggID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG,
		Children: []int32{joinID},
	}, ctx)

	newID, err := builder.applyIndices(aggID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a fulltext_match surviving into the plan throws 20105 (#27962 correlated)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the LEFT join-child rewrite must serve the MATCH via a fulltext index scan")
}

// applyIndices runs AFTER swapJoinChildren, which can physically swap the children and
// convert LEFT->RIGHT (or right-swap SINGLE) based on input-size statistics. The two tests
// below pin those EXACT post-swap shapes so eligibility must come from the null-extension
// contract (nodeNullExtendsChild), not a hard-coded child index -- otherwise the same
// #27962 query leaves the match unrewritten and fails with 20105 (#27952). The MATCH scan
// is placed at child 0 (the non-preserved side after the swap) in both.

// TestFullTextCorrelatedScalarSubqueryLeftToRightSwapped: LEFT physically swapped to RIGHT
// (smaller outer input becomes the RIGHT build side). JoinType=RIGHT, MATCH scan at child 0
// (the null-extending side for a RIGHT join).
func TestFullTextCorrelatedScalarSubqueryLeftToRightSwapped(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)
	outerDef := makeFullTextJoinTestTableDef("outer", false)
	outerTag := builder.genNewBindTag()
	outerScanID := builder.appendNode(makeJoinIndexTestScan(outerDef, outerTag), ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_RIGHT, // LEFT swapped to RIGHT: non-preserved (match) is now child 0
		Children: []int32{matchScanID, outerScanID},
	}, ctx)
	aggID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG,
		Children: []int32{joinID},
	}, ctx)

	newID, err := builder.applyIndices(aggID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"LEFT->RIGHT swap must still serve the MATCH (was 20105 with a hard-coded child index)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the RIGHT join's null-extending child 0 must be rewritten to a fulltext index scan")
}

// TestFullTextCorrelatedScalarSubqueryRightSwappedSingle: SINGLE physically swapped
// (IsRightJoin=true) without changing JoinType. MATCH scan at child 0 (the null-extending
// side for a right-swapped SINGLE); the old hard-coded "child 1 only" inspected the wrong
// relation and left the match unrewritten.
func TestFullTextCorrelatedScalarSubqueryRightSwappedSingle(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)
	outerDef := makeFullTextJoinTestTableDef("outer", false)
	outerTag := builder.genNewBindTag()
	outerScanID := builder.appendNode(makeJoinIndexTestScan(outerDef, outerTag), ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_JOIN,
		JoinType:    planpb.Node_SINGLE,
		IsRightJoin: true, // right-swapped: non-preserved (match) is now child 0
		Children:    []int32{matchScanID, outerScanID},
	}, ctx)
	aggID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG,
		Children: []int32{joinID},
	}, ctx)

	newID, err := builder.applyIndices(aggID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"right-swapped SINGLE must still serve the MATCH (was 20105 inspecting the wrong child)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the right-swapped SINGLE's null-extending child 0 must be rewritten to a fulltext index scan")
}
