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

// #29079: a top-level indexed MATCH is rejected (20105) when the query block also has a
// NOT EXISTS (ANTI join) or correlated EXISTS (MARK join). The MATCH sits on the PRESERVED
// (driving) side of that join -- the outer WHERE's pure filter on the fulltext relation -- so
// driving it is row-equivalent. These pin the ANTI/MARK preserved-child relaxation; the last one
// pins the safety invariant that the PROBE side is never driven.

// TestFullTextNotExistsAntiJoinPreservedChild: `docs WHERE match(...) AND NOT EXISTS(q)` flattens
// to docs(match) ANTI JOIN q, with the match on the preserved child 0.
func TestFullTextNotExistsAntiJoinPreservedChild(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)
	qDef := makeFullTextJoinTestTableDef("q", false)
	qTag := builder.genNewBindTag()
	qScanID := builder.appendNode(makeJoinIndexTestScan(qDef, qTag), ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_ANTI, // docs(match) preserved at child 0; q is the probe at child 1
		Children: []int32{matchScanID, qScanID},
	}, ctx)
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{projTag},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"NOT EXISTS must not leave the MATCH unrewritten (#29079)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the ANTI join's preserved child must be served by a fulltext index scan")
}

// TestFullTextCorrelatedExistsMarkJoinPreservedChild: `docs WHERE match(...) AND EXISTS(q WHERE
// q.n < docs.id)` flattens to docs(match) MARK JOIN q, with the match on the preserved child 0.
func TestFullTextCorrelatedExistsMarkJoinPreservedChild(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)
	qDef := makeFullTextJoinTestTableDef("q", false)
	qTag := builder.genNewBindTag()
	qScanID := builder.appendNode(makeJoinIndexTestScan(qDef, qTag), ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_MARK, // docs(match) preserved/marked at child 0; q is the probe at child 1
		Children: []int32{matchScanID, qScanID},
	}, ctx)
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{projTag},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"correlated EXISTS must not leave the MATCH unrewritten (#29079)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the MARK join's preserved child must be served by a fulltext index scan")
}

// TestFullTextAntiRightSwappedPreservesChild1: an ANTI join right-swapped by swapJoinChildren
// (IsRightJoin=true) moves the preserved side to child 1; eligibility must follow IsRightJoin, not
// a hard-coded index.
func TestFullTextAntiRightSwappedPreservesChild1(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	qDef := makeFullTextJoinTestTableDef("q", false)
	qTag := builder.genNewBindTag()
	qScanID := builder.appendNode(makeJoinIndexTestScan(qDef, qTag), ctx)
	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_JOIN,
		JoinType:    planpb.Node_ANTI,
		IsRightJoin: true, // right-swapped: preserved docs(match) is now child 1, probe q at child 0
		Children:    []int32{qScanID, matchScanID},
	}, ctx)
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{projTag},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"right-swapped ANTI must serve the MATCH on its preserved child 1 (#29079)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the right-swapped ANTI's preserved child 1 must be rewritten to a fulltext index scan")
}

// TestFullTextAntiProbeSideNotDriven pins the safety invariant of the join-children path: it drives
// only the preserved side of an ANTI/MARK join, never the probe side (filtering the anti/mark probe
// is not a pure filter under null-aware NOT IN / three-valued MARK). Here a bare match scan sits as
// the ANTI probe child, so the join-children rewrite leaves it untouched. A real subquery
// (`NOT EXISTS(SELECT 1 FROM ftdocs WHERE MATCH ...)`) carries its own PROJECT above the scan and is
// served by that subquery's own scan-level rewrite instead -- this test guards against a future
// change that naively marks both ANTI/MARK children eligible.
func TestFullTextAntiProbeSideNotDriven(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	outerDef := makeFullTextJoinTestTableDef("outer", false)
	outerTag := builder.genNewBindTag()
	outerScanID := builder.appendNode(makeJoinIndexTestScan(outerDef, outerTag), ctx)
	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)

	joinID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_JOIN,
		JoinType: planpb.Node_ANTI, // outer preserved at child 0; match scan is the PROBE at child 1
		Children: []int32{outerScanID, matchScanID},
	}, ctx)
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{joinID}, BindingTags: []int32{projTag},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Equal(t, 1, countReachableFullTextMatches(builder.qry),
		"a MATCH on the ANTI probe side must be left unrewritten (driving it is not a pure filter)")
	require.Zero(t, countReachableFullTextScans(builder.qry),
		"the probe-side MATCH must not be served by a fulltext index scan")
}
