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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// topKPlan is a Top-K over an indexed vector column, plus whatever consumer the test
// wants above it. Mirrors the plan shapes reported in #25967 / #25974:
//
//	project -> sort(limit)        -> project -> scan   (the shape that always worked)
//	project -> sort -> sort(limit)-> project -> scan   (#25967, outer ORDER BY)
//	project -> join -> [scan, sort(limit) -> project -> scan]  (#25974, join consumer)
type topKPlan struct {
	builder    *QueryBuilder
	ctx        *BindContext
	sortNode   *plan.Node
	sortNodeID int32
	childNode  *plan.Node
}

// newTopKPlan builds sort(limit) -> project -> scan and returns the pieces. projListLen
// controls the child project's width so a test can model the pruned projection that
// `select count(*) from (<top-k>) t` produces.
func newTopKPlan(t *testing.T, limit uint64, projListLen int) topKPlan {
	t.Helper()

	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	ctx := NewBindContext(builder, nil)
	tableDef := newVectorJoinTableDef(true, false)

	scanNode := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    tableDef,
		ObjRef:      &plan.ObjectRef{SchemaName: "db"},
		BindingTags: []int32{builder.genNewBindTag()},
	}
	scanTag := scanNode.BindingTags[0]
	scanNodeID := builder.appendNode(scanNode, ctx)

	distFn := &plan.Function{
		Func: &plan.ObjectRef{ObjName: "l2_distance"},
		Args: []*plan.Expr{
			newVectorJoinColExpr(scanTag, 1, "v", tableDef.Cols[1].Typ),
			newVectorJoinStringLitExpr(),
		},
	}

	projectTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, 0, 2)
	if projListLen > 0 {
		projectList = append(projectList,
			newVectorJoinColExpr(scanTag, 0, "id", tableDef.Cols[0].Typ),
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: distFn}},
		)
		projectList = projectList[:projListLen]
	}
	childNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{scanNodeID},
		ProjectList: projectList,
		BindingTags: []int32{projectTag},
	}
	childNodeID := builder.appendNode(childNode, ctx)

	// ORDER BY references the child project's distance column (ColPos 1), which is the
	// shape a derived table / CTE produces.
	sortNode := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{childNodeID},
		OrderBy: []*plan.OrderBySpec{{
			Expr: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_float64)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: 1}},
			},
			Flag: plan.OrderBySpec_ASC,
		}},
		Limit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: limit},
		}}},
	}
	sortNodeID := builder.appendNode(sortNode, ctx)

	for i := 0; i < 40; i++ {
		builder.ctxByNode = append(builder.ctxByNode, ctx)
	}

	return topKPlan{
		builder:    builder,
		ctx:        ctx,
		sortNode:   sortNode,
		sortNodeID: sortNodeID,
		childNode:  childNode,
	}
}

// TestBuildVectorSortContextFromSort_AnchorsAtTopK: the Top-K is recognised from the SORT
// itself, with no PROJECT above it. This is what lets the rewrite fire when a consumer
// (outer ORDER BY #25967, join #25974) sits between the Top-K and any project — before
// this the context could only be built from a project whose direct child was the sort, so
// both shapes silently fell back to a full scan with an exact sort.
func TestBuildVectorSortContextFromSort_AnchorsAtTopK(t *testing.T) {
	tc := newTopKPlan(t, 10, 2)

	vecCtx := tc.builder.buildVectorSortContextFromSort(tc.sortNode)
	require.NotNil(t, vecCtx)
	require.Nil(t, vecCtx.projNode, "sort-anchored context must not claim a project it does not own")
	require.Equal(t, tc.sortNode, vecCtx.sortNode)
	require.NotNil(t, vecCtx.scanNode)
	require.Equal(t, tc.childNode, vecCtx.childNode)
	require.NotNil(t, vecCtx.distFnExpr)
	require.Equal(t, "l2_distance", vecCtx.distFnExpr.Func.ObjName)
	require.NotNil(t, vecCtx.limit)
}

// TestBuildVectorSortContextFromSort_RejectsNonTopK: a SORT with no limit is a plain
// ordering node — the outer ORDER BY of #25967 — and rewriting it would replace the
// user's ordering with an index scan. Only a limit-carrying sort is a Top-K.
func TestBuildVectorSortContextFromSort_RejectsNonTopK(t *testing.T) {
	tc := newTopKPlan(t, 10, 2)

	outerSort := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{tc.sortNodeID},
		OrderBy:  tc.sortNode.OrderBy,
	}
	require.Nil(t, tc.builder.buildVectorSortContextFromSort(outerSort),
		"a limit-less ordering node is not a Top-K")

	// Guard the other non-Top-K inputs the dispatch can hand this function.
	require.Nil(t, tc.builder.buildVectorSortContextFromSort(nil))
	require.Nil(t, tc.builder.buildVectorSortContextFromSort(tc.childNode), "PROJECT is not a SORT")

	multiKey := &plan.Node{
		NodeType: plan.Node_SORT,
		Children: tc.sortNode.Children,
		OrderBy:  []*plan.OrderBySpec{tc.sortNode.OrderBy[0], tc.sortNode.OrderBy[0]},
		Limit:    tc.sortNode.Limit,
	}
	require.Nil(t, tc.builder.buildVectorSortContextFromSort(multiKey),
		"only a single ORDER BY key is a vector Top-K")
}

// TestBuildVectorSortContextFromSort_PrunedChildProjection: `select count(*) from
// (<top-k>) t` prunes the derived table's projection to nothing while the sort still
// carries the pre-pruning ColPos. Indexing the project list blind panicked with
// "index out of range [N] with length 0"; the builder must decline instead.
func TestBuildVectorSortContextFromSort_PrunedChildProjection(t *testing.T) {
	tc := newTopKPlan(t, 10, 0)
	require.Empty(t, tc.childNode.ProjectList)

	require.NotPanics(t, func() {
		require.Nil(t, tc.builder.buildVectorSortContextFromSort(tc.sortNode))
	})
}

// TestApplyIndicesForSort_YieldsToProjectAnchor: applyIndices walks children before the
// node itself, so for the classic PROJECT -> SORT -> SCAN shape the SORT-anchored entry
// point runs FIRST and would claim a Top-K the project should own. That anchor has no
// project to read column requirements from, so ivfflat disables the index-only scan —
// every plain `ORDER BY dist LIMIT k` silently grows a join back to the base table.
// collectSpecialIndexGuards marks such sorts; applyIndicesForSort must yield to the mark.
func TestApplyIndicesForSort_YieldsToProjectAnchor(t *testing.T) {
	tc := newTopKPlan(t, 10, 2)

	// Unmarked: the sort anchor owns this Top-K (the #25967 / #25974 shapes).
	require.NotNil(t, tc.builder.buildVectorSortContextFromSort(tc.sortNode))

	// Marked by the guard pre-pass: the project above will anchor it, so the sort entry
	// point must decline and hand the node back untouched.
	tc.builder.markProjectAnchoredSort(tc.sortNodeID)
	gotID, err := tc.builder.applyIndicesForSort(tc.sortNodeID, tc.sortNode,
		map[[2]int32]int{}, map[[2]int32]*plan.Expr{})
	require.NoError(t, err)
	require.Equal(t, tc.sortNodeID, gotID, "a project-anchored Top-K must be left for the project")
	require.Equal(t, plan.Node_SORT, tc.sortNode.NodeType, "the sort must not have been rewritten")
	require.NotNil(t, tc.sortNode.Limit, "the Top-K limit must survive untouched")
}

// TestApplyVectorIndexForSortContext_NilIdxColMap: a sort-anchored rewrite has nowhere to
// publish its column remap without idxColMap, so it must decline rather than write to a
// nil map (panic) or splice a tree whose ancestors still point at the replaced project.
// The planner always passes a map, but ApplyForSortOpts.IdxColMap does not require one and
// every unit-test call site currently passes nil.
func TestApplyVectorIndexForSortContext_NilIdxColMap(t *testing.T) {
	tc := newTopKPlan(t, 10, 2)
	vecCtx := tc.builder.buildVectorSortContextFromSort(tc.sortNode)
	require.NotNil(t, vecCtx)
	require.Nil(t, vecCtx.projNode)

	require.NotPanics(t, func() {
		gotID, handled, err := tc.builder.applyVectorIndexForSortContext(tc.sortNodeID, vecCtx, nil, nil)
		require.NoError(t, err)
		require.False(t, handled)
		require.Equal(t, tc.sortNodeID, gotID)
	})
}

// TestSpliceVectorRewrite_AnchorAwareness pins the two splice behaviours the anchors need:
// with a project, the project is repointed and its own id returned so the parent keeps its
// identity; without one, the new subtree root is returned for applyIndices to repoint, and
// the column remap goes into idxColMap so ancestors pick it up on the way out.
func TestSpliceVectorRewrite_AnchorAwareness(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, newVectorJoinMockCtx(), false, true)
	scoreExpr := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 99, ColPos: 1}},
	}
	remap := map[[2]int32]*plan.Expr{{7, 1}: scoreExpr}

	// Project anchor: repoint + apply remap locally, return the original id.
	projNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{11},
		ProjectList: []*plan.Expr{{Typ: scoreExpr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 7, ColPos: 1}}}},
	}
	idxColMap := map[[2]int32]*plan.Expr{}
	got := builder.spliceVectorRewrite(&vectorSortContext{projNode: projNode}, 5, 42, remap, idxColMap)
	require.Equal(t, int32(5), got)
	require.Equal(t, int32(42), projNode.Children[0])
	require.Empty(t, idxColMap, "a project anchor applies the remap itself")
	require.Equal(t, int32(99), projNode.ProjectList[0].GetCol().RelPos)

	// Sort anchor: return the new root and publish the remap for ancestors.
	idxColMap = map[[2]int32]*plan.Expr{}
	got = builder.spliceVectorRewrite(&vectorSortContext{}, 5, 42, remap, idxColMap)
	require.Equal(t, int32(42), got)
	require.Len(t, idxColMap, 1)
	require.Equal(t, scoreExpr, idxColMap[[2]int32{7, 1}])
}

// TestIvfIndexOnlyBoundary pins which projection bounds an index-only scan. Getting this
// wrong is not a missed optimization but a broken plan: the index-only scan drops the base
// table, so a column outside the boundary fails column remap at build time.
func TestIvfIndexOnlyBoundary(t *testing.T) {
	projNode := &plan.Node{NodeType: plan.Node_PROJECT}
	childProj := &plan.Node{NodeType: plan.Node_PROJECT}

	// Project-anchored: the project above the Top-K bounds it, resolved through the child.
	gotProj, gotChild := ivfIndexOnlyBoundary(projNode, childProj)
	require.Equal(t, projNode, gotProj)
	require.Equal(t, childProj, gotChild)

	// Sort-anchored (#25967 / #25974): no project above, so the Top-K's own child project
	// bounds the consumers -- they can only read what the derived table exposes. Its
	// expressions are already in scan terms, so there is no child map to resolve through.
	gotProj, gotChild = ivfIndexOnlyBoundary(nil, childProj)
	require.Equal(t, childProj, gotProj)
	require.Nil(t, gotChild, "the boundary's own expressions must not be remapped through itself")

	// Sort straight on the scan (the ORDER BY expression is the distance call itself):
	// nothing narrows the scan's columns, so index-only must stay off.
	gotProj, gotChild = ivfIndexOnlyBoundary(nil, nil)
	require.Nil(t, gotProj)
	require.Nil(t, gotChild)

	gotProj, _ = ivfIndexOnlyBoundary(nil, &plan.Node{NodeType: plan.Node_AGG})
	require.Nil(t, gotProj, "only a PROJECT bounds the readable columns")
}
