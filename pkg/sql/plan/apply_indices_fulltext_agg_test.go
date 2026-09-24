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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// ftAggFn wraps args in an aggregate function expression (max(...), min(...), ...) so a MATCH can be
// placed INSIDE an aggregate the way the binder builds `max(match(...))`.
func ftAggFn(name string, args ...*planpb.Expr) *planpb.Expr {
	ftyp := types.T_float32.ToType()
	return &planpb.Expr{
		Typ: makePlan2Type(&ftyp),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: args,
		}},
	}
}

// TestFullTextAggMatchRewrittenToScore pins the #28681 fix: a MATCH that appears INSIDE an aggregate
// expression must be rewritten to the score column the fulltext index scan produces. Before the fix
// applyIndicesForAggUsingFullTextIndex reparented the aggregate's child to the index-scan join but
// left the raw fulltext_match in AggList, so it survived into the plan and threw error 20105 at
// execution. The plan shape is PROJECT -> AGG(AggList holds max/min(match)) -> SCAN(match filter);
// after applyIndices no fulltext_match may remain anywhere in the plan, and the index scan must exist.
func TestFullTextAggMatchRewrittenToScore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		aggList func(match func() *planpb.Expr) []*planpb.Expr
		groupBy func(match func() *planpb.Expr) []*planpb.Expr
	}{
		{
			// max(match(...)) -- the headline repro.
			name: "max_of_match",
			aggList: func(match func() *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{ftAggFn("max", match())}
			},
		},
		{
			// Two aggregates each holding the same served MATCH: proves the rewrite covers EVERY
			// AggList entry, not just the first.
			name: "multiple_aggregates",
			aggList: func(match func() *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{ftAggFn("max", match()), ftAggFn("min", match())}
			},
		},
		{
			// GROUP BY match(...): the served MATCH lands in aggNode.GroupBy, a sibling of the
			// AggList repro. It must be rewritten too, else it errors 20105 in the GROUP BY clause.
			name: "group_by_match",
			aggList: func(match func() *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{ftAggFn("count", match())}
			},
			groupBy: func(match func() *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{match()}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
			ctx := NewBindContext(builder, nil)

			matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
			scanTag := scanNode.BindingTags[0]
			// The aggregate references the SAME MATCH as the scan filter (pattern/mode/cols), so the
			// index scan built for the filter serves it.
			match := func() *planpb.Expr {
				return makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3})
			}

			var groupBy []*planpb.Expr
			if tc.groupBy != nil {
				groupBy = tc.groupBy(match)
			}
			aggTag := builder.genNewBindTag()
			aggID := builder.appendNode(&planpb.Node{
				NodeType:    planpb.Node_AGG,
				Children:    []int32{matchScanID},
				AggList:     tc.aggList(match),
				GroupBy:     groupBy,
				BindingTags: []int32{aggTag},
			}, ctx)

			ftyp := types.T_float32.ToType()
			projTag := builder.genNewBindTag()
			projID := builder.appendNode(&planpb.Node{
				NodeType:    planpb.Node_PROJECT,
				Children:    []int32{aggID},
				BindingTags: []int32{projTag},
				ProjectList: []*planpb.Expr{{
					Typ:  makePlan2Type(&ftyp),
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: 0}},
				}},
			}, ctx)

			newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			builder.qry.Steps = []int32{newID}

			require.Zero(t, countReachableFullTextMatches(builder.qry),
				"a fulltext_match left inside an aggregate throws 20105 at execution (#28681)")
			require.Equal(t, 1, countReachableFullTextScans(builder.qry),
				"the aggregate's MATCH must be served by a fulltext index scan")
		})
	}
}

// A WINDOW between the query block and the base scan -- SELECT ..., ROW_NUMBER() OVER (...) FROM t
// WHERE MATCH(...) -- hides the scan's WHERE-clause fulltext_match from the PROJECT-anchored rewrite
// (which hops only SORT/AGG), so the raw fulltext_match survives to execution as error 20105
// (#28974). Plan shape PROJECT -> WINDOW -> SCAN(match filter); after applyIndices no fulltext_match
// may remain anywhere and the index scan must exist.
func TestFullTextWindowMatchRewritten(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, _ := matchScanWithFulltextIndex(builder, ctx)

	winTag := builder.genNewBindTag()
	winID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{matchScanID},
		BindingTags: []int32{winTag},
	}, ctx)

	ityp := types.T_int64.ToType()
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{winID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a fulltext_match beneath a WINDOW throws 20105 at execution (#28974)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the WHERE MATCH beneath the WINDOW must be served by a fulltext index scan")
}

// A predicate that survives predicate-pushdown onto the WINDOW because it also references a window
// column lands on windowNode.FilterList and is evaluated AFTER the window (Node_WINDOW runs
// compileRestrict on it). Predicate pushdown expands a projected `score` alias back to the raw
// fulltext_match, so `rn = 1 OR score > 0` carries a served MATCH in that post-window filter. The
// window anchor must rewrite that copy too, or the raw fulltext_match reaches execution as 20105
// (#28974 P2). Plan shape PROJECT -> WINDOW(FilterList: OR(served MATCH > 0, winCol = 1)) ->
// SCAN(match filter): after applyIndices no fulltext_match may remain, and exactly one index scan
// serves both the scan's WHERE MATCH and the post-window copy.
func TestFullTextWindowFilterListMatchRewritten(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]

	// The SAME MATCH the scan's WHERE clause carries, as it reappears (via score pushdown) in the
	// post-window filter: OR(match('hello') > 0, winCol = 1). A projected score is float, so the
	// comparison copy is float-typed (the rewrite matches on the fulltext_match function, not the
	// wrapper type).
	match := makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3})
	match.Typ = planpb.Type{Id: int32(types.T_float32)}
	matchGt0, err := BindFuncExprImplByPlanExpr(context.Background(), ">",
		[]*planpb.Expr{match, makePlan2Float64ConstExprWithType(0)})
	require.NoError(t, err)

	winTag := builder.genNewBindTag()
	ityp := types.T_int64.ToType()
	winCol := &planpb.Expr{Typ: makePlan2Type(&ityp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}}}
	rnEq1, err := BindFuncExprImplByPlanExpr(context.Background(), "=",
		[]*planpb.Expr{winCol, makePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	orExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "or", []*planpb.Expr{matchGt0, rnEq1})
	require.NoError(t, err)

	winID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{matchScanID},
		FilterList:  []*planpb.Expr{orExpr},
		BindingTags: []int32{winTag},
	}, ctx)

	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{winID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a served fulltext_match left in WINDOW.FilterList reaches execution as 20105 (#28974 P2)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"one index scan serves both the WHERE MATCH and its post-window filter copy")
}

// TestFullTextOuterFilterAboveWindowMatchRewritten pins the #28974 P2 follow-up: an independent
// FILTER above a WINDOW can carry a served fulltext_match that predicate pushdown could not move
// below the window (it references neither a window column -- which would land it in WINDOW.FilterList
// -- nor a partition key), e.g. an outer `where score > 0` inlined to `fulltext_match(...) > 0`. The
// WINDOW anchor serves the score below during child recursion; the FILTER anchor must rewrite this
// independent copy too, preserving its post-window position, or the raw fulltext_match reaches
// execution as 20105. Plan shape PROJECT -> FILTER(match > 0) -> WINDOW -> SCAN(match filter): after
// applyIndices no fulltext_match may remain, and exactly one index scan serves both copies.
func TestFullTextOuterFilterAboveWindowMatchRewritten(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]

	ityp := types.T_int64.ToType()
	winTag := builder.genNewBindTag()
	winID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{matchScanID},
		BindingTags: []int32{winTag},
	}, ctx)

	// The SAME served MATCH as the scan's WHERE clause, in an independent FILTER above the window --
	// the shape `where score > 0` leaves when it cannot push below the window.
	match := makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3})
	match.Typ = planpb.Type{Id: int32(types.T_float32)}
	matchGt0, err := BindFuncExprImplByPlanExpr(context.Background(), ">",
		[]*planpb.Expr{match, makePlan2Float64ConstExprWithType(0)})
	require.NoError(t, err)
	filterID := builder.appendNode(&planpb.Node{
		NodeType:   planpb.Node_FILTER,
		Children:   []int32{winID},
		FilterList: []*planpb.Expr{matchGt0},
	}, ctx)

	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{filterID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a served fulltext_match left in a FILTER above the WINDOW reaches execution as 20105 (#28974 P2)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"one index scan serves both the WHERE MATCH and the outer-filter copy")
}

// ftWindowSpecWithMatch builds a WINDOW node's WinSpecList entry (an Expr_W) whose OVER order-by
// references match, the way the binder builds ROW_NUMBER() OVER (ORDER BY MATCH(...)).
func ftWindowSpecWithMatch(match *planpb.Expr) *planpb.Expr {
	ityp := types.T_int64.ToType()
	return &planpb.Expr{
		Typ: makePlan2Type(&ityp),
		Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "rank"}}}},
			OrderBy:    []*planpb.OrderBySpec{{Expr: match}},
		}},
	}
}

// OVER (PARTITION BY col ORDER BY id) makes the binder place a Node_PARTITION between the WINDOW
// and the base scan (appendWindowNode). The WINDOW fulltext anchor must descend that partition to
// reach the scan's WHERE MATCH and reparent BELOW it, or the partition is dropped and the raw
// fulltext_match survives to execution as 20105 (#28974 P2). Plan shape
// PROJECT -> WINDOW -> PARTITION -> SCAN(match filter): after applyIndices no fulltext_match may
// remain, the index scan must exist, and the PARTITION node must still be in the plan.
func TestFullTextWindowPartitionMatchRewritten(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]

	winTag := builder.genNewBindTag()
	partID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PARTITION,
		Children:    []int32{matchScanID},
		OrderBy:     []*planpb.OrderBySpec{{Expr: ftjColExpr(scanNode.TableDef, scanTag, 1), Flag: planpb.OrderBySpec_INTERNAL}},
		BindingTags: []int32{winTag},
	}, ctx)
	winID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{partID},
		BindingTags: []int32{winTag},
	}, ctx)

	ityp := types.T_int64.ToType()
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{winID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a fulltext_match beneath a WINDOW -> PARTITION throws 20105 at execution (#28974)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the WHERE MATCH beneath the partitioned WINDOW must be served by a fulltext index scan")
	require.True(t, planHasReachableNodeType(builder.qry, newID, planpb.Node_PARTITION),
		"the PARTITION node must survive the rewrite -- reparenting the window directly would drop it")
}

// A MATCH projected in the SELECT list above a WINDOW is served by the same scan the window's WHERE
// MATCH drives, but only if the window anchor publishes its served scores so the PROJECT anchor's
// resolveProjectMatchesOverJoin can rewrite the projection. Without that publication the projected
// fulltext_match survives to 20105 despite the index scan existing (#28974 P2). Plan shape
// PROJECT(projects match) -> WINDOW -> SCAN(match filter).
func TestFullTextWindowProjectedMatchPropagated(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]

	winTag := builder.genNewBindTag()
	winID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{matchScanID},
		BindingTags: []int32{winTag},
	}, ctx)

	ftyp := types.T_float32.ToType()
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{winID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{
			makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3}),
			{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}}},
		},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a projected fulltext_match over a WINDOW must be rewritten to the served score (#28974 P2)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the projected MATCH must resolve to the WHERE MATCH's fulltext index scan")
}

// Two window functions each referencing the served MATCH in their OVER clause build stacked WINDOW
// nodes (WINDOW_outer -> WINDOW_inner -> SCAN). Post-order rewrites the inner window (which serves
// the scan's WHERE MATCH); the outer window's own spec must then be resolved against the published
// served scores, or its fulltext_match survives to 20105 (#28974 P2 subsequent windows).
func TestFullTextWindowStackedSpecMatchPropagated(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]
	match := func() *planpb.Expr {
		return makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3})
	}

	winTag := builder.genNewBindTag()
	innerWinID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{matchScanID},
		WinSpecList: []*planpb.Expr{ftWindowSpecWithMatch(match())},
		BindingTags: []int32{winTag},
	}, ctx)
	outerWinID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_WINDOW,
		Children:    []int32{innerWinID},
		WinSpecList: []*planpb.Expr{ftWindowSpecWithMatch(match())},
		BindingTags: []int32{winTag},
	}, ctx)

	ityp := types.T_int64.ToType()
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{outerWinID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"a MATCH in a stacked outer window's spec must be rewritten to the served score (#28974 P2)")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"both windows' MATCHes must resolve to a single WHERE-MATCH fulltext index scan")
}

// replaceScoreFnInExprBy must rewrite a served MATCH wherever it sits, including inside a window
// spec (window function argument, PARTITION BY, ORDER BY) reached from the WINDOW fulltext anchor
// (#28974). Covers every traversal branch: Expr_F (direct hit + recurse into args), Expr_List, and
// Expr_W, plus the nil guard.
func TestReplaceScoreFnInExprByTraversesWindowSpec(t *testing.T) {
	sentinel := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 9, ColPos: 9}}}
	match := func() *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "fulltext_match"}}}}
	}
	rewrite := func(fn *planpb.Function) *planpb.Expr {
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "fulltext_match" {
			return sentinel
		}
		return nil
	}

	require.Nil(t, replaceScoreFnInExprBy(nil, rewrite))

	// Expr_F: a direct hit is replaced; a non-matching function recurses into its args.
	require.Same(t, sentinel, replaceScoreFnInExprBy(match(), rewrite))
	fn := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "gt"}, Args: []*planpb.Expr{match()}}}}
	require.Same(t, sentinel, replaceScoreFnInExprBy(fn, rewrite).GetF().Args[0])

	// Expr_List recurses into every element.
	lst := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{match()}}}}
	require.Same(t, sentinel, replaceScoreFnInExprBy(lst, rewrite).GetList().List[0])

	// Expr_W recurses into the window function, PARTITION BY, and ORDER BY.
	w := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc:  &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "sum"}, Args: []*planpb.Expr{match()}}}},
		PartitionBy: []*planpb.Expr{match()},
		OrderBy:     []*planpb.OrderBySpec{{Expr: match()}},
	}}}
	ws := replaceScoreFnInExprBy(w, rewrite).GetW()
	require.Same(t, sentinel, ws.WindowFunc.GetF().Args[0])
	require.Same(t, sentinel, ws.PartitionBy[0])
	require.Same(t, sentinel, ws.OrderBy[0].Expr)
}

// resolveScanNodeUnderWindow must descend the PARTITION and single-input PROJECT passthroughs the
// binder can place between a WINDOW and its base scan, stop at a scan that carries indexes, and
// refuse to descend a WINDOW child (stacked windows are handled innermost-first) or a JOIN.
func TestResolveScanNodeUnderWindow(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	scanID, scanNode := matchScanWithFulltextIndex(builder, ctx)

	require.Same(t, scanNode, builder.resolveScanNodeUnderWindow(scanNode),
		"a direct indexed scan resolves to itself")

	partID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_PARTITION, Children: []int32{scanID}}, ctx)
	require.Same(t, scanNode, builder.resolveScanNodeUnderWindow(builder.qry.Nodes[partID]),
		"PARTITION passthrough is descended to the scan")

	projID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{partID}}, ctx)
	require.Same(t, scanNode, builder.resolveScanNodeUnderWindow(builder.qry.Nodes[projID]),
		"PROJECT -> PARTITION -> scan chain is fully descended")

	winID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_WINDOW, Children: []int32{scanID}}, ctx)
	require.Nil(t, builder.resolveScanNodeUnderWindow(builder.qry.Nodes[winID]),
		"a WINDOW child is NOT descended -- stacked windows resolve innermost-first")

	joinID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_JOIN, Children: []int32{scanID, scanID}}, ctx)
	require.Nil(t, builder.resolveScanNodeUnderWindow(builder.qry.Nodes[joinID]),
		"a JOIN is not a passthrough")

	noIdx := builder.appendNode(&planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{}}, ctx)
	require.Nil(t, builder.resolveScanNodeUnderWindow(builder.qry.Nodes[noIdx]),
		"a scan with no indexes cannot serve a fulltext rewrite")
}

// exprCallsFunc must detect a fulltext_match anywhere inside a window spec (Expr_W) -- window
// function argument, PARTITION BY, or ORDER BY -- mirroring replaceScoreFnInExprBy, so the WINDOW
// rewrite guard does not skip a MATCH that lives in the OVER clause (#28974).
func TestExprCallsFuncTraversesWindowSpec(t *testing.T) {
	match := func() *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "fulltext_match"}}}}
	}
	inFunc := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "sum"}, Args: []*planpb.Expr{match()}}}},
	}}}
	require.True(t, exprCallsFunc(inFunc, "fulltext_match"))
	inPart := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{PartitionBy: []*planpb.Expr{match()}}}}
	require.True(t, exprCallsFunc(inPart, "fulltext_match"))
	inOrder := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{OrderBy: []*planpb.OrderBySpec{{Expr: match()}}}}}
	require.True(t, exprCallsFunc(inOrder, "fulltext_match"))

	noMatch := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "row_number"}}}},
	}}}
	require.False(t, exprCallsFunc(noMatch, "fulltext_match"))
	require.False(t, exprCallsFunc(&planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{}}}, "fulltext_match"),
		"an empty window spec is nil-safe")
	require.False(t, exprCallsFunc(&planpb.Expr{Expr: &planpb.Expr_W{W: nil}}, "fulltext_match"),
		"a nil window spec must not panic, matching replaceScoreFnInExprBy's guard")
}

// OVER (PARTITION BY match(...)) puts the MATCH in BOTH the WinSpecList Expr_W.PartitionBy AND the
// sibling Node_PARTITION's OrderBy (appendWindowNode builds both). The WINDOW rewrite must resolve
// BOTH copies to the served score, or the un-rewritten copy reaches execution as 20105 (#28974 P2).
func TestFullTextWindowPartitionByMatchServed(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	matchScanID, scanNode := matchScanWithFulltextIndex(builder, ctx)
	scanTag := scanNode.BindingTags[0]
	match := func() *planpb.Expr {
		return makeFullTextMatchExpr("hello", 0, scanNode.TableDef, scanTag, []int32{2, 3})
	}

	winTag := builder.genNewBindTag()
	partID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PARTITION,
		Children:    []int32{matchScanID},
		OrderBy:     []*planpb.OrderBySpec{{Expr: match(), Flag: planpb.OrderBySpec_INTERNAL}},
		BindingTags: []int32{winTag},
	}, ctx)
	winID := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_WINDOW,
		Children: []int32{partID},
		WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc:  &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "row_number"}}}},
			PartitionBy: []*planpb.Expr{match()},
		}}}},
		BindingTags: []int32{winTag},
	}, ctx)

	ityp := types.T_int64.ToType()
	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{winID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{
			Typ:  makePlan2Type(&ityp),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: winTag, ColPos: 0}},
		}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry),
		"both the WinSpecList and the PARTITION-node copies of a PARTITION BY MATCH must be rewritten")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry),
		"the partition-by MATCH must resolve to the WHERE MATCH's fulltext index scan")
}

func planHasReachableNodeType(query *planpb.Query, from int32, nt planpb.Node_NodeType) bool {
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || seen[nodeID] {
			return false
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		if node.NodeType == nt {
			return true
		}
		for _, childID := range node.Children {
			if visit(childID) {
				return true
			}
		}
		return false
	}
	return visit(from)
}
