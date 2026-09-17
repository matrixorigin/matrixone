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

// #29065: PROJECT -> FILTER(HAVING max(match)>0 as colref) -> AGG(AggList max(match)) -> SCAN(no where).
// The membership-implying HAVING must let the aggregate MATCH drive the index scan.
func TestFullTextAggHavingDrivesIndex(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	tableDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	scanTag := builder.genNewBindTag()
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, nil), ctx)

	match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	maxMatch := ftAggFn("max", match)
	groupTag := builder.genNewBindTag()
	aggTag := builder.genNewBindTag()
	ftyp := types.T_float32.ToType()
	aggID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_AGG,
		Children:    []int32{scanID},
		AggList:     []*planpb.Expr{maxMatch},
		GroupBy:     []*planpb.Expr{ftjColExpr(tableDef, scanTag, 1)},
		BindingTags: []int32{groupTag, aggTag},
	}, ctx)

	aggCol := &planpb.Expr{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: 0}}}
	havingPred, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{aggCol, makePlan2Float64ConstExprWithType(0)})
	require.NoError(t, err)
	filterID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_FILTER, Children: []int32{aggID}, FilterList: []*planpb.Expr{havingPred}}, ctx)

	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{filterID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: 0}}}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Zero(t, countReachableFullTextMatches(builder.qry), "aggregate MATCH proven by membership HAVING must be rewritten")
	require.Equal(t, 1, countReachableFullTextScans(builder.qry), "the aggregate MATCH must drive one fulltext index scan")
}

// #29065 safety: a co-aggregate (COUNT here) over a group would be computed over matchers only if
// the aggregate MATCH drove the index (driving drops non-matching rows before aggregation), so the
// query must NOT be driven -- the raw match survives (left to 20105) rather than returning wrong
// counts for a multi-row group.
func TestFullTextAggHavingCoAggregateNotDriven(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)

	tableDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	scanTag := builder.genNewBindTag()
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, nil), ctx)

	match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	countAgg := ftAggFn("count", ftjColExpr(tableDef, scanTag, 1))
	maxMatch := ftAggFn("max", match)
	groupTag := builder.genNewBindTag()
	aggTag := builder.genNewBindTag()
	ftyp := types.T_float32.ToType()
	aggID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_AGG,
		Children:    []int32{scanID},
		AggList:     []*planpb.Expr{countAgg, maxMatch}, // count(*) alongside max(match)
		GroupBy:     []*planpb.Expr{ftjColExpr(tableDef, scanTag, 1)},
		BindingTags: []int32{groupTag, aggTag},
	}, ctx)

	aggCol := &planpb.Expr{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: 1}}}
	havingPred, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{aggCol, makePlan2Float64ConstExprWithType(0)})
	require.NoError(t, err)
	filterID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_FILTER, Children: []int32{aggID}, FilterList: []*planpb.Expr{havingPred}}, ctx)

	projTag := builder.genNewBindTag()
	projID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{filterID},
		BindingTags: []int32{projTag},
		ProjectList: []*planpb.Expr{{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: 1}}}},
	}, ctx)

	newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newID}

	require.Equal(t, 0, countReachableFullTextScans(builder.qry), "a co-aggregate query must not drive the index")
	require.Positive(t, countReachableFullTextMatches(builder.qry), "the raw MATCH survives (query stays unsupported)")
}

func TestSafeAggForFullTextDriver(t *testing.T) {
	require.True(t, safeAggForFullTextDriver("max"))
	require.True(t, safeAggForFullTextDriver("sum"))
	for _, n := range []string{"min", "avg", "count", "starcount", "group_concat", ""} {
		require.False(t, safeAggForFullTextDriver(n), n)
	}
}

func TestUnwrapMonotoneScalar(t *testing.T) {
	require.Nil(t, unwrapMonotoneScalar(nil))
	col := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	require.Same(t, col, unwrapMonotoneScalar(col), "a non-function expr is returned as-is")
	wrap := func(name string, arg *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name}, Args: []*planpb.Expr{arg}}}}
	}
	for _, n := range []string{"cast", "round", "floor", "ceil"} {
		require.Same(t, col, unwrapMonotoneScalar(wrap(n, col)), n)
	}
	require.Same(t, col, unwrapMonotoneScalar(wrap("cast", wrap("round", col))), "nested wrappers are peeled")
	plus := wrap("+", col)
	require.Same(t, plus, unwrapMonotoneScalar(plus), "a non-order-preserving function stops the peel")
	emptyCast := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "cast"}}}}
	require.Same(t, emptyCast, unwrapMonotoneScalar(emptyCast), "a wrapper with no args is returned as-is")
}

func TestExprContainsFullTextMatch(t *testing.T) {
	match := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "fulltext_match"}}}}
	col := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	require.False(t, exprContainsFullTextMatch(nil))
	require.False(t, exprContainsFullTextMatch(col))
	require.True(t, exprContainsFullTextMatch(match))
	nested := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: ">"}, Args: []*planpb.Expr{col, match}}}}
	require.True(t, exprContainsFullTextMatch(nested))
	noMatch := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: ">"}, Args: []*planpb.Expr{col, col}}}}
	require.False(t, exprContainsFullTextMatch(noMatch))
}

func TestFullTextDriverFuncs(t *testing.T) {
	match := func() *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "fulltext_match"}}}}
	}
	require.Empty(t, fullTextDriverFuncs(nil, []int32{0}, nil), "a nil scan contributes no filter funcs")
	scan := &planpb.Node{FilterList: []*planpb.Expr{match(), {Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}}}
	// id 0 -> a fulltext_match filter (kept); id 5 -> out of range (skipped); id 1 -> a colref (no fn).
	funcs := fullTextDriverFuncs(scan, []int32{0, 5, 1}, []*planpb.Expr{match()})
	require.Len(t, funcs, 2, "scan filter 0 plus the one wrapped expr; out-of-range and non-func skipped")
}

// aggHavingFixture builds a QueryBuilder with a registered fulltext index and a base scan, for
// exercising getFullTextMatchFromAggHaving / aggOutputInvariantToMatcherFilter directly.
func aggHavingFixture(t *testing.T) (*QueryBuilder, *planpb.TableDef, int32, *planpb.Node) {
	t.Helper()
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	tableDef := makeFullTextJoinTestTableDef("ft", true)
	registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
	scanTag := builder.genNewBindTag()
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, nil), ctx)
	return builder, tableDef, scanTag, builder.qry.Nodes[scanID]
}

func TestGetFullTextMatchFromAggHaving(t *testing.T) {
	const aggTag = 999
	ftyp := types.T_float32.ToType()
	aggCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: pos}}}
	}
	cmp := func(t *testing.T, op string, l, r *planpb.Expr) *planpb.Expr {
		e, err := BindFuncExprImplByPlanExpr(context.Background(), op, []*planpb.Expr{l, r})
		require.NoError(t, err)
		return e
	}
	f0 := makePlan2Float64ConstExprWithType(0)
	f1 := makePlan2Float64ConstExprWithType(1)

	for _, tc := range []struct {
		name     string
		aggList  func(m *planpb.Expr) []*planpb.Expr
		groupBy  func(m *planpb.Expr) []*planpb.Expr
		having   func(t *testing.T, m *planpb.Expr) []*planpb.Expr
		wrongIdx bool // match on a column the index cannot serve
		want     int
	}{
		{name: "max>0 colref", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">", aggCol(0), f0)} }, want: 1},
		{name: "max>=0 rejected", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">=", aggCol(0), f0)} }, want: 0},
		{name: "max>=1 accepted", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">=", aggCol(0), f1)} }, want: 1},
		{name: "max<5 rejected", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, "<", aggCol(0), makePlan2Float64ConstExprWithType(5))}
			}, want: 0},
		{name: "reversed 0<max", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, "<", f0, aggCol(0))} }, want: 1},
		{name: "inline max>0", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, ">", ftAggFn("max", m), f0)}
			}, want: 1},
		{name: "sum>0", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("sum", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">", aggCol(0), f0)} }, want: 1},
		{name: "min>0 not-safe", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("min", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, ">", ftAggFn("min", m), f0)}
			}, want: 0},
		{name: "and both membership dedups to 1", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, "and", cmp(t, ">", aggCol(0), f0), cmp(t, ">=", aggCol(0), f1))}
			}, want: 1},
		{name: "no membership", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, "<", aggCol(0), makePlan2Float64ConstExprWithType(5))}
			}, want: 0},
		{name: "co-aggregate refused", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("count", m), ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">", aggCol(1), f0)} }, want: 0},
		{name: "groupby match refused", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			groupBy: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{m} },
			having:  func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">", aggCol(0), f0)} }, want: 0},
		{name: "no matching index", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{cmp(t, ">", aggCol(0), f0)} }, wrongIdx: true, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, tableDef, scanTag, scanNode := aggHavingFixture(t)
			cols := []int32{2, 3}
			if tc.wrongIdx {
				cols = []int32{2} // the index covers {2,3}; a {2}-only match cannot be served
			}
			m := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, cols)
			var groupBy []*planpb.Expr
			if tc.groupBy != nil {
				groupBy = tc.groupBy(m)
			}
			aggNode := &planpb.Node{NodeType: planpb.Node_AGG, AggList: tc.aggList(m), GroupBy: groupBy, BindingTags: []int32{aggTag - 1, aggTag}}
			exprs, idxs := builder.getFullTextMatchFromAggHaving(tc.having(t, m), aggNode, scanNode, nil)
			require.Len(t, exprs, tc.want, tc.name)
			require.Len(t, idxs, tc.want, tc.name)
		})
	}

	t.Run("dedup against existing driver", func(t *testing.T) {
		builder, tableDef, scanTag, scanNode := aggHavingFixture(t)
		m := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
		aggNode := &planpb.Node{NodeType: planpb.Node_AGG, AggList: []*planpb.Expr{ftAggFn("max", m)}, BindingTags: []int32{aggTag - 1, aggTag}}
		having := []*planpb.Expr{cmp(t, ">", aggCol(0), f0)}
		existing := []*planpb.Function{makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3}).GetF()}
		exprs, _ := builder.getFullTextMatchFromAggHaving(having, aggNode, scanNode, existing)
		require.Empty(t, exprs, "a match already driving a stream is not collected again")
	})

	t.Run("nil agg / no bindingtags", func(t *testing.T) {
		builder, _, _, scanNode := aggHavingFixture(t)
		exprs, _ := builder.getFullTextMatchFromAggHaving(nil, nil, scanNode, nil)
		require.Empty(t, exprs)
		exprs, _ = builder.getFullTextMatchFromAggHaving(nil, &planpb.Node{NodeType: planpb.Node_AGG}, scanNode, nil)
		require.Empty(t, exprs, "an agg with fewer than 2 binding tags is skipped")
	})
}

func TestAggOutputInvariantToMatcherFilter(t *testing.T) {
	builder, tableDef, scanTag, _ := aggHavingFixture(t)
	match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	other := makeFullTextMatchExpr("world", 0, tableDef, scanTag, []int32{2, 3})
	drivers := []*planpb.Expr{match}
	agg := func(aggList, groupBy []*planpb.Expr) *planpb.Node {
		return &planpb.Node{NodeType: planpb.Node_AGG, AggList: aggList, GroupBy: groupBy}
	}

	require.False(t, builder.aggOutputInvariantToMatcherFilter(nil, drivers))
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg(nil, nil), drivers), "empty AggList")
	require.True(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", match)}, nil), drivers))
	require.True(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", match), ftAggFn("sum", match)}, nil), drivers))
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("count", match)}, nil), drivers), "count is not a safe agg")
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", ftjColExpr(tableDef, scanTag, 1))}, nil), drivers), "max over a non-match column")
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", other)}, nil), drivers), "max over a different match")
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", match)}, []*planpb.Expr{match}), drivers), "grouping key is a match")
	require.True(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("max", match)}, []*planpb.Expr{ftjColExpr(tableDef, scanTag, 1)}), drivers))
}
