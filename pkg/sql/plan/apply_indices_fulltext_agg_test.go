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
	// floor(v): an order-preserving-but-NOT-value-preserving wrapper around a constant. The
	// effective threshold is floor(v), which the proof must evaluate rather than read the input v.
	floor := func(t *testing.T, v float64) *planpb.Expr {
		e, err := BindFuncExprImplByPlanExpr(context.Background(), "floor", []*planpb.Expr{makePlan2Float64ConstExprWithType(v)})
		require.NoError(t, err)
		return e
	}

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
		// #29065: a wrapped constant threshold must be EVALUATED, not have its wrapper input read.
		// `>= floor(1e-1)` has effective threshold floor(0.1)=0, so zero-score groups qualify and the
		// query must NOT drive (reading the input 0.1 wrongly treated it as a positive threshold and
		// dropped those groups).
		{name: "max>=floor(0.1) effective 0 rejected", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, ">=", aggCol(0), floor(t, 0.1))}
			}, want: 0},
		// `>= floor(1.5)` has effective threshold 1 > 0, so it still drives -- the fix evaluates the
		// constant rather than over-rejecting every wrapped threshold.
		{name: "max>=floor(1.5) effective 1 accepted", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, ">=", aggCol(0), floor(t, 1.5))}
			}, want: 1},
		// `> floor(1e-1)` is `> 0`, which excludes zero-score groups regardless of the wrapper, so it drives.
		{name: "max>floor(0.1) is >0 accepted", aggList: func(m *planpb.Expr) []*planpb.Expr { return []*planpb.Expr{ftAggFn("max", m)} },
			having: func(t *testing.T, m *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{cmp(t, ">", aggCol(0), floor(t, 0.1))}
			}, want: 1},
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

// #29065 P1: multiple DISTINCT aggregate MATCHes must NOT drive the index. applyJoinFullTextIndices
// INNER-joins each driver's stream by doc_id, so driving `HAVING MAX(match(alpha))>0 AND
// MAX(match(beta))>0` would demand ONE document match both patterns, silently dropping a group that
// satisfies the HAVING with alpha on one row and beta on another. getFullTextMatchFromAggHaving must
// collect no drivers (leaving the query at 20105) unless every aggregate candidate is the same MATCH.
func TestFullTextAggHavingMultipleDistinctMatchesNotDriven(t *testing.T) {
	builder, tableDef, scanTag, scanNode := aggHavingFixture(t)
	alpha := makeFullTextMatchExpr("alpha", 0, tableDef, scanTag, []int32{2, 3})
	beta := makeFullTextMatchExpr("beta", 0, tableDef, scanTag, []int32{2, 3})

	const aggTag = 999
	ftyp := types.T_float32.ToType()
	aggCol := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: aggTag, ColPos: pos}}}
	}
	cmp := func(op string, l, r *planpb.Expr) *planpb.Expr {
		e, err := BindFuncExprImplByPlanExpr(context.Background(), op, []*planpb.Expr{l, r})
		require.NoError(t, err)
		return e
	}
	f0 := makePlan2Float64ConstExprWithType(0)

	aggNode := &planpb.Node{
		NodeType:    planpb.Node_AGG,
		AggList:     []*planpb.Expr{ftAggFn("max", alpha), ftAggFn("max", beta)},
		BindingTags: []int32{aggTag - 1, aggTag},
	}
	having := []*planpb.Expr{cmp("and", cmp(">", aggCol(0), f0), cmp(">", aggCol(1), f0))}

	exprs, idxs := builder.getFullTextMatchFromAggHaving(having, aggNode, scanNode, nil)
	require.Empty(t, exprs, "two distinct aggregate MATCHes are not doc-level-intersection safe")
	require.Empty(t, idxs)

	// A single unique MATCH repeated across aggregates/predicates still drives (one stream).
	aggSame := &planpb.Node{
		NodeType:    planpb.Node_AGG,
		AggList:     []*planpb.Expr{ftAggFn("max", alpha), ftAggFn("sum", alpha)},
		BindingTags: []int32{aggTag - 1, aggTag},
	}
	havingSame := []*planpb.Expr{cmp("and", cmp(">", aggCol(0), f0), cmp(">", aggCol(1), f0))}
	exprsSame, _ := builder.getFullTextMatchFromAggHaving(havingSame, aggSame, scanNode, nil)
	require.Len(t, exprsSame, 1, "the same MATCH across aggregates collapses to one driver")
}

// #29065 P1: a FILTER separated from the AGG by a cardinality/order/position-sensitive barrier
// (WINDOW / FILL / PARTITION / a LIMIT node) is NOT that AGG's HAVING. resolveFullTextIndexPath must
// not harvest it -- driving the index below the AGG from a post-window predicate would drop groups
// before ROW_NUMBER etc. are computed and silently shift their output.
func TestResolveFullTextIndexPathHavingBarrier(t *testing.T) {
	newFilter := func(builder *QueryBuilder, ctx *BindContext, child int32) int32 {
		ftyp := types.T_float32.ToType()
		aggCol := &planpb.Expr{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
		pred, err := BindFuncExprImplByPlanExpr(context.Background(), ">", []*planpb.Expr{aggCol, makePlan2Float64ConstExprWithType(0)})
		require.NoError(t, err)
		return builder.appendNode(&planpb.Node{NodeType: planpb.Node_FILTER, Children: []int32{child}, FilterList: []*planpb.Expr{pred}}, ctx)
	}
	// build PROJECT over `mid(agg over scan)`, where mid() inserts the middle nodes above the AGG.
	buildPath := func(mid func(b *QueryBuilder, c *BindContext, aggID int32) int32) *fullTextIndexPath {
		builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
		ctx := NewBindContext(builder, nil)
		tableDef := makeFullTextJoinTestTableDef("ft", true)
		registerFullTextJoinRegularIndexTable(builder, tableDef.Indexes[0].IndexTableName)
		scanTag := builder.genNewBindTag()
		scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, nil), ctx)
		match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
		groupTag := builder.genNewBindTag()
		aggTag := builder.genNewBindTag()
		aggID := builder.appendNode(&planpb.Node{
			NodeType:    planpb.Node_AGG,
			Children:    []int32{scanID},
			AggList:     []*planpb.Expr{ftAggFn("max", match)},
			GroupBy:     []*planpb.Expr{ftjColExpr(tableDef, scanTag, 1)},
			BindingTags: []int32{groupTag, aggTag},
		}, ctx)
		top := mid(builder, ctx, aggID)
		ftyp := types.T_float32.ToType()
		projTag := builder.genNewBindTag()
		projID := builder.appendNode(&planpb.Node{
			NodeType:    planpb.Node_PROJECT,
			Children:    []int32{top},
			BindingTags: []int32{projTag},
			ProjectList: []*planpb.Expr{{Typ: makePlan2Type(&ftyp), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: projTag, ColPos: 0}}}},
		}, ctx)
		return builder.resolveFullTextIndexPath(builder.qry.Nodes[projID])
	}

	t.Run("filter directly above agg is HAVING", func(t *testing.T) {
		path := buildPath(func(b *QueryBuilder, c *BindContext, aggID int32) int32 { return newFilter(b, c, aggID) })
		require.NotNil(t, path)
		require.NotNil(t, path.havingNode, "a FILTER adjacent to the AGG is its HAVING")
	})

	t.Run("filter above WINDOW is not HAVING", func(t *testing.T) {
		path := buildPath(func(b *QueryBuilder, c *BindContext, aggID int32) int32 {
			win := b.appendNode(&planpb.Node{NodeType: planpb.Node_WINDOW, Children: []int32{aggID}, BindingTags: []int32{b.genNewBindTag()}}, c)
			return newFilter(b, c, win)
		})
		require.NotNil(t, path)
		require.Nil(t, path.havingNode, "a WINDOW between the FILTER and the AGG disqualifies the FILTER")
	})

	t.Run("filter above a LIMIT node is not HAVING", func(t *testing.T) {
		path := buildPath(func(b *QueryBuilder, c *BindContext, aggID int32) int32 {
			lim := b.appendNode(&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{aggID}, Limit: makePlan2Int64ConstExprWithType(5), BindingTags: []int32{b.genNewBindTag()}}, c)
			return newFilter(b, c, lim)
		})
		require.NotNil(t, path)
		require.Nil(t, path.havingNode, "a LIMIT between the FILTER and the AGG disqualifies the FILTER")
	})

	t.Run("real HAVING below a WINDOW is still found", func(t *testing.T) {
		path := buildPath(func(b *QueryBuilder, c *BindContext, aggID int32) int32 {
			filt := newFilter(b, c, aggID)
			return b.appendNode(&planpb.Node{NodeType: planpb.Node_WINDOW, Children: []int32{filt}, BindingTags: []int32{b.genNewBindTag()}}, c)
		})
		require.NotNil(t, path)
		require.NotNil(t, path.havingNode, "a FILTER adjacent to the AGG below a WINDOW is still the HAVING")
	})
}

func TestIsFullTextAggHavingBarrier(t *testing.T) {
	for _, nt := range []planpb.Node_NodeType{planpb.Node_WINDOW, planpb.Node_TIME_WINDOW, planpb.Node_FILL, planpb.Node_PARTITION} {
		require.True(t, isFullTextAggHavingBarrier(&planpb.Node{NodeType: nt}), nt.String())
	}
	for _, nt := range []planpb.Node_NodeType{planpb.Node_PROJECT, planpb.Node_SORT, planpb.Node_FILTER, planpb.Node_AGG} {
		require.False(t, isFullTextAggHavingBarrier(&planpb.Node{NodeType: nt}), nt.String())
	}
	require.True(t, isFullTextAggHavingBarrier(&planpb.Node{NodeType: planpb.Node_PROJECT, Limit: makePlan2Int64ConstExprWithType(5)}),
		"any node carrying a LIMIT is a barrier")
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

	// Wrapped drivers: round with a constant, non-null digits is drop-safe (round(0,c)=0 is the
	// SUM identity), but a nullable or per-row (column) digits is NOT -- dropping a non-matching
	// row whose round(0,digits) is a non-null 0 while the kept rows round to NULL flips SUM/MAX
	// from 0 to NULL, silently losing the group (#29065).
	wrap := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name}, Args: args}}}
	}
	nullLit := &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	colDigits := ftjColExpr(tableDef, scanTag, 1)
	require.True(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("sum", wrap("round", match, makePlan2Int64ConstExprWithType(2)))}, nil), drivers), "round with constant digits is drop-safe")
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("sum", wrap("round", match, nullLit))}, nil), drivers), "round with NULL digits is not drop-safe")
	require.False(t, builder.aggOutputInvariantToMatcherFilter(agg([]*planpb.Expr{ftAggFn("sum", wrap("round", match, colDigits))}, nil), drivers), "round with per-row column digits is not drop-safe")
}

func TestWrappedMatchDropSafe(t *testing.T) {
	match := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "fulltext_match"}}}}
	col := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	nullLit := &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	constDigits := makePlan2Int64ConstExprWithType(2)
	wrap := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name}, Args: args}}}
	}
	ok := func(e *planpb.Expr) bool { m, s := wrappedMatchDropSafe(e); return m != nil && s }

	// Bare match and single-value monotone wrappers are drop-safe (each maps 0 -> non-null 0).
	require.True(t, ok(match))
	require.True(t, ok(wrap("floor", match)))
	require.True(t, ok(wrap("ceil", match)))
	require.True(t, ok(wrap("round", match)), "round with no digits")
	require.True(t, ok(wrap("round", match, constDigits)), "round with constant non-null digits")
	require.True(t, ok(wrap("cast", match, constDigits)), "cast's extra arg is a compile-time type")
	require.True(t, ok(wrap("cast", wrap("round", match, constDigits))), "nested safe wrappers")

	// round with a nullable or per-row (column) digits is NOT drop-safe (#29065).
	require.False(t, ok(wrap("round", match, nullLit)), "NULL digits maps kept rows to NULL")
	require.False(t, ok(wrap("round", match, col)), "column digits varies per row")

	// no match, non-monotone wrapper, or an empty wrapper.
	require.False(t, ok(col))
	require.False(t, ok(wrap("+", match)))
	require.False(t, ok(wrap("round")), "no args")
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
