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
