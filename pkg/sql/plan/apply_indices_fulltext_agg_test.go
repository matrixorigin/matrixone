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
