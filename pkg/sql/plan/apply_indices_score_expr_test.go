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

func scoreFn(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float32)},
		Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}},
	}
}

func scoreLit() *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
}

// TestExprCallsFunc: an index placeholder nested inside a larger expression is still a
// placeholder. The "is this expression exactly the placeholder?" tests used elsewhere walk
// straight past `MATCH(...) > 0`, which is how such a predicate survived the rewrite and
// reached execution, where it throws.
func TestExprCallsFunc(t *testing.T) {
	require.False(t, exprCallsFunc(nil, "fulltext_match"))
	require.True(t, exprCallsFunc(scoreFn("fulltext_match"), "fulltext_match"))
	require.False(t, exprCallsFunc(scoreFn("fulltext_match"), "l2_distance"))

	// wrapped in a comparison -- the shape that motivated this
	require.True(t, exprCallsFunc(
		scoreFn(">", scoreFn("fulltext_match"), scoreLit()), "fulltext_match"))
	// nested two deep
	require.True(t, exprCallsFunc(
		scoreFn("and", scoreFn(">", scoreFn("round", scoreFn("fulltext_match")), scoreLit()), scoreLit()),
		"fulltext_match"))
	// inside an expression list
	require.True(t, exprCallsFunc(
		&plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{
			List: []*plan.Expr{scoreFn("fulltext_match")}}}}, "fulltext_match"))
	// an ordinary predicate must not be claimed
	require.False(t, exprCallsFunc(scoreFn(">", scoreLit(), scoreLit()), "fulltext_match"))
}

// TestReplaceScoreFnInExprBy: the shared walk. The callback sees the whole *plan.Function, so
// a caller with several candidate index scans can decide WHICH one a call belongs to (fulltext
// does this by argument) and leave alone the ones no scan answers.
func TestReplaceScoreFnInExprBy(t *testing.T) {
	col := func() *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float32)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 12, ColPos: 1}},
		}
	}
	byName := func(name string) func(*plan.Function) *plan.Expr {
		return func(fn *plan.Function) *plan.Expr {
			if fn.Func != nil && fn.Func.ObjName == name {
				return col()
			}
			return nil
		}
	}
	rewrite := byName("fulltext_match")

	// the placeholder itself becomes the score column
	got := replaceScoreFnInExprBy(scoreFn("fulltext_match"), rewrite)
	require.NotNil(t, got.GetCol())
	require.Equal(t, int32(12), got.GetCol().RelPos)

	// wrapped: the comparison survives, only the inner call is swapped
	pred := scoreFn(">", scoreFn("fulltext_match"), scoreLit())
	got = replaceScoreFnInExprBy(pred, rewrite)
	require.NotNil(t, got.GetF())
	require.Equal(t, ">", got.GetF().Func.ObjName)
	require.NotNil(t, got.GetF().Args[0].GetCol(), "the inner MATCH must become the score column")

	// every occurrence gets its OWN node, so a later pass mutating one cannot corrupt another
	two := scoreFn("and", scoreFn("fulltext_match"), scoreFn("fulltext_match"))
	got = replaceScoreFnInExprBy(two, rewrite)
	a, b := got.GetF().Args[0], got.GetF().Args[1]
	require.NotNil(t, a.GetCol())
	require.NotNil(t, b.GetCol())
	require.NotSame(t, a, b)

	// a callback returning nil leaves the call in place and the walk descends into its args
	kept := replaceScoreFnInExprBy(scoreFn(">", scoreFn("fulltext_match"), scoreLit()),
		func(fn *plan.Function) *plan.Expr { return nil })
	require.NotNil(t, kept.GetF().Args[0].GetF(), "an unmatched call must survive untouched")

	// unrelated expressions are untouched
	plain := scoreFn(">", scoreLit(), scoreLit())
	require.Equal(t, plain, replaceScoreFnInExprBy(plain, rewrite))
	require.Nil(t, replaceScoreFnInExprBy(nil, rewrite))
}

// matchFn builds a bound fulltext_match: (pattern, mode, index-part columns...), the shape
// equalsFullTextMatchFunc compares.
func matchFn(pattern string, mode int64, cols ...string) *plan.Function {
	args := []*plan.Expr{
		makePlan2StringConstExprWithType(pattern),
		makePlan2Int64ConstExprWithType(mode),
	}
	for _, c := range cols {
		args = append(args, &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: c}},
		})
	}
	return &plan.Function{Func: &plan.ObjectRef{ObjName: "fulltext_match"}, Args: args}
}

func matchExpr(fn *plan.Function) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_F{F: fn}}
}

// ftScanNode is a stand-in for the fulltext TVF node: col 1 is the score.
func ftScanNode(tag int32) *plan.Node {
	return &plan.Node{
		BindingTags: []int32{tag},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: "doc_id"},
			{Name: "score", Typ: plan.Type{Id: int32(types.T_float32)}},
		}},
	}
}

// TestServedFullTextScoreMatchesArguments pins the defect that motivated the argument-aware
// rewrite: two MATCHes on the same column are two different questions. Answering one with the
// other's index scan reports a wrong relevance for every row, silently -- worse than the error
// an unrewritten MATCH raises.
func TestServedFullTextScoreMatchesArguments(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(7)}}}
	served := []fulltextServedMatch{{fn: matchFn("hello", 0, "body"), nodeID: 0}}

	// the same match resolves to its scan's score column
	got := builder.servedFullTextScore(matchFn("hello", 0, "body"), served)
	require.NotNil(t, got)
	require.Equal(t, int32(7), got.GetCol().RelPos)
	require.Equal(t, int32(1), got.GetCol().ColPos, "score is col 1")

	// a different pattern, mode or column list is NOT this match
	require.Nil(t, builder.servedFullTextScore(matchFn("world", 0, "body"), served))
	require.Nil(t, builder.servedFullTextScore(matchFn("hello", 1, "body"), served))
	require.Nil(t, builder.servedFullTextScore(matchFn("hello", 0, "title"), served))
	require.Nil(t, builder.servedFullTextScore(matchFn("hello", 0, "body", "title"), served))

	// not a MATCH at all, and defensive shapes
	require.Nil(t, builder.servedFullTextScore(nil, served))
	require.Nil(t, builder.servedFullTextScore(
		&plan.Function{Func: &plan.ObjectRef{ObjName: "round"}}, served))
	require.Nil(t, builder.servedFullTextScore(matchFn("hello", 0, "body"), nil))

	// a served entry whose node was never built resolves to nothing rather than panicking
	pending := []fulltextServedMatch{{fn: matchFn("hello", 0, "body"), nodeID: -1}}
	require.Nil(t, builder.servedFullTextScore(matchFn("hello", 0, "body"), pending))
	// isServedFullTextMatch asks only about the match, so it answers before the node exists
	require.True(t, builder.isServedFullTextMatch(matchFn("hello", 0, "body"), pending))
	require.False(t, builder.isServedFullTextMatch(matchFn("world", 0, "body"), pending))
}

// TestHasUnservedFullTextMatch: a predicate is rewritten in place, so a half-rewritable one
// must be recognised while it is still intact -- otherwise the served half becomes a column
// reference the node it stays on cannot resolve.
func TestHasUnservedFullTextMatch(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(7)}}}
	served := []fulltextServedMatch{{fn: matchFn("hello", 0, "body"), nodeID: 0}}
	isServed := func(fn *plan.Function) bool { return builder.servedFullTextScore(fn, served) != nil }

	hello := func() *plan.Expr { return matchExpr(matchFn("hello", 0, "body")) }
	world := func() *plan.Expr { return matchExpr(matchFn("world", 0, "body")) }

	require.False(t, hasUnservedFullTextMatch(nil, isServed))
	require.False(t, hasUnservedFullTextMatch(scoreFn(">", hello(), scoreLit()), isServed))
	require.True(t, hasUnservedFullTextMatch(scoreFn(">", world(), scoreLit()), isServed))
	// mixed: one served, one not -- the whole predicate must be treated as unservable
	require.True(t, hasUnservedFullTextMatch(
		scoreFn("and", scoreFn(">", hello(), scoreLit()), scoreFn(">", world(), scoreLit())), isServed))
	// nested inside a scalar wrapper
	require.True(t, hasUnservedFullTextMatch(
		scoreFn(">", scoreFn("round", world(), scoreLit()), scoreLit()), isServed))
	// inside an expression list
	require.True(t, hasUnservedFullTextMatch(
		&plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{world()}}}}, isServed))
	// no MATCH at all
	require.False(t, hasUnservedFullTextMatch(scoreFn(">", scoreLit(), scoreLit()), isServed))
}

// TestReplaceScoreFnInExprByPicksTheRightStream: with two streams, each wrapped MATCH must be
// rewritten to the score of the scan built for THAT match.
func TestReplaceScoreFnInExprByPicksTheRightStream(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(7), ftScanNode(9)}}}
	served := []fulltextServedMatch{
		{fn: matchFn("hello", 0, "body"), nodeID: 0},
		{fn: matchFn("world", 0, "body"), nodeID: 1},
	}
	rewriter := builder.fullTextScoreRewriter(served)

	expr := scoreFn("+",
		scoreFn("round", matchExpr(matchFn("world", 0, "body")), scoreLit()),
		matchExpr(matchFn("hello", 0, "body")))
	got := replaceScoreFnInExprBy(expr, rewriter)

	require.Equal(t, int32(9), got.GetF().Args[0].GetF().Args[0].GetCol().RelPos, "world -> its own scan")
	require.Equal(t, int32(7), got.GetF().Args[1].GetCol().RelPos, "hello -> its own scan")

	// an unserved match is left in place: execution raises 20105 rather than reporting a
	// number that belongs to a different search.
	other := matchExpr(matchFn("other", 0, "body"))
	require.NotNil(t, replaceScoreFnInExprBy(other, rewriter).GetF())
}

// TestFulltext2ScoreRangeFromFilters pins what may be pushed into the engine as a relevance
// interval, and -- more importantly -- what may not.
func TestFulltext2ScoreRangeFromFilters(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(7)}}}
	hello := matchFn("hello", 0, "body")
	m := func() *plan.Expr { return matchExpr(matchFn("hello", 0, "body")) }
	cst := func(v float64) *plan.Expr { return makePlan2Float64ConstExprWithType(v) }

	// `score > 0.5`
	r := builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{scoreFn(">", m(), cst(0.5))}, hello)
	require.NotNil(t, r)
	require.True(t, r.HasMin)
	require.False(t, r.MinInclusive)
	require.False(t, r.HasMax)
	require.LessOrEqual(t, r.Min, float32(0.5), "the bound is widened outward, never inward")

	// `score >= 0.5` keeps the bound
	r = builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{scoreFn(">=", m(), cst(0.5))}, hello)
	require.NotNil(t, r)
	require.True(t, r.MinInclusive)

	// `score < 0.5` is an upper bound -- pushable as a filter (it just cannot PRUNE)
	r = builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{scoreFn("<", m(), cst(0.5))}, hello)
	require.NotNil(t, r)
	require.True(t, r.HasMax)
	require.False(t, r.HasMin)
	require.GreaterOrEqual(t, r.Max, float32(0.5))

	// reversed operand order: `0.5 < score` is `score > 0.5`
	r = builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{scoreFn("<", cst(0.5), m())}, hello)
	require.NotNil(t, r)
	require.True(t, r.HasMin)
	require.False(t, r.HasMax)

	// two AND-ed bounds collapse into one interval
	r = builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{
		scoreFn("and", scoreFn(">", m(), cst(0.2)), scoreFn("<=", m(), cst(0.8)))}, hello)
	require.NotNil(t, r)
	require.True(t, r.HasMin && r.HasMax)
	require.True(t, r.MaxInclusive)

	// the TIGHTEST of several bounds on the same side wins
	r = builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{
		scoreFn("and", scoreFn(">", m(), cst(0.2)), scoreFn(">", m(), cst(0.6)))}, hello)
	require.NotNil(t, r)
	require.Greater(t, r.Min, float32(0.5))

	// NOT pushable ---------------------------------------------------------------
	// under an OR the predicate need not hold for a returned row
	require.Nil(t, builder.fulltext2ScoreRangeFromFilters(
		[]*plan.Expr{scoreFn("or", scoreFn(">", m(), cst(0.5)), scoreLit())}, hello))
	// a bound on a DIFFERENT match says nothing about this stream
	require.Nil(t, builder.fulltext2ScoreRangeFromFilters(
		[]*plan.Expr{scoreFn(">", matchExpr(matchFn("world", 0, "body")), cst(0.5))}, hello))
	// a wrapper changes the compared value: round(score,3) > 0.5 is not score > 0.5
	require.Nil(t, builder.fulltext2ScoreRangeFromFilters(
		[]*plan.Expr{scoreFn(">", scoreFn("round", m(), scoreLit()), cst(0.5))}, hello))
	// a non-constant right-hand side
	require.Nil(t, builder.fulltext2ScoreRangeFromFilters(
		[]*plan.Expr{scoreFn(">", m(), scoreFn("+", cst(1), cst(2)))}, hello))
	// no score predicate at all
	require.Nil(t, builder.fulltext2ScoreRangeFromFilters([]*plan.Expr{scoreFn(">", scoreLit(), cst(1))}, hello))
}
