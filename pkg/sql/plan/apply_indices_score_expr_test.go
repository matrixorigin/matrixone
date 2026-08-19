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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
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

// pushKeeps mirrors fulltext2.ScoreRange.contains (unexported there) so this package can
// assert what the engine will actually do with the bounds the planner hands it.
func pushKeeps(r *fulltext2.ScoreRange, score float32) bool {
	if r.HasMin {
		if r.MinInclusive && score < r.Min {
			return false
		}
		if !r.MinInclusive && score <= r.Min {
			return false
		}
	}
	if r.HasMax {
		if r.MaxInclusive && score > r.Max {
			return false
		}
		if !r.MaxInclusive && score >= r.Max {
			return false
		}
	}
	return true
}

// TestFulltext2ScoreRangeRoundsOutwardAtMidpoints pins the conversion direction.
//
// The engine scores in float32 while the SQL literal is a double, so the pushed bound must
// land OUTWARD of the literal: any float32 score the SQL predicate keeps has to survive
// ScoreRange.contains, because the engine drops rejected rows before the plan's own Filter
// ever sees them. Rounding inward silently loses rows.
//
// Every literal here sits strictly between two float32 values, more than half a float32 ULP
// from one of them -- the case an exactly representable literal like 0.5 cannot express, and
// the case a float64-space nudge gets wrong, because float64->float32 rounds to nearest and
// swallows the nudge.
func TestFulltext2ScoreRangeRoundsOutwardAtMidpoints(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(7)}}}
	hello := matchFn("hello", 0, "body")
	m := func() *plan.Expr { return matchExpr(matchFn("hello", 0, "body")) }
	cst := func(v float64) *plan.Expr { return makePlan2Float64ConstExprWithType(v) }

	// float32(lowLit) rounds UP, away from a correct lower bound.
	const lowLit = 0.5000000447034836
	// float32(highLit) rounds DOWN, away from a correct upper bound.
	const highLit = 0.5000000149011612

	// The two float32 neighbours straddling both literals.
	half := float32(0.5)
	nextUp := math.Nextafter32(half, float32(math.Inf(1))) // 0.5000000596046448

	for _, tc := range []struct {
		op    string
		lit   float64
		score float32 // a float32 score the SQL predicate keeps
		lower bool
	}{
		{">", lowLit, nextUp, true},
		{">=", lowLit, nextUp, true},
		{"<", highLit, half, false},
		{"<=", highLit, half, false},
	} {
		t.Run(tc.op, func(t *testing.T) {
			r := builder.fulltext2ScoreRangeFromFilters(
				[]*plan.Expr{scoreFn(tc.op, m(), cst(tc.lit))}, hello)
			require.NotNil(t, r)

			if tc.lower {
				require.True(t, r.HasMin)
				require.LessOrEqual(t, float64(r.Min), tc.lit,
					"a lower bound must never round inward past the SQL literal")
			} else {
				require.True(t, r.HasMax)
				require.GreaterOrEqual(t, float64(r.Max), tc.lit,
					"an upper bound must never round inward past the SQL literal")
			}

			// The SQL predicate keeps this score; so must the pushed range.
			var sqlKeeps bool
			switch tc.op {
			case ">":
				sqlKeeps = float64(tc.score) > tc.lit
			case ">=":
				sqlKeeps = float64(tc.score) >= tc.lit
			case "<":
				sqlKeeps = float64(tc.score) < tc.lit
			case "<=":
				sqlKeeps = float64(tc.score) <= tc.lit
			}
			require.True(t, sqlKeeps, "test setup: the probe score must satisfy the SQL predicate")
			require.True(t, pushKeeps(r, tc.score),
				"the engine would drop a row the SQL predicate keeps")
		})
	}
}

// TestScoreBoundHelpersPreserveInvariant sweeps the bound helpers across float32
// neighbourhoods and their midpoints: down <= v <= up, with no exceptions.
func TestScoreBoundHelpersPreserveInvariant(t *testing.T) {
	for _, base := range []float32{0, 0.5, 1, 1e-8, 12.34, 3.4e38} {
		f := base
		for i := 0; i < 500; i++ {
			next := math.Nextafter32(f, float32(math.Inf(1)))
			for _, v := range []float64{float64(f), (float64(f) + float64(next)) / 2} {
				require.LessOrEqual(t, float64(scoreBoundDown(v)), v)
				require.GreaterOrEqual(t, float64(scoreBoundUp(v)), v)
			}
			f = next
		}
	}
	// An exactly representable literal needs no slack at all.
	require.Equal(t, float32(0.5), scoreBoundDown(0.5))
	require.Equal(t, float32(0.5), scoreBoundUp(0.5))
}

// ftScanNodeWithIndex builds a TABLE_SCAN carrying one classic FULLTEXT index on `body`,
// which is the minimum findMatchFullTextIndex needs to resolve a MATCH.
func ftScanNodeWithIndex(tag int32) *plan.Node {
	return &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef: &plan.TableDef{
			Name: "docs",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "body", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			Indexes: []*plan.IndexDef{{
				IndexName:  "ft",
				IndexAlgo:  catalog.MOIndexFullTextAlgo.ToString(),
				TableExist: true,
				Parts:      []string{"body"},
			}},
		},
	}
}

// bodyMatch builds `fulltext_match(pattern, 0, docs.body)` bound to scan tag `tag`.
func bodyMatch(tag int32, pattern string) *plan.Expr {
	return matchExpr(&plan.Function{
		Func: &plan.ObjectRef{ObjName: "fulltext_match"},
		Args: []*plan.Expr{
			makePlan2StringConstExprWithType(pattern),
			makePlan2Int64ConstExprWithType(0),
			{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: 1, Name: "body"}},
			},
		},
	})
}

// TestGetWrappedFullTextMatches pins DISCOVERY: which nested MATCHes are allowed to build a
// stream of their own. The stream is INNER JOINed to the base scan, so harvesting one from a
// position the query does not require is how `… > 0.5 or id = 4` came to return no rows.
func TestGetWrappedFullTextMatches(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{}}
	const tag int32 = 7
	scan := ftScanNodeWithIndex(tag)
	cst := func(v float64) *plan.Expr { return makePlan2Float64ConstExprWithType(v) }

	scan.FilterList = []*plan.Expr{
		bodyMatch(tag, "hello"),                                                     // 0: the bare driver
		scoreFn(">", bodyMatch(tag, "hello"), cst(0.5)),                             // wrapped copy of the driver
		scoreFn(">", bodyMatch(tag, "world"), cst(0.5)),                             // a NEW match, membership-implying
		scoreFn("<", bodyMatch(tag, "other"), cst(0.5)),                             // upper bound: must NOT drive
		scoreFn("or", scoreFn(">", bodyMatch(tag, "orterm"), cst(0.5)), scoreLit()), // under OR
		scoreFn("not", scoreFn(">", bodyMatch(tag, "notterm"), cst(0.5))),           // under NOT
	}

	exprs, defs := builder.getWrappedFullTextMatches(nil, scan, []int32{0}, nil)
	require.Len(t, exprs, 1, "only the membership-implying NEW match may drive")
	require.Len(t, defs, 1)
	require.NotNil(t, defs[0])
	got := exprs[0].GetF()
	require.NotNil(t, got)
	require.Equal(t, "world", got.Args[0].GetLit().GetSval(),
		"the wrapped copy of the bare driver is deduplicated; 'other'/'orterm'/'notterm' cannot drive")

	// AND-reachable is enough — nesting inside AND still drives.
	scan2 := ftScanNodeWithIndex(tag)
	scan2.FilterList = []*plan.Expr{
		scoreFn("and", scoreFn(">", bodyMatch(tag, "deep"), cst(0.1)), scoreLit()),
	}
	exprs, _ = builder.getWrappedFullTextMatches(nil, scan2, nil, nil)
	require.Len(t, exprs, 1)
	require.Equal(t, "deep", exprs[0].GetF().Args[0].GetLit().GetSval())

	// A projection MATCH drives too, and a bare projection position is skipped as already served.
	proj := &plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{
		bodyMatch(tag, "bare"),                                  // projids says this is served
		scoreFn("round", bodyMatch(tag, "wrapped"), scoreLit()), // this one needs a stream
	}}
	exprs, _ = builder.getWrappedFullTextMatches(proj, ftScanNodeWithIndex(tag), nil, []int32{0})
	require.Len(t, exprs, 1)
	require.Equal(t, "wrapped", exprs[0].GetF().Args[0].GetLit().GetSval())

	// No scan node, and a MATCH no index can serve, both yield nothing.
	e, d := builder.getWrappedFullTextMatches(nil, nil, nil, nil)
	require.Nil(t, e)
	require.Nil(t, d)
	noIdx := ftScanNodeWithIndex(tag)
	noIdx.TableDef.Indexes = nil
	noIdx.FilterList = []*plan.Expr{scoreFn(">", bodyMatch(tag, "hello"), cst(0.5))}
	e, _ = builder.getWrappedFullTextMatches(nil, noIdx, nil, nil)
	require.Empty(t, e, "a MATCH no index serves is left to raise 20105")
}

// TestResolveProjectMatchesOverJoin: a MATCH in the select list above a JOIN resolves against
// the streams the join's children built — and only against the stream for ITS OWN table.
func TestResolveProjectMatchesOverJoin(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(11)}}}
	const tagA, tagB int32 = 7, 8
	builder.ftJoinServed = []fulltextServedMatch{
		{fn: bodyMatch(tagA, "hello").GetF(), nodeID: 0},
	}

	proj := &plan.Node{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{
		bodyMatch(tagA, "hello"),                               // a's stream
		scoreFn("round", bodyMatch(tagA, "hello"), scoreLit()), // wrapped, same stream
		bodyMatch(tagB, "hello"),                               // SAME text, OTHER table
	}}
	sort := &plan.Node{NodeType: plan.Node_SORT, OrderBy: []*plan.OrderBySpec{
		{Expr: bodyMatch(tagA, "hello")},
	}}

	require.True(t, builder.resolveProjectMatchesOverJoin(proj, sort))
	require.NotNil(t, proj.ProjectList[0].GetCol(), "bare MATCH becomes the score column")
	require.Equal(t, int32(11), proj.ProjectList[0].GetCol().RelPos)
	require.NotNil(t, proj.ProjectList[1].GetF().Args[0].GetCol(), "wrapped MATCH rewritten in place")
	require.NotNil(t, proj.ProjectList[2].GetF(),
		"a MATCH on another table must be LEFT ALONE — matching by column name alone would "+
			"report table a's relevance for table b")
	require.NotNil(t, sort.OrderBy[0].Expr.GetCol(), "ORDER BY MATCH is its own expression")

	// Nothing to resolve against, or nothing to resolve.
	require.False(t, builder.resolveProjectMatchesOverJoin(nil, nil))
	empty := &QueryBuilder{qry: &plan.Query{}}
	require.False(t, empty.resolveProjectMatchesOverJoin(proj, nil))
	plainProj := &plan.Node{ProjectList: []*plan.Expr{scoreLit()}}
	require.False(t, builder.resolveProjectMatchesOverJoin(plainProj, nil))
}

// TestServedFullTextScoreSameTable: the binding-tag-aware lookup used across a join.
func TestServedFullTextScoreSameTable(t *testing.T) {
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{ftScanNode(11)}}}
	served := []fulltextServedMatch{{fn: bodyMatch(7, "hello").GetF(), nodeID: 0}}

	got := builder.servedFullTextScoreSameTable(bodyMatch(7, "hello").GetF(), served)
	require.NotNil(t, got)
	require.Equal(t, int32(11), got.GetCol().RelPos)
	require.Equal(t, int32(1), got.GetCol().ColPos)

	require.Nil(t, builder.servedFullTextScoreSameTable(bodyMatch(8, "hello").GetF(), served),
		"same column name, different binding — not the same question")
	require.Nil(t, builder.servedFullTextScoreSameTable(bodyMatch(7, "world").GetF(), served))
	require.Nil(t, builder.servedFullTextScoreSameTable(nil, served))
	require.Nil(t, builder.servedFullTextScoreSameTable(
		&plan.Function{Func: &plan.ObjectRef{ObjName: "round"}}, served))

	// node id out of range resolves to nothing rather than panicking
	require.Nil(t, builder.servedFullTextScoreSameTable(bodyMatch(7, "hello").GetF(),
		[]fulltextServedMatch{{fn: bodyMatch(7, "hello").GetF(), nodeID: 99}}))
}

// TestNonNegativeConstValue: the guard that keeps `score > -1` (true for a relevance of 0)
// from being treated as membership-implying.
func TestNonNegativeConstValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want float64
		ok   bool
	}{
		{"float", makePlan2Float64ConstExprWithType(0.25), 0.25, true},
		{"zero", makePlan2Float64ConstExprWithType(0), 0, true},
		{"int", makePlan2Int64ConstExprWithType(3), 3, true},
		{"negative float", makePlan2Float64ConstExprWithType(-0.5), 0, false},
		{"negative int", makePlan2Int64ConstExprWithType(-2), 0, false},
		{"not a literal", scoreFn("+", scoreLit(), scoreLit()), 0, false},
		{"string literal", makePlan2StringConstExprWithType("x"), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := nonNegativeConstValue(tc.expr)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.InDelta(t, tc.want, v, 1e-9)
			}
		})
	}
}

// TestConstValueAsFloat covers the literal kinds a score bound can be written as. Unlike
// nonNegativeConstValue this one accepts negatives — it answers "what number is this",
// not "can this predicate imply membership".
func TestConstValueAsFloat(t *testing.T) {
	f32 := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.5}}}}
	u64 := &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 7}}}}

	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want float64
		ok   bool
	}{
		{"dval", makePlan2Float64ConstExprWithType(0.25), 0.25, true},
		{"i64", makePlan2Int64ConstExprWithType(3), 3, true},
		{"negative i64", makePlan2Int64ConstExprWithType(-2), -2, true},
		{"fval", f32, 1.5, true},
		{"u64", u64, 7, true},
		{"not a literal", scoreFn("+", scoreLit(), scoreLit()), 0, false},
		{"string literal", makePlan2StringConstExprWithType("x"), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := constValueAsFloat(tc.expr)
			require.Equal(t, tc.ok, ok)
			if tc.ok {
				require.InDelta(t, tc.want, v, 1e-9)
			}
		})
	}
}

// TestCollectNestedFullTextMatches: the discovery walk descends through scalars and lists,
// which is how a MATCH buried in a projection is found at all.
func TestCollectNestedFullTextMatches(t *testing.T) {
	m := func() *plan.Expr { return bodyMatch(7, "hello") }

	require.Empty(t, collectNestedFullTextMatches(nil, nil))
	require.Empty(t, collectNestedFullTextMatches(scoreFn(">", scoreLit(), scoreLit()), nil))
	require.Len(t, collectNestedFullTextMatches(m(), nil), 1, "the call itself")
	require.Len(t, collectNestedFullTextMatches(
		scoreFn("round", scoreFn("+", m(), scoreLit()), scoreLit()), nil), 1, "nested two deep")
	require.Len(t, collectNestedFullTextMatches(scoreFn("and", m(), m()), nil), 2)
	require.Len(t, collectNestedFullTextMatches(
		&plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{m(), scoreLit()}}}},
		nil), 1, "inside an expression list")
}
