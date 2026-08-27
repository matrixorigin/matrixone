// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// zeroRelevanceSatisfies is the plan-time half of the guard. It must agree with
// collectDrivingFullTextMatches, which harvests exactly the comparisons where a
// relevance of 0 does NOT satisfy the predicate; if the two ever disagree, a query is
// either refused when it is answerable or answered when it must be refused.
func TestZeroRelevanceSatisfies(t *testing.T) {
	cases := []struct {
		op    string
		bound float64
		want  bool
	}{
		{">", 0, false},   // 0 > 0     -- the ordinary `MATCH > 0`
		{">", 0.5, false}, //
		{">", -1, true},   // 0 > -1    -- every row qualifies, index cannot answer
		{">=", 0, true},   // 0 >= 0    -- every row qualifies
		{">=", 0.001, false},
		{">=", -1, true},
		{"<", 5, true},  // 0 < 5     -- a non-matching row qualifies
		{"<", 0, false}, // 0 < 0
		{"<=", 0, true}, // 0 <= 0
		{"<=", -1, false},
	}
	for _, c := range cases {
		require.Equalf(t, c.want, zeroRelevanceSatisfies(c.op, c.bound),
			"relevance 0 %s %v", c.op, c.bound)
	}
	// An operator that is not a score comparison is never treated as safe.
	require.True(t, zeroRelevanceSatisfies("=", 0))
}

func ftMatchFn(colName string, colPos int32) *plan.Function {
	strTyp, intTyp := types.T_varchar.ToType(), types.T_int64.ToType()
	return &plan.Function{
		Func: &plan.ObjectRef{ObjName: "fulltext_match"},
		Args: []*plan.Expr{
			{Typ: makePlan2Type(&strTyp), Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "fox"}}}},
			{Typ: makePlan2Type(&intTyp), Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}},
			{Typ: makePlan2Type(&strTyp), Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: colPos, Name: colName}}},
		},
	}
}

func ftMatchExpr(fn *plan.Function) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float32)},
		Expr: &plan.Expr_F{F: fn},
	}
}

// cmp builds `<match> <op> <bound>`; flipped puts the bound on the left.
func ftCmp(t *testing.T, b *QueryBuilder, op string, match *plan.Expr, bound *plan.Expr, flipped bool) *plan.Expr {
	args := []*plan.Expr{match, bound}
	if flipped {
		args = []*plan.Expr{bound, match}
	}
	e, err := BindFuncExprImplByPlanExpr(b.GetContext(), op, args)
	require.NoError(t, err)
	return e
}

func TestFulltextRuntimeScoreGuard(t *testing.T) {
	newB := func() *QueryBuilder {
		return NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	}
	lit := func(v float64) *plan.Expr { return makePlan2Float64ConstExprWithType(v) }

	t.Run("a literal threshold needs no guard", func(t *testing.T) {
		b := newB()
		fn := ftMatchFn("body", 1)
		g, err := b.fulltextRuntimeScoreGuard(
			[]*plan.Expr{ftCmp(t, b, ">", ftMatchExpr(fn), lit(0), false)}, fn)
		require.NoError(t, err)
		require.Nil(t, g, "the planner already decided this; no runtime cost")
	})

	t.Run("a runtime threshold yields 0 <op> bound", func(t *testing.T) {
		for _, op := range []string{">", ">="} {
			b := newB()
			fn := ftMatchFn("body", 1)
			g, err := b.fulltextRuntimeScoreGuard(
				[]*plan.Expr{ftCmp(t, b, op, ftMatchExpr(fn), ftScoreParamExpr(0), false)}, fn)
			require.NoError(t, err, op)
			require.NotNil(t, g, op)
			require.Equal(t, op, g.GetF().Func.ObjName, "the operator must be the one written")
			// Binding may wrap the marker in a cast; what matters is that the bound is
			// the parameter and not something re-derived.
			require.True(t, isExecutionConstantExpr(g.GetF().Args[1]),
				"the bound must carry the parameter")
			zero, isLit := constValueAsFloat(g.GetF().Args[0])
			require.True(t, isLit, "the left side must be the relevance-0 constant")
			require.Equal(t, float64(0), zero)
		}
	})

	t.Run("a bound on the left mirrors the operator", func(t *testing.T) {
		b := newB()
		fn := ftMatchFn("body", 1)
		// `? < MATCH` is `MATCH > ?`
		g, err := b.fulltextRuntimeScoreGuard(
			[]*plan.Expr{ftCmp(t, b, "<", ftMatchExpr(fn), ftScoreParamExpr(0), true)}, fn)
		require.NoError(t, err)
		require.NotNil(t, g)
		require.Equal(t, ">", g.GetF().Func.ObjName)
	})

	t.Run("conjuncts are ANDed, not ORed", func(t *testing.T) {
		// `MATCH > ? AND MATCH < ?` is satisfied at relevance 0 only when BOTH are.
		// ORing refuses `> 0 AND < 5`, which the same literals are accepted for.
		b := newB()
		fn := ftMatchFn("body", 1)
		g, err := b.fulltextRuntimeScoreGuard([]*plan.Expr{
			ftCmp(t, b, ">", ftMatchExpr(fn), ftScoreParamExpr(0), false),
			ftCmp(t, b, "<", ftMatchExpr(fn), ftScoreParamExpr(1), false),
		}, fn)
		require.NoError(t, err)
		require.NotNil(t, g)
		require.Equal(t, "and", g.GetF().Func.ObjName)
	})

	t.Run("a literal conjunct that excludes relevance 0 removes the guard", func(t *testing.T) {
		// `MATCH > 0 AND MATCH < ?`: the first conjunct already makes the rewrite safe
		// for every value of ?, so no guard is emitted at all.
		b := newB()
		fn := ftMatchFn("body", 1)
		g, err := b.fulltextRuntimeScoreGuard([]*plan.Expr{
			ftCmp(t, b, ">", ftMatchExpr(fn), lit(0), false),
			ftCmp(t, b, "<", ftMatchExpr(fn), ftScoreParamExpr(0), false),
		}, fn)
		require.NoError(t, err)
		require.Nil(t, g)
	})

	t.Run("a threshold on a DIFFERENT match is ignored", func(t *testing.T) {
		b := newB()
		mine, other := ftMatchFn("body", 1), ftMatchFn("title", 2)
		g, err := b.fulltextRuntimeScoreGuard(
			[]*plan.Expr{ftCmp(t, b, ">", ftMatchExpr(other), ftScoreParamExpr(0), false)}, mine)
		require.NoError(t, err)
		require.Nil(t, g, "another stream's score says nothing about this one")
	})

	t.Run("a per-row bound contributes nothing", func(t *testing.T) {
		b := newB()
		fn := ftMatchFn("body", 1)
		g, err := b.fulltextRuntimeScoreGuard(
			[]*plan.Expr{ftCmp(t, b, ">", ftMatchExpr(fn), ftScoreColExpr(), false)}, fn)
		require.NoError(t, err)
		require.Nil(t, g, "a column has no single value to test relevance 0 against")
	})

	t.Run("AND is descended into", func(t *testing.T) {
		b := newB()
		fn := ftMatchFn("body", 1)
		inner, err := BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{
			ftCmp(t, b, ">", ftMatchExpr(fn), ftScoreParamExpr(0), false),
			ftCmp(t, b, ">", ftMatchExpr(fn), ftScoreParamExpr(1), false),
		})
		require.NoError(t, err)
		g, gerr := b.fulltextRuntimeScoreGuard([]*plan.Expr{inner}, fn)
		require.NoError(t, gerr)
		require.NotNil(t, g)
		require.Equal(t, "and", g.GetF().Func.ObjName)
	})

	t.Run("no score comparison at all", func(t *testing.T) {
		b := newB()
		fn := ftMatchFn("body", 1)
		g, err := b.fulltextRuntimeScoreGuard([]*plan.Expr{ftMatchExpr(fn)}, fn)
		require.NoError(t, err)
		require.Nil(t, g)
	})
}
