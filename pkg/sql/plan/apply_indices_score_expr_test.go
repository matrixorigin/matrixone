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

// TestReplaceScoreFnInExpr: the generic half of replaceDistFnInExpr. Vector decides WHAT to
// replace with metric and query-vector tests; fulltext needs only the name, so the two share
// the walk and not the predicate.
func TestReplaceScoreFnInExpr(t *testing.T) {
	col := func() *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float32)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 12, ColPos: 1}},
		}
	}

	// the placeholder itself becomes the score column
	got := replaceScoreFnInExpr(scoreFn("fulltext_match"), "fulltext_match", col)
	require.NotNil(t, got.GetCol())
	require.Equal(t, int32(12), got.GetCol().RelPos)

	// wrapped: the comparison survives, only the inner call is swapped
	pred := scoreFn(">", scoreFn("fulltext_match"), scoreLit())
	got = replaceScoreFnInExpr(pred, "fulltext_match", col)
	require.NotNil(t, got.GetF())
	require.Equal(t, ">", got.GetF().Func.ObjName)
	require.NotNil(t, got.GetF().Args[0].GetCol(), "the inner MATCH must become the score column")

	// every occurrence gets its OWN node, so a later pass mutating one cannot corrupt another
	two := scoreFn("and", scoreFn("fulltext_match"), scoreFn("fulltext_match"))
	got = replaceScoreFnInExpr(two, "fulltext_match", col)
	a, b := got.GetF().Args[0], got.GetF().Args[1]
	require.NotNil(t, a.GetCol())
	require.NotNil(t, b.GetCol())
	require.NotSame(t, a, b)

	// unrelated expressions are untouched
	plain := scoreFn(">", scoreLit(), scoreLit())
	require.Equal(t, plain, replaceScoreFnInExpr(plain, "fulltext_match", col))
	require.Nil(t, replaceScoreFnInExpr(nil, "fulltext_match", col))
}
