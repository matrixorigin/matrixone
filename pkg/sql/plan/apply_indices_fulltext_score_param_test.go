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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func ftScoreParamExpr(pos int32) *plan.Expr {
	t := types.T_text.ToType()
	return &plan.Expr{Typ: makePlan2Type(&t), Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}}}
}

func ftScoreColExpr() *plan.Expr {
	t := types.T_float64.ToType()
	return &plan.Expr{Typ: makePlan2Type(&t), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 3}}}
}

// The distinction the optimizer rules depend on is execution-constant vs per-row, NOT
// literal vs non-literal. A parameter folds once before a scan and can be pushed as a
// bound; a column reference has no single value and must stay a residual filter.
func TestIsExecutionConstantExpr(t *testing.T) {
	castTo := func(e *plan.Expr, oid types.T) *plan.Expr {
		tt := oid.ToType()
		out, err := makePlan2CastExpr(context.Background(), e, makePlan2Type(&tt))
		require.NoError(t, err)
		return out
	}

	require.True(t, isExecutionConstantExpr(ftScoreParamExpr(0)), "a bare parameter")
	require.True(t, isExecutionConstantExpr(castTo(ftScoreParamExpr(1), types.T_float64)),
		"CAST(? AS DOUBLE) is still one value per execution")

	require.False(t, isExecutionConstantExpr(ftScoreColExpr()),
		"a column varies per row and can never be folded to a bound")
	require.False(t, isExecutionConstantExpr(castTo(ftScoreColExpr(), types.T_float64)),
		"casting a column does not make it constant")
	require.False(t, isExecutionConstantExpr(makePlan2Float64ConstExprWithType(0.5)),
		"a literal is handled by the literal path, not this one")
	require.False(t, isExecutionConstantExpr(nil))

	// A wrapper argument OTHER than the first can make the whole expression vary per
	// row. ROUND's second argument is the digit count: `round(?, per_row_col)` is a
	// different value on every row even though its first argument is a parameter, so
	// it must not be peeled into a single scan-wide bound.
	roundPerRow := &plan.Expr{
		Typ: makePlan2Type(&[]types.Type{types.T_float64.ToType()}[0]),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "round"},
			Args: []*plan.Expr{ftScoreParamExpr(0), ftScoreColExpr()},
		}},
	}
	require.False(t, isExecutionConstantExpr(roundPerRow),
		"round(?, per_row_column) varies per row")

	// ...while a fully constant ROUND is still fine.
	roundConst := &plan.Expr{
		Typ: makePlan2Type(&[]types.Type{types.T_float64.ToType()}[0]),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "round"},
			Args: []*plan.Expr{ftScoreParamExpr(0), makePlan2Float64ConstExprWithType(2)},
		}},
	}
	require.True(t, isExecutionConstantExpr(roundConst),
		"round(?, 2) is one value for the whole execution")
}
