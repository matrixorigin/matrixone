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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestXMLFunctionBinding(t *testing.T) {
	for _, sql := range []string{
		`select extractvalue('<a>x</a>', '/a')`,
		`select extractvalue('<a>', concat('/', 'a'))`,
		`select updatexml('<a>x</a>', '/a', '<b/>')`,
		`select extractvalue(null, '[')`, // Must remain runtime validation, not folded NULL.
	} {
		p, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err, sql)
		name := "extractvalue"
		if sql[7] == 'u' {
			name = "updatexml"
		}
		expr := findPlanFunctionExpr(p, name)
		require.NotNil(t, expr, sql)
		require.False(t, expr.Typ.NotNullable, sql)
		require.Equal(t, int32(types.T_text), expr.Typ.Id, sql)
		require.Equal(t, int32(types.MaxLongTextLen), expr.Typ.Width, sql)
	}
	for _, sql := range []string{
		`select extractvalue('<a/>', p) from (select '/a' as p) q`,
		`select updatexml('<a/>', p, '<b/>') from (select '/a' as p) q`,
	} {
		_, err := runOneStmt(NewMockOptimizer(false), t, sql)
		require.ErrorContains(t, err, "Only constant XPATH queries are supported")
	}
	ctx := context.Background()
	param := &Expr{Typ: planpb.Type{Id: int32(types.T_any)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	expr, err := BindFuncExprImplByPlanExpr(ctx, "extractvalue", []*Expr{makePlan2StringConstExprWithType("<a>x</a>"), param})
	require.NoError(t, err)
	require.NotNil(t, expr.GetF())
	fn, ok := function.GetFunctionByIdWithoutError(expr.GetF().Func.Obj)
	require.True(t, ok)
	require.True(t, fn.CannotFold())
}
