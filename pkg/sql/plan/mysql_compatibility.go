// Copyright 2022 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func (builder *QueryBuilder) wrapBareColRefsInAnyValue(expr *plan.Expr, ctx *BindContext) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == ctx.groupTag || exprImpl.Col.RelPos == ctx.aggregateTag {
			return expr
		}
		newExpr, _ := bindFuncExprImplByPlanExpr("any_value", []*plan.Expr{expr})
		colPos := len(ctx.aggregates)
		ctx.aggregates = append(ctx.aggregates, newExpr)
		return &plan.Expr{
			Typ: ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.aggregateTag,
					ColPos: int32(colPos),
				},
			},
		}

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = builder.wrapBareColRefsInAnyValue(arg, ctx)
		}
		return expr

	default:
		return expr
	}
}
