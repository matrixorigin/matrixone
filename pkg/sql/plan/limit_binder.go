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

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewLimitBinder(builder *QueryBuilder, ctx *BindContext) *LimitBinder {
	lb := &LimitBinder{}
	lb.sysCtx = builder.GetContext()
	lb.builder = builder
	lb.ctx = ctx
	lb.impl = lb
	return lb
}

func (b *LimitBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	switch astExpr.(type) {
	case *tree.UnqualifiedStar:
		// need check other expr
		return nil, moerr.NewSyntaxError(b.GetContext(), "unsupported expr in limit clause")
	}

	expr, err := b.baseBindExpr(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	if expr.Typ.Id != int32(types.T_uint64) {
		if expr.Typ.Id == int32(types.T_int64) {
			if cExpr, ok := expr.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_I64Val); ok {
					if c.I64Val < 0 {
						return nil, moerr.NewSyntaxError(b.GetContext(), "offset value must be nonnegative")
					}
					//convert to uint64 instead of CAST
					expr = makePlan2Uint64ConstExprWithType(uint64(c.I64Val))
					return expr, nil
				}
			}
		}
		// limit '10' / offset '2'
		// the valid string should be cast to int64
		if expr.Typ.Id == int32(types.T_varchar) || expr.Typ.Id == int32(types.T_int64) {
			targetType := types.T_uint64.ToType()
			planTargetType := makePlan2Type(&targetType)
			var err error
			expr, err = appendCastBeforeExpr(b.GetContext(), expr, planTargetType)
			if err != nil {
				return nil, err
			}
		} else if expr.GetP() != nil {
			targetType := types.T_uint64.ToType()
			planTargetType := makePlan2Type(&targetType)
			return appendCastBeforeExpr(b.GetContext(), expr, planTargetType)
		} else if expr.GetV() != nil {
			// SELECT IFNULL(CAST(@var AS BIGINT), 1) => CASE( ISNULL(@var), 1, CAST(@var AS BIGINT))

			//ISNULL(@var)
			arg0, err := BindFuncExprImplByPlanExpr(b.GetContext(), "isnull", []*plan.Expr{
				expr,
			})
			if err != nil {
				return nil, err
			}

			// CAST( 1 AS BIGINT UNSIGNED)
			arg1 := makePlan2Uint64ConstExprWithType(1)

			// CAST(@var AS BIGINT UNSIGNED)
			targetType := types.T_uint64.ToType()
			planTargetType := makePlan2Type(&targetType)
			arg2, err := appendCastBeforeExpr(b.GetContext(), expr, planTargetType)
			if err != nil {
				return nil, err
			}

			return BindFuncExprImplByPlanExpr(b.GetContext(), "case", []*plan.Expr{
				arg0,
				arg1,
				arg2,
			})
		} else {
			return nil, moerr.NewSyntaxError(b.GetContext(), "only uint64 support in limit/offset clause")
		}
	}

	return expr, nil
}

func (b *LimitBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(b.GetContext(), "column not allowed in limit clause")
}

func (b *LimitBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(b.GetContext(), "aggregate function not allowed in limit clause")
}

func (b *LimitBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(b.GetContext(), "window function not allowed in limit clause")
}

func (b *LimitBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(b.GetContext(), "subquery not allowed in limit clause")
}

func (b *LimitBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
