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

func NewLimitBinder(builder *QueryBuilder, ctx *BindContext, isOffset bool) *LimitBinder {
	lb := &LimitBinder{}
	lb.sysCtx = builder.GetContext()
	lb.builder = builder
	lb.ctx = ctx
	lb.impl = lb
	lb.isOffset = isOffset
	return lb
}

func (b *LimitBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	switch astExpr.(type) {
	case *tree.UnqualifiedStar:
		return nil, moerr.NewSyntaxError(b.GetContext(), "unsupported expr in limit clause")
	}

	// Handle LIMIT -N / OFFSET -N: unary minus applied to a numeric literal.
	// The parser produces tree.UnaryExpr{UNARY_MINUS, NumVal}, and baseBindExpr
	// does not fold this into a negative literal, so we must intercept it here.
	if unary, ok := astExpr.(*tree.UnaryExpr); ok && unary.Op == tree.UNARY_MINUS {
		if _, ok := unary.Expr.(*tree.NumVal); ok {
			clause := "LIMIT"
			if b.isOffset {
				clause = "OFFSET"
			}
			return nil, moerr.NewSyntaxErrorf(b.GetContext(),
				"%s must be a non-negative integer", clause)
		}
	}

	expr, err := b.baseBindExpr(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	// NULL check: reject NULL in LIMIT/OFFSET with a clear message.
	if cExpr, ok := expr.Expr.(*plan.Expr_Lit); ok && cExpr.Lit.GetIsnull() {
		return nil, moerr.NewSyntaxError(b.GetContext(), "LIMIT/OFFSET cannot be NULL")
	}

	if expr.Typ.Id != int32(types.T_uint64) {
		if expr.Typ.Id == int32(types.T_int64) {
			if cExpr, ok := expr.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_I64Val); ok {
					if c.I64Val < 0 {
						clause := "LIMIT"
						if b.isOffset {
							clause = "OFFSET"
						}
						return nil, moerr.NewSyntaxErrorf(b.GetContext(),
							"%s must be a non-negative integer", clause)
					}
					// convert to uint64 instead of CAST
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
			arg0, err := BindFuncExprImplByPlanExpr(b.GetContext(), "isnull", []*plan.Expr{
				expr,
			})
			if err != nil {
				return nil, err
			}

			arg1 := makePlan2Uint64ConstExprWithType(1)

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
			clause := "LIMIT"
			if b.isOffset {
				clause = "OFFSET"
			}
			return nil, moerr.NewSyntaxErrorf(b.GetContext(),
				"only uint64 support in %s clause", clause)
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
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
