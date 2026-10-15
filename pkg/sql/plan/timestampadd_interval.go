// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func timestampAddSecondUnit(args []*Expr) bool {
	if len(args) == 0 || args[0] == nil {
		return false
	}
	unit, known := timestampAddUnitFromPlanExpr(args[0])
	return known && unit == types.Second
}

// Preserve SECOND's decimal evaluation context before the ordinary integer
// parameter binder runs. Released TIMESTAMPADD overloads still consume int64;
// the rewritten unit makes the normalized count and result precision explicit.
func bindTimestampAddSecond(ctx context.Context, args []*Expr) ([]*Expr, error) {
	if len(args) != 3 || !timestampAddSecondUnit(args) || args[1] == nil || decimalIntervalSourceIsBinary(args[1]) {
		return args, nil
	}
	source := types.T(args[1].Typ.Id)
	if !source.IsDecimal() && !source.IsFloat() && !source.IsMySQLString() && source != types.T_any {
		return args, nil
	}
	count, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd_second_interval", []*Expr{args[1]})
	if err != nil {
		return nil, err
	}
	return []*Expr{MakePlan2StringConstExprWithType("MICROSECOND"), count, args[2]}, nil
}

// HEX/BIT literals and ENUM/SET columns retain numeric storage semantics. Bind
// them before transparent selectors reconcile their branches, while keeping
// fractional sources and explicit casts in their own value domains.
func (b *baseBinder) bindTimestampAddSecondSourceAst(ast tree.Expr, depth int32) (*Expr, error) {
	ctx := b.GetContext()
	switch value := unwrapParenExpr(ast).(type) {
	case *tree.CaseExpr:
		args := make([]*Expr, 0, 2*len(value.Whens)+1)
		for _, when := range value.Whens {
			condition := bindCaseConditionParam(when.Cond)
			if value.Expr != nil {
				condition = tree.NewComparisonExpr(tree.EQUAL, value.Expr, when.Cond)
			}
			predicate, err := b.impl.BindExpr(condition, depth, false)
			if err != nil {
				return nil, err
			}
			branch, err := b.bindTimestampAddSecondSourceAst(when.Val, depth)
			if err != nil {
				return nil, err
			}
			args = append(args, predicate, branch)
		}
		otherwise := value.Else
		if otherwise == nil {
			otherwise = tree.NewNumVal("", "", false, tree.P_null)
		}
		branch, err := b.bindTimestampAddSecondSourceAst(otherwise, depth)
		if err != nil {
			return nil, err
		}
		return BindFuncExprImplByPlanExpr(ctx, "case", append(args, branch))
	case *tree.FuncExpr:
		name := numericAstFunctionName(value)
		if name == "nullif" && len(value.Exprs) == 2 {
			predicate, err := b.impl.BindExpr(tree.NewComparisonExpr(tree.EQUAL, value.Exprs[0], value.Exprs[1]), depth, false)
			if err != nil {
				return nil, err
			}
			branch, err := b.bindTimestampAddSecondSourceAst(value.Exprs[0], depth)
			if err != nil {
				return nil, err
			}
			return BindFuncExprImplByPlanExpr(ctx, "case", []*Expr{predicate, makePlan2NullConstExprWithType(), branch})
		}
		if (name == "if" || name == "iff") && len(value.Exprs) == 3 {
			predicate, err := b.impl.BindExpr(bindCaseConditionParam(value.Exprs[0]), depth, false)
			if err != nil {
				return nil, err
			}
			yes, err := b.bindTimestampAddSecondSourceAst(value.Exprs[1], depth)
			if err != nil {
				return nil, err
			}
			no, err := b.bindTimestampAddSecondSourceAst(value.Exprs[2], depth)
			if err != nil {
				return nil, err
			}
			return BindFuncExprImplByPlanExpr(ctx, name, []*Expr{predicate, yes, no})
		}
	}
	source, err := b.impl.BindExpr(ast, depth, false)
	if err != nil {
		return nil, err
	}
	source, err = b.mysqlSpecialNumericOperand(ctx, source)
	if err != nil {
		return nil, err
	}
	if decimalIntervalSourceIsBinary(source) || types.T(source.Typ.Id) == types.T_enum {
		return appendIntegerArgument(ctx, source, types.T_int64, false)
	}
	return source, nil
}
