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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewGroupBinder(builder *QueryBuilder, ctx *BindContext, selectList tree.SelectExprs) *GroupBinder {
	b := &GroupBinder{}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b
	b.selectList = selectList
	b.projectionExprPos = -1

	return b
}

func (b *GroupBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	var numericTarget *plan.Type
	reusesProjection := false
	if isRoot && b.projectionExprPos >= 0 {
		pos := b.projectionExprPos
		astExpr = b.selectList[pos].Expr
		reusesProjection = true
		if int(pos) < len(b.ctx.numericProjectionTypes) {
			target := b.ctx.numericProjectionTypes[pos]
			if target.Id != 0 {
				numericTarget = &target
			}
		}
	}
	if isRoot {
		if numVal, ok := astExpr.(*tree.NumVal); ok {
			switch numVal.Kind() {
			case tree.Int:
				reusesProjection = true
				colPos, _ := numVal.Int64()
				if colPos < 1 || int(colPos) > len(b.selectList) {
					return nil, moerr.NewSyntaxErrorf(b.GetContext(), "GROUP BY position %v is not in select list", colPos)
				}

				astExpr = b.selectList[colPos-1].Expr
				if int(colPos) <= len(b.ctx.numericProjectionTypes) {
					target := b.ctx.numericProjectionTypes[colPos-1]
					if target.Id != 0 {
						numericTarget = &target
					}
				}

			case tree.Unknown:
				if numVal.ValType != tree.P_null {
					return nil, moerr.NewSyntaxError(b.GetContext(), "non-integer constant in GROUP BY")
				}

			default:
				return nil, moerr.NewSyntaxError(b.GetContext(), "non-integer constant in GROUP BY")
			}
		}
	}

	var expr *plan.Expr
	var err error
	if numericTarget != nil {
		expr, err = b.bindNumericExprWithContext(astExpr, depth, numericTarget)
	} else {
		expr, err = b.baseBindExpr(astExpr, depth, isRoot)
	}
	if err != nil {
		return nil, err
	}

	if isRoot && !b.ctx.isGroupingSet {
		astStr := tree.String(astExpr, dialect.MYSQL)
		// Independently written prepared expressions have different parameter
		// identities even when their formatted SQL is identical. Ordinal and alias
		// GROUP BY references are guaranteed to reuse the SELECT expression itself.
		hasParam := containsDynamicParam(expr)
		registerAst := reusesProjection || !hasParam
		if registerAst {
			if _, ok := b.ctx.groupByAst[astStr]; ok {
				return nil, nil
			}
			b.ctx.groupByAst[astStr] = int32(len(b.ctx.groups))
		}
		if hasParam {
			key := parameterizedGroupByKey(astStr, expr)
			if _, ok := b.ctx.groupByParamAst[key]; ok {
				return nil, nil
			}
			b.ctx.groupByParamAst[key] = int32(len(b.ctx.groups))
		}
		if !registerAst {
			b.ctx.groups = append(b.ctx.groups, expr)
			return nil, nil
		}
		b.ctx.groups = append(b.ctx.groups, expr)
	}

	if isRoot && b.ctx.isGroupingSet {
		astStr := tree.String(astExpr, dialect.MYSQL)
		pos, ok := b.ctx.groupByAst[astStr]
		if containsDynamicParam(expr) {
			pos, ok = b.ctx.groupByParamAst[parameterizedGroupByKey(astStr, expr)]
		}
		if !ok || int(pos) >= len(b.ctx.groupingFlag) {
			return nil, moerr.NewInternalErrorf(b.GetContext(), "grouping expression position not found: %s", astStr)
		}
		b.ctx.groupingFlag[pos] = true
	}

	return expr, err
}

func parameterizedGroupByKey(ast string, expr *plan.Expr) string {
	positions := make([]int32, 0, 2)
	collectGroupByParamPositions(expr, &positions)
	var key strings.Builder
	key.WriteString(ast)
	for _, pos := range positions {
		key.WriteByte('#')
		key.WriteString(strconv.FormatInt(int64(pos), 10))
	}
	return key.String()
}

func collectGroupByParamPositions(expr *plan.Expr, positions *[]int32) {
	if expr == nil {
		return
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_P:
		*positions = append(*positions, item.P.Pos)
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			collectGroupByParamPositions(arg, positions)
		}
	}
}

func (b *GroupBinder) BindProjectionExpr(pos int32) (*plan.Expr, error) {
	b.projectionExprPos = pos
	defer func() { b.projectionExprPos = -1 }()
	return b.BindExpr(b.selectList[pos].Expr, 0, true)
}

func (b *GroupBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	expr, err := b.baseBindColRef(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
		return nil, moerr.NewNYI(b.GetContext(), "correlated columns in GROUP BY clause")
	}

	return expr, nil
}

func (b *GroupBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "GROUP BY clause cannot contain aggregate functions")
}

func (b *GroupBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "GROUP BY clause cannot contain window functions")
}

func (b *GroupBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(b.GetContext(), "subquery in GROUP BY clause")
}

func (b *GroupBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
