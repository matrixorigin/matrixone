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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func NewHavingBinder(builder *QueryBuilder, ctx *BindContext) *HavingBinder {
	b := &HavingBinder{
		insideAgg: false,
	}
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *HavingBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if !b.insideAgg {
		if colPos, ok := b.ctx.groupByAst[astStr]; ok {
			return &plan.Expr{
				Typ: b.ctx.groups[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.groupTag,
						ColPos: colPos,
					},
				},
			}, nil
		}
	}

	if colPos, ok := b.ctx.aggregateByAst[astStr]; ok {
		if !b.insideAgg {
			return &plan.Expr{
				Typ: b.ctx.aggregates[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.aggregateTag,
						ColPos: colPos,
					},
				},
			}, nil
		} else {
			return nil, errors.New(errno.GroupingError, "aggregate function calls cannot be nested")
		}
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *HavingBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, errors.New(errno.GroupingError, "correlated columns in aggregate function not yet supported")
		}

		return expr, nil
	} else {
		return nil, errors.New(errno.GroupingError, fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", tree.String(astExpr, dialect.MYSQL)))
	}
}

func (b *HavingBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot be nested")
	}

	b.insideAgg = true
	expr, err := b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		expr.GetF().Func.Obj = int64(int64(uint64(expr.GetF().Func.Obj) | function.Distinct))
	}
	b.insideAgg = false

	colPos := int32(len(b.ctx.aggregates))
	astStr := tree.String(astExpr, dialect.MYSQL)
	b.ctx.aggregateByAst[astStr] = colPos
	b.ctx.aggregates = append(b.ctx.aggregates, expr)

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.aggregateTag,
				ColPos: colPos,
			},
		},
	}, nil
}

func (b *HavingBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot contain window function calls")
	} else {
		return nil, errors.New(errno.WindowingError, "window functions not allowed here")
	}
}

func (b *HavingBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}
