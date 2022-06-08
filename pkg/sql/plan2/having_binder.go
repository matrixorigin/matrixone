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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewHavingBinder(ctx *BindContext) *HavingBinder {
	b := &HavingBinder{
		insideAgg: false,
	}
	b.impl = b
	b.ctx = ctx

	return b
}

func (b *HavingBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

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

	return b.baseBindExpr(astExpr, depth)
}

func (b *HavingBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32) (*plan.Expr, error) {
	if b.insideAgg {
		return b.baseBindColRef(astExpr, depth)
	} else {
		return nil, errors.New(errno.GroupingError, fmt.Sprintf("'%v' must appear in the GROUP BY clause or be used in an aggregate function", tree.String(astExpr, dialect.MYSQL)))
	}
}

func (b *HavingBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32) (expr *plan.Expr, err error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot be nested")
	}

	b.insideAgg = true
	expr, err = b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	b.insideAgg = false

	if err != nil {
		return
	}

	astStr := tree.String(astExpr, dialect.MYSQL)
	b.ctx.aggregateByAst[astStr] = int32(len(b.ctx.aggregates))
	b.ctx.aggregates = append(b.ctx.aggregates, expr)

	return
}

func (b *HavingBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot contain window function calls")
	} else {
		return nil, errors.New(errno.WindowingError, "window functions not allowed here")
	}
}
