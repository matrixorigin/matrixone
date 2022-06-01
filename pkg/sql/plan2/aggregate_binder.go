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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (b *AggregateBinder) BindExpr(alias string, astExpr tree.Expr, ctx *BindContext) (*plan.Expr, error) {
	if colPos, ok := b.groupByMap[alias]; ok {
		return &plan.Expr{
			Typ:       ctx.groups[colPos].Typ,
			TableName: ctx.groups[colPos].TableName,
			ColName:   alias,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.groupTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.aggregateMap[alias]; ok {
		if !b.insideAgg {
			return &plan.Expr{
				Typ:       ctx.aggregates[colPos].Typ,
				TableName: ctx.aggregates[colPos].TableName,
				ColName:   alias,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.aggregateTag,
						ColPos: colPos,
					},
				},
			}, nil
		} else {
			return nil, errors.New(errno.GroupingError, "aggregate function calls cannot be nested")
		}
	}

	expr, err := b.bindExprImpl(alias, astExpr, ctx)
	if err != nil {
		return nil, err
	}

	return expr, nil
}

func (b *AggregateBinder) BindColRef(alias string, astExpr *tree.UnresolvedName, ctx *BindContext) (*plan.Expr, error) {
	if colId, ok := ctx.groupByName[alias]; ok {
		return &plan.Expr{
			Typ: ctx.groups[colId].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.groupTag,
					ColPos: colId,
				},
			},
		}, nil
	}

	if b.insideAgg {
		return b.tableBinder.BindColRef(alias, astExpr, ctx)
	}

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q does not exist", alias))
}

func (b *AggregateBinder) BindAggFunc(alias, funcName string, astExpr *tree.FuncExpr, ctx *BindContext) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot be nested")
	}

	b.insideAgg = true
	expr, err := b.bindFuncExprImpl(funcName, astExpr, ctx)
	b.insideAgg = false

	if err != nil {
		ctx.aggregates = append(ctx.aggregates, expr)
		ctx.aggregateByName[alias] = int32(len(ctx.aggregates)) - 1
	}

	return expr, err
}

func (b *AggregateBinder) BindWinFunc(alias, funcName string, astExpr *tree.FuncExpr, scope *BindContext) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, errors.New(errno.GroupingError, "aggregate function calls cannot contain window function calls")
	} else {
		return nil, errors.New(errno.GroupingError, "window functions not allowed here")
	}
}
