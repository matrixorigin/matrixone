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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewSelectBinder(ctx *BindContext, agg *AggregateBinder) *SelectBinder {
	b := &SelectBinder{
		agg: agg,
	}
	b.impl = b
	b.ctx = ctx

	return b
}

func (b *SelectBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if colPos, ok := b.ctx.groupMapByAst[astStr]; ok {
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

	if colPos, ok := b.ctx.aggregateMapByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	return b.baseBindExpr(astExpr, depth)
}

func (b *SelectBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32) (*plan.Expr, error) {
	return b.baseBindColRef(astExpr, depth)
}

func (b *SelectBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32) (*plan.Expr, error) {
	return b.agg.BindAggFunc(funcName, astExpr, depth)
}

func (b *SelectBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32) (*plan.Expr, error) {
	return nil, errors.New(errno.WindowingError, "window functions not yet supported")
}
