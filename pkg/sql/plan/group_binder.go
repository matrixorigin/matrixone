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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewGroupBinder(builder *QueryBuilder, ctx *BindContext) *GroupBinder {
	b := &GroupBinder{}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *GroupBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	expr, err := b.baseBindExpr(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	if isRoot {
		astStr := tree.String(astExpr, dialect.MYSQL)
		if _, ok := b.ctx.groupByAst[astStr]; ok {
			return nil, nil
		}

		b.ctx.groupByAst[astStr] = int32(len(b.ctx.groups))
		b.ctx.groups = append(b.ctx.groups, expr)
	}

	return expr, err
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
