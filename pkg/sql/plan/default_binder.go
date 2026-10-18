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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewDefaultBinder(sysCtx context.Context, builder *QueryBuilder, ctx *BindContext, typ Type, cols []string) *DefaultBinder {
	b := &DefaultBinder{typ: typ, cols: cols}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

// NewDefaultBinderWithColumns binds an expression default against the
// complete row schema. A default expression is evaluated with the value of
// each referenced column in the same row, so resolving it against an outer
// query would either report an ambiguous column or bind the wrong relation.
// The referenced column types are retained because they need not match the
// destination column's type. The schema is borrowed for synchronous binding;
// callers keep it stable until binding completes.
func NewDefaultBinderWithColumns(sysCtx context.Context, typ Type, cols []*ColDef) *DefaultBinder {
	b := NewDefaultBinder(sysCtx, nil, nil, typ, nil)
	b.columns = cols
	if b.columns == nil {
		// Even an empty row schema is a local scope, not an outer query.
		b.columns = []*ColDef{}
	}
	return b
}

func (b *DefaultBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *DefaultBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.columns != nil || b.cols != nil {
		return b.bindColRef(astExpr, depth, isRoot)
	}
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *DefaultBinder) bindColRef(astExpr *tree.UnresolvedName, _ int32, _ bool) (expr *plan.Expr, err error) {
	if astExpr.NumParts > 1 {
		return nil, moerr.NewInvalidInputf(
			b.GetContext(),
			"default expression cannot use qualified column name '%s'",
			astExpr.ColNameOrigin(),
		)
	}
	col := astExpr.ColName()
	idx := -1
	typ := b.typ
	if b.columns != nil {
		for i, c := range b.columns {
			if c != nil && strings.EqualFold(c.Name, col) {
				idx, typ = i, c.Typ
				break
			}
		}
	} else {
		for i, name := range b.cols {
			if strings.EqualFold(name, col) {
				idx = i
				break
			}
		}
	}
	if idx == -1 {
		err = moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", astExpr.ColNameOrigin())
		return
	}
	expr = &plan.Expr{Typ: typ}
	expr.Expr = &plan.Expr_Col{
		Col: &plan.ColRef{
			RelPos: 0,
			ColPos: int32(idx),
			Name:   col,
		},
	}
	return
}

func (b *DefaultBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (b *DefaultBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind window functions '%s'", funcName)
}

func (b *DefaultBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	if !b.allowSubquery {
		return nil, moerr.NewNYI(b.GetContext(), "subquery in JOIN condition")
	}
	if err := b.prepareValuesSubqueryContext(); err != nil {
		return nil, err
	}
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *DefaultBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
