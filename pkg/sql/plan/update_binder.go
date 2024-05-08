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

// use for on duplicate key update clause:  eg: insert into t1 values(1,1),(2,2) on duplicate key update a = a + abs(b), b = values(b)-2
func NewUpdateBinder(sysCtx context.Context, builder *QueryBuilder, ctx *BindContext, cols []*ColDef) *UpdateBinder {
	b := &UpdateBinder{cols: cols}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *UpdateBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *UpdateBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.cols != nil {
		return b.bindColRef(astExpr, depth, isRoot)
	}
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *UpdateBinder) bindColRef(astExpr *tree.UnresolvedName, _ int32, _ bool) (expr *plan.Expr, err error) {
	col := strings.ToLower(astExpr.Parts[0])
	idx := -1
	var typ *Type
	for i, c := range b.cols {
		if c.Name == col {
			idx = i
			typ = &c.Typ
			break
		}
	}
	if idx == -1 {
		err = moerr.NewInvalidInput(b.GetContext(), "column '%s' does not exist", col)
		return
	}
	expr = &plan.Expr{
		Typ: *typ,
	}
	// a = a +1 ,  a = values(a) + 1.  'a' actually have different idx. you need replace to exact idx before eval expr.
	// binder only check the column name exists.
	expr.Expr = &plan.Expr_Col{
		Col: &plan.ColRef{
			RelPos: 0,
			ColPos: int32(idx),
			Name:   col,
		},
	}
	return
}

func (b *UpdateBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (b *UpdateBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "cannot bind window functions '%s'", funcName)
}

func (b *UpdateBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(b.GetContext(), "subquery in JOIN condition")
}

func (b *UpdateBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
