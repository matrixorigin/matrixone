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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// use for on duplicate key update clause:  eg: insert into t1 values(1,1),(2,2) on duplicate key update a = a + abs(b), b = values(b)-2
func NewOndupUpdateBinder(sysCtx context.Context, builder *QueryBuilder, ctx *BindContext, tag int32, colName2Idx map[string]int32) *OndupUpdateBinder {
	b := &OndupUpdateBinder{
		tag:         tag,
		colName2Idx: colName2Idx,
	}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *OndupUpdateBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *OndupUpdateBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	col := astExpr.ColName()
	idx, ok := b.colName2Idx[col]
	var typ *Type
	if !ok {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", astExpr.ColNameOrigin())
	}

	return &plan.Expr{
		Typ: *typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.tag,
				ColPos: int32(idx),
				Name:   col,
			},
		},
	}, nil
}

func (b *OndupUpdateBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (b *OndupUpdateBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind window functions '%s'", funcName)
}

func (b *OndupUpdateBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(b.GetContext(), "subquery in JOIN condition")
}

func (b *OndupUpdateBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
