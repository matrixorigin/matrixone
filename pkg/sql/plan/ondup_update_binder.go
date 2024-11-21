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
func NewOndupUpdateBinder(sysCtx context.Context, builder *QueryBuilder, ctx *BindContext, scanTag, selectTag int32, tableDef *plan.TableDef) *OndupUpdateBinder {
	b := &OndupUpdateBinder{
		scanTag:   scanTag,
		selectTag: selectTag,
		tableDef:  tableDef,
	}
	b.sysCtx = sysCtx
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *OndupUpdateBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	if funcExpr, ok := astExpr.(*tree.FuncExpr); ok {
		funcRef, ok := funcExpr.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, moerr.NewNYIf(b.GetContext(), "function expr '%v'", astExpr)
		}

		if funcRef.ColName() == "values" {
			if len(funcExpr.Exprs) != 1 {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", funcExpr.Exprs)
			}

			col, ok := funcExpr.Exprs[0].(*tree.UnresolvedName)
			if !ok {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", funcExpr.Exprs[0])
			}

			colName := col.ColName()
			idx, ok := b.tableDef.Name2ColIndex[colName]
			if !ok {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", col.ColNameOrigin())
			}

			return &plan.Expr{
				Typ: b.tableDef.Cols[idx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.selectTag,
						ColPos: int32(idx),
						Name:   colName,
					},
				},
			}, nil
		}
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *OndupUpdateBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	colName := astExpr.ColName()
	idx, ok := b.tableDef.Name2ColIndex[colName]
	if !ok {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist", astExpr.ColNameOrigin())
	}

	return &plan.Expr{
		Typ: b.tableDef.Cols[idx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.scanTag,
				ColPos: int32(idx),
				Name:   colName,
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
