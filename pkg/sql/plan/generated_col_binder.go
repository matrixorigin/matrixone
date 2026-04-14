// Copyright 2025 Matrix Origin
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

// GeneratedColBinder binds expressions for generated columns.
// Unlike DefaultBinder, it resolves each column reference with the
// column's own type instead of a single shared type.
type GeneratedColBinder struct {
	baseBinder
	colNames []string
	colTypes []plan.Type
}

func NewGeneratedColBinder(sysCtx context.Context, colNames []string, colTypes []plan.Type) *GeneratedColBinder {
	b := &GeneratedColBinder{
		colNames: colNames,
		colTypes: colTypes,
	}
	b.sysCtx = sysCtx
	b.impl = b
	return b
}

func (b *GeneratedColBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *GeneratedColBinder) BindColRef(astExpr *tree.UnresolvedName, _ int32, _ bool) (*plan.Expr, error) {
	if astExpr.NumParts > 1 {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "generated column expression cannot use qualified column name '%s'", astExpr.ColNameOrigin())
	}
	col := astExpr.ColName()
	for i, c := range b.colNames {
		if c == col {
			return &plan.Expr{
				Typ: b.colTypes[i],
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}, nil
		}
	}
	return nil, moerr.NewInvalidInputf(b.GetContext(), "column '%s' does not exist or cannot be referenced by a generated column", astExpr.ColNameOrigin())
}

func (b *GeneratedColBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "expression of generated column cannot contain aggregate function '%s'", funcName)
}

func (b *GeneratedColBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "expression of generated column cannot contain window function '%s'", funcName)
}

func (b *GeneratedColBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "expression of generated column cannot contain subquery")
}

func (b *GeneratedColBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "expression of generated column cannot contain time window function '%s'", funcName)
}
