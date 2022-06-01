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

func (b *TableBinder) BindColRef(alias string, astExpr *tree.UnresolvedName, ctx *BindContext) (*plan.Expr, error) {
	var table, col string
	col = astExpr.Parts[0]
	if astExpr.NumParts > 1 {
		table = astExpr.Parts[1]
	}

	if len(table) == 0 {
		if using, ok := ctx.usingCols[col]; ok {
			if len(using) > 1 {
				panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", tree.String(astExpr, dialect.MYSQL))))
			}
			binding := using[0].primary
			colId := binding.FindColumn(col)
			return &plan.Expr{
				Typ: binding.types[colId],
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: binding.tag,
						ColPos: colId,
					},
				},
			}, nil
		} else {
			var found *Binding
			colId := NotFound
			for _, binding := range ctx.bindings {
				j := binding.FindColumn(col)
				if j == AmbiguousName {
					panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", tree.String(astExpr, dialect.MYSQL))))
				}
				if j != NotFound {
					if colId != NotFound {
						panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", tree.String(astExpr, dialect.MYSQL))))
					} else {
						found = binding
						colId = j
					}
				}
			}
			if colId != NotFound {
				return &plan.Expr{
					Typ: found.types[colId],
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: found.tag,
							ColPos: colId,
						},
					},
				}, nil
			}
		}
	} else {
		if binding, ok := ctx.bindingsByName[table]; ok {
			colId := binding.FindColumn(col)
			if colId != NotFound {
				return &plan.Expr{
					Typ: binding.types[colId],
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: binding.tag,
							ColPos: colId,
						},
					},
				}, nil
			}
		}
	}

	return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q does not exist", tree.String(astExpr, dialect.MYSQL)))
}

func (b *TableBinder) BindExpr(alias string, astExpr tree.Expr, ctx *BindContext) (*plan.Expr, error) {
	return b.bindExprImpl(alias, astExpr, ctx)
}

func (b *TableBinder) BindAggFunc(alias, funcName string, astExpr *tree.FuncExpr, ctx *BindContext) (*plan.Expr, error) {
	return nil, errors.New(errno.GroupingError, "aggregate functions not allowed here")
}

func (b *TableBinder) BindWinFunc(alias, funcName string, astExpr *tree.FuncExpr, ctx *BindContext) (*plan.Expr, error) {
	return nil, errors.New(errno.GroupingError, "window functions not allowed here")
}
