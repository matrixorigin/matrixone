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

func NewOrderBinder(projectionBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderBinder {
	return &OrderBinder{
		ProjectionBinder: projectionBinder,
		selectList:       selectList,
	}
}

func (b *OrderBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	if colRef, ok := astExpr.(*tree.UnresolvedName); ok && colRef.NumParts == 1 {
		if frequency, ok := b.ctx.aliasFrequency[colRef.ColName()]; ok && frequency > 1 {
			return nil, moerr.NewInvalidInputf(b.GetContext(), "Column '%s' in order clause is ambiguous", colRef.ColName())
		}

		if selectItem, ok := b.ctx.aliasMap[colRef.ColName()]; ok {
			for _, selectField := range b.ctx.projectByAst {
				if selectField.aliasName != "" {
					continue
				}
				if projectField, ok1 := selectField.ast.(*tree.UnresolvedName); ok1 && projectField.ColName() == colRef.ColName() {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "Column '%s' in order clause is ambiguous", colRef.ColName())
				}
			}

			return &plan.Expr{
				Typ: b.ctx.projects[selectItem.idx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.projectTag,
						ColPos: selectItem.idx,
					},
				},
			}, nil
		} else {
			// SelectField index used to record matches
			matchedFields := make(map[string]int)
			var matchedExpr *plan.Expr // Used to save matched expr

			for _, selectField := range b.ctx.projectByAst {
				// alias has already been matched earlier, no further processing is needed
				if selectField.aliasName != "" {
					continue
				} else if projectField, ok1 := selectField.ast.(*tree.UnresolvedName); ok1 && projectField.ColName() == colRef.ColName() {
					// Record the selectField index that matches
					field := tree.String(selectField.ast, dialect.MYSQL)
					matchedFields[field] += 1
					// Save matching expr
					matchedExpr = &plan.Expr{
						Typ: b.ctx.projects[selectField.pos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: b.ctx.projectTag,
								ColPos: selectField.pos,
							},
						},
					}
					continue
				}
			}

			// If multiple selectFields are matched, an error occurs
			if len(matchedFields) > 1 {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "Column '%s' in order clause is ambiguous", colRef.ColName())
			}

			// If there is only one matching expr, return that expr
			if matchedExpr != nil {
				return matchedExpr, nil
			}
		}
	}

	if numVal, ok := astExpr.(*tree.NumVal); ok {
		switch numVal.Kind() {
		case tree.Int:
			colPos, _ := numVal.Int64()
			if numVal.Negative() {
				colPos = -colPos
			}
			if colPos < 1 || int(colPos) > len(b.ctx.projects) {
				return nil, moerr.NewSyntaxErrorf(b.GetContext(), "ORDER BY position %v is not in select list", colPos)
			}

			colPos = colPos - 1
			return &plan.Expr{
				Typ: b.ctx.projects[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.projectTag,
						ColPos: int32(colPos),
					},
				},
			}, nil

		default:
			return nil, moerr.NewSyntaxError(b.GetContext(), "non-integer constant in ORDER BY")
		}
	}

	astExpr, err := b.ctx.qualifyColumnNames(astExpr, AliasBeforeColumn)
	if err != nil {
		return nil, err
	}

	expr, err := b.ProjectionBinder.BindExpr(astExpr, 0, true)
	if err != nil {
		return nil, err
	}

	var colPos int32
	var ok bool

	exprStr := expr.String()
	if colPos, ok = b.ctx.projectByExpr[exprStr]; !ok {
		if b.ctx.isDistinct {
			return nil, moerr.NewSyntaxError(b.GetContext(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		}

		colPos = int32(len(b.ctx.projects))
		b.ctx.projectByExpr[exprStr] = colPos
		b.ctx.projects = append(b.ctx.projects, expr)
	}

	expr = &plan.Expr{
		Typ: b.ctx.projects[colPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.projectTag,
				ColPos: colPos,
			},
		},
	}

	return expr, err
}
