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

// distinctOrderBinder binds an ORDER BY expression against the output of a
// DISTINCT projection. It deliberately exposes only select-list aliases and
// directly selected columns; it never falls back to the input-table scope.
type distinctOrderBinder struct {
	baseBinder
}

var errDistinctOrderNotProjected = moerr.NewInternalErrorNoCtx("DISTINCT ORDER BY expression is not available from the projection")

var _ Binder = (*distinctOrderBinder)(nil)

func newDistinctOrderBinder(projectionBinder *ProjectionBinder) *distinctOrderBinder {
	b := &distinctOrderBinder{}
	b.sysCtx = projectionBinder.sysCtx
	b.builder = projectionBinder.builder
	b.ctx = projectionBinder.ctx
	b.impl = b
	return b
}

func (b *distinctOrderBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	if _, ok := astExpr.(*tree.FullTextMatchExpr); ok {
		return nil, errDistinctOrderNotProjected
	}
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *distinctOrderBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.NumParts == 1 {
		name := astExpr.ColName()
		if isRoot {
			if b.ctx.aliasFrequency[name] > 1 {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "Column '%s' in order clause is ambiguous", name)
			}
			if selectItem, ok := b.ctx.aliasMap[name]; ok {
				return makeProjectColRef(b.ctx, selectItem.idx), nil
			}
		} else if _, found := b.ctx.bindingByCol[name]; !found {
			if b.ctx.aliasFrequency[name] > 1 {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "Column '%s' in order clause is ambiguous", name)
			}
			if selectItem, ok := b.ctx.aliasMap[name]; ok {
				return makeProjectColRef(b.ctx, selectItem.idx), nil
			}
		}
	}

	qualified, err := b.ctx.qualifyColumnNames(astExpr, NoAlias)
	if err != nil {
		return nil, err
	}
	if pos, ok := b.ctx.projectColByAst[windowExprAstKey(qualified)]; ok {
		return makeProjectColRef(b.ctx, pos), nil
	}
	return nil, errDistinctOrderNotProjected
}

func (b *distinctOrderBinder) BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error) {
	return nil, errDistinctOrderNotProjected
}

func (b *distinctOrderBinder) BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error) {
	return nil, errDistinctOrderNotProjected
}

func (b *distinctOrderBinder) BindSubquery(*tree.Subquery, bool) (*plan.Expr, error) {
	return nil, errDistinctOrderNotProjected
}

func (b *distinctOrderBinder) BindTimeWindowFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error) {
	return nil, errDistinctOrderNotProjected
}

func makeProjectColRef(ctx *BindContext, pos int32) *plan.Expr {
	return GetColExpr(ctx.projects[pos].Typ, ctx.projectTag, pos)
}

func NewOrderBinder(projectionBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderBinder {
	return &OrderBinder{
		ProjectionBinder: projectionBinder,
		selectList:       selectList,
	}
}

func (b *OrderBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	rootExpr := astExpr
	if b.ctx.isDistinct {
		rootExpr = unwrapParenExpr(rootExpr)
	}
	if colRef, ok := rootExpr.(*tree.UnresolvedName); ok && colRef.NumParts == 1 {
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

	if b.ctx.isDistinct {
		if b.distinctBinder == nil {
			b.distinctBinder = newDistinctOrderBinder(b.ProjectionBinder)
		}
		distinctExpr, distinctErr := b.distinctBinder.BindExpr(astExpr, 0, true)
		if distinctErr == nil {
			return distinctExpr, nil
		}
		if distinctErr != errDistinctOrderNotProjected {
			return nil, distinctErr
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

	exprKey, err := projectExprKey(expr)
	if err != nil {
		return nil, err
	}

	if colPos, ok = b.ctx.projectByExpr[exprKey]; !ok {
		if b.ctx.isDistinct {
			return nil, moerr.NewSyntaxError(b.GetContext(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		}

		colPos = int32(len(b.ctx.projects))
		b.ctx.projectByExpr[exprKey] = colPos
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
