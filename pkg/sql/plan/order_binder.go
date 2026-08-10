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
			if pos, found, err := resolveOrderOutputName(b.GetContext(), b.ctx, name); err != nil {
				return nil, err
			} else if found {
				return makeProjectColRef(b.ctx, pos), nil
			}
		} else if _, found := b.ctx.bindingByCol[name]; !found {
			if selectItem, found, err := resolveOrderAlias(b.GetContext(), b.ctx, name); err != nil {
				return nil, err
			} else if found {
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

// resolveOrderAlias applies MySQL's duplicate-output-name reduction. Direct
// column aliases remain candidates until they disagree (ambiguous) or an
// expression alias appears; the first expression alias then wins. Repeated
// references to the same source column are equivalent.
func resolveOrderAlias(sysCtx context.Context, ctx *BindContext, name string) (*aliasItem, bool, error) {
	item, found, ambiguous, err := inspectOrderAlias(sysCtx, ctx, name)
	if err != nil {
		return nil, false, err
	}
	if ambiguous {
		return nil, false, ambiguousOrderColumn(sysCtx, name)
	}
	return item, found, nil
}

func inspectOrderAlias(sysCtx context.Context, ctx *BindContext, name string) (*aliasItem, bool, bool, error) {
	var selected *SelectField
	for i := range ctx.projectByAst {
		field := &ctx.projectByAst[i]
		if field.aliasName != name {
			continue
		}
		if selected == nil {
			selected = field
			continue
		}
		if !isDirectOrderColumn(selected.ast) {
			continue
		}
		if !isDirectOrderColumn(field.ast) {
			selected = field
			continue
		}
		same, err := sameOrderProject(sysCtx, ctx, selected.pos, field.pos)
		if err != nil {
			return nil, false, false, err
		}
		if !same {
			return nil, true, true, nil
		}
	}
	if selected == nil {
		if len(ctx.projectByAst) > 0 {
			return nil, false, false, nil
		}
		// Set-operation contexts have output headings but no source select-list
		// AST. Preserve their existing positional lookup and ambiguity rule.
		item, found := ctx.aliasMap[name]
		if !found {
			return nil, false, false, nil
		}
		if ctx.aliasFrequency[name] > 1 {
			return nil, true, true, nil
		}
		return item, true, false, nil
	}

	return &aliasItem{idx: selected.pos, astExpr: selected.ast}, true, false, nil
}

// resolveOrderOutputName applies the top-level ORDER BY name rule. Explicit
// aliases take precedence over natural output names. If the winning explicit
// alias is a direct column, other direct-column outputs with the same name must
// denote the same bound expression; expression aliases remain first-match.
func resolveOrderOutputName(sysCtx context.Context, ctx *BindContext, name string) (int32, bool, error) {
	alias, found, err := resolveOrderAlias(sysCtx, ctx, name)
	if err != nil {
		return 0, false, err
	}
	if found {
		if isDirectOrderColumn(alias.astExpr) {
			for i := range ctx.projectByAst {
				field := &ctx.projectByAst[i]
				if field.aliasName != "" || !isNaturalOrderOutput(field.ast, name) {
					continue
				}
				same, keyErr := sameOrderProject(sysCtx, ctx, alias.idx, field.pos)
				if keyErr != nil {
					return 0, false, keyErr
				}
				if !same {
					return 0, false, ambiguousOrderColumn(sysCtx, name)
				}
			}
		}
		return alias.idx, true, nil
	}

	var first *SelectField
	for i := range ctx.projectByAst {
		field := &ctx.projectByAst[i]
		if field.aliasName != "" || !isNaturalOrderOutput(field.ast, name) {
			continue
		}
		if first == nil {
			first = field
			continue
		}
		same, keyErr := sameOrderProject(sysCtx, ctx, first.pos, field.pos)
		if keyErr != nil {
			return 0, false, keyErr
		}
		if !same {
			return 0, false, ambiguousOrderColumn(sysCtx, name)
		}
	}
	if first != nil {
		return first.pos, true, nil
	}
	return 0, false, nil
}

func isDirectOrderColumn(astExpr tree.Expr) bool {
	name, ok := unwrapParenExpr(astExpr).(*tree.UnresolvedName)
	return ok && !name.Star
}

func isNaturalOrderOutput(astExpr tree.Expr, name string) bool {
	column, ok := unwrapParenExpr(astExpr).(*tree.UnresolvedName)
	return ok && !column.Star && column.ColName() == name
}

func sameOrderProject(sysCtx context.Context, ctx *BindContext, left, right int32) (bool, error) {
	if left >= 0 && right >= 0 && int(left) < len(ctx.projectSemanticKeys) && int(right) < len(ctx.projectSemanticKeys) {
		return ctx.projectSemanticKeys[left] == ctx.projectSemanticKeys[right], nil
	}
	if left < 0 || right < 0 || int(left) >= len(ctx.projects) || int(right) >= len(ctx.projects) {
		return false, moerr.NewInternalError(sysCtx, "ORDER BY select item is outside projection")
	}
	leftKey, err := projectExprKey(ctx.projects[left])
	if err != nil {
		return false, err
	}
	rightKey, err := projectExprKey(ctx.projects[right])
	if err != nil {
		return false, err
	}
	return leftKey == rightKey, nil
}

func ambiguousOrderColumn(sysCtx context.Context, name string) error {
	return moerr.NewInvalidInputf(sysCtx, "Column '%s' in order clause is ambiguous", name)
}

// qualifyOrderExpression applies the source-column-first rule used inside a
// compound ORDER BY expression. When preserveAliases is true, fallback aliases
// remain names and selectedAliases records the precise projected item to use;
// the grouping-set DISTINCT path needs that form to stay inside the visible
// projection. Otherwise aliases expand to their source AST for regular binding.
func qualifyOrderExpression(
	sysCtx context.Context,
	ctx *BindContext,
	astExpr tree.Expr,
	protectGroupingArgs bool,
	preserveAliases bool,
) (tree.Expr, map[string]*aliasItem, error) {
	if !protectGroupingArgs && !preserveAliases {
		hasDuplicateAlias := false
		for _, frequency := range ctx.aliasFrequency {
			if frequency > 1 {
				hasDuplicateAlias = true
				break
			}
		}
		if !hasDuplicateAlias {
			qualified, err := ctx.qualifyColumnNames(astExpr, AliasAfterColumn)
			return qualified, nil, err
		}
	}

	qualified := astExpr
	if protectGroupingArgs || preserveAliases {
		qualified = cloneTreeExpr(astExpr)
	}
	if protectGroupingArgs {
		var bindErr error
		walkGroupingSetOrderByExpr(qualified, func(expr tree.Expr) bool {
			function, ok := expr.(*tree.FuncExpr)
			if !ok || function.FuncName == nil || function.FuncName.Compare() != "grouping" {
				return true
			}
			for i := range function.Exprs {
				function.Exprs[i], bindErr = ctx.qualifyColumnNames(function.Exprs[i], NoAlias)
				if bindErr != nil {
					return false
				}
			}
			return false
		})
		if bindErr != nil {
			return nil, nil, bindErr
		}
	}

	fallbackNames := make(map[string]struct{})
	walkGroupingSetOrderByExpr(qualified, func(expr tree.Expr) bool {
		if _, subquery := expr.(*tree.Subquery); subquery {
			return false
		}
		if protectGroupingArgs {
			if function, ok := expr.(*tree.FuncExpr); ok && function.FuncName != nil && function.FuncName.Compare() == "grouping" {
				return false
			}
		}
		name, ok := expr.(*tree.UnresolvedName)
		if !ok || name.Star || name.NumParts != 1 {
			return true
		}
		if _, sourceExists := ctx.bindingByCol[name.ColName()]; sourceExists {
			return true
		}
		fallbackNames[name.ColName()] = struct{}{}
		return true
	})

	orderAliasMap := make(map[string]*aliasItem, len(fallbackNames))
	for name := range fallbackNames {
		item, found, ambiguous, err := inspectOrderAlias(sysCtx, ctx, name)
		if err != nil {
			return nil, nil, err
		}
		if ambiguous {
			return nil, nil, ambiguousOrderColumn(sysCtx, name)
		}
		if found {
			orderAliasMap[name] = &aliasItem{
				idx:     item.idx,
				astExpr: item.astExpr,
			}
		}
	}

	if preserveAliases {
		qualified, err := ctx.qualifyColumnNames(qualified, NoAlias)
		return qualified, orderAliasMap, err
	}
	for _, item := range orderAliasMap {
		item.astExpr = cloneTreeExpr(item.astExpr)
	}
	orderCtx := *ctx
	orderCtx.aliasMap = orderAliasMap
	qualified, err := orderCtx.qualifyColumnNames(qualified, AliasAfterColumn)
	return qualified, nil, err
}

func NewOrderBinder(projectionBinder *ProjectionBinder, selectList tree.SelectExprs) *OrderBinder {
	return &OrderBinder{
		ProjectionBinder: projectionBinder,
		selectList:       selectList,
	}
}

func (b *OrderBinder) BindExpr(astExpr tree.Expr) (*plan.Expr, error) {
	// Parentheses do not turn a top-level ORDER BY name into a nested
	// expression. Keep the same output-name precedence for `ORDER BY name` and
	// `ORDER BY (name)`.
	rootExpr := unwrapParenExpr(astExpr)
	if colRef, ok := rootExpr.(*tree.UnresolvedName); ok && colRef.NumParts == 1 {
		if pos, found, err := resolveOrderOutputName(b.GetContext(), b.ctx, colRef.ColName()); err != nil {
			return nil, err
		} else if found {
			return makeProjectColRef(b.ctx, pos), nil
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

	// Within an ORDER BY expression, input columns take precedence over select
	// aliases. An alias remains available as a fallback when no input column has
	// that name. This is intentionally different from the top-level-name rule
	// above and matches the DISTINCT binder's existing resolution contract.
	astExpr, _, err := qualifyOrderExpression(b.GetContext(), b.ctx, astExpr, false, false)
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
