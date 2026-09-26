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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewWhereBinder(builder *QueryBuilder, ctx *BindContext) *WhereBinder {
	b := &WhereBinder{}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *WhereBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *WhereBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if depth == 0 && astExpr.NumParts == 1 && !astExpr.Star {
		if alias, ok := b.aliases[astExpr.ColName()]; ok {
			if b.ctx.timeTag > 0 && (astExpr.ColName() == TimeWindowStart || astExpr.ColName() == TimeWindowEnd) {
				return b.baseBindColRef(astExpr, depth, isRoot)
			}
			// Preserve existing column resolution, including ambiguous and outer
			// columns. An outer SELECT's aliases are never correlation targets.
			for ctx := b.ctx; ctx != nil; ctx = ctx.parent {
				if _, exists := ctx.bindingByCol[astExpr.ColName()]; exists {
					return b.baseBindColRef(astExpr, depth, isRoot)
				}
			}
			if alias == nil {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%s'", astExpr.ColNameOrigin())
			}
			// Expand as a WHERE expression, not a projected result. Disable alias
			// lookup within it to reject alias chains/cycles and clone the AST so
			// binding cannot mutate the SELECT expression used later.
			aliases := b.aliases
			b.aliases = nil
			defer func() { b.aliases = aliases }()
			expanded, err := b.ctx.qualifyColumnNames(cloneTreeExpr(alias), NoAlias)
			if err != nil {
				return nil, err
			}
			return b.BindExpr(expanded, depth, isRoot)
		}
	}
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *WhereBinder) setSelectAliases(selectList tree.SelectExprs) {
	b.aliases = make(map[string]tree.Expr)
	for _, item := range selectList {
		if item.As == nil || item.As.Empty() {
			continue
		}
		name := item.As.Compare()
		if _, exists := b.aliases[name]; exists {
			b.aliases[name] = nil
		} else {
			b.aliases[name] = item.Expr
		}
	}
}

func (b *WhereBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxErrorf(b.GetContext(), "aggregate function %s not allowed in WHERE clause", funcName)
}

func (b *WhereBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxErrorf(b.GetContext(), "window function %s not allowed in WHERE clause", funcName)
}

func (b *WhereBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *WhereBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
