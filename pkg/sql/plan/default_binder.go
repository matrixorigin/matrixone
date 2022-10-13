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
	"strings"
)

func NewDefaultBinder(builder *QueryBuilder, ctx *BindContext, typ *Type, cols []string) *DefaultBinder {
	b := &DefaultBinder{typ: typ, cols: cols}
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *DefaultBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *DefaultBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.cols != nil {
		return b.bindColRef(astExpr, depth, isRoot)
	}
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *DefaultBinder) bindColRef(astExpr *tree.UnresolvedName, _ int32, _ bool) (expr *plan.Expr, err error) {
	col := strings.ToLower(astExpr.Parts[0])
	idx := -1
	for i, c := range b.cols {
		if c == col {
			idx = i
			break
		}
	}
	if idx == -1 {
		err = moerr.NewInvalidInput("column '%s' does not exist", col)
		return
	}
	expr = &plan.Expr{
		Typ: b.typ,
	}
	expr.Expr = &plan.Expr_Col{
		Col: &plan.ColRef{
			RelPos: 0,
			ColPos: int32(idx),
		},
	}
	return
}

func (b *DefaultBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput("cannot bind agregate functions '%s'", funcName)
}

func (b *DefaultBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput("cannot bind window functions '%s'", funcName)
}

func (b *DefaultBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI("subquery in JOIN condition")
}
