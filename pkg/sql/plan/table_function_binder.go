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

func NewTableFunctionBinder(builder *QueryBuilder, ctx *BindContext) *TableFunctionBinder { // this binder may change in the future, but now it is just a copy of table binder
	b := &TableFunctionBinder{}
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *TableFunctionBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindExpr(astExpr, depth, isRoot)
}
func (b *TableFunctionBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *TableFunctionBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("aggregate function %s not allowed", funcName)
}

func (b *TableFunctionBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("window function %s not allowed", funcName)
}

func (b *TableFunctionBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI("subquery in JOIN condition")
}
