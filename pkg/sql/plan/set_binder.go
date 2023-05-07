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

func NewSetVarBinder(builder *QueryBuilder, ctx *BindContext) *SetBinder {
	p := &SetBinder{}
	p.sysCtx = builder.GetContext()
	p.builder = builder
	p.ctx = ctx
	p.impl = p
	return p
}

var funcNeedsTxn = map[string]int{
	"nextval":          0,
	"setval":           0,
	"mo_ctl":           0,
	"mo_table_rows":    0,
	"mo_table_size":    0,
	"mo_table_col_max": 0,
	"mo_table_col_min": 0,
}

// BindExpr prohibits functions in set expr
func (s *SetBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	switch exprImpl := expr.(type) {
	case *tree.FuncExpr:
		//supported function
		funcRef, ok := exprImpl.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, moerr.NewNYI(s.GetContext(), "invalid function expr '%v'", exprImpl)
		}
		funcName := strings.ToLower(funcRef.Parts[0])
		if _, ok := funcNeedsTxn[funcName]; ok {
			return nil, moerr.NewInvalidInput(s.GetContext(), "function %s is not allowed in the set expression", funcName)
		}
	}
	return s.baseBindExpr(expr, i, b)
}

func (s *SetBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return s.baseBindColRef(name, i, b)
}

func (s *SetBinder) BindAggFunc(_ string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(s.GetContext(), "aggregate functions not allowed in partition clause")
}

func (s *SetBinder) BindWinFunc(_ string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(s.GetContext(), "window functions not allowed in partition clause")
}

func (s *SetBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(s.GetContext(), "subquery not allowed in partition clause")
}
