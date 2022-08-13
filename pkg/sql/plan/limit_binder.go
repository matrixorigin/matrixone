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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewLimitBinder() *LimitBinder {
	return &LimitBinder{}
}

func (b *LimitBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	switch astExpr.(type) {
	case *tree.VarExpr, *tree.UnqualifiedStar:
		// need check other expr
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "unsupported expr in limit clause")
	}

	expr, err := b.baseBindExpr(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}
	if expr.Typ.Id == int32(types.T_decimal128) || expr.Typ.Id == int32(types.T_decimal64) {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "only int64 support in limit/offset clause")
	}
	return expr, nil
}

func (b *LimitBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, errors.New(errno.InvalidColumnReference, "column name not allowed in limit clause")
}

func (b *LimitBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, errors.New(errno.GroupingError, "aggregate functions not allowed in limit clause")
}

func (b *LimitBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, errors.New(errno.WindowingError, "window functions not allowed in limit clause")
}

func (b *LimitBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, errors.New(errno.WindowingError, "subquery functions not allowed in limit clause")
}
