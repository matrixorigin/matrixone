// Copyright 2026 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func selectListHasAggregate(selectList tree.SelectExprs) bool {
	for _, selectExpr := range selectList {
		if exprHasTopLevelAggregate(selectExpr.Expr) {
			return true
		}
	}
	return false
}

func exprHasTopLevelAggregate(expr tree.Expr) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case *tree.Subquery:
		return false
	case *tree.FuncExpr:
		funcRef, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
		if ok && e.WindowSpec == nil && function.GetFunctionIsAggregateByName(funcRef.ColName()) {
			return true
		}
		return exprsHaveTopLevelAggregate(e.Exprs)
	case *tree.BinaryExpr:
		return exprHasTopLevelAggregate(e.Left) || exprHasTopLevelAggregate(e.Right)
	case *tree.UnaryExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.ComparisonExpr:
		return exprHasTopLevelAggregate(e.Left) ||
			exprHasTopLevelAggregate(e.Right) ||
			exprHasTopLevelAggregate(e.Escape)
	case *tree.AndExpr:
		return exprHasTopLevelAggregate(e.Left) || exprHasTopLevelAggregate(e.Right)
	case *tree.XorExpr:
		return exprHasTopLevelAggregate(e.Left) || exprHasTopLevelAggregate(e.Right)
	case *tree.OrExpr:
		return exprHasTopLevelAggregate(e.Left) || exprHasTopLevelAggregate(e.Right)
	case *tree.NotExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsNullExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsNotNullExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsUnknownExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsNotUnknownExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsTrueExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsNotTrueExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsFalseExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.IsNotFalseExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.ExprList:
		return exprsHaveTopLevelAggregate(e.Exprs)
	case *tree.ParenExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.CastExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.BitCastExpr:
		return exprHasTopLevelAggregate(e.Expr)
	case *tree.Tuple:
		return exprsHaveTopLevelAggregate(e.Exprs)
	case *tree.RangeCond:
		return exprHasTopLevelAggregate(e.Left) ||
			exprHasTopLevelAggregate(e.From) ||
			exprHasTopLevelAggregate(e.To)
	case *tree.CaseExpr:
		if exprHasTopLevelAggregate(e.Expr) || exprHasTopLevelAggregate(e.Else) {
			return true
		}
		for _, when := range e.Whens {
			if when != nil && (exprHasTopLevelAggregate(when.Cond) || exprHasTopLevelAggregate(when.Val)) {
				return true
			}
		}
	}
	return false
}

func exprsHaveTopLevelAggregate(exprs tree.Exprs) bool {
	for _, expr := range exprs {
		if exprHasTopLevelAggregate(expr) {
			return true
		}
	}
	return false
}
