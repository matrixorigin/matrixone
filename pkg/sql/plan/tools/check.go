// Copyright 2024 Matrix Origin
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

package tools

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type ExprChecker struct {
	Aliases UnorderedMap[string, string]
}

func (checker *ExprChecker) Check(astExpr tree.Expr, expr *plan2.Expr) (bool, error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		switch exprImpl.ValType {
		case tree.P_int64:
			val, ok := exprImpl.Int64()
			if !ok {
				return false, moerr.NewInvalidInputf(context.Background(), "invalid int value '%s'", exprImpl.String())
			}
			ival := expr.GetLit().GetI64Val()
			return val == ival, nil
		default:
			return false, moerr.NewInternalErrorf(context.Background(), "unsupport numval type %v", exprImpl.ValType)
		}
	case *tree.BinaryExpr:
		fun := expr.GetF()
		if fun == nil {
			return false, nil
		}
		return checker.checkBinaryExpr(exprImpl, expr)
	case *tree.ComparisonExpr:
		fun := expr.GetF()
		if fun == nil {
			return false, nil
		}
		return checker.checkComparisonExpr(exprImpl, expr)
	case *tree.FuncExpr:
		fun := expr.GetF()
		if fun == nil {
			return false, nil
		}
		return checker.checkFuncExpr(exprImpl, expr)
	case *tree.UnresolvedName:
		ok, eName := checker.Aliases.Find(exprImpl.ColNameOrigin())
		if !ok {
			return false, nil
		}
		colRef := expr.GetCol()
		if colRef == nil {
			return false, nil
		}
		aName := strings.Split(colRef.Name, ".")[1]
		return eName == aName, nil
	default:
		return false, moerr.NewInternalError(context.Background(), "unsupport expr")
	}
}

func (checker *ExprChecker) checkFuncExpr(astExpr *tree.FuncExpr, expr *plan2.Expr) (bool, error) {
	fun := expr.GetF()
	if len(astExpr.Exprs) != len(fun.GetArgs()) {
		return false, nil
	}
	if fun.GetFunc().ObjName != astExpr.FuncName.Compare() {
		return false, nil
	}
	for i, arg := range astExpr.Exprs {
		ret, err := checker.Check(arg, fun.GetArgs()[i])
		if err != nil {
			return false, err
		}
		if !ret {
			return false, nil
		}
	}
	return true, nil
}

func (checker *ExprChecker) checkBinaryExpr(astExpr *tree.BinaryExpr, expr *plan2.Expr) (bool, error) {
	switch astExpr.Op {
	case tree.PLUS:
		fun := expr.GetF()

		lret, err := checker.Check(astExpr.Left, fun.Args[0])
		if err != nil {
			return false, err
		}
		if !lret {
			return false, nil
		}
		rret, err := checker.Check(astExpr.Right, fun.Args[1])
		if err != nil {
			return false, err
		}
		if !rret {
			return false, nil
		}
		return lret && rret, nil
	default:
		return false, moerr.NewInternalError(context.Background(), "unsupport expr 2")
	}
}

func (checker *ExprChecker) checkComparisonExpr(astExpr *tree.ComparisonExpr, expr *plan2.Expr) (bool, error) {
	switch astExpr.Op {
	case tree.EQUAL:
		fun := expr.GetF()

		lret, err := checker.Check(astExpr.Left, fun.Args[0])
		if err != nil {
			return false, err
		}
		if !lret {
			return false, nil
		}
		rret, err := checker.Check(astExpr.Right, fun.Args[1])
		if err != nil {
			return false, err
		}
		if !rret {
			return false, nil
		}
		return lret && rret, nil
	default:
		return false, moerr.NewInternalError(context.Background(), "unsupport expr 2")
	}
}
