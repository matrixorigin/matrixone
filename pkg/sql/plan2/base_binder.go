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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
)

func (b *baseBinder) splitAndBindCondition(astExpr tree.Expr, ctx *BindContext) ([]*plan.Expr, error) {
	conds := splitConjunctiveCondition(astExpr)
	exprs := make([]*plan.Expr, len(conds))

	for i, cond := range conds {
		alias := tree.String(cond, dialect.MYSQL)
		expr, err := b.bindExprImpl(alias, cond, ctx)
		if err != nil {
			return nil, err
		}
		exprs[i] = expr
	}

	return exprs, nil
}

func (b *baseBinder) bindExprImpl(alias string, astExpr tree.Expr, ctx *BindContext) (expr *plan.Expr, err error) {
	switch astExpr := astExpr.(type) {
	case *tree.ParenExpr:
		expr, err = b.BindExpr(alias, astExpr.Expr, ctx)
	case *tree.UnresolvedName:
		expr, err = b.BindColRef(alias, astExpr, ctx)
	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(alias, astExpr, ctx)
	}

	parent := ctx.parent
	depth := 0
	dummyErr := err
	for dummyErr != nil && parent != nil && parent.binder != nil {
		expr, dummyErr = parent.binder.BindExpr(alias, astExpr, parent)
		depth++
		parent = parent.parent
	}

	if dummyErr != nil {
		return nil, err
	}
	if depth > 0 {
		ctx.addCorrCol(expr)
		return expr, nil
	}

	return expr, nil
}

func (b *baseBinder) bindFuncExpr(alias string, astExpr *tree.FuncExpr, ctx *BindContext) (*plan.Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function expr '%v' is not support now", astExpr))
	}
	funcName := funcRef.Parts[0]

	if isAggFunc(funcName) {
		return b.BindAggFunc(alias, funcName, astExpr, ctx)
	} else if isWinFunc(funcName) {
		return b.BindWinFunc(alias, funcName, astExpr, ctx)
	}

	return b.bindFuncExprImpl(funcName, astExpr, ctx)
}

func (b *baseBinder) bindFuncExprImpl(name string, astExpr *tree.FuncExpr, ctx *BindContext) (*plan.Expr, error) {
	// deal with special function
	//switch name {
	//case "+", "-":
	//	if len(astExprs) != 2 {
	//		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "operator function need two args")
	//	}
	//	if astExprs[0].Typ.Id == plan.Type_DATE && astExprs[1].Typ.Id == plan.Type_INTERVAL {
	//		resultExpr, err = getIntervalFunction(name, astExprs[0], astExprs[1])
	//		return
	//	}
	//	if astExprs[0].Typ.Id == plan.Type_INTERVAL && astExprs[1].Typ.Id == plan.Type_DATE {
	//		resultExpr, err = getIntervalFunction(name, astExprs[1], astExprs[0])
	//		return
	//	}
	//case "and", "or", "not":
	//	if err := convertValueIntoBool(name, astExprs, true); err != nil {
	//		return nil, err
	//	}
	//case "=", "<", "<=", ">", ">=", "<>":
	//	if err := convertValueIntoBool(name, astExprs, false); err != nil {
	//		return nil, err
	//	}
	//}

	// get args(exprs) & types
	argsLength := len(astExpr.Exprs)
	argsType := make([]types.T, argsLength)
	for idx, param := range astExpr.Exprs {
		argsType[idx] = types.T(param.Typ.Id)
	}

	// get function definition
	funcDef, funcId, argsCastType, err := function.GetFunctionByName(name, argsType)
	return nil, nil
}

func isAggFunc(name string) bool {
	if funcSig, ok := BuiltinFunctionsMap[name]; ok {
		return funcSig.Flag == plan.Function_AGG
	}
	return false
}

func isWinFunc(name string) bool {
	if funcSig, ok := BuiltinFunctionsMap[name]; ok {
		return funcSig.Flag == plan.Function_WIN
	}
	return false
}

//splitConjunctiveCondition split a expression to a list of AND conditions.
func splitConjunctiveCondition(astExpr tree.Expr) []tree.Expr {
	var astExprs []tree.Expr
	switch typ := astExpr.(type) {
	case nil:
	case *tree.AndExpr:
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Left)...)
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Right)...)
	case *tree.ParenExpr:
		astExprs = append(astExprs, splitConjunctiveCondition(typ.Expr)...)
	default:
		astExprs = append(astExprs, astExpr)
	}
	return astExprs
}
