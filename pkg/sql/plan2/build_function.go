// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan2

import (
	"go/constant"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
)

func getFunctionExprByNameAndPlanExprs(name string, exprs []*Expr) (*Expr, error) {
	// get args(exprs) & types
	argsLength := len(exprs)
	argsType := make([]types.T, argsLength)
	for idx, expr := range exprs {
		argsType[idx] = types.T(expr.Typ.Id)
	}
	name = strings.ToLower(name)

	// get function definition
	funcDef, funcId, argsCastType, err := function.GetFunctionByName(name, argsType)
	if err != nil {
		return nil, err
	}
	if argsCastType != nil {
		if len(argsCastType) != argsLength {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "cast types length not match args length")
		}
		for idx, castType := range argsCastType {
			if argsType[idx] != castType {
				exprs[idx], err = appendCastExpr(exprs[idx], &plan.Type{
					Id: plan.Type_TypeId(castType),
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// return new expr
	returnType := &Type{
		Id: plan.Type_TypeId(funcDef.ReturnTyp),
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcDef, funcId, name),
				Args: exprs,
			},
		},
		Typ: returnType,
	}, nil
}

func getFunctionExprByNameAndAstExprs(name string, exprs []tree.Expr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext) (*Expr, error) {
	name = strings.ToLower(name)
	args := make([]*Expr, len(exprs))
	// deal with special function
	switch name {
	case "extract":
		kindExpr := exprs[0].(*tree.UnresolvedName)
		exprs[0] = tree.NewNumVal(constant.MakeString(kindExpr.Parts[0]), kindExpr.Parts[0], false)
	}

	// get args
	for idx, astExpr := range exprs {
		expr, err := buildExpr(astExpr, ctx, query, node, binderCtx)
		if err != nil {
			return nil, err
		}
		args[idx] = expr
	}

	// deal with special function
	switch name {
	case "date":
		return appendCastExpr(args[0], &plan.Type{
			Id: plan.Type_DATE,
		})
	case "interval":
		return appendCastExpr(args[0], &plan.Type{
			Id: plan.Type_INTERVAL,
		})
	default:
		return getFunctionExprByNameAndPlanExprs(name, args)
	}

}

func appendCastExpr(expr *Expr, toType *Type) (*Expr, error) {
	argsType := []types.T{
		types.T(expr.Typ.Id),
		types.T(toType.Id),
	}
	funcDef, funcId, _, err := function.GetFunctionByName("cast", argsType)
	if err != nil {
		return nil, err
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcDef, funcId, "cast"),
				Args: []*Expr{expr},
			},
		},
		Typ: toType,
	}, nil
}

func getFunctionObjRef(funcDef function.Function, funcId int, name string) *ObjectRef {
	return &ObjectRef{
		Schema:  int64(funcId),
		Obj:     int64(funcDef.Index),
		ObjName: name,
	}
}
