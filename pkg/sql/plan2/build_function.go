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
				Func: getFunctionObjRef(funcId, name),
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
		expr, err = convertValueIntoBool(name, expr)
		if err != nil {
			return nil, err
		}
		args[idx] = expr
	}

	if err := convertValueIntoBool2(name, args); err != nil {
		return nil, err
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
	_, funcId, _, err := function.GetFunctionByName("cast", argsType)
	if err != nil {
		return nil, err
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcId, "cast"),
				Args: []*Expr{expr},
			},
		},
		Typ: toType,
	}, nil
}

func getFunctionObjRef(funcId int64, name string) *ObjectRef {
	return &ObjectRef{
		Obj:     funcId,
		ObjName: name,
	}
}

func convertValueIntoBool(name string, expr *plan.Expr) (*plan.Expr, error) {
	if name != "and" && name != "or" && name != "not" {
		return expr, nil
	}

	if expr.Typ.Id == plan.Type_BOOL {
		return expr, nil
	}
	switch ex := expr.Expr.(type) {
	case *plan.Expr_C:
		expr.Typ.Id = plan.Type_BOOL
		switch value := ex.C.Value.(type) {
		case *plan.Const_Ival:
			if value.Ival == 0 {
				ex.C.Value = &plan.Const_Bval{Bval : false}
			} else if value.Ival == 1 {
				ex.C.Value = &plan.Const_Bval{Bval : true}
			} else {
				return nil, errors.New("", "the params type is not right")
			}
		}
	default:
		return nil, errors.New("", "the expr type is not right")
	}
	return expr, nil
}

func convertValueIntoBool2(name string, args []*Expr) (error) {
	if name != "=" && name != "<" && name != "<=" && name != ">" && name != ">=" && name != "<>" {
		return nil
	}
	if len(args) != 2 || (args[0].Typ.Id != plan.Type_BOOL && args[1].Typ.Id != plan.Type_BOOL) {
		return nil
	}
	for _, arg := range args {
		switch ex := arg.Expr.(type) {
		case *plan.Expr_C:
			switch value := ex.C.Value.(type) {
			case *plan.Const_Ival:
				if value.Ival == 0 {
					arg.Typ.Id = plan.Type_BOOL
					ex.C.Value = &plan.Const_Bval{Bval : false}
				} else if value.Ival == 1 {
					arg.Typ.Id = plan.Type_BOOL
					ex.C.Value = &plan.Const_Bval{Bval : true}
				} else {
					return errors.New("", "the params type is not right")
				}
			}
		}
	}
	return nil
}