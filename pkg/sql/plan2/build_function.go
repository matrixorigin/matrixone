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
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func getFunctionExprByNameAndExprs(name string, exprs []tree.Expr, ctx CompilerContext, query *Query, node *Node, binderCtx *BinderContext) (*Expr, error) {
	// Get function
	functionSig, ok := BuiltinFunctionsMap[name]
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function name '%v' is not exist", name))
	}

	// Check parameters length
	if len(functionSig.ArgType) != len(exprs) {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("number of parameters does not match for function '%v'", functionSig.Name))
	}

	// Get original input expr
	args := make([]*Expr, 0, len(exprs))
	// TODO: special case  need check
	if name == "EXTRACT" {
		kindExpr := exprs[0].(*tree.UnresolvedName)
		args = append(args, &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: kindExpr.Parts[0],
					},
				},
			},
			Typ: &plan.Type{
				Id:        plan.Type_VARCHAR,
				Nullable:  false,
				Width:     math.MaxInt32,
				Precision: 0,
			},
		})
		exprs = []tree.Expr{exprs[1]}
	}

	for _, astExpr := range exprs {
		expr, err := buildExpr(astExpr, ctx, query, node, binderCtx)
		if err != nil {
			return nil, err
		}
		args = append(args, expr)
	}

	// Convert input parameter types if necessary
	returnType, err := covertArgsTypeAndGetReturnType(functionSig, args)
	if err != nil {
		return nil, err
	}

	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(name),
				Args: args,
			},
		},
		Typ: returnType,
	}, nil
}

func covertArgsTypeAndGetReturnType(fun *FunctionSig, args []*Expr) (*plan.Type, error) {
	var returnType *plan.Type
	switch fun.Name {
	case "+", "-", "*", "/", "%":
		leftIsNumber := checkNumberType(args[0].Typ.Id, args[0].ColName) == nil
		rightIsNumber := checkNumberType(args[1].Typ.Id, args[1].ColName) == nil

		if !leftIsNumber && !rightIsNumber {
			newExpr, err := appendCastExpr(args[0], plan.Type_INT64) // todo need research
			if err != nil {
				return nil, err
			}
			args[0] = newExpr

			newExpr, err = appendCastExpr(args[1], plan.Type_INT64) // todo need research
			if err != nil {
				return nil, err
			}
			args[1] = newExpr
			return &plan.Type{
				Id: fun.ArgTypeClass[0],
			}, nil
		}

		if !leftIsNumber {
			newExpr, err := appendCastExpr(args[0], args[1].Typ.Id) // todo need research
			if err != nil {
				return nil, err
			}
			args[0] = newExpr
			return &plan.Type{
				Id: args[1].Typ.Id,
			}, nil
		}
		if !rightIsNumber {
			newExpr, err := appendCastExpr(args[1], args[0].Typ.Id) // todo need research
			if err != nil {
				return nil, err
			}
			args[0] = newExpr
			return &plan.Type{
				Id: args[0].Typ.Id,
			}, nil
		}

		// equal type, return directly
		if args[0].Typ.Id == args[1].Typ.Id {
			return &plan.Type{
				Id: args[0].Typ.Id,
			}, nil
		}

		// cast low type to high type
		_, ok := CastLowTypeToHighTypeMap[args[0].Typ.Id]
		if !ok {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type mapping not found, arg[0] type= %v", args[0].Typ.Id))
		}
		highType, ok := CastLowTypeToHighTypeMap[args[0].Typ.Id][args[1].Typ.Id]
		if !ok {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type mapping not found, arg[1] type= %v", args[1].Typ.Id))
		}
		if args[0].Typ.Id != highType {
			newExpr, err := appendCastExpr(args[0], highType)
			if err != nil {
				return nil, err
			}
			args[0] = newExpr
		}
		if args[1].Typ.Id != highType {
			newExpr, err := appendCastExpr(args[1], highType)
			if err != nil {
				return nil, err
			}
			args[1] = newExpr
		}
		return &plan.Type{
			Id: args[0].Typ.Id,
		}, nil
	case "UNARY_PLUS", "UNARY_MINUS":
		expr := args[0]
		isNumberType := checkNumberType(expr.Typ.Id, expr.ColName) == nil
		if !isNumberType {
			newExpr, err := appendCastExpr(expr, plan.Type_INT64)
			if err != nil {
				return nil, err
			}
			args[0] = newExpr
			returnType = &plan.Type{ //need check
				Id: plan.Type_INT64,
			}
		} else {
			returnType = expr.Typ
		}
		return returnType, nil
	default:
		return &plan.Type{
			Id: fun.ArgTypeClass[0],
		}, nil
	}
}

func appendCastExpr(expr *Expr, toType plan.Type_TypeId) (*Expr, error) {
	// todo check and cast constant expr in buildding
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef("CAST"),
				Args: []*Expr{expr},
			},
		},
		Typ: &plan.Type{
			Id: toType,
		},
	}, nil
}

func getFunctionObjRef(name string) *ObjectRef {
	return &ObjectRef{
		ObjName: name,
	}
}

func checkFloatType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_FLOAT32, plan.Type_FLOAT64:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not float", alias))
	}
}

func checkIntType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_INT8, plan.Type_INT16, plan.Type_INT32, plan.Type_INT64, plan.Type_INT128,
		plan.Type_UINT8, plan.Type_UINT16, plan.Type_UINT32, plan.Type_UINT64, plan.Type_UINT128:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not int", alias))
	}
}

func checkDecimalType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_DECIMAL, plan.Type_DECIMAL64, plan.Type_DECIMAL128:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not decimal", alias))
	}
}

func checkNumberType(typ plan.Type_TypeId, alias string) error {
	err := checkIntType(typ, alias)
	if err == nil {
		return nil
	}
	err = checkFloatType(typ, alias)
	if err == nil {
		return nil
	}
	err = checkDecimalType(typ, alias)
	if err != nil {
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not number", alias))
	}
	return nil
}

//todo use in time cast function
// func checkTimeType(typ plan.Type_TypeId, alias string) error {
// 	switch typ {
// 	case plan.Type_DATE, plan.Type_TIME, plan.Type_DATETIME, plan.Type_TIMESTAMP, plan.Type_INTERVAL:
// 		return nil
// 	default:
// 		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not time", alias))
// 	}
// }
