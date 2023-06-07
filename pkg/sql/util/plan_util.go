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

package util

import (
	"context"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func SimpleBindFuncExpr(ctx context.Context, name string, args []*plan.Expr) (*plan.Expr, error) {
	// get args(exprs) & types
	argsLength := len(args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range args {
		argsType[idx] = MakeTypeByPlan2Expr(expr)
	}

	var funcID int64
	var returnType types.Type
	var argsCastType []types.Type

	// get function definition
	fGet, err := function.GetFunctionByName(ctx, name, argsType)
	if err != nil {
		return nil, err
	}
	funcID = fGet.GetEncodedOverloadID()
	returnType = fGet.GetReturnType()
	argsCastType, _ = fGet.ShouldDoImplicitTypeCast()

	if function.GetFunctionIsAggregateByName(name) {
		if constExpr, ok := args[0].Expr.(*plan.Expr_C); ok && constExpr.C.Isnull {
			args[0].Typ = MakePlan2Type(&returnType)
		}
	}

	// rewrite some cast rule:  expr:  int32Col > 10,
	// old rule: cast(int32Col as int64) >10 ,   new rule: int32Col > (cast 10 as int32)
	switch name {
	case "=", "<", "<=", ">", ">=", "<>", "like":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch leftExpr := args[0].Expr.(type) {
		case *plan.Expr_C:
			if _, ok := args[1].Expr.(*plan.Expr_Col); ok {
				if checkNoNeedCast(argsType[0], argsType[1], leftExpr) {
					tmpType := argsType[1] // cast const_expr as column_expr's type
					argsCastType = []types.Type{tmpType, tmpType}
					// need to update function id
					fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
					funcID = fGet.GetEncodedOverloadID()
				}
			}
		case *plan.Expr_Col:
			if rightExpr, ok := args[1].Expr.(*plan.Expr_C); ok {
				if checkNoNeedCast(argsType[1], argsType[0], rightExpr) {
					tmpType := argsType[0] // cast const_expr as column_expr's type
					argsCastType = []types.Type{tmpType, tmpType}
					fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
					funcID = fGet.GetEncodedOverloadID()
				}
			}
		}

	case "in", "not_in":
		//if all the expr in the in list can safely cast to left type, we call it safe
		safe := true
		if rightList, ok := args[1].Expr.(*plan.Expr_List); ok {
			typLeft := MakeTypeByPlan2Expr(args[0])
			lenList := len(rightList.List.List)

			for i := 0; i < lenList && safe; i++ {
				if constExpr, ok := rightList.List.List[i].Expr.(*plan.Expr_C); ok {
					safe = checkNoNeedCast(MakeTypeByPlan2Expr(rightList.List.List[i]), typLeft, constExpr)
				} else {
					safe = false
				}
			}

			if safe {
				//if safe, try to cast the in list to left type
				for i := 0; i < lenList; i++ {
					rightList.List.List[i], err = AppendCastBeforeExpr(ctx, rightList.List.List[i], args[0].Typ)
					if err != nil {
						return nil, err
					}
				}
			} else {
				//expand the in list to col=a or col=b or ......
				if name == "in" {
					newExpr, _ := SimpleBindFuncExpr(ctx, "=", []*plan.Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[0])})
					for i := 1; i < lenList; i++ {
						tmpExpr, _ := SimpleBindFuncExpr(ctx, "=", []*plan.Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[i])})
						newExpr, _ = SimpleBindFuncExpr(ctx, "or", []*plan.Expr{newExpr, tmpExpr})
					}
					return newExpr, nil
				} else {
					//expand the not in list to col!=a and col!=b and ......
					newExpr, _ := SimpleBindFuncExpr(ctx, "!=", []*plan.Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[0])})
					for i := 1; i < lenList; i++ {
						tmpExpr, _ := SimpleBindFuncExpr(ctx, "!=", []*plan.Expr{DeepCopyExpr(args[0]), DeepCopyExpr(rightList.List.List[i])})
						newExpr, _ = SimpleBindFuncExpr(ctx, "and", []*plan.Expr{newExpr, tmpExpr})
					}
					return newExpr, nil
				}
			}
		}

	case "timediff":
		if len(argsType) == len(argsCastType) {
			for i := range argsType {
				if int(argsType[i].Oid) == int(types.T_time) && int(argsCastType[i].Oid) == int(types.T_datetime) {
					return nil, moerr.NewInvalidInput(ctx, name+" function have invalid input args type")
				}
			}
		}
	}

	if len(argsCastType) != 0 {
		if len(argsCastType) != argsLength {
			return nil, moerr.NewInvalidArg(ctx, "cast types length not match args length", "")
		}
		for idx, castType := range argsCastType {
			if _, ok := args[idx].Expr.(*plan.Expr_P); ok {
				continue
			}
			if _, ok := args[idx].Expr.(*plan.Expr_V); ok {
				continue
			}
			if !argsType[idx].Eq(castType) && castType.Oid != types.T_any {
				if argsType[idx].Oid == castType.Oid && castType.Oid.IsDecimal() && argsType[idx].Scale == castType.Scale {
					continue
				}
				typ := MakePlan2Type(&castType)
				args[idx], err = AppendCastBeforeExpr(ctx, args[idx], typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// return new expr
	Typ := MakePlan2Type(&returnType)
	Typ.NotNullable = function.DeduceNotNullable(funcID, args)
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     funcID,
					ObjName: name,
				},
				Args: args,
			},
		},
		Typ: Typ,
	}, nil
}

func HaveVarOrParam(t *plan.Expr_F) bool {
	for i, arg := range t.F.Args {
		if _, ok := arg.Expr.(*plan.Expr_V); ok {
			return true
		} else if _, ok := t.F.Args[i].Expr.(*plan.Expr_P); ok {
			return true
		}
	}
	return false
}

func AnyToExpr(ctx context.Context, val any, e *plan.Expr) (*plan.Expr, error) {
	switch val := val.(type) {
	case string:
		return MakePlan2StringConstExprWithType(val), nil
	case int:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case uint8:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case uint16:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case uint32:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case int8:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case int16:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case int32:
		return MakePlan2Int64ConstExprWithType(int64(val)), nil
	case int64:
		return MakePlan2Int64ConstExprWithType(val), nil
	case uint64:
		return MakePlan2Uint64ConstExprWithType(val), nil
	case float32:
		// when we build plan with constant in float, we cast them to decimal.
		// so we cast @float_var to decimal too.
		strVal := strconv.FormatFloat(float64(val), 'f', -1, 64)
		return MakePlan2DecimalExprWithType(ctx, strVal)
	case float64:
		// when we build plan with constant in float, we cast them to decimal.
		// so we cast @float_var to decimal too.
		strVal := strconv.FormatFloat(val, 'f', -1, 64)
		return MakePlan2DecimalExprWithType(ctx, strVal)
	case bool:
		return MakePlan2BoolConstExprWithType(val), nil
	case nil:
		if e == nil {
			return nil, moerr.NewNYI(ctx, "type of var nil is not supported now")
		} else {
			if e.Typ.Id == int32(types.T_any) {
				return MakePlan2NullConstExprWithType(), nil
			} else {
				return &plan.Expr{
					Expr: &plan.Expr_C{
						C: &plan.Const{
							Isnull: true,
						},
					},
					Typ: e.Typ,
				}, nil
			}

		}
	case *plan.Expr:
		return val, nil
	case types.Decimal64, types.Decimal128:
		return nil, moerr.NewNYI(ctx, "decimal var")
	default:
		return nil, moerr.NewNYI(ctx, "type of var %q is not supported now", e)
	}
}

func GetVarValue(
	ctx context.Context,
	proc *process.Process,
	e *plan.Expr,
) (*plan.Expr, error) {
	exprImpl := e.Expr.(*plan.Expr_V)
	getVal, err := proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
	if err != nil {
		return nil, err
	}
	expr, err := AnyToExpr(ctx, getVal, e)
	if err != nil {
		return nil, err
	}
	if e.Typ.Id != int32(types.T_any) && expr.Typ.Id != e.Typ.Id {
		expr, err = AppendCastBeforeExpr(ctx, expr, e.Typ)
	}
	if err != nil {
		return nil, err
	}
	return expr, err
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(constT, columnT types.Type, constExpr *plan.Expr_C) bool {
	switch constT.Oid {
	case types.T_char, types.T_varchar, types.T_text:
		switch columnT.Oid {
		case types.T_char, types.T_varchar:
			if constT.Width <= columnT.Width {
				return true
			} else {
				return false
			}
		case types.T_text:
			return true
		default:
			return false
		}

	case types.T_binary, types.T_varbinary, types.T_blob:
		switch columnT.Oid {
		case types.T_binary, types.T_varbinary:
			if constT.Width <= columnT.Width {
				return true
			} else {
				return false
			}
		case types.T_blob:
			return true
		default:
			return false
		}

	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val
		switch columnT.Oid {
		case types.T_int8:
			return constVal <= int64(math.MaxInt8) && constVal >= int64(math.MinInt8)
		case types.T_int16:
			return constVal <= int64(math.MaxInt16) && constVal >= int64(math.MinInt16)
		case types.T_int32:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		case types.T_int64:
			return true
		case types.T_uint8:
			return constVal <= math.MaxUint8 && constVal >= 0
		case types.T_uint16:
			return constVal <= math.MaxUint16 && constVal >= 0
		case types.T_uint32:
			return constVal <= math.MaxUint32 && constVal >= 0
		case types.T_uint64:
			return constVal >= 0
		case types.T_varchar:
			return true
		case types.T_float32:
			//float32 has 6 significant digits.
			return constVal <= 100000 && constVal >= -100000
		case types.T_float64:
			//float64 has 15 significant digits.
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		case types.T_decimal64:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		default:
			return false
		}

	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		val_u, valOk := constExpr.C.Value.(*plan.Const_U64Val)
		if !valOk {
			return false
		}
		constVal := val_u.U64Val
		switch columnT.Oid {
		case types.T_int8:
			return constVal <= math.MaxInt8
		case types.T_int16:
			return constVal <= math.MaxInt16
		case types.T_int32:
			return constVal <= math.MaxInt32
		case types.T_int64:
			return constVal <= math.MaxInt64
		case types.T_uint8:
			return constVal <= math.MaxUint8
		case types.T_uint16:
			return constVal <= math.MaxUint16
		case types.T_uint32:
			return constVal <= math.MaxUint32
		case types.T_uint64:
			return true
		case types.T_float32:
			//float32 has 6 significant digits.
			return constVal <= 100000
		case types.T_float64:
			//float64 has 15 significant digits.
			return constVal <= math.MaxUint32
		case types.T_decimal64:
			return constVal <= math.MaxInt32
		default:
			return false
		}

	case types.T_decimal64, types.T_decimal128:
		return columnT.Oid == types.T_decimal64 || columnT.Oid == types.T_decimal128

	default:
		return false
	}

}
