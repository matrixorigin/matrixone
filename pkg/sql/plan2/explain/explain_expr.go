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

package explain

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
)

func describeExpr(expr *plan.Expr, options *ExplainOptions) (string, error) {
	var result string

	if expr.Expr == nil {
		result += expr.ColName
	} else {
		switch expr.Expr.(type) {
		case *plan.Expr_Col:
			result += expr.ColName
		case *plan.Expr_C:
			constExpr := expr.Expr.(*plan.Expr_C)
			if intConst, ok := constExpr.C.Value.(*plan.Const_Ival); ok {
				result += strconv.FormatInt(intConst.Ival, 10)
			}

			if floatConst, ok := constExpr.C.Value.(*plan.Const_Dval); ok {
				result += strconv.FormatFloat(floatConst.Dval, 'f', -1, 64)
			}

			if strConst, ok := constExpr.C.Value.(*plan.Const_Sval); ok {
				result += "'" + strConst.Sval + "'"
			}
		case *plan.Expr_F:
			funcExpr := expr.Expr.(*plan.Expr_F)
			funcDesc, err := funcExprExplain(funcExpr, expr.Typ, options)
			if err != nil {
				return result, err
			}
			result += funcDesc
		case *plan.Expr_Sub:
			subqryExpr := expr.Expr.(*plan.Expr_Sub)
			result += "subquery nodeId = " + strconv.FormatInt(int64(subqryExpr.Sub.NodeId), 10)
		case *plan.Expr_Corr:
			result += expr.ColName
		case *plan.Expr_V:
			panic("unimplement Expr_V")
		case *plan.Expr_P:
			panic("unimplement Expr_P")
		case *plan.Expr_List:
			exprlist := expr.Expr.(*plan.Expr_List)
			if exprlist.List.List != nil {
				exprListDescImpl := NewExprListDescribeImpl(exprlist.List.List)
				desclist, err := exprListDescImpl.GetDescription(options)
				if err != nil {
					return result, err
				}
				result += desclist
			}
		default:
			panic("error Expr")
		}
	}
	return result, nil
}

func funcExprExplain(funcExpr *plan.Expr_F, Typ *plan.Type, options *ExplainOptions) (string, error) {
	// SysFunsAndOperatorsMap
	var result string
	funcName := funcExpr.F.GetFunc().GetObjName()
	funcDef := funcExpr.F.GetFunc()
	// Get function explain type
	// funcProtoType, ok := plan2.BuiltinFunctionsMap[funcName]
	// if !ok {
	// 	return result, errors.New(errno.InvalidName, "invalid function or opreator name '"+funcName+"'")
	// }

	funcProtoType, err := function.GetFunctionByID(funcDef.Obj)
	if err != nil {
		return result, errors.New(errno.InvalidName, "invalid function or opreator name '"+funcName+"'")
	}

	switch funcProtoType.Layout {
	case function.STANDARD_FUNCTION:
		result += funcExpr.F.Func.GetObjName() + "("
		if len(funcExpr.F.Args) > 0 {
			var first = true
			for _, v := range funcExpr.F.Args {
				if !first {
					result += ", "
				}
				first = false
				exprDesc, err := describeExpr(v, options)
				if err != nil {
					return result, err
				}
				result += exprDesc
			}
		}
		result += ")"
	case function.UNARY_ARITHMETIC_OPERATOR:
		var opertator string
		if funcExpr.F.Func.GetObjName() == "UNARY_PLUS" {
			opertator = "+"
		} else {
			opertator = "-"
		}
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += "(" + opertator + describeExpr + ")"
	case function.UNARY_LOGICAL_OPERATOR:
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += "(" + funcExpr.F.Func.GetObjName() + " " + describeExpr + ")"
	case function.BINARY_ARITHMETIC_OPERATOR:
		fallthrough
	case function.BINARY_LOGICAL_OPERATOR:
		fallthrough
	case function.COMPARISON_OPERATOR:
		left, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		right, err := describeExpr(funcExpr.F.Args[1], options)
		if err != nil {
			return result, err
		}
		result += "(" + left + " " + funcExpr.F.Func.GetObjName() + " " + right + ")"
	case function.CAST_EXPRESSION:
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += "CAST(" + describeExpr + " AS " + plan.Type_TypeId_name[int32(Typ.Id)] + ")"
	case function.CASE_WHEN_EXPRESSION:
		// TODO need rewrite to deal with case is nil
		result += "CASE"
		// result += "CASE"
		// // case when expression has three part (case expression, when expression, else expression)
		// if len(funcExpr.F.Args) != 3 {
		// 	return result, errors.New(errno.SyntaxErrororAccessRuleViolation, "case expression parameter number error")
		// }
		// // case expression can be null
		// if funcExpr.F.Args[0] != nil {
		// 	// get case expression
		// 	caseDesc, err := describeExpr(funcExpr.F.Args[0], options)
		// 	if err != nil {
		// 		return result, err
		// 	}
		// 	result += " " + caseDesc
		// }

		// // get when expression
		// var whenExpr *plan.Expr = funcExpr.F.Args[1]
		// var whenlist *plan.Expr_List = whenExpr.Expr.(*plan.Expr_List)
		// var list *plan.ExprList = whenlist.List

		// for i := 0; i < len(list.List); i++ {
		// 	whenThenExpr := list.List[i].Expr.(*plan.Expr_List)
		// 	if len(whenThenExpr.List.List) != 2 {
		// 		return result, errors.New(errno.SyntaxErrororAccessRuleViolation, "case when expression parameter number is not equal to 2")
		// 	}

		// 	whenExprDesc, err := describeExpr(whenThenExpr.List.List[0], options)
		// 	if err != nil {
		// 		return result, err
		// 	}
		// 	thenExprDesc, err := describeExpr(whenThenExpr.List.List[1], options)
		// 	if err != nil {
		// 		return result, err
		// 	}
		// 	result += " WHEN " + whenExprDesc + " THEN " + thenExprDesc
		// }
		// // when expression can be null
		// if funcExpr.F.Args[2] != nil {
		// 	// get else expression
		// 	elseExprDesc, err := describeExpr(funcExpr.F.Args[2], options)
		// 	if err != nil {
		// 		return result, err
		// 	}
		// 	result += " ELSE " + elseExprDesc
		// }
		// result += " END"
	case function.IN_PREDICATE:
		if len(funcExpr.F.Args) != 2 {
			panic("Nested query predicate,such as in,exist,all,any parameter number error!")
		}
		descExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		descExprlist, err := describeExpr(funcExpr.F.Args[1], options)
		if err != nil {
			return result, err
		}
		result += descExpr + " " + funcExpr.F.Func.GetObjName() + "(" + descExprlist + ")"
	case function.EXISTS_ANY_PREDICATE:
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += funcExpr.F.Func.GetObjName() + "(" + describeExpr + ")"
	case function.IS_NULL_EXPRESSION:
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += "(" + describeExpr + " IS NULL)"
	case function.NOPARAMETER_FUNCTION:
		result += funcExpr.F.Func.GetObjName()
	case function.DATE_INTERVAL_EXPRESSION:
		describeExpr, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		result += funcExpr.F.Func.GetObjName() + " " + describeExpr + ""
	case function.EXTRACT_FUNCTION:
		first, err := describeExpr(funcExpr.F.Args[0], options)
		if err != nil {
			return result, err
		}
		second, err := describeExpr(funcExpr.F.Args[1], options)
		if err != nil {
			return result, err
		}

		result += funcExpr.F.Func.GetObjName() + "(" + first + " from " + second + ")"
	case function.UNKNOW_KIND_FUNCTION:
		return result, errors.New(errno.UndefinedFunction, "UNKNOW_KIND_FUNCTION is not support now")
	}
	return result, nil
}
