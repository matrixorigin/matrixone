// Copyright 2021 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
)

func DescribeExpr(expr *plan.Expr) string {
	var result string
	switch expr.Expr.(type) {
	case *plan.Expr_Col:
		colExpr := expr.Expr.(*plan.Expr_Col)
		result += colExpr.Col.GetName()
	case *plan.Expr_C:
		constExpr := expr.Expr.(*plan.Expr_C)
		if intConst, ok := constExpr.C.Value.(*plan.Const_Ival); ok {
			result += fmt.Sprintf("%v", intConst.Ival)
		}

		if floatConst, ok := constExpr.C.Value.(*plan.Const_Dval); ok {
			result += fmt.Sprintf("%v", floatConst.Dval)
		}

		if strConst, ok := constExpr.C.Value.(*plan.Const_Sval); ok {
			result += fmt.Sprintf("%v", strConst.Sval)
		}
	case *plan.Expr_F:
		funcExpr := expr.Expr.(*plan.Expr_F)
		result += FuncExprExplain(funcExpr)
	case *plan.Expr_V:
		return "unimplement Expr_V"
	case *plan.Expr_P:
		return "unimplement Expr_P"
	case *plan.Expr_List:
		return "unimplement Expr_List"
	default:
		return "error Expr"
	}
	return result
}

func FuncExprExplain(funcExpr *plan.Expr_F) string {
	//SysFunsAndOperatorsMap
	var result string
	funcName := funcExpr.F.GetFunc().GetObjName()
	funcProtoType, ok := plan2.BuiltinFunctionsMap[funcName]
	if !ok {
		panic("unkonw expression")
	}
	switch funcProtoType.Kind {
	case plan2.STANDARD_FUNCTION:
		result += funcExpr.F.Func.GetObjName() + "("
		if len(funcExpr.F.Args) > 0 {
			var first bool = true
			for _, v := range funcExpr.F.Args {
				if !first {
					result += ", "
				}
				first = false
				result += DescribeExpr(v)
			}
		}
		result += ")"
	case plan2.UNARY_ARITHMETIC_OPERATOR:
		var opertator string
		if funcExpr.F.Func.GetObjName() == "UNARY_PLUS" {
			opertator = "+"
		} else {
			opertator = "-"
		}
		result += opertator + DescribeExpr(funcExpr.F.Args[0])
	case plan2.BINARY_ARITHMETIC_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case plan2.UNARY_LOGICAL_OPERATOR:
		result += funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[0])
	case plan2.BINARY_LOGICAL_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case plan2.COMPARISON_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case plan2.CAST_EXPRESSION:
		panic("CAST_EXPRESSION is not support now")
	case plan2.CASE_WHEN_EXPRESSION:
		panic("CASE_WHEN_EXPRESSION is not support now")
	case plan2.BETWEEN_AND_EXPRESSION:
		panic("CASE_WHEN_EXPRESSION is not support now")
	case plan2.IN_EXISTS_EXPRESSION:
		panic("CASE_WHEN_EXPRESSION is not support now")
	case plan2.IS_NULL_EXPRESSION:
		result += DescribeExpr(funcExpr.F.Args[0]) + " IS NULL"
	case plan2.NOPARAMETER_FUNCTION:
		result += funcExpr.F.Func.GetObjName()
	case plan2.UNKNOW_KIND_FUNCTION:
		panic("UNKNOW_KIND_FUNCTION is not support now")
	}
	return result
}
