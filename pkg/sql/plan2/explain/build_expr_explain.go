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
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"strconv"
)

func DescribeExpr(expr *plan.Expr) (string, error) {
	var result string
	switch expr.Expr.(type) {
	case *plan.Expr_Col:
		colExpr := expr.Expr.(*plan.Expr_Col)
		result += colExpr.Col.GetName()
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
		funcDesc, err := FuncExprExplain(funcExpr, expr.Typ)
		if err != nil {
			return result, err
		}
		result += funcDesc
	case *plan.Expr_Sub:
		subqryExpr := expr.Expr.(*plan.Expr_Sub)
		result += "subquery at nodes[" + strconv.FormatInt(int64(subqryExpr.Sub.NodeId), 10) + "]"
	case *plan.Expr_Corr:
		result += expr.Expr.(*plan.Expr_Corr).Corr.GetName()
	case *plan.Expr_V:
		panic("unimplement Expr_V")
	case *plan.Expr_P:
		panic("unimplement Expr_P")
	case *plan.Expr_List:
		panic("unimplement Expr_List")
	default:
		panic("error Expr")
	}
	return result, nil
}

func FuncExprExplain(funcExpr *plan.Expr_F, Typ *plan.Type) (string, error) {
	//SysFunsAndOperatorsMap
	var result string
	funcName := funcExpr.F.GetFunc().GetObjName()
	// Get function explain type
	funcProtoType, ok := plan2.BuiltinFunctionsMap[funcName]
	if !ok {
		return result, errors.New(errno.InvalidName, "invalid function or opreator name '"+funcName+"'")
	}
	switch funcProtoType.Layout {
	case plan2.STANDARD_FUNCTION:
		result += funcExpr.F.Func.GetObjName() + "("
		if len(funcExpr.F.Args) > 0 {
			var first bool = true
			for _, v := range funcExpr.F.Args {
				if !first {
					result += ", "
				}
				first = false
				exprDesc, err := DescribeExpr(v)
				if err != nil {
					return result, err
				}
				result += exprDesc
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
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += "(" + opertator + describeExpr + ")"
	case plan2.UNARY_LOGICAL_OPERATOR:
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += "(" + funcExpr.F.Func.GetObjName() + " " + describeExpr + ")"
	case plan2.BINARY_ARITHMETIC_OPERATOR:
		fallthrough
	case plan2.BINARY_LOGICAL_OPERATOR:
		fallthrough
	case plan2.COMPARISON_OPERATOR:
		left, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		right, err := DescribeExpr(funcExpr.F.Args[1])
		if err != nil {
			return result, err
		}
		result += "(" + left + " " + funcExpr.F.Func.GetObjName() + " " + right + ")"
	case plan2.CAST_EXPRESSION:
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += "CAST(" + describeExpr + " AS " + plan.Type_TypeId_name[int32(Typ.Id)] + ")"
	case plan2.CASE_WHEN_EXPRESSION:
		result += "CASE"
		// case when expresion has threee part (case exprssion, when exprssion, else exprssion)
		if len(funcExpr.F.Args) != 3 {
			return result, errors.New(errno.SyntaxErrororAccessRuleViolation, "case expression parameter number error")
		}
		// case exprssion can be null
		if funcExpr.F.Args[0] != nil {
			// get case exprssion
			caseDesc, err := DescribeExpr(funcExpr.F.Args[0])
			if err != nil {
				return result, err
			}
			result += " " + caseDesc
		}

		// get when exprssion
		var whenExpr *plan.Expr = funcExpr.F.Args[1]
		var whenlist *plan.Expr_List = whenExpr.Expr.(*plan.Expr_List)
		var list *plan.ExprList = whenlist.List

		for i := 0; i < len(list.List); i++ {
			whenThenExpr := list.List[i].Expr.(*plan.Expr_List)
			if len(whenThenExpr.List.List) != 2 {
				return result, errors.New(errno.SyntaxErrororAccessRuleViolation, "case when expression parameter number is not equal to 2")
			}

			whenExprDesc, err := DescribeExpr(whenThenExpr.List.List[0])
			if err != nil {
				return result, err
			}
			thenExprDesc, err := DescribeExpr(whenThenExpr.List.List[1])
			if err != nil {
				return result, err
			}
			result += " WHEN " + whenExprDesc + " THEN " + thenExprDesc
		}
		// when expression can be null
		if funcExpr.F.Args[2] != nil {
			// get else exprssion
			elseExprDesc, err := DescribeExpr(funcExpr.F.Args[2])
			if err != nil {
				return result, err
			}
			result += " ELSE " + elseExprDesc
		}
		result += " END"
	case plan2.NESTED_QUERY_PREDICATE:
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += funcExpr.F.Func.GetObjName() + "(" + describeExpr + ")"
	case plan2.IS_NULL_EXPRESSION:
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += "(" + describeExpr + " IS NULL)"
	case plan2.NOPARAMETER_FUNCTION:
		result += funcExpr.F.Func.GetObjName()
	case plan2.DATE_INTERVAL_EXPRESSION:
		describeExpr, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		result += funcExpr.F.Func.GetObjName() + " '" + describeExpr + "'"
	case plan2.EXTRACT_FUNCTION:
		first, err := DescribeExpr(funcExpr.F.Args[0])
		if err != nil {
			return result, err
		}
		second, err := DescribeExpr(funcExpr.F.Args[1])
		if err != nil {
			return result, err
		}

		result += funcExpr.F.Func.GetObjName() + "(" + first + " from " + second + ")"
	case plan2.UNKNOW_KIND_FUNCTION:
		return result, errors.New(errno.UndefinedFunction, "UNKNOW_KIND_FUNCTION is not support now")
	}
	return result, nil
}

//isSimpleNode - check if given node is simple (doesn't need parenthesizing)
func isSimpleExprFunc(funcExpr *plan.Expr_F) (bool, error) {
	if funcExpr == nil {
		return false, nil
	}
	funcName := funcExpr.F.Func.GetObjName()
	functionSig, ok := plan2.BuiltinFunctionsMap[funcName]
	if !ok {
		return false, errors.New(errno.InvalidName, "invalid function or opreator name '"+funcName+"'")
	}

	switch functionSig.Layout {
	case plan2.STANDARD_FUNCTION:
		return true, nil
	case plan2.UNARY_ARITHMETIC_OPERATOR:
	case plan2.BINARY_ARITHMETIC_OPERATOR:
		return true, nil
	case plan2.UNARY_LOGICAL_OPERATOR:
		return true, nil
	case plan2.BINARY_LOGICAL_OPERATOR:
		return true, nil
	case plan2.COMPARISON_OPERATOR:
		return true, nil
	case plan2.CAST_EXPRESSION:
		return true, nil
	case plan2.CASE_WHEN_EXPRESSION:
		return true, nil
	case plan2.BETWEEN_AND_EXPRESSION:
	case plan2.NESTED_QUERY_PREDICATE:
		return true, nil
	case plan2.IS_NULL_EXPRESSION:
		return true, nil
	case plan2.NOPARAMETER_FUNCTION:
		return false, nil
	case plan2.DATE_INTERVAL_EXPRESSION:
		return false, nil
	case plan2.EXTRACT_FUNCTION:
		return true, nil
	case plan2.POSITION_FUNCTION:
		return true, nil
	case plan2.UNKNOW_KIND_FUNCTION:
		return false, nil
	}
	return false, nil
}
