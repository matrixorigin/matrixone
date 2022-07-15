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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func describeExpr(expr *plan.Expr, options *ExplainOptions) (string, error) {
	var result string

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if len(exprImpl.Col.Name) > 0 {
			result += exprImpl.Col.Name
		} else {
			result += "#["
			result += strconv.FormatInt(int64(exprImpl.Col.RelPos), 10)
			result += ","
			result += strconv.FormatInt(int64(exprImpl.Col.ColPos), 10)
			result += "]"
		}
	case *plan.Expr_C:
		if exprImpl.C.Isnull {
			result += expr.Typ.Id.String() + "(null)"
			break
		}

		switch val := exprImpl.C.Value.(type) {
		case *plan.Const_Ival:
			result += strconv.FormatInt(val.Ival, 10)

		case *plan.Const_Dval:
			result += strconv.FormatFloat(val.Dval, 'f', -1, 64)

		case *plan.Const_Sval:
			result += "'" + val.Sval + "'"

		case *plan.Const_Bval:
			result += strconv.FormatBool(val.Bval)
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
		result += "#["
		result += strconv.FormatInt(int64(exprImpl.Corr.RelPos), 10)
		result += ","
		result += strconv.FormatInt(int64(exprImpl.Corr.ColPos), 10)
		result += ":"
		result += strconv.FormatInt(int64(exprImpl.Corr.Depth), 10)
		result += "]"
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

	return result, nil
}

// generator function expression(Expr_F) explain information
func funcExprExplain(funcExpr *plan.Expr_F, Typ *plan.Type, options *ExplainOptions) (string, error) {
	// SysFunsAndOperatorsMap
	var result string
	funcName := funcExpr.F.GetFunc().GetObjName()
	funcDef := funcExpr.F.GetFunc()

	funcProtoType, err := function.GetFunctionByID(funcDef.Obj & function.DistinctMask)
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
		// case when expression has two part(case when condition and else exression)
		condSize := len(funcExpr.F.Args) / 2
		for i := 0; i < condSize; i++ {
			whenExpr := funcExpr.F.Args[i]
			thenExpr := funcExpr.F.Args[i+1]
			whenExprDesc, err := describeExpr(whenExpr, options)
			if err != nil {
				return result, err
			}
			thenExprDesc, err := describeExpr(thenExpr, options)
			if err != nil {
				return result, err
			}
			result += " WHEN " + whenExprDesc + " THEN " + thenExprDesc
		}

		if len(funcExpr.F.Args)%2 == 1 {
			lastIndex := len(funcExpr.F.Args) - 1
			elseExpr := funcExpr.F.Args[lastIndex]
			// get else expression
			elseExprDesc, err := describeExpr(elseExpr, options)
			if err != nil {
				return result, err
			}
			result += " ELSE " + elseExprDesc
		}
		result += " END"
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
