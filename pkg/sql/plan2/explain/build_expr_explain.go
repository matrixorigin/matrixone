package explain

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	funcProtoType, ok := SysFunsAndOperatorsMap[funcName]
	if !ok {
		panic("implement me")
	}
	switch funcProtoType.Kind {
	case STANDARD_FUNCTION:
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
	case UNARY_ARITHMETIC_OPERATOR:
		result += funcExpr.F.Func.GetObjName() + DescribeExpr(funcExpr.F.Args[0])
	case BINARY_ARITHMETIC_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case UNARY_LOGICAL_OPERATOR:
		result += funcExpr.F.Func.GetObjName() + DescribeExpr(funcExpr.F.Args[0])
	case BINARY_LOGICAL_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case COMPARISON_OPERATOR:
		result += DescribeExpr(funcExpr.F.Args[0]) + " " + funcExpr.F.Func.GetObjName() + " " + DescribeExpr(funcExpr.F.Args[1])
	case CAST_EXPRESSION:
		fmt.Printf("CAST_EXPRESSION is not support now")
		panic("implement me")
	case CASE_WHEN_EXPRESSION:
		fmt.Printf("CASE_WHEN_EXPRESSION is not support now")
		panic("implement me")
	case BETWEEN_AND_EXPRESSION:
		fmt.Printf("CASE_WHEN_EXPRESSION is not support now")
		panic("implement me")
	case IN_EXISTS_EXPRESSION:
		fmt.Printf("CASE_WHEN_EXPRESSION is not support now")
		panic("implement me")
	case NOPARAMETER_FUNCTION:
		result += funcExpr.F.Func.GetObjName()
	case UNKNOW_KIND_FUNCTION:
		fmt.Printf("UNKNOW_KIND_FUNCTION is not support now")
		panic("implement me")
	}
	return result
}
