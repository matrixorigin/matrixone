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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func describeExpr(ctx context.Context, expr *plan.Expr, options *ExplainOptions, buf *bytes.Buffer) error {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if len(exprImpl.Col.Name) > 0 {
			buf.WriteString(exprImpl.Col.Name)
		} else {
			buf.WriteString("#[")
			buf.WriteString(strconv.Itoa(int(exprImpl.Col.RelPos)))
			buf.WriteString(",")
			buf.WriteString(strconv.Itoa(int(exprImpl.Col.ColPos)))
			buf.WriteString("]")
		}

	case *plan.Expr_C:
		if exprImpl.C.Isnull {
			buf.WriteString("(null)")
			break
		}

		switch val := exprImpl.C.Value.(type) {
		case *plan.Const_I8Val:
			fmt.Fprintf(buf, "%d", val.I8Val)
		case *plan.Const_I16Val:
			fmt.Fprintf(buf, "%d", val.I16Val)
		case *plan.Const_I32Val:
			fmt.Fprintf(buf, "%d", val.I32Val)
		case *plan.Const_I64Val:
			fmt.Fprintf(buf, "%d", val.I64Val)
		case *plan.Const_U8Val:
			fmt.Fprintf(buf, "%d", val.U8Val)
		case *plan.Const_U16Val:
			fmt.Fprintf(buf, "%d", val.U16Val)
		case *plan.Const_U32Val:
			fmt.Fprintf(buf, "%d", val.U32Val)
		case *plan.Const_U64Val:
			fmt.Fprintf(buf, "%d", val.U64Val)
		case *plan.Const_Fval:
			fmt.Fprintf(buf, "%v", strconv.FormatFloat(float64(val.Fval), 'f', -1, 32))
		case *plan.Const_Dval:
			fmt.Fprintf(buf, "%v", strconv.FormatFloat(val.Dval, 'f', -1, 64))
		case *plan.Const_Dateval:
			fmt.Fprintf(buf, "%s", types.Date(val.Dateval))
		case *plan.Const_Datetimeval:
			fmt.Fprintf(buf, "%s", types.Date(val.Datetimeval))
		case *plan.Const_Timeval:
			fmt.Fprintf(buf, "%s", types.Date(val.Timeval))
		case *plan.Const_Sval:
			buf.WriteString("'" + val.Sval + "'")
		case *plan.Const_Bval:
			fmt.Fprintf(buf, "%v", val.Bval)
		case *plan.Const_Decimal64Val:
			fmt.Fprintf(buf, "%s", types.Decimal64(val.Decimal64Val.A).Format(expr.Typ.GetScale()))
		case *plan.Const_Decimal128Val:
			fmt.Fprintf(buf, "%s",
				types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}.Format(expr.Typ.GetScale()))
		}

	case *plan.Expr_F:
		funcExpr := expr.Expr.(*plan.Expr_F)
		err := funcExprExplain(ctx, funcExpr, expr.Typ, options, buf)
		if err != nil {
			return err
		}
	case *plan.Expr_W:
		w := exprImpl.W
		funcExpr := w.WindowFunc.Expr.(*plan.Expr_F)
		err := funcExprExplain(ctx, funcExpr, expr.Typ, options, buf)
		if err != nil {
			return err
		}

		if len(w.PartitionBy) > 0 {
			buf.WriteString("; Partition By: ")
			for i, arg := range w.PartitionBy {
				if i > 0 {
					buf.WriteString(", ")
				}
				err = describeExpr(ctx, arg, options, buf)
				if err != nil {
					return err
				}
			}
		}

		if len(w.OrderBy) > 0 {
			buf.WriteString("; Order By: ")
			for i, arg := range w.OrderBy {
				if i > 0 {
					buf.WriteString(", ")
				}
				err = describeExpr(ctx, arg.Expr, options, buf)
				if err != nil {
					return err
				}
			}
		}
	case *plan.Expr_Sub:
		subqryExpr := expr.Expr.(*plan.Expr_Sub)
		buf.WriteString("subquery nodeId = " + strconv.FormatInt(int64(subqryExpr.Sub.NodeId), 10))
	case *plan.Expr_Corr:
		buf.WriteString("#[")
		buf.WriteString(strconv.FormatInt(int64(exprImpl.Corr.RelPos), 10))
		buf.WriteString(",")
		buf.WriteString(strconv.FormatInt(int64(exprImpl.Corr.ColPos), 10))
		buf.WriteString(":")
		buf.WriteString(strconv.FormatInt(int64(exprImpl.Corr.Depth), 10))
		buf.WriteString("]")
	case *plan.Expr_V:
		if exprImpl.V.System {
			if exprImpl.V.Global {
				buf.WriteString("@@global." + exprImpl.V.Name)
			} else {
				buf.WriteString("@@session." + exprImpl.V.Name)
			}
		} else {
			buf.WriteString("@" + exprImpl.V.Name)
		}
	case *plan.Expr_P:
		panic("unimplement Expr_P")
	case *plan.Expr_List:
		exprlist := expr.Expr.(*plan.Expr_List)
		if exprlist.List.List != nil {
			exprListDescImpl := NewExprListDescribeImpl(exprlist.List.List)
			err := exprListDescImpl.GetDescription(ctx, options, buf)
			if err != nil {
				return err
			}
		}
	default:
		panic("error Expr")
	}
	return nil
}

// generator function expression(Expr_F) explain information
func funcExprExplain(ctx context.Context, funcExpr *plan.Expr_F, Typ *plan.Type, options *ExplainOptions, buf *bytes.Buffer) error {
	// SysFunsAndOperatorsMap
	funcName := funcExpr.F.GetFunc().GetObjName()
	funcDef := funcExpr.F.GetFunc()

	layout, err := function.GetLayoutById(ctx, funcDef.Obj&function.DistinctMask)
	if err != nil {
		return moerr.NewInvalidInput(ctx, "invalid function or opreator '%s'", funcName)
	}

	switch layout {
	case function.STANDARD_FUNCTION:
		buf.WriteString(funcExpr.F.Func.GetObjName() + "(")
		if len(funcExpr.F.Args) > 0 {
			var first = true
			for _, v := range funcExpr.F.Args {
				if !first {
					buf.WriteString(", ")
				}
				first = false
				err = describeExpr(ctx, v, options, buf)
				if err != nil {
					return err
				}
			}
		}
		buf.WriteString(")")
	case function.UNARY_ARITHMETIC_OPERATOR:
		var opertator string
		if funcExpr.F.Func.GetObjName() == "UNARY_PLUS" {
			opertator = "+"
		} else {
			opertator = "-"
		}
		buf.WriteString("(" + opertator)
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.UNARY_LOGICAL_OPERATOR:
		buf.WriteString("(" + funcExpr.F.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
		//result += "(" + funcExpr.F.Func.GetObjName() + " " + describeExpr + ")"
	case function.BINARY_ARITHMETIC_OPERATOR:
		fallthrough
	case function.BINARY_LOGICAL_OPERATOR:
		fallthrough
	case function.COMPARISON_OPERATOR:
		buf.WriteString("(")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" " + funcExpr.F.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.F.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.CAST_EXPRESSION:
		buf.WriteString("CAST(")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		tt := types.T(Typ.Id)
		if tt == types.T_decimal64 || tt == types.T_decimal128 {
			fmt.Fprintf(buf, " AS %s(%d, %d))", tt.String(), Typ.Width, Typ.Scale)
		} else {
			fmt.Fprintf(buf, " AS %s)", tt.String())
		}
	case function.CASE_WHEN_EXPRESSION:
		// TODO need rewrite to deal with case is nil
		buf.WriteString("CASE")
		// case when expression has two part(case when condition and else exression)
		condSize := len(funcExpr.F.Args) - 1
		for i := 0; i < condSize; i += 2 {
			whenExpr := funcExpr.F.Args[i]
			thenExpr := funcExpr.F.Args[i+1]
			buf.WriteString(" WHEN ")
			err = describeExpr(ctx, whenExpr, options, buf)
			if err != nil {
				return err
			}
			buf.WriteString(" THEN ")
			err = describeExpr(ctx, thenExpr, options, buf)
			if err != nil {
				return err
			}
		}

		if len(funcExpr.F.Args)%2 == 1 {
			lastIndex := len(funcExpr.F.Args) - 1
			elseExpr := funcExpr.F.Args[lastIndex]
			// get else expression
			buf.WriteString(" ELSE ")
			err = describeExpr(ctx, elseExpr, options, buf)
			if err != nil {
				return err
			}
		}
		buf.WriteString(" END")
	case function.IN_PREDICATE:
		if len(funcExpr.F.Args) != 2 {
			panic("Nested query predicate,such as in,exist,all,any parameter number error!")
		}
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" " + funcExpr.F.Func.GetObjName() + "(")
		err = describeExpr(ctx, funcExpr.F.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.EXISTS_ANY_PREDICATE:
		buf.WriteString(funcExpr.F.Func.GetObjName() + "(")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.IS_EXPRESSION:
		buf.WriteString("(")
		err := describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(fmt.Sprintf(" IS %s)", strings.ToUpper(funcExpr.F.Func.GetObjName()[2:])))
	case function.IS_NOT_EXPRESSION:
		buf.WriteString("(")
		err := describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(fmt.Sprintf(" IS NOT %s)", strings.ToUpper(funcExpr.F.Func.GetObjName()[5:])))
	case function.NOPARAMETER_FUNCTION:
		buf.WriteString(funcExpr.F.Func.GetObjName())
	case function.DATE_INTERVAL_EXPRESSION:
		buf.WriteString(funcExpr.F.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
	case function.EXTRACT_FUNCTION:
		buf.WriteString(funcExpr.F.Func.GetObjName() + "(")
		err = describeExpr(ctx, funcExpr.F.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" from ")
		err = describeExpr(ctx, funcExpr.F.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.UNKNOW_KIND_FUNCTION:
		return moerr.NewInvalidInput(ctx, "explain contains UNKNOW_KIND_FUNCTION")
	}
	return nil
}
