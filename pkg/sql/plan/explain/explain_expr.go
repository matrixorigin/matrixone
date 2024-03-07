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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func describeMessage(m *plan.MsgHeader, buf *bytes.Buffer) {
	buf.WriteString("[tag ")
	fmt.Fprintf(buf, "%d", m.MsgTag)
	buf.WriteString(" , type ")
	msgType := process.MsgType(m.MsgType)
	buf.WriteString(msgType.MessageName())
	buf.WriteString("]")
}

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

	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			buf.WriteString("(null)")
			break
		}

		switch val := exprImpl.Lit.Value.(type) {
		case *plan.Literal_I8Val:
			fmt.Fprintf(buf, "%d", val.I8Val)
		case *plan.Literal_I16Val:
			fmt.Fprintf(buf, "%d", val.I16Val)
		case *plan.Literal_I32Val:
			fmt.Fprintf(buf, "%d", val.I32Val)
		case *plan.Literal_I64Val:
			fmt.Fprintf(buf, "%d", val.I64Val)
		case *plan.Literal_U8Val:
			fmt.Fprintf(buf, "%d", val.U8Val)
		case *plan.Literal_U16Val:
			fmt.Fprintf(buf, "%d", val.U16Val)
		case *plan.Literal_U32Val:
			fmt.Fprintf(buf, "%d", val.U32Val)
		case *plan.Literal_U64Val:
			fmt.Fprintf(buf, "%d", val.U64Val)
		case *plan.Literal_Fval:
			fmt.Fprintf(buf, "%v", strconv.FormatFloat(float64(val.Fval), 'f', -1, 32))
		case *plan.Literal_Dval:
			fmt.Fprintf(buf, "%v", strconv.FormatFloat(val.Dval, 'f', -1, 64))
		case *plan.Literal_Dateval:
			fmt.Fprintf(buf, "%s", types.Date(val.Dateval))
		case *plan.Literal_Datetimeval:
			fmt.Fprintf(buf, "%s", types.Datetime(val.Datetimeval).String2(expr.Typ.Scale))
		case *plan.Literal_Timeval:
			fmt.Fprintf(buf, "%s", types.Time(val.Timeval).String2(expr.Typ.Scale))
		case *plan.Literal_Sval:
			buf.WriteString("'" + val.Sval + "'")
		case *plan.Literal_Bval:
			fmt.Fprintf(buf, "%v", val.Bval)
		case *plan.Literal_EnumVal:
			fmt.Fprintf(buf, "%v", val.EnumVal)
		case *plan.Literal_Decimal64Val:
			fmt.Fprintf(buf, "%s", types.Decimal64(val.Decimal64Val.A).Format(expr.Typ.GetScale()))
		case *plan.Literal_Decimal128Val:
			fmt.Fprintf(buf, "%s",
				types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}.Format(expr.Typ.GetScale()))
		}

	case *plan.Expr_F:
		err := funcExprExplain(ctx, expr.GetF(), &expr.Typ, options, buf)
		if err != nil {
			return err
		}
	case *plan.Expr_W:
		w := exprImpl.W
		err := funcExprExplain(ctx, w.WindowFunc.GetF(), &expr.Typ, options, buf)
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
		buf.WriteString("?")
	case *plan.Expr_List:
		exprlist := expr.Expr.(*plan.Expr_List)
		if exprlist.List.List != nil {
			exprListDescImpl := NewExprListDescribeImpl(exprlist.List.List)
			err := exprListDescImpl.GetDescription(ctx, options, buf)
			if err != nil {
				return err
			}
		}
	case *plan.Expr_Vec:
		vec := vector.NewVec(types.T_any.ToType())
		vec.UnmarshalBinary(exprImpl.Vec.Data)
		if vec.Length() > 16 {
			//don't display too long data in explain
			originalLen := vec.Length()
			vec.SetLength(16)
			buf.WriteString(vec.String())
			s := fmt.Sprintf("... %v values", originalLen)
			buf.WriteString(s)
		} else {
			buf.WriteString(vec.String())
		}
	case *plan.Expr_T:
		tt := types.T(expr.Typ.Id)
		if tt == types.T_decimal64 || tt == types.T_decimal128 {
			fmt.Fprintf(buf, "%s(%d, %d))", tt.String(), expr.Typ.Width, expr.Typ.Scale)
		} else {
			fmt.Fprintf(buf, "%s)", tt.String())
		}
	default:
		panic("unsupported expr")
	}
	return nil
}

// generator function expression(Expr_F) explain information
func funcExprExplain(ctx context.Context, funcExpr *plan.Function, Typ *plan.Type, options *ExplainOptions, buf *bytes.Buffer) error {
	// SysFunsAndOperatorsMap
	funcName := funcExpr.GetFunc().GetObjName()
	funcDef := funcExpr.GetFunc()

	layout, err := function.GetLayoutById(ctx, funcDef.Obj&function.DistinctMask)
	if err != nil {
		return moerr.NewInvalidInput(ctx, "invalid function or opreator '%s'", funcName)
	}

	switch layout {
	case function.STANDARD_FUNCTION:
		buf.WriteString(funcExpr.Func.GetObjName() + "(")
		if len(funcExpr.Args) > 0 {
			var first = true
			for _, v := range funcExpr.Args {
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
		if funcExpr.Func.GetObjName() == "UNARY_PLUS" {
			opertator = "+"
		} else {
			opertator = "-"
		}
		buf.WriteString("(" + opertator)
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.UNARY_LOGICAL_OPERATOR:
		buf.WriteString("(" + funcExpr.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
		//result += "(" + funcExpr.Func.GetObjName() + " " + describeExpr + ")"
	case function.BINARY_ARITHMETIC_OPERATOR:
		fallthrough
	case function.BINARY_LOGICAL_OPERATOR:
		fallthrough
	case function.COMPARISON_OPERATOR:
		buf.WriteString("(")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" " + funcExpr.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.CAST_EXPRESSION:
		buf.WriteString(funcName)
		buf.WriteString("(")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
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
		condSize := len(funcExpr.Args) - 1
		for i := 0; i < condSize; i += 2 {
			whenExpr := funcExpr.Args[i]
			thenExpr := funcExpr.Args[i+1]
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

		if len(funcExpr.Args)%2 == 1 {
			lastIndex := len(funcExpr.Args) - 1
			elseExpr := funcExpr.Args[lastIndex]
			// get else expression
			buf.WriteString(" ELSE ")
			err = describeExpr(ctx, elseExpr, options, buf)
			if err != nil {
				return err
			}
		}
		buf.WriteString(" END")
	case function.BETWEEN_AND_EXPRESSION:
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" BETWEEN ")
		err = describeExpr(ctx, funcExpr.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" AND ")
		err = describeExpr(ctx, funcExpr.Args[2], options, buf)
		if err != nil {
			return err
		}
	case function.IN_PREDICATE:
		if len(funcExpr.Args) != 2 {
			panic("Nested query predicate,such as in,exist,all,any parameter number error!")
		}
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" " + funcExpr.Func.GetObjName() + " (")
		err = describeExpr(ctx, funcExpr.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.EXISTS_ANY_PREDICATE:
		buf.WriteString(funcExpr.Func.GetObjName() + "(")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.IS_EXPRESSION:
		buf.WriteString("(")
		err := describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(fmt.Sprintf(" IS %s)", strings.ToUpper(funcExpr.Func.GetObjName()[2:])))
	case function.IS_NOT_EXPRESSION:
		buf.WriteString("(")
		err := describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(fmt.Sprintf(" IS NOT %s)", strings.ToUpper(funcExpr.Func.GetObjName()[5:])))
	case function.NOPARAMETER_FUNCTION:
		buf.WriteString(funcExpr.Func.GetObjName())
	case function.DATE_INTERVAL_EXPRESSION:
		buf.WriteString(funcExpr.Func.GetObjName() + " ")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
	case function.EXTRACT_FUNCTION:
		buf.WriteString(funcExpr.Func.GetObjName() + "(")
		err = describeExpr(ctx, funcExpr.Args[0], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(" from ")
		err = describeExpr(ctx, funcExpr.Args[1], options, buf)
		if err != nil {
			return err
		}
		buf.WriteString(")")
	case function.UNKNOW_KIND_FUNCTION:
		return moerr.NewInvalidInput(ctx, "explain contains UNKNOW_KIND_FUNCTION")
	}
	return nil
}
