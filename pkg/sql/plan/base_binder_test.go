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

package plan

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// TestBindFuncExprImplByPlanExpr_PowAlias tests that "pow" is correctly
// remapped to "power" (line ~1781 in base_binder.go:
// case "pow": name = "power").
func TestBindFuncExprImplByPlanExpr_PowAlias(t *testing.T) {
	ctx := context.Background()

	t.Run("pow with two int args", func(t *testing.T) {
		x := makeInt64ConstPlanExpr(2)
		y := makeInt64ConstPlanExpr(10)
		result, err := BindFuncExprImplByPlanExpr(ctx, "pow", []*plan.Expr{x, y})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "result should be a function")
		// "pow" is remapped to "power"
		require.Equal(t, "power", f.Func.GetObjName())
	})

	t.Run("power with two int args", func(t *testing.T) {
		x := makeInt64ConstPlanExpr(3)
		y := makeInt64ConstPlanExpr(4)
		result, err := BindFuncExprImplByPlanExpr(ctx, "power", []*plan.Expr{x, y})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f)
		require.Equal(t, "power", f.Func.GetObjName())
	})
}

func TestBindSQLUDFUsesStoredParserMode(t *testing.T) {
	binder := NewDefaultBinder(context.Background(), nil, nil, plan.Type{}, nil)

	t.Run("legacy UDF uses historical pipe concat mode", func(t *testing.T) {
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "legacy_pipe", &function.Udf{
			Body:     "0 || 1",
			Language: string(tree.SQL),
		}, nil, 0)
		require.NoError(t, err)
		require.Equal(t, "concat", expr.GetF().GetFunc().GetObjName())
	})

	t.Run("stored empty mode keeps logical or semantics", func(t *testing.T) {
		emptyMode := ""
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "logical_or", &function.Udf{
			Body:     "0 || 1",
			Language: string(tree.SQL),
			SQLMode:  &emptyMode,
		}, nil, 0)
		require.NoError(t, err)
		require.Equal(t, "or", expr.GetF().GetFunc().GetObjName())
	})
}

func TestCombinePlanExprsBalancedHasLogarithmicDepth(t *testing.T) {
	const leafCount = 1024

	exprs := make([]*plan.Expr, leafCount)
	for i := range exprs {
		exprs[i] = &plan.Expr{
			Typ: plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: true,
			},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(i),
				},
			},
		}
	}

	combined, err := combinePlanExprsBalanced(context.Background(), "or", exprs)
	require.NoError(t, err)

	depth, leaves := planExprDepthAndLeaves(combined)
	require.Equal(t, leafCount, leaves)
	require.LessOrEqual(t, depth, 11)
}

func TestHandleTupleInBuildsBalancedTree(t *testing.T) {
	const tupleCount = 1024

	left := &plan.Expr_List{
		List: &plan.ExprList{
			List: []*plan.Expr{
				makeTupleInTestColumn(0),
				makeTupleInTestColumn(1),
			},
		},
	}
	right := &plan.ExprList{List: make([]*plan.Expr, tupleCount)}
	for i := range right.List {
		right.List[i] = &plan.Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: []*plan.Expr{
						MakePlan2Int64ConstExprWithType(int64(i)),
						MakePlan2Int64ConstExprWithType(int64(i + 1)),
					},
				},
			},
		}
	}

	combined, err := handleTupleIn(context.Background(), "in", left, right)
	require.NoError(t, err)

	depth, leaves := planExprDepthAndLeaves(combined)
	require.Equal(t, tupleCount*4, leaves)
	require.LessOrEqual(t, depth, 13)
}

func makeTupleInTestColumn(colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: colPos,
			},
		},
	}
}

func planExprDepthAndLeaves(expr *plan.Expr) (int, int) {
	f := expr.GetF()
	if f == nil {
		return 1, 1
	}

	maxChildDepth := 0
	leaves := 0
	for _, arg := range f.Args {
		childDepth, childLeaves := planExprDepthAndLeaves(arg)
		maxChildDepth = max(maxChildDepth, childDepth)
		leaves += childLeaves
	}
	return maxChildDepth + 1, leaves
}

func TestBindSerialFunctionMapsExprListItems(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{function.SerialFunctionName, function.SerialFullFunctionName} {
		t.Run(name, func(t *testing.T) {
			arg := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{
							MakePlan2Int64ConstExprWithType(1),
							MakePlan2Int64ConstExprWithType(2),
						},
					},
				},
			}

			result, err := BindFuncExprImplByPlanExpr(ctx, name, []*plan.Expr{arg})
			require.NoError(t, err)
			require.Same(t, arg, result)
			require.Equal(t, int32(types.T_varchar), result.Typ.Id)

			list := result.GetList()
			require.NotNil(t, list)
			require.Len(t, list.List, 2)
			for i, item := range list.List {
				f := item.GetF()
				require.NotNil(t, f)
				require.Equal(t, name, f.Func.GetObjName())
				require.Len(t, f.Args, 1)
				require.Equal(t, int64(i+1), f.Args[0].GetLit().GetI64Val())
			}
		})
	}
}

func TestBindScoreBinaryHexnumKeepsBinarySemanticsExceptNumericCast(t *testing.T) {
	binder := &baseBinder{sysCtx: context.Background()}
	hex := tree.NewNumVal("0x3132", "0x3132", false, tree.P_ScoreBinaryHexnum)

	rawExpr, err := binder.bindNumVal(hex, plan.Type{})
	require.NoError(t, err)
	require.Equal(t, "12", rawExpr.GetLit().GetSval())
	require.Equal(t, int32(types.T_varbinary), rawExpr.Typ.Id)
	require.False(t, rawExpr.GetLit().GetIsBin())

	testCases := []struct {
		name  string
		typ   plan.Type
		isBin bool
	}{
		{name: "integer numeric cast parses text", typ: plan.Type{Id: int32(types.T_uint64)}, isBin: false},
		{name: "decimal numeric cast parses text", typ: plan.Type{Id: int32(types.T_decimal128)}, isBin: false},
		{name: "float numeric cast parses text", typ: plan.Type{Id: int32(types.T_float64)}, isBin: false},
		{name: "binary cast keeps binary string type", typ: plan.Type{Id: int32(types.T_binary)}, isBin: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			castExpr, err := binder.bindNumVal(hex, tc.typ)
			require.NoError(t, err)
			castFunc := castExpr.GetF()
			require.NotNil(t, castFunc)
			require.Len(t, castFunc.Args, 2)
			require.Equal(t, "12", castFunc.Args[0].GetLit().GetSval())
			require.Equal(t, tc.isBin, castFunc.Args[0].GetLit().GetIsBin())
		})
	}

	target := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_T{
			T: &plan.TargetType{},
		},
	}
	explicitCast, err := BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{rawExpr, target})
	require.NoError(t, err)
	explicitCastFunc := explicitCast.GetF()
	require.NotNil(t, explicitCastFunc)
	require.Equal(t, int32(types.T_varbinary), explicitCastFunc.Args[0].Typ.Id)
	require.False(t, explicitCastFunc.Args[0].GetLit().GetIsBin())

	plainHex := tree.NewNumVal("0x3132", "0x3132", false, tree.P_hexnum)
	plainHexExpr, err := binder.bindNumVal(plainHex, plan.Type{})
	require.NoError(t, err)
	require.True(t, plainHexExpr.GetLit().GetIsBin())

	bitOrExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "|", []*plan.Expr{rawExpr, plainHexExpr})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_varbinary), bitOrExpr.Typ.Id)

	bitCountExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "bit_count", []*plan.Expr{rawExpr})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), bitCountExpr.Typ.Id)
	require.Equal(t, int32(types.T_varbinary), bitCountExpr.GetF().Args[0].Typ.Id)
}

func TestBindScoreBinaryStringUsesBinaryStringSemantics(t *testing.T) {
	binder := &baseBinder{sysCtx: context.Background()}
	binStr := tree.NewNumVal("1", "1", false, tree.P_ScoreBinary)

	rawExpr, err := binder.bindNumVal(binStr, plan.Type{})
	require.NoError(t, err)
	require.Equal(t, "1", rawExpr.GetLit().GetSval())
	require.Equal(t, int32(types.T_varbinary), rawExpr.Typ.Id)
	require.False(t, rawExpr.GetLit().GetIsBin())

	castExpr, err := binder.bindNumVal(binStr, plan.Type{Id: int32(types.T_uint64)})
	require.NoError(t, err)
	castFunc := castExpr.GetF()
	require.NotNil(t, castFunc)
	require.Len(t, castFunc.Args, 2)
	require.Equal(t, "1", castFunc.Args[0].GetLit().GetSval())
	require.Equal(t, int32(types.T_varbinary), castFunc.Args[0].Typ.Id)
	require.False(t, castFunc.Args[0].GetLit().GetIsBin())
}

func TestBindSerialFunctionOverEmptyExprListDoesNotPanic(t *testing.T) {
	ctx := context.Background()

	for _, name := range []string{function.SerialFunctionName, function.SerialFullFunctionName} {
		t.Run(name, func(t *testing.T) {
			arg := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_List{
					List: &plan.ExprList{},
				},
			}

			result, err := BindFuncExprImplByPlanExpr(ctx, name, []*plan.Expr{arg})
			require.NoError(t, err)
			require.Same(t, arg, result)
			require.NotNil(t, result.GetList())
			require.Empty(t, result.GetList().List)
		})
	}
}

func TestBindUnaryMinusUint64MinInt64Boundary(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name       string
		sql        string
		checkValue func(t *testing.T, expr *plan.Expr)
	}{
		{
			name: "min int64 boundary",
			sql:  "-9223372036854775808",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_int64), expr.Typ.Id)
				require.Equal(t, int64(math.MinInt64), expr.GetLit().GetI64Val())
			},
		},
		{
			name: "below min int64 keeps decimal",
			sql:  "-9223372036854775809",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				require.NotNil(t, expr.GetLit().GetDecimal128Val())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			require.NoError(t, err)

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			unaryExpr, ok := selectClause.Exprs[0].Expr.(*tree.UnaryExpr)
			require.True(t, ok)

			expr, err := whereBinder.bindUnaryExpr(unaryExpr, 0, false)
			require.NoError(t, err)
			require.NotNil(t, expr.GetLit())
			tc.checkValue(t, expr)
		})
	}
}

// TestBindFuncExprImplByPlanExpr_JsonValid tests that json_valid binds
// correctly with string and json inputs.
func TestBindFuncExprImplByPlanExpr_JsonValid(t *testing.T) {
	ctx := context.Background()

	t.Run("json_valid with varchar literal", func(t *testing.T) {
		arg := makePlan2StringConstExprWithType(`{"a":1}`)
		result, err := BindFuncExprImplByPlanExpr(ctx, "json_valid", []*plan.Expr{arg})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "should be a function expression")
		require.Equal(t, "json_valid", f.Func.GetObjName())
		require.Equal(t, 1, len(f.Args))
		require.Equal(t, int32(types.T_bool), result.Typ.Id, "return type should be bool")
	})

	t.Run("json_valid with json column ref", func(t *testing.T) {
		arg := &plan.Expr{
			Typ: plan.Type{
				Id:          int32(types.T_json),
				NotNullable: true,
			},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0, Name: "a"},
			},
		}
		result, err := BindFuncExprImplByPlanExpr(ctx, "json_valid", []*plan.Expr{arg})
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f)
		require.Equal(t, int32(types.T_bool), result.Typ.Id)
	})
}

func TestBindFuncExprImplByPlanExpr_JsonOrderingWithDynamicParam(t *testing.T) {
	ctx := context.Background()

	makeJsonExpr := func() *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_json)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{ColPos: 0, Name: "j"},
			},
		}
	}
	makeParamExpr := func(pos int32) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_P{
				P: &plan.ParamRef{Pos: pos},
			},
		}
	}
	requireExactJSONParam := func(t *testing.T, expr *plan.Expr) *plan.Expr {
		t.Helper()
		require.Equal(t, int32(types.T_json), expr.Typ.Id)
		normalize := expr.GetF()
		require.NotNil(t, normalize)
		require.Equal(t, function.JsonOrderingParamFunctionName, normalize.GetFunc().GetObjName())
		require.Len(t, normalize.GetArgs(), 1)
		return normalize.GetArgs()[0]
	}

	t.Run("json on left", func(t *testing.T) {
		param := makeParamExpr(0)
		result, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{makeJsonExpr(), param})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_bool), result.Typ.Id)

		args := result.GetF().Args
		require.Len(t, args, 2)
		require.Equal(t, int32(types.T_json), args[0].Typ.Id)
		require.NotNil(t, args[0].GetCol())
		paramArg := requireExactJSONParam(t, args[1])
		require.Equal(t, int32(types.T_text), paramArg.Typ.Id)
		require.NotNil(t, paramArg.GetP())
	})

	t.Run("json on right", func(t *testing.T) {
		param := makeParamExpr(0)
		result, err := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{param, makeJsonExpr()})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_bool), result.Typ.Id)

		args := result.GetF().Args
		require.Len(t, args, 2)
		paramArg := requireExactJSONParam(t, args[0])
		require.Equal(t, int32(types.T_text), paramArg.Typ.Id)
		require.NotNil(t, paramArg.GetP())
		require.Equal(t, int32(types.T_json), args[1].Typ.Id)
		require.NotNil(t, args[1].GetCol())
	})

	t.Run("string literal remains rejected", func(t *testing.T) {
		_, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{makeJsonExpr(), makePlan2StringConstExprWithType("1")})
		require.Error(t, err)
	})

	t.Run("non-binary ordering comparison is ignored", func(t *testing.T) {
		err := adjustJsonOrderingDynamicParamType(ctx, ">", []*plan.Expr{makeJsonExpr()})
		require.NoError(t, err)
	})

	t.Run("non-ordering comparison is ignored", func(t *testing.T) {
		err := adjustJsonOrderingDynamicParamType(ctx, "=", []*plan.Expr{makeJsonExpr(), makeParamExpr(0)})
		require.NoError(t, err)
	})
}

func TestMixedStringNumericCastTypes(t *testing.T) {
	ctx := context.Background()
	makeCol := func(typ types.T, pos int32) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(typ)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
		}
	}
	requireArgTypes := func(t *testing.T, expr *plan.Expr, expected ...types.T) {
		t.Helper()
		require.Len(t, expr.GetF().Args, len(expected))
		for i, typ := range expected {
			require.Equal(t, int32(typ), expr.GetF().Args[i].Typ.Id)
		}
	}

	t.Run("numeric column and string literal stay exact", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			makeCol(types.T_int64, 0),
			makePlan2StringConstExprWithType("9007199254740993"),
		})
		require.NoError(t, err)
		requireArgTypes(t, expr, types.T_int64, types.T_int64)
		require.NotNil(t, expr.GetF().Args[0].GetCol())
	})

	t.Run("numeric column string range uses exact decimal bounds", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*plan.Expr{
			makeCol(types.T_int64, 0),
			makePlan2StringConstExprWithType("9007199254740992.5"),
			makePlan2StringConstExprWithType("9007199254740993.5"),
		})
		require.NoError(t, err)
		requireArgTypes(t, expr, types.T_decimal256, types.T_decimal256, types.T_decimal256)
		for _, arg := range expr.GetF().Args {
			require.True(t, types.T(arg.Typ.Id).IsDecimal())
		}
	})

	t.Run("runtime numeric parameter uses approximate comparison", func(t *testing.T) {
		param := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			makeCol(types.T_varchar, 0),
			param,
		})
		require.NoError(t, err)
		requireArgTypes(t, expr, types.T_float64, types.T_float64)
	})

	t.Run("decimal256 and string use approximate comparison", func(t *testing.T) {
		decimal := makeCol(types.T_decimal256, 0)
		decimal.Typ.Width = 65
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			decimal,
			makeCol(types.T_varchar, 1),
		})
		require.NoError(t, err)
		requireArgTypes(t, expr, types.T_float64, types.T_float64)
	})

	t.Run("integer div keeps exact operand", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "div", []*plan.Expr{
			makePlan2StringConstExprWithType("9007199254740993"),
			makePlan2Int64ConstExprWithType(1),
		})
		require.NoError(t, err)
		require.NotEqual(t, int32(types.T_float64), expr.GetF().Args[0].Typ.Id)
		require.NotEqual(t, int32(types.T_float64), expr.GetF().Args[1].Typ.Id)
	})

	t.Run("time arithmetic uses approximate numeric operands", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "-", []*plan.Expr{
			makeCol(types.T_time, 0),
			makeCol(types.T_time, 1),
		})
		require.NoError(t, err)
		requireArgTypes(t, expr, types.T_decimal64, types.T_decimal64)
	})
}

func TestBindNameConstConstArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "string name and int value",
			sql:  "select name_const('myname', 14)",
		},
		{
			name: "numeric name and negative value",
			sql:  "select name_const(123, -456)",
		},
		{
			name: "parenthesized literals",
			sql:  "select name_const(('myname'), (14))",
		},
		{
			name: "null value",
			sql:  "select name_const('myname', null)",
		},
		{
			name: "decimal value",
			sql:  "select name_const('myname', 12.34)",
		},
		{
			name: "negative decimal value",
			sql:  "select name_const('myname', -12.34)",
		},
		{
			name: "positive signed integer value",
			sql:  "select name_const('myname', +1)",
		},
		{
			name: "positive signed decimal value",
			sql:  "select name_const('myname', +12.34)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, bindNameConstSelect(tc.sql))
		})
	}
}

func TestBindNameConstInvalidArgs(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "wrong arg count",
			sql:  "select name_const('myname')",
		},
		{
			name: "null name",
			sql:  "select name_const(null, 1)",
		},
		{
			name: "unary minus name",
			sql:  "select name_const(-123, -456)",
		},
		{
			name: "column name",
			sql:  "select name_const(a, 1) from t",
		},
		{
			name: "column value",
			sql:  "select name_const('myname', a) from t",
		},
		{
			name: "cast function value",
			sql:  "select name_const('myname', cast(14 as signed))",
		},
		{
			name: "decimal cast function value",
			sql:  "select name_const('myname', cast('12.34' as decimal(10,2)))",
		},
		{
			name: "foldable function value",
			sql:  "select name_const('myname', abs(-1))",
		},
		{
			name: "non-foldable function value",
			sql:  "select name_const('myname', now())",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, bindNameConstSelect(tc.sql))
		})
	}
}

func TestBindNameConstNilProcReturnsError(t *testing.T) {
	args := []*plan.Expr{
		makePlan2StringConstExprWithType("myname"),
		makePlan2Int64ConstExprWithType(14),
	}

	require.NotPanics(t, func() {
		_, err := bindFuncExprAndConstFold(context.Background(), nil, "name_const", args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name_const")
	})
}

func TestGeneratedColBinderRejectsNameConstColumnValue(t *testing.T) {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, "select name_const('x', a)", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	funcExpr := selectClause.Exprs[0].Expr

	binder := NewGeneratedColBinder(
		context.Background(),
		[]string{"a"},
		[]plan.Type{{Id: int32(types.T_int64), Width: 64}},
	)
	_, err = binder.BindExpr(funcExpr, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NAME_CONST")
}

func TestGeneratedColBinderAcceptsNameConstUnaryPlusLiteral(t *testing.T) {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, "select name_const('x', +1)", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	funcExpr := selectClause.Exprs[0].Expr

	binder := NewGeneratedColBinder(context.Background(), nil, nil)
	_, err = binder.BindExpr(funcExpr, 0, false)
	require.NoError(t, err)
}

func bindNameConstSelect(sql string) error {
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return err
	}
	_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	return err
}

func TestBindFuncExprImplByAstExpr_IntervalDisambiguation(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	t.Run("function style keeps interval builtin", func(t *testing.T) {
		args := []tree.Expr{
			tree.NewNumVal(int64(5), "5", false, tree.P_int64),
			tree.NewNumVal("day", "day", false, tree.P_char),
		}
		result, err := whereBinder.bindFuncExprImplByAstExpr("interval", args, 0)
		require.NoError(t, err)
		require.NotNil(t, result)

		f := result.GetF()
		require.NotNil(t, f, "interval(5, 'day') should bind to the interval builtin")
		require.Equal(t, "interval", f.Func.GetObjName())
		require.Len(t, f.Args, 2)
		require.NotEqual(t, int32(types.T_interval), result.Typ.Id)
	})

	t.Run("interval expression rewrites to interval list", func(t *testing.T) {
		args := []tree.Expr{
			tree.NewNumVal(int64(5), "5", false, tree.P_int64),
			tree.NewTimeUnitExpr("day"),
		}
		result, err := whereBinder.bindFuncExprImplByAstExpr("interval", args, 0)
		require.NoError(t, err)
		require.NotNil(t, result)

		require.Equal(t, int32(types.T_interval), result.Typ.Id)
		list := result.GetList()
		require.NotNil(t, list, "INTERVAL 5 DAY should bind as an interval expression list")
		require.Len(t, list.List, 2)
		require.Equal(t, "day", list.List[1].GetLit().GetSval())
	})
}
