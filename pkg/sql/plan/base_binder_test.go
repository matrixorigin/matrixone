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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestStoredProcedureVariablesUseDeclaredDecimalType(t *testing.T) {
	scopes := []map[string]interface{}{{
		"p1": "10.00",
		"v1": "6.00",
	}}
	declaredType := plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}
	typeScopes := []map[string]plan.Type{{
		"p1": declaredType,
		"v1": declaredType,
	}}
	ctx := context.WithValue(context.Background(), defines.VarScopeKey{}, &scopes)
	ctx = context.WithValue(ctx, defines.VarScopeTypeKey{}, &typeScopes)
	ctx = context.WithValue(ctx, defines.InSp{}, true)

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select v1 > p1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	comparison := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewDefaultBinder(ctx, nil, nil, plan.Type{}, nil)
	bound, err := binder.BindExpr(comparison, 0, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_bool), bound.Typ.Id)

	args := bound.GetF().Args
	require.Len(t, args, 2)
	for _, arg := range args {
		require.Equal(t, declaredType.Id, arg.Typ.Id)
		require.Equal(t, declaredType.Width, arg.Typ.Width)
		require.Equal(t, declaredType.Scale, arg.Typ.Scale)
		require.Equal(t, "cast", arg.GetF().GetFunc().GetObjName())
		require.NotNil(t, arg.GetF().Args[0].GetV())
	}
}

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

func TestIsPositiveIntegerLiteral(t *testing.T) {
	tests := []struct {
		name string
		lit  *plan.Literal
		want bool
	}{
		{"int8 positive", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 1}}, true},
		{"int8 zero", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 0}}, false},
		{"int16 positive", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 1}}, true},
		{"int16 negative", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: -1}}, false},
		{"int32 positive", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}, true},
		{"int32 zero", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 0}}, false},
		{"int64 positive", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}, true},
		{"int64 negative", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -1}}, false},
		{"uint8 positive", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 1}}, true},
		{"uint8 zero", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 0}}, false},
		{"uint16 positive", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 1}}, true},
		{"uint16 zero", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 0}}, false},
		{"uint32 positive", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 1}}, true},
		{"uint32 zero", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 0}}, false},
		{"uint64 positive", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}, true},
		{"uint64 zero", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 0}}, false},
		{"non-integer literal", &plan.Literal{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isPositiveIntegerLiteral(tc.lit))
		})
	}
}

func TestValidateNthValueArgsRequiresProcessAndTwoArgs(t *testing.T) {
	err := validateNthValueArgs(context.Background(), nil, nil)
	require.Error(t, err)
	require.Equal(t, moerr.ER_WRONG_ARGUMENTS, err.(*moerr.Error).MySQLCode())
}

func TestBindSQLUDFUsesStoredParserMode(t *testing.T) {
	binder := NewDefaultBinder(context.Background(), nil, nil, plan.Type{}, nil)

	t.Run("legacy UDF uses historical pipe concat mode", func(t *testing.T) {
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "legacy_pipe", &function.Udf{
			Body:     "0 || 1",
			Language: string(tree.SQL),
		}, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, "concat", expr.GetF().GetFunc().GetObjName())
	})

	t.Run("stored empty mode keeps logical or semantics", func(t *testing.T) {
		emptyMode := ""
		expr, err := bindFuncExprImplUdf(&binder.baseBinder, "logical_or", &function.Udf{
			Body:     "0 || 1",
			Language: string(tree.SQL),
			SQLMode:  &emptyMode,
		}, nil, nil, 0)
		require.NoError(t, err)
		require.Equal(t, "or", expr.GetF().GetFunc().GetObjName())
	})
}

type sqlUdfMockCompilerContext struct {
	*MockCompilerContext
}

func (c *sqlUdfMockCompilerContext) ResolveUdf(name string, _ []*plan.Expr) (*function.Udf, error) {
	switch name {
	case "f_lookup":
		return &function.Udf{
			Body:     "select n_regionkey from nation where n_nationkey = $1",
			Language: string(tree.SQL),
		}, nil
	case "f_ansi_quotes":
		mode := "ANSI_QUOTES"
		return &function.Udf{
			Body:     `select 1 from (select 1) as "a\" where $1 = 2`,
			Language: string(tree.SQL),
			SQLMode:  &mode,
		}, nil
	case "f_ansi":
		mode := "ANSI"
		return &function.Udf{
			Body:     `select 1 from (select 1) as "a\" where $1 = 2`,
			Language: string(tree.SQL),
			SQLMode:  &mode,
		}, nil
	case "f_executable_comment":
		return &function.Udf{
			Body:     "/*! $1 + */ 1",
			Language: string(tree.SQL),
		}, nil
	case "f_slash_comment":
		return &function.Udf{
			Body:     "select 1 // '\n + $1",
			Language: string(tree.SQL),
		}, nil
	default:
		return nil, nil
	}
}

func TestBindSQLUDFTableReadCorrelatesColumnArgument(t *testing.T) {
	stmts, err := parsers.Parse(
		context.Background(),
		dialect.MYSQL,
		"select n_nationkey, f_lookup(n_nationkey) from nation",
		1,
	)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()

	ctx := &sqlUdfMockCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}
	built, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)

	query := built.GetQuery()
	require.NotNil(t, query)
	require.True(t, queryContainsCrossRelationEquality(query), "SQL UDF parameter must bind to the outer scan column")
}

func TestBindSQLUDFArgumentMarkersFollowLexerSemantics(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "ANSI_QUOTES identifier", query: "select f_ansi_quotes(2)"},
		{name: "ANSI composite mode identifier", query: "select f_ansi(2)"},
		{name: "executable comment", query: "select f_executable_comment(2)"},
		{name: "slash line comment", query: "select f_slash_comment(2)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, test.query, 1)
			require.NoError(t, err)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()

			ctx := &sqlUdfMockCompilerContext{MockCompilerContext: NewMockCompilerContext(true)}
			_, err = BuildPlan(ctx, stmts[0], false)
			require.NoError(t, err)
		})
	}
}

func TestExpandSQLUdfArgumentsAvoidsIdentifierCollision(t *testing.T) {
	const userColumn = "__mo_sql_udf_1_arg_1"

	builder := &QueryBuilder{}
	bindCtx := NewBindContext(nil, nil)
	binder := NewDefaultBinder(
		context.Background(),
		builder,
		bindCtx,
		plan.Type{Id: int32(types.T_int64)},
		[]string{userColumn},
	)
	arg := makeInt64ConstPlanExpr(42)
	body := "select " + userColumn + ", $1"
	rewritten, markers := binder.expandSQLUdfArguments(body, []*plan.Expr{arg}, "")

	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, rewritten, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()

	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	require.Len(t, selectClause.Exprs, 2)
	originalName := selectClause.Exprs[0].Expr.(*tree.UnresolvedName)
	argumentName := selectClause.Exprs[1].Expr.(*tree.UnresolvedName)
	require.Equal(t, userColumn, originalName.ColName())
	require.NotEqual(t, originalName.ColName(), argumentName.ColName())
	require.NotContains(t, strings.ToLower(body), strings.ToLower(argumentName.ColName()))
	require.NotContains(t, markers, originalName.ColName())
	require.Contains(t, markers, argumentName.ColName())

	restore := binder.pushSQLUdfArguments(markers)
	defer restore()
	boundOriginal, err := binder.BindExpr(originalName, 0, false)
	require.NoError(t, err)
	require.NotNil(t, boundOriginal.GetCol(), "user-authored identifier must retain normal column binding")
	boundArgument, err := binder.BindExpr(argumentName, 0, false)
	require.NoError(t, err)
	require.Equal(t, int64(42), boundArgument.GetLit().GetI64Val())
}

func TestReplaceSQLUdfArgMarkers(t *testing.T) {
	marker := func(ordinal int) string { return fmt.Sprintf("<arg%d>", ordinal) }
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "repeated parameter",
			sql:  "select $1 + $1, $2",
			want: "select <arg1> + <arg1>, <arg2>",
		},
		{
			name: "quoted text and identifiers",
			sql:  "select '$1', \"$1\", `$1`, $1",
			want: "select '$1', \"$1\", `$1`, <arg1>",
		},
		{
			name: "comments",
			sql:  "select $1 -- $2\n, $2 /* $1 */ # $1\n",
			want: "select <arg1> -- $2\n, <arg2> /* $1 */ # $1\n",
		},
		{
			name: "double minus without comment whitespace",
			sql:  "select 1--$1",
			want: "select 1--<arg1>",
		},
		{
			name: "out of range parameter",
			sql:  "select $0, $3, $1",
			want: "select $0, $3, <arg1>",
		},
		{
			name: "parameter-like identifier",
			sql:  "select $1suffix, $1",
			want: "select $1suffix, <arg1>",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, replaceSQLUdfArgMarkers(test.sql, 2, "", marker))
		})
	}
}

func TestCorrelateSQLUdfArgumentTraversesNestedExpressions(t *testing.T) {
	column := func(relPos, colPos int32) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: relPos, ColPos: colPos}},
		}
	}

	original := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{Args: []*plan.Expr{
			nil,
			{Expr: &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: 1, ColPos: 2, Depth: 3}}},
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: column(2, 3)}}},
			{Expr: &plan.Expr_W{W: &plan.WindowSpec{
				WindowFunc:  column(3, 4),
				PartitionBy: []*plan.Expr{column(4, 5)},
				OrderBy: []*plan.OrderBySpec{
					nil,
					{Expr: column(5, 6)},
				},
				Frame: &plan.FrameClause{
					Start: &plan.FrameBound{Val: column(6, 7)},
					End:   &plan.FrameBound{Val: column(7, 8)},
				},
			}}},
			{Expr: &plan.Expr_Sub{Sub: &plan.SubqueryRef{Child: column(8, 9)}}},
			{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{column(9, 10)}}}},
		}}},
	}

	correlatedExpr, correlated := correlateSQLUdfArgument(original, 2)
	require.True(t, correlated)
	require.NotSame(t, original, correlatedExpr)
	require.Equal(t, int32(3), original.GetF().Args[1].GetCorr().Depth, "the caller-owned expression must not be mutated")

	args := correlatedExpr.GetF().Args
	require.Nil(t, args[0])
	require.Equal(t, int32(5), args[1].GetCorr().Depth)
	require.Equal(t, int32(2), args[2].GetLit().Src.GetCorr().Depth)

	window := args[3].GetW()
	require.Equal(t, int32(2), window.WindowFunc.GetCorr().Depth)
	require.Equal(t, int32(2), window.PartitionBy[0].GetCorr().Depth)
	require.Nil(t, window.OrderBy[0])
	require.Equal(t, int32(2), window.OrderBy[1].Expr.GetCorr().Depth)
	require.Equal(t, int32(2), window.Frame.Start.Val.GetCorr().Depth)
	require.Equal(t, int32(2), window.Frame.End.Val.GetCorr().Depth)
	require.Equal(t, int32(2), args[4].GetSub().Child.GetCorr().Depth)
	require.Equal(t, int32(2), args[5].GetList().List[0].GetCorr().Depth)

	localOriginal := column(10, 11)
	localExpr, correlated := correlateSQLUdfArgument(localOriginal, 0)
	require.False(t, correlated)
	require.NotSame(t, localOriginal, localExpr)
	require.Equal(t, int32(10), localExpr.GetCol().RelPos)
	require.Equal(t, int32(11), localExpr.GetCol().ColPos)
}

func queryContainsCrossRelationEquality(query *plan.Query) bool {
	for _, node := range query.Nodes {
		for _, exprs := range [][]*plan.Expr{
			node.ProjectList,
			node.OnList,
			node.FilterList,
			node.BlockFilterList,
		} {
			for _, expr := range exprs {
				if exprContainsCrossRelationEquality(expr) {
					return true
				}
			}
		}
	}
	return false
}

func exprContainsCrossRelationEquality(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func.GetObjName() == "=" && len(fn.Args) == 2 {
		left, right := fn.Args[0].GetCol(), fn.Args[1].GetCol()
		if left != nil && right != nil && left.RelPos != right.RelPos {
			return true
		}
	}
	for _, arg := range fn.Args {
		if exprContainsCrossRelationEquality(arg) {
			return true
		}
	}
	return false
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

func TestBinaryLiteralComparisonKeepsVarbinaryColumnUncast(t *testing.T) {
	testCases := []struct {
		name   string
		filter string
		colArg int
	}{
		{name: "column on left", filter: "a = binary x'41'", colArg: 0},
		{name: "column on right", filter: "binary x'41' = a", colArg: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			mock.ctxt.tables["bind_select"].Cols[0].Typ = plan.Type{
				Id:      int32(types.T_varbinary),
				Width:   8,
				Charset: uint32(types.CharsetBinary),
			}

			p, err := runOneStmt(mock, t,
				"select a from select_test.bind_select where "+tc.filter)
			require.NoError(t, err)

			var filter *plan.Expr
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Name == "bind_select" {
					require.Len(t, node.FilterList, 1)
					filter = node.FilterList[0]
					break
				}
			}
			require.NotNil(t, filter)
			eq := filter.GetF()
			require.NotNil(t, eq)
			require.Equal(t, "=", eq.Func.ObjName)
			require.Len(t, eq.Args, 2)
			require.NotNil(t, eq.Args[tc.colArg].GetCol(),
				"the indexed VARBINARY column must not be wrapped in a cast")
		})
	}
}

func TestMinMaxSerialExpressionsKeepBinaryCollation(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(true), t,
		"select min(serial(a, b)), max(serial(a, b)), "+
			"min(serial_full(a, b)), max(serial_full(a, b)) "+
			"from select_test.bind_select")
	require.NoError(t, err)

	var aggregates []*plan.Expr
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == plan.Node_AGG {
			aggregates = node.AggList
			break
		}
	}
	require.Len(t, aggregates, 4)
	for _, aggregate := range aggregates {
		fn := aggregate.GetF()
		require.NotNil(t, fn)
		require.Contains(t, []string{"min", "max"}, fn.Func.ObjName)
		require.Equal(t, uint32(types.CharsetBinary), aggregate.Typ.Charset)
		require.Len(t, fn.Args, 1)
		require.Equal(t, uint32(types.CharsetBinary), fn.Args[0].Typ.Charset)
	}
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
		{
			name: "string value with backslash",
			sql:  `select name_const('myname', 'a\\b')`,
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
			name: "cast hex name",
			sql:  "select name_const(cast(0x61 as varchar), 1)",
		},
		{
			name: "cast hex value",
			sql:  "select name_const('x', cast(0x31 as varchar))",
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
