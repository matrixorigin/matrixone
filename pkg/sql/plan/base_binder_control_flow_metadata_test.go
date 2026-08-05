// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestBindControlFlowMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("if mixed string numeric keeps bounded varchar", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
			makePlan2BoolConstExprWithType(true),
			makePlan2StringConstExprWithType("2"),
			makePlan2Int64ConstExprWithType(3),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(2), expr.Typ.Width)
		require.True(t, expr.Typ.NotNullable)
	})

	t.Run("if signed unsigned promotes to decimal", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
			makePlan2BoolConstExprWithType(true),
			makePlan2Uint64ConstExprWithType(1),
			makePlan2Int64ConstExprWithType(-1),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
		require.Equal(t, int32(21), expr.Typ.Width)
		require.Zero(t, expr.Typ.Scale)
		require.True(t, expr.Typ.NotNullable)
	})

	t.Run("coalesce is non-null when an argument is non-null", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", []*planpb.Expr{
			MakePlan2NullTextConstExprWithType(""),
			makePlan2StringConstExprWithType("8"),
			makePlan2Int64ConstExprWithType(9),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(2), expr.Typ.Width)
		require.True(t, expr.Typ.NotNullable)
	})

	for _, test := range []struct {
		name   string
		args   []*planpb.Expr
		argPos int
	}{
		{
			name: "text column keeps conservative varchar capacity",
			args: []*planpb.Expr{
				makePlan2BoolConstExprWithType(true),
				{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
				makePlan2Int64ConstExprWithType(3),
			},
			argPos: 1,
		},
		{
			name: "blob column keeps conservative varchar capacity",
			args: []*planpb.Expr{
				makePlan2BoolConstExprWithType(true),
				{Typ: planpb.Type{Id: int32(types.T_blob)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
				makePlan2Int64ConstExprWithType(3),
			},
			argPos: 1,
		},
		{
			name: "float column keeps conservative varchar capacity",
			args: []*planpb.Expr{
				makePlan2BoolConstExprWithType(false),
				makePlan2StringConstExprWithType("x"),
				{Typ: planpb.Type{Id: int32(types.T_float32)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
			},
			argPos: 2,
		},
		{
			name: "double expression keeps conservative varchar capacity",
			args: []*planpb.Expr{
				makePlan2BoolConstExprWithType(false),
				makePlan2StringConstExprWithType("x"),
				{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "cast"}}}},
			},
			argPos: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "if", test.args)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
			require.Equal(t, int32(types.MaxVarcharLen), expr.Typ.Width)

			// The branch must not be rewritten to a narrow VARCHAR cast; otherwise
			// runtime values such as a TEXT column's "abcdef" would be truncated.
			valueArg := expr.GetF().Args[test.argPos]
			require.Equal(t, int32(types.T_varchar), valueArg.Typ.Id)
			require.Equal(t, int32(types.MaxVarcharLen), valueArg.Typ.Width)
		})
	}

	for _, test := range []struct {
		name     string
		function string
		args     func(*planpb.Expr) []*planpb.Expr
	}{
		{
			name:     "if decimal and integer literal keeps decimal precision",
			function: "if",
			args: func(decimal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{makePlan2BoolConstExprWithType(true), decimal, makePlan2Int64ConstExprWithType(0)}
			},
		},
		{
			name:     "case decimal and integer literal keeps decimal precision",
			function: "case",
			args: func(decimal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{makePlan2BoolConstExprWithType(true), decimal, makePlan2Int64ConstExprWithType(0)}
			},
		},
		{
			name:     "coalesce decimal and integer literal keeps decimal precision",
			function: "coalesce",
			args: func(decimal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{decimal, makePlan2Int64ConstExprWithType(0)}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			decimal, err := makePlan2DecimalExprWithType(ctx, "12.50")
			require.NoError(t, err)
			decimal.Typ.Width = 8
			decimal.Typ.Scale = 2

			expr, err := BindFuncExprImplByPlanExpr(ctx, test.function, test.args(decimal))
			require.NoError(t, err)
			require.True(t, types.T(expr.Typ.Id).IsDecimal())
			require.Equal(t, int32(8), expr.Typ.Width)
			require.Equal(t, int32(2), expr.Typ.Scale)
			for _, arg := range expr.GetF().Args {
				if types.T(arg.Typ.Id).IsDecimal() {
					require.Equal(t, int32(8), arg.Typ.Width)
					require.Equal(t, int32(2), arg.Typ.Scale)
				}
			}
		})
	}

	for _, test := range []struct {
		name  string
		args  func(*planpb.Expr) []*planpb.Expr
		width int32
	}{
		{
			name: "coalesce date and string uses date display width",
			args: func(temporal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{temporal, makePlan2StringConstExprWithType("fallback")}
			},
			width: 10,
		},
		{
			name: "coalesce datetime and string uses datetime display width",
			args: func(temporal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{temporal, makePlan2StringConstExprWithType("fallback")}
			},
			width: 19,
		},
		{
			name: "coalesce timestamp fsp and string uses timestamp display width",
			args: func(temporal *planpb.Expr) []*planpb.Expr {
				return []*planpb.Expr{temporal, makePlan2StringConstExprWithType("fallback")}
			},
			width: 26,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var temporal *planpb.Expr
			switch test.width {
			case 10:
				temporal = makePlan2DateConstExprWithType(0)
			case 19:
				temporal = makePlan2DateTimeConstExprWithType(0)
			default:
				temporal = makePlan2TimestampConstExprWithType(0)
				temporal.Typ.Scale = 6
			}

			expr, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", test.args(temporal))
			require.NoError(t, err)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
			require.Equal(t, test.width, expr.Typ.Width)
			for _, arg := range expr.GetF().Args {
				if types.T(arg.Typ.Id) == types.T_varchar {
					require.Equal(t, test.width, arg.Typ.Width)
				}
			}
		})
	}

	t.Run("greatest remains nullable in metadata", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "greatest", []*planpb.Expr{
			makePlan2DateConstExprWithType(0),
			makePlan2DateConstExprWithType(1),
		})
		require.NoError(t, err)
		require.False(t, expr.Typ.NotNullable)
	})

	t.Run("case temporal promotion remains nullable in metadata", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{
			makePlan2BoolConstExprWithType(true),
			makePlan2DateConstExprWithType(0),
			makePlan2DateTimeConstExprWithType(0),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id)
		require.False(t, expr.Typ.NotNullable)
	})

	t.Run("case temporal promotion in else remains nullable in metadata", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{
			makePlan2BoolConstExprWithType(false),
			makePlan2DateTimeConstExprWithType(0),
			makePlan2DateConstExprWithType(0),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.Typ.Id)
		require.False(t, expr.Typ.NotNullable)
	})

	t.Run("case binary character keeps binary metadata", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{
			makePlan2BoolConstExprWithType(true),
			makePlan2VarBinaryConstExprWithType("a"),
			makePlan2StringConstExprWithType("bc"),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varbinary), expr.Typ.Id)
		require.Equal(t, int32(8), expr.Typ.Width)
		require.True(t, expr.Typ.NotNullable)
	})

	t.Run("if binary character keeps binary metadata", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
			makePlan2BoolConstExprWithType(true),
			makePlan2VarBinaryConstExprWithType("a"),
			makePlan2StringConstExprWithType("bc"),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varbinary), expr.Typ.Id)
		require.Equal(t, int32(8), expr.Typ.Width)
		require.True(t, expr.Typ.NotNullable)
	})
}

func TestBuildCaseSignedUnsignedMetadataWithNull(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "leading null",
			sql: `select case
				when false then null
				when true then cast(18446744073709551615 as unsigned)
				else cast(-1 as signed)
			end`,
		},
		{
			name: "middle null",
			sql: `select case
				when false then cast(18446744073709551615 as unsigned)
				when false then null
				else cast(-1 as signed)
			end`,
		},
		{
			name: "trailing null",
			sql: `select case
				when true then cast(18446744073709551615 as unsigned)
				when false then cast(-1 as signed)
				else null
			end`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_decimal128), projectList[0].Typ.Id)
			require.Equal(t, int32(21), projectList[0].Typ.Width)
			require.Zero(t, projectList[0].Typ.Scale)
			require.False(t, projectList[0].Typ.NotNullable)
		})
	}
}

func TestBuildControlFlowUTF8MB4BinaryWidth(t *testing.T) {
	for _, sql := range []string{
		`select case when 0 then _binary 0x61 else "😀" end as c`,
		`select if(0, _binary 0x61, "😀") as c`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_varbinary), projectList[0].Typ.Id)
			require.Equal(t, int32(4), projectList[0].Typ.Width)
		})
	}
}

func TestBuildControlFlowDecimalStringMetadataWidth(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		width int32
	}{
		{
			name:  "if positive decimal literal",
			sql:   `select if(true, 'x', 1.23)`,
			width: 5,
		},
		{
			name:  "case negative decimal literal",
			sql:   `select case when true then 'x' else cast(-123.45 as decimal(5, 2)) end`,
			width: 7,
		},
		{
			name:  "coalesce positive decimal literal",
			sql:   `select coalesce(null, 1.23, 'x')`,
			width: 5,
		},
		{
			name:  "if decimal column",
			sql:   `select if(true, 'x', d) from (select cast(-123.45 as decimal(5, 2)) as d) t`,
			width: 7,
		},
		{
			name:  "case decimal column",
			sql:   `select case when true then 'x' else d end from (select cast(-123.45 as decimal(5, 2)) as d) t`,
			width: 7,
		},
		{
			name:  "coalesce decimal column",
			sql:   `select coalesce(null, d, 'x') from (select cast(-123.45 as decimal(5, 2)) as d) t`,
			width: 7,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_varchar), projectList[0].Typ.Id)
			require.Equal(t, test.width, projectList[0].Typ.Width)
		})
	}
}

func TestBuildCaseSameFixedBinaryMetadata(t *testing.T) {
	for _, sql := range []string{
		`select case when true then cast('a' as binary(4)) else cast('b' as binary(4)) end`,
		`select if(true, cast('a' as binary(4)), cast('b' as binary(4)))`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_binary), projectList[0].Typ.Id)
			require.Equal(t, int32(4), projectList[0].Typ.Width)
		})
	}
}

func TestBuildControlFlowDifferentFixedBinaryMetadata(t *testing.T) {
	for _, sql := range []string{
		`select case when true then cast('a' as binary(4)) else cast('b' as binary(8)) end`,
		`select if(true, cast('a' as binary(4)), cast('b' as binary(8)))`,
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_varbinary), projectList[0].Typ.Id)
			require.Equal(t, int32(8), projectList[0].Typ.Width)
		})
	}
}

func TestBuildCaseBinaryMetadataWithNullBranches(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		oid  types.T
	}{
		{
			name: "leading null fixed binary",
			sql:  `select case when true then null else cast('a' as binary(4)) end`,
			oid:  types.T_binary,
		},
		{
			name: "trailing null fixed binary",
			sql:  `select case when true then cast('a' as binary(4)) else null end`,
			oid:  types.T_binary,
		},
		{
			name: "middle null fixed binary",
			sql:  `select case when false then cast('a' as binary(4)) when true then null else cast('b' as binary(4)) end`,
			oid:  types.T_binary,
		},
		{
			name: "leading null varbinary",
			sql:  `select case when true then null else cast('a' as varbinary(4)) end`,
			oid:  types.T_varbinary,
		},
		{
			name: "trailing null varbinary",
			sql:  `select case when true then cast('a' as varbinary(4)) else null end`,
			oid:  types.T_varbinary,
		},
		{
			name: "middle null varbinary",
			sql:  `select case when false then cast('a' as varbinary(4)) when true then null else cast('b' as varbinary(4)) end`,
			oid:  types.T_varbinary,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(test.oid), projectList[0].Typ.Id)
			require.Equal(t, int32(4), projectList[0].Typ.Width)
			require.False(t, projectList[0].Typ.NotNullable)
		})
	}
}

func TestDecimalDisplayWidth(t *testing.T) {
	for _, test := range []struct {
		name  string
		typ   types.Type
		width int32
	}{
		{name: "fractional", typ: types.New(types.T_decimal64, 3, 2), width: 5},
		{name: "all fractional digits include leading zero", typ: types.New(types.T_decimal64, 3, 3), width: 6},
		{name: "integer", typ: types.New(types.T_decimal64, 5, 0), width: 6},
		{name: "unknown precision", typ: types.New(types.T_decimal64, 0, 0), width: types.MaxVarcharLen},
		{name: "maximum precision caps", typ: types.New(types.T_decimal256, types.MaxVarcharLen, 0), width: types.MaxVarcharLen},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.width, decimalDisplayWidth(test.typ))
		})
	}
}

func TestDecimalLiteralMetadataPrecision(t *testing.T) {
	expr, err := makePlan2DecimalExprWithType(context.Background(), "9.5")
	require.NoError(t, err)
	require.Equal(t, int32(2), expr.Typ.Width)
	require.Equal(t, int32(1), expr.Typ.Scale)
}

func TestBuildIfNullMetadata(t *testing.T) {
	for _, sql := range []string{
		"select ifnull(null, 9.5)",
		"select ifnull(null, 9.5) from nation",
		"select distinct ifnull(null, 9.5) from nation",
		"select ifnull(null, 9.5) from nation group by n_regionkey",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			query := pl.GetQuery()
			projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
			require.Len(t, projectList, 1)
			require.Equal(t, int32(types.T_decimal64), projectList[0].Typ.Id)
			require.Equal(t, int32(2), projectList[0].Typ.Width)
			require.Equal(t, int32(1), projectList[0].Typ.Scale)
			require.True(t, projectList[0].Typ.NotNullable)
		})
	}
}

func TestBuildIfNullMetadataAfterOuterJoin(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, `
		select ifnull(r.r_regionkey, 9.5), ifnull(r.r_regionkey, n.n_comment)
		from nation n left join region r on n.n_regionkey = r.r_regionkey`, 1)
	require.NoError(t, err)

	pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)
	query := pl.GetQuery()
	projectList := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.Len(t, projectList, 2)
	require.True(t, projectList[0].Typ.NotNullable)
	require.False(t, projectList[1].Typ.NotNullable)
}
