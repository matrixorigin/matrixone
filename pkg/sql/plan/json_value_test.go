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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestJSONValueBindingContract(t *testing.T) {
	bind := func(sql string, prepare bool) (*plan.Expr, error) {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), prepare, true)
		binder := NewDefaultBinder(context.Background(), builder, nil, plan.Type{}, nil)
		return binder.BindExpr(selectStmt.Exprs[0].Expr, 0, false)
	}

	t.Run("implicit returning type", func(t *testing.T) {
		expr, err := bind(`select json_value('{"a":[1]}', '$.a')`, false)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(512), expr.Typ.Width)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), expr.Typ.Charset)
		require.False(t, expr.Typ.NotNullable)
		require.Len(t, expr.GetF().Args, 2)
	})

	t.Run("explicit unsigned default", func(t *testing.T) {
		expr, err := bind(`select json_value('{"a":1}', '$.a' returning unsigned default 0 on error)`, false)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
		require.False(t, expr.Typ.NotNullable)
		require.Equal(t, int64(3), expr.GetF().Args[5].GetLit().GetI64Val())
		require.NotNil(t, expr.GetF().Args[6])
	})
	t.Run("quoted name cannot expose the internal overload", func(t *testing.T) {
		_, err := bind("select `json_value`('1', '$', '', 3, 7, 3, 9)", false)
		require.Error(t, err)
		// Quoted function names are not accepted by the function-call grammar;
		// they do not provide an alternate route around the dedicated syntax.
		_, err = bind("select `json_value`('1', '$')", false)
		require.Error(t, err)
	})

	t.Run("prepared document and path remain parameters", func(t *testing.T) {
		expr, err := bind(`select json_value(?, ? returning char(12))`, true)
		require.NoError(t, err)
		require.NotNil(t, expr.GetF().Args[0].GetP())
		require.NotNil(t, expr.GetF().Args[1].GetP())
		require.Equal(t, int32(types.T_char), expr.Typ.Id)
		require.Equal(t, int32(12), expr.Typ.Width)
	})
}

func TestJSONValueProtocolGatePreservesLegacyPlans(t *testing.T) {
	compilerContext := NewMockCompilerContext(true)
	proc := compilerContext.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	bind := func(version int64, sql string) (*plan.Expr, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)
		builder := NewQueryBuilder(plan.Query_SELECT, compilerContext, false, true)
		binder := NewDefaultBinder(context.Background(), builder, nil, plan.Type{}, nil)
		return binder.BindExpr(selectStmt.Exprs[0].Expr, 0, false)
	}

	legacy, err := bind(defines.MORPCVersion57, `select json_value('1', '$')`)
	require.NoError(t, err)
	require.Len(t, legacy.GetF().Args, 2)

	_, err = bind(defines.MORPCVersion57, `select json_value('1', '$' returning unsigned)`)
	require.ErrorContains(t, err, "MORPC protocol version 58")

	contract, err := bind(defines.MORPCVersion58, `select json_value('1', '$' returning unsigned)`)
	require.NoError(t, err)
	require.Len(t, contract.GetF().Args, 7)
}

func TestJSONValueBindingRejectsInvalidUnusedDefault(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		`select json_value('{"a":1}', '$.a' returning unsigned default -1 on error)`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	binder := NewDefaultBinder(context.Background(), builder, nil, plan.Type{}, nil)
	_, err = binder.BindExpr(selectStmt.Exprs[0].Expr, 0, false)
	require.Error(t, err)
}

func TestJSONValueImplicitTypeSurvivesSQLRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		oid     types.T
		width   int32
		charset uint8
	}{
		{`select json_value('{"a":[12]}', '$.a' default 'empty' on empty)`, types.T_varchar, 512, types.CharsetUTF8MB4Bin},
		{`select json_value('{"a":[12]}', '$.a' returning char(12) character set utf8mb4 default 'empty' on empty default 'error' on error)`, types.T_char, 12, types.CharsetUTF8},
	} {
		sql := tc.sql
		for range 2 {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
			binder := NewDefaultBinder(context.Background(), builder, nil, plan.Type{}, nil)
			bound, err := binder.BindExpr(expr, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(tc.oid), bound.Typ.Id)
			require.Equal(t, tc.width, bound.Typ.Width)
			require.Equal(t, uint32(tc.charset), bound.Typ.Charset)
			sql = tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithQuoteString(true))
			stmt.Free()
		}
	}
}

func TestJSONValueBindingTypeAndDefaultBoundaries(t *testing.T) {
	bind := func(sql string) (*plan.Expr, error) {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)
		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		binder := NewDefaultBinder(context.Background(), builder, nil, plan.Type{}, nil)
		return binder.BindExpr(selectStmt.Exprs[0].Expr, 0, false)
	}

	t.Run("all supported target metadata", func(t *testing.T) {
		cases := []struct {
			sql   string
			oid   types.T
			width int32
			scale int32
		}{
			{`select json_value('{"a":1}', '$.a' returning signed)`, types.T_int64, 64, -1},
			{`select json_value('{"a":1}', '$.a' returning unsigned)`, types.T_uint64, 64, -1},
			{`select json_value('{"a":1}', '$.a' returning decimal(10,2))`, types.T_decimal64, 10, 2},
			{`select json_value('{"a":1}', '$.a' returning float)`, types.T_float32, 0, -1},
			{`select json_value('{"a":1}', '$.a' returning double)`, types.T_float64, 0, -1},
			{`select json_value('{"a":1}', '$.a' returning date)`, types.T_date, 0, 0},
			{`select json_value('{"a":1}', '$.a' returning time(3))`, types.T_time, 3, 3},
			{`select json_value('{"a":1}', '$.a' returning datetime(6))`, types.T_datetime, 6, 6},
			{`select json_value('{"a":1}', '$.a' returning year)`, types.T_year, 4, 0},
			{`select json_value('{"a":1}', '$.a' returning char(12) character set utf8mb4)`, types.T_char, 12, 0},
			{`select json_value('{"a":1}', '$.a' returning binary(12))`, types.T_binary, 12, -1},
			{`select json_value('{"a":1}', '$.a' returning json)`, types.T_json, 0, 0},
		}
		for _, tc := range cases {
			expr, err := bind(tc.sql)
			require.NoError(t, err, tc.sql)
			require.Equal(t, int32(tc.oid), expr.Typ.Id, tc.sql)
			require.Equal(t, tc.width, expr.Typ.Width, tc.sql)
			require.Equal(t, tc.scale, expr.Typ.Scale, tc.sql)
			require.False(t, expr.Typ.NotNullable, tc.sql)
		}
	})

	for _, sql := range []string{
		`select json_value('{"a":1}', '$.a' returning float(10))`,
		`select json_value('{"a":1}', '$.a' returning double(10,2))`,
		`select json_value('{"a":1}', '$.a' returning year(4))`,
		`select json_value('{"a":1}', '$.a' returning decimal(4,5))`,
		`select json_value('{"a":1}', '$.a' returning time(7))`,
		`select json_value('{"a":1}', '$.a' returning datetime(7))`,
		`select json_value('{"a":1}', '$.a' returning binary(12) character set utf8mb4)`,
		`select json_value('{"a":1}', '$.a' returning binary)`,
	} {
		t.Run("reject "+sql, func(t *testing.T) {
			_, err := bind(sql)
			require.Error(t, err)
		})
	}

	_, err := bind(`select json_value('{"a":1}', '$.a' returning decimal(4,2) default 1.234 on error)`)
	require.Error(t, err)
}

func TestJSONValueDefaultValidationMatrix(t *testing.T) {
	ctx := context.Background()
	literal := func(value string) tree.Expr {
		return tree.NewNumVal(value, value, false, tree.P_char)
	}
	boolLiteral := func(value bool) tree.Expr {
		text := "false"
		if value {
			text = "true"
		}
		return tree.NewNumVal(value, text, false, tree.P_bool)
	}

	require.Equal(t, "1", jsonValueDefaultNumericText("  TRUE "))
	require.Equal(t, "0", jsonValueDefaultNumericText("false"))
	require.Equal(t, "1.25", jsonValueDefaultNumericText("1.25"))
	gotInt, ok := jsonValueDefaultInt64("true")
	require.True(t, ok)
	require.Equal(t, int64(1), gotInt)
	gotUint, ok := jsonValueDefaultUint64("42")
	require.True(t, ok)
	require.Equal(t, uint64(42), gotUint)
	require.True(t, jsonValueDefaultNumericExactAtScale("1.25", 2))
	require.True(t, jsonValueDefaultNumericExactAtScale("1", -1))
	require.False(t, jsonValueDefaultNumericExactAtScale("1.255", 2))
	require.False(t, jsonValueDefaultNumericExactAtScale("not-a-number", 0))

	text, err := jsonValueDefaultLiteralText(&tree.UnaryExpr{
		Op:   tree.UNARY_MINUS,
		Expr: literal("1"),
	})
	require.NoError(t, err)
	require.Equal(t, "-1", text)
	_, err = jsonValueDefaultLiteralText(tree.NewNumVal("", "", false, tree.P_null))
	require.Error(t, err)
	_, err = jsonValueDefaultLiteralText(&tree.UnaryExpr{Op: tree.UNARY_PLUS, Expr: tree.NewStrVal("1")})
	require.Error(t, err)
	_, err = jsonValueDefaultLiteralText(tree.NewStrVal("1"))
	require.Error(t, err)

	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64} {
		require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal("7"), types.New(oid, 0, 0)), oid)
	}
	for _, tc := range []struct {
		name  string
		value string
		typ   types.Type
	}{
		{"int8 overflow", "128", types.New(types.T_int8, 0, 0)},
		{"int16 overflow", "32768", types.New(types.T_int16, 0, 0)},
		{"int32 overflow", "2147483648", types.New(types.T_int32, 0, 0)},
		{"signed fraction", "1.5", types.New(types.T_int64, 0, 0)},
		{"signed invalid", "not-a-number", types.New(types.T_int64, 0, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateJSONValueDefaultLiteral(ctx, literal(tc.value), tc.typ))
		})
	}

	for _, oid := range []types.T{types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64} {
		require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal("7"), types.New(oid, 0, 0)), oid)
	}
	for _, tc := range []struct {
		name  string
		value string
		typ   types.Type
	}{
		{"uint8 overflow", "256", types.New(types.T_uint8, 0, 0)},
		{"uint16 overflow", "65536", types.New(types.T_uint16, 0, 0)},
		{"uint32 overflow", "4294967296", types.New(types.T_uint32, 0, 0)},
		{"unsigned negative", "-1", types.New(types.T_uint64, 0, 0)},
		{"unsigned fraction", "1.5", types.New(types.T_uint64, 0, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateJSONValueDefaultLiteral(ctx, literal(tc.value), tc.typ))
		})
	}
	require.NoError(t, validateJSONValueDefaultLiteral(ctx, boolLiteral(true), types.New(types.T_float32, 0, 0)))
	require.NoError(t, validateJSONValueDefaultLiteral(ctx, boolLiteral(false), types.New(types.T_float64, 0, 0)))
	require.Error(t, validateJSONValueDefaultLiteral(ctx, literal("1e1000"), types.New(types.T_float64, 0, 0)))

	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		typ := types.New(oid, 6, 2)
		require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal("1.25"), typ), oid)
		require.Error(t, validateJSONValueDefaultLiteral(ctx, literal("1.255"), typ), oid)
	}

	for _, tc := range []struct {
		name  string
		value string
		typ   types.Type
	}{
		{"date valid", "2024-01-02", types.New(types.T_date, 0, 0)},
		{"time valid", "12:34:56.123", types.New(types.T_time, 0, 3)},
		{"datetime valid", "2024-01-02 12:34:56.123", types.New(types.T_datetime, 0, 3)},
		{"year valid", "2024", types.New(types.T_year, 0, 0)},
		{"year numeric fallback", "+2024", types.New(types.T_year, 0, 0)},
		{"json valid", "1", types.New(types.T_json, 0, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal(tc.value), tc.typ))
		})
	}
	for _, tc := range []struct {
		name  string
		value string
		typ   types.Type
	}{
		{"date invalid", "not-a-date", types.New(types.T_date, 0, 0)},
		{"time invalid", "not-a-time", types.New(types.T_time, 0, 3)},
		{"datetime invalid", "not-a-datetime", types.New(types.T_datetime, 0, 3)},
		{"year invalid", "2156", types.New(types.T_year, 0, 0)},
		{"json invalid", "not-json", types.New(types.T_json, 0, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateJSONValueDefaultLiteral(ctx, literal(tc.value), tc.typ))
		})
	}

	require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal("abc"), types.New(types.T_varchar, 3, 0)))
	require.Error(t, validateJSONValueDefaultLiteral(ctx, literal("abcd"), types.New(types.T_varchar, 3, 0)))
	require.NoError(t, validateJSONValueDefaultLiteral(ctx, literal("abc"), types.New(types.T_binary, 3, 0)))
	require.Error(t, validateJSONValueDefaultLiteral(ctx, literal("abcd"), types.New(types.T_binary, 3, 0)))
}

func TestJSONValueTargetValidationBoundaries(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, requireJSONValueContractProtocol(ctx, nil))
	require.NoError(t, validateJSONValueTarget(ctx, types.New(types.T_char, 4, 0)))
	require.NoError(t, validateJSONValueTarget(ctx, types.New(types.T_json, 0, 0)))
	require.Error(t, validateJSONValueTarget(ctx, types.New(types.T_bool, 0, 0)))
	require.Error(t, validateJSONValueTarget(ctx, types.New(types.T_binary, 0, 0)))
	require.Error(t, validateJSONValueTarget(ctx, types.New(types.T_time, 0, 7)))
	require.Error(t, validateJSONValueTarget(ctx, types.New(types.T_decimal64, 4, 5)))

	require.Error(t, validateJSONValueTargetSyntax(ctx, nil))
	require.NoError(t, validateJSONValueTargetSyntax(ctx, &tree.T{InternalType: tree.InternalType{
		FamilyString: "float",
		DisplayWith:  tree.NotDefineDisplayWidth,
		Scale:        tree.NotDefineDec,
	}}))
	require.Error(t, validateJSONValueTargetSyntax(ctx, &tree.T{InternalType: tree.InternalType{
		FamilyString: "double",
		DisplayWith:  10,
	}}))
	require.Error(t, validateJSONValueTargetSyntax(ctx, &tree.T{InternalType: tree.InternalType{
		FamilyString: "year",
		DisplayWith:  4,
	}}))
}

func TestJSONValueBindingRejectsMalformedStructuredCalls(t *testing.T) {
	ctx := context.Background()
	binder := NewDefaultBinder(ctx, NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true), nil, plan.Type{}, nil)
	document := tree.NewNumVal("1", "1", false, tree.P_char)
	path := tree.NewNumVal("$", "$", false, tree.P_char)

	_, err := binder.bindJsonValueExpr(&tree.FuncExpr{
		Exprs:     tree.Exprs{document},
		JsonValue: &tree.JsonValueSpec{},
	}, 0)
	require.Error(t, err)
	_, err = binder.bindJsonValueExpr(&tree.FuncExpr{
		Exprs: tree.Exprs{document, path},
		JsonValue: &tree.JsonValueSpec{
			Returning: &tree.JsonValueReturning{},
		},
	}, 0)
	require.Error(t, err)
	_, err = binder.bindJsonValueExpr(&tree.FuncExpr{
		Exprs: tree.Exprs{document, path},
		JsonValue: &tree.JsonValueSpec{
			OnError: tree.JsonValueResponse{Mode: tree.JsonValueDefaultResponse},
		},
	}, 0)
	require.Error(t, err)
}

func TestJSONValueSemanticNormalizationAndClone(t *testing.T) {
	parseExpr := func(sql string) tree.Expr {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)
		expr := selectStmt.Exprs[0].Expr
		// Keep the expression alive after the parser statement is released by
		// cloning it through the same path used by view/grouping planning.
		cloned := cloneTreeExpr(expr)
		stmt.Free()
		return cloned
	}

	implicit := parseExpr(`select json_value('1', '$')`)
	explicitNull := parseExpr(`select json_value('1', '$' null on empty null on error)`)
	require.Equal(t, semanticAstKey(implicit), semanticAstKey(explicitNull))
	require.NotContains(t, tree.String(implicit, dialect.MYSQL), "returning")
	require.Contains(t, tree.String(explicitNull, dialect.MYSQL), "null on empty null on error")

	withDefault := parseExpr(`select json_value('1', '$' default 0 on error)`)
	require.NotEqual(t, semanticAstKey(implicit), semanticAstKey(withDefault))

	original := withDefault.(*tree.FuncExpr)
	copyExpr := cloneTreeExpr(original).(*tree.FuncExpr)
	copyExpr.JsonValue.OnError = tree.JsonValueResponse{}
	require.NotNil(t, original.JsonValue)
	require.Equal(t, tree.JsonValueDefaultResponse, original.JsonValue.OnError.Mode)
	require.Equal(t, tree.JsonValueImplicitResponse, copyExpr.JsonValue.OnError.Mode)
}

func TestJSONValueVisitorVisitsDefaults(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		`select json_value('1', '$' default 0 on empty default 1 on error)`, 1)
	require.NoError(t, err)
	defer stmt.Free()
	selectStmt := stmt.(*tree.Select).Select.(*tree.SelectClause)

	visits := 0
	visitor := jsonValueTestVisitor{visits: &visits}
	_, ok := selectStmt.Exprs[0].Expr.Accept(visitor)
	require.True(t, ok)
	// document, path, and both DEFAULT literals are all visited.
	require.GreaterOrEqual(t, visits, 4)
}

type jsonValueTestVisitor struct {
	visits *int
}

func (v jsonValueTestVisitor) Enter(expr tree.Expr) (tree.Expr, bool) {
	(*v.visits)++
	return expr, false
}

func (v jsonValueTestVisitor) Exit(expr tree.Expr) (tree.Expr, bool) {
	return expr, true
}
