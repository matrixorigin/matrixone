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
		require.Len(t, expr.GetF().Args, 7)
		require.Equal(t, int32(types.T_varchar), expr.GetF().Args[2].Typ.Id)
		require.Equal(t, int32(512), expr.GetF().Args[2].Typ.Width)
		require.NotNil(t, expr.GetF().Args[2].GetT())
	})

	t.Run("explicit unsigned default", func(t *testing.T) {
		expr, err := bind(`select json_value('{"a":1}', '$.a' returning unsigned default 0 on error)`, false)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
		require.False(t, expr.Typ.NotNullable)
		require.Equal(t, int64(3), expr.GetF().Args[5].GetLit().GetI64Val())
		require.NotNil(t, expr.GetF().Args[6])
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
	require.Contains(t, tree.String(implicit, dialect.MYSQL), "returning char(512)")
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
