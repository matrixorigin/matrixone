// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestColumnDefaultPublicBinding(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock := NewMockOptimizer(true, proc)
	column := requireMockColumn(t, mock, "nation", "n_regionkey")
	column.Typ.NotNullable = false
	column.Default = &plan.Default{NullAbility: true, OriginString: "7", Expr: makePlan2Int32ConstExprWithType(7)}
	textColumn := requireMockColumn(t, mock, "nation", "n_name")
	textColumn.Default = &plan.Default{OriginString: "'abc'", Expr: makePlan2StringConstExprWithType("abc")}
	for _, sql := range []string{
		"SELECT DEFAULT(n_regionkey)=7 FROM nation",
		"SELECT DEFAULT(n.n_regionkey)=7 FROM nation n",
		"SELECT DEFAULT(tpch.nation.n_regionkey)=7 FROM nation",
		"SELECT DEFAULT(n_comment) IS NULL FROM nation",
		"SELECT DEFAULT(x)=7 FROM (SELECT n_regionkey x FROM nation) d",
		"SELECT DEFAULT(x)=7 FROM (SELECT +n_regionkey x FROM nation) d",
		"SELECT DEFAULT(x)=7 FROM (SELECT +(+n_regionkey) x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)=7 FROM (SELECT +x x FROM (SELECT +n_regionkey x FROM nation) inner_d LIMIT 1) d",
		"WITH d AS (SELECT +n_regionkey x FROM nation) SELECT DEFAULT(x)=7 FROM d",
		"SELECT DEFAULT(x)='abc' FROM (SELECT +n_name x FROM nation) d",
		"SELECT DEFAULT(x)='abc' FROM (SELECT +n_name x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)='' FROM (SELECT +x x FROM (SELECT 'abc' x) inner_d) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT +n_regionkey x FROM nation UNION ALL SELECT +n_regionkey x FROM nation) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT -n_regionkey x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)=7 FROM (SELECT n_regionkey x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)=7, x+1 FROM (SELECT +n_regionkey x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x) IS NULL, x+1 FROM (SELECT n_regionkey+1 x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)=0, x+1 FROM (SELECT 1 x UNION ALL SELECT 2 x) d",
		"WITH d AS (SELECT n_regionkey x FROM nation) SELECT DEFAULT(x)=7 FROM d",
		"SELECT DEFAULT(x)=7 FROM (SELECT n_regionkey x FROM nation GROUP BY n_regionkey) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT n_regionkey x FROM nation UNION ALL SELECT n_regionkey x FROM nation) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT n_regionkey+0 x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(x)=0 FROM (SELECT 1 x) d",
		"SELECT DEFAULT(x)='' FROM (SELECT 'abc' x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT NULL x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT NULL x UNION SELECT NULL x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT n_regionkey x FROM nation UNION SELECT n_regionkey x FROM nation) d",
		"SELECT DEFAULT(x)=0.00 FROM (SELECT 1.23 x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST('2020-01-01' AS DATE) x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST('2020-01-01' AS DATE) x UNION SELECT CAST('2020-01-01' AS DATE) x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST('12:34:56' AS TIME) x UNION SELECT CAST('12:34:56' AS TIME) x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST('12:34:56' AS TIME) x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST('2020-01-01' AS DATETIME) x) d",
		"SELECT DEFAULT(x) IS NOT NULL FROM (SELECT DATE '2020-01-01' x) d",
		"SELECT DEFAULT(x) IS NOT NULL FROM (SELECT TIME '12:34:56' x) d",
		"SELECT DEFAULT(x) IS NOT NULL FROM (SELECT TIMESTAMP '2020-01-01 00:00:00' x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT DATE('2020-01-01') x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT TIME('12:34:56') x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT TIMESTAMP('2020-01-01 00:00:00') x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT x FROM (SELECT CAST('2020-01-01' AS DATE) x) inner_d) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT DATE '2020-01-01' x UNION ALL SELECT CAST('2020-01-01' AS DATE) x) d",
		"SELECT DEFAULT(x) IS NOT NULL FROM (SELECT DATE '2020-01-01' x UNION ALL SELECT DATE '2021-01-01' x) d",
		"SELECT DEFAULT(x) IS NULL FROM (SELECT CAST(n_regionkey AS SIGNED) x FROM nation LIMIT 1) d",
		"SELECT DEFAULT(n_regionkey)=7, COUNT(*) FROM nation",
		"SELECT DEFAULT(n_regionkey)=7 FROM nation GROUP BY n_regionkey",
		"SELECT DEFAULT(r.n_regionkey)=7 FROM nation l LEFT JOIN nation r ON FALSE",
	} {
		t.Run(sql, func(t *testing.T) {
			logical, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
			query := logical.GetQuery()
			require.NoError(t, query.ValidateIntegerPrecision())
			root := query.Nodes[query.Steps[0]]
			require.NotEmpty(t, root.ProjectList)
			value, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(root.ProjectList[0]), proc, false, true)
			require.NoError(t, err)
			require.NotNil(t, value.GetLit(), "DEFAULT must not depend on the input row: %s", value.String())
			require.True(t, value.GetLit().GetBval(), "%s", value.String())
		})
	}
	for _, sql := range []string{
		"SELECT (SELECT DEFAULT(n.n_regionkey)) FROM nation n",
		"SELECT (SELECT DEFAULT(n.n_regionkey) FROM (SELECT 1 z) n) FROM nation n",
		"SELECT n_regionkey AS n_name FROM nation HAVING DEFAULT(n_name)=7",
		"SELECT n_regionkey AS x FROM nation HAVING DEFAULT(x)=7",
		"SELECT +n_regionkey AS x FROM nation HAVING DEFAULT(x)=7",
		"SELECT n_regionkey AS x FROM nation HAVING DEFAULT(n_regionkey)=7",
		"SELECT n.n_regionkey AS x FROM nation n HAVING DEFAULT(n.n_regionkey)=7",
		"SELECT 1 FROM nation GROUP BY n_regionkey HAVING DEFAULT(n_regionkey)=7",
		"SELECT 1 FROM nation HAVING MAX(DEFAULT(n_regionkey))=7",
		"SELECT (SELECT 1 HAVING DEFAULT(n.n_regionkey)=7) FROM nation n",
		"SELECT +n_regionkey AS x FROM nation ORDER BY DEFAULT(x)",
		"SELECT n_regionkey AS x FROM nation ORDER BY DEFAULT(x)",
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',DEFAULT(n_regionkey))",
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',DEFAULT(nation.n_regionkey))",
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',DEFAULT(tpch.nation.n_regionkey))",
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',DEFAULT)",
		"UPDATE nation SET n_nationkey=DEFAULT(n_regionkey)",
		"UPDATE nation SET n_regionkey=9,n_nationkey=DEFAULT(n_regionkey)",
		"UPDATE nation SET n_nationkey=DEFAULT(n_regionkey),n_regionkey=9",
		"UPDATE nation SET n_regionkey=DEFAULT",
		"REPLACE INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',DEFAULT(n_regionkey))",
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) VALUES(1,'a',1) ON DUPLICATE KEY UPDATE n_name=DEFAULT(n_regionkey)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(mock, t, sql)
			require.NoError(t, err)
		})
	}
}

func TestColumnDefaultUnionTemporalModes(t *testing.T) {
	for _, mode := range []string{"", "STRICT_TRANS_TABLES"} {
		for _, tc := range []struct {
			expression  string
			strictError bool
		}{
			{"DATE '2020-01-01'", true},
			{"TIMESTAMP '2020-01-01 00:00:00'", true},
			{"TIME '12:34:56'", false},
			{"CAST('2020-01-01' AS DATE)", false},
			{"1", false},
		} {
			for _, operator := range []string{"UNION", "UNION ALL"} {
				t.Run(mode+"/"+operator+"/"+tc.expression, func(t *testing.T) {
					mock := NewMockOptimizer(true, newPlanTestProcess(t))
					currentMode := "STRICT_TRANS_TABLES"
					if mode != "" {
						currentMode = ""
					}
					mock.ctxt.sqlModeOverride = &currentMode
					sql := "SELECT DEFAULT(y) FROM (SELECT x AS y FROM (SELECT " + tc.expression + " x " + operator + " SELECT " + tc.expression + " x) d) outer_d"
					statement, err := mysql.ParseOneWithSQLMode(t.Context(), sql, 1, mode)
					require.NoError(t, err)
					defer statement.Free()
					_, err = BuildPlan(&mock.ctxt, statement, false)
					if mode != "" && tc.strictError {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoDefaultForColumn), "%v", err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestColumnDefaultResultSchema(t *testing.T) {
	for _, tc := range []struct {
		oid                                types.T
		members, defaultOrigin, wantOrigin string
		value                              *plan.Expr
	}{
		{types.T_int32, "", "7", "0", makePlan2Int32ConstExprWithType(7)},
		{types.T_enum, "red,blue", "'blue'", "'red'", &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_EnumVal{EnumVal: 2}}}}},
		{types.T_uint64, "a,b", "'a,b'", "''", makePlan2Uint64ConstExprWithType(3)},
	} {
		t.Run(tc.oid.String(), func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			column := requireMockColumn(t, mock, "nation", "n_name")
			column.Typ = tc.oid.ToType().PlanType()
			column.Typ.NotNullable, column.Typ.Enumvalues = true, tc.members
			tc.value.Typ = column.Typ
			column.Default = &plan.Default{OriginString: tc.defaultOrigin, Expr: tc.value}
			logical, err := runOneStmt(mock, t, "CREATE TABLE default_shape AS SELECT DEFAULT(n_name) AS x FROM nation")
			require.NoError(t, err)
			output := logical.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.Equal(t, int32(tc.oid), output.Typ.Id)
			require.Equal(t, tc.members, output.Typ.Enumvalues)
			require.Equal(t, tc.wantOrigin, output.Default.OriginString)
			require.False(t, output.Default.NullAbility)
			_, err = runOneStmt(mock, t, "UPDATE nation SET n_regionkey=9,n_name=DEFAULT(n_name)")
			require.NoError(t, err)
			_, err = runOneStmt(mock, t, "SELECT DEFAULT(x) FROM (SELECT DEFAULT(n_name) x FROM nation) d")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoDefaultForColumn), "%v", err)
		})
	}
}

func TestColumnDefaultBindingErrors(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	for _, tc := range []struct {
		sql  string
		code uint16
	}{
		{"SELECT DEFAULT(missing) FROM nation", moerr.ErrBadFieldError},
		{"SELECT n_regionkey AS x,DEFAULT(x) FROM nation", moerr.ErrBadFieldError},
		{"SELECT DEFAULT(n_name) FROM nation", moerr.ErrNoDefaultForColumn},
		{"SELECT n_regionkey AS n_name FROM nation ORDER BY DEFAULT(n_name)", moerr.ErrNoDefaultForColumn},
		{"SELECT 1 AS x FROM nation HAVING DEFAULT(x)=0", moerr.ErrNoDefaultForColumn},
		{"SELECT 1 FROM nation HAVING DEFAULT(n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT n_regionkey AS x,n_name AS x FROM nation HAVING DEFAULT(x)=7", moerr.ErrInvalidInput},
		{"SELECT 1 FROM nation n HAVING DEFAULT(n.n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT n_regionkey+1 FROM nation HAVING DEFAULT(n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT COUNT(n_regionkey) FROM nation HAVING DEFAULT(n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT DEFAULT(n_regionkey) FROM nation HAVING DEFAULT(n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT 1 FROM nation GROUP BY n_regionkey HAVING DEFAULT(n_name) IS NULL", moerr.ErrBadFieldError},
		{"SELECT 1 FROM nation GROUP BY n_regionkey+1 HAVING DEFAULT(n_regionkey)=7", moerr.ErrBadFieldError},
		{"SELECT DEFAULT(x) FROM (SELECT n_regionkey+0 x FROM nation) d", moerr.ErrNoDefaultForColumn},
		{"SELECT DEFAULT(x) FROM (SELECT -n_regionkey x FROM nation) d", moerr.ErrNoDefaultForColumn},
		{"SELECT DEFAULT(x) FROM (SELECT +CAST(n_regionkey AS SIGNED) x FROM nation) d", moerr.ErrNoDefaultForColumn},
		{"SELECT DEFAULT(x) FROM (SELECT +n_name x FROM nation) d", moerr.ErrNoDefaultForColumn},
		{"SELECT DEFAULT(x) FROM (SELECT CAST(n_regionkey AS SIGNED) x FROM nation) d", moerr.ErrNoDefaultForColumn},
		{"INSERT INTO nation(n_nationkey) VALUES(DEFAULT(other.n_regionkey))", moerr.ErrBadFieldError},
		{"SELECT DEFAULT(n_regionkey) FROM nation a,nation b", moerr.ErrInvalidInput},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := runOneStmt(mock, t, tc.sql)
			require.True(t, moerr.IsMoErrCode(err, tc.code), "%v", err)
		})
	}
	column := requireMockColumn(t, mock, "nation", "n_regionkey")
	column.Default = &plan.Default{NullAbility: true, OriginString: "(7)", Expr: makePlan2Int32ConstExprWithType(7)}
	_, err := runOneStmt(mock, t, "SELECT DEFAULT(n_regionkey) FROM nation")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDefaultExpression), "%v", err)
	column.GeneratedCol = &plan.GeneratedCol{Expr: makePlan2Int32ConstExprWithType(7)}
	_, err = runOneStmt(mock, t, "SELECT DEFAULT(n_regionkey) FROM nation")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoDefaultForColumn), "%v", err)
}

func TestColumnDefaultMetadataIsImmutable(t *testing.T) {
	binder := NewDefaultBinder(t.Context(), nil, nil, plan.Type{}, nil)
	column := &plan.ColDef{Name: "a", Typ: types.T_int32.ToType().PlanType(),
		Default: &plan.Default{NullAbility: true, OriginString: "7", Expr: makePlan2Int32ConstExprWithType(7)}}
	metadata := snapshotSourceColumnMetadata(column)
	value, err := binder.sourceColumnDefault(metadata, column.Typ, "a")
	require.NoError(t, err)
	value.GetLit().Value = &plan.Literal_I32Val{I32Val: 9}
	require.Equal(t, int32(7), metadata.Default.Expr.GetLit().GetI32Val())
	require.Equal(t, int32(7), column.Default.Expr.GetLit().GetI32Val())
	column.GeneratedCol = &plan.GeneratedCol{}
	require.False(t, metadata.Generated)
	require.True(t, snapshotSourceColumnMetadata(column).Generated)

	for _, oid := range []types.T{types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_int32, types.T_varchar, types.T_decimal128} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := oid.ToType().PlanType()
			typ.NotNullable = true
			if oid == types.T_decimal128 {
				typ.Width, typ.Scale = 20, 3
			}
			value, err := binder.columnTypeDefault(typ)
			require.NoError(t, err)
			require.Equal(t, typ.Id, value.Typ.Id)
			require.Equal(t, typ.Scale, value.Typ.Scale)
			typ.NotNullable = false
			value, err = binder.columnTypeDefault(typ)
			require.NoError(t, err)
			require.True(t, value.GetLit().Isnull)
		})
	}
}
