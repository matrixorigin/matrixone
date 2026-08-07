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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26232ViewDefaultAndCTASContracts(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dbConn, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer dbConn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		execSQLRequire(t, ctx, dbConn, "set role moadmin")

		const db = "issue_26232"
		for _, stmt := range []string{
			"drop database if exists " + db,
			"create database " + db,
			"create table " + db + ".source_t (" +
				"id int primary key, qty int not null default 7, nullable_col int, null_col int default null, " +
				"str_col varchar(20) not null default 'seed', amount decimal(10,2) not null default 1.25, " +
				"date_col date not null default '2024-01-02', datetime_col datetime not null default '2024-01-02 03:04:05', " +
				"time_col time not null default '03:04:05', timestamp_col timestamp not null default '2024-01-02 03:04:05', " +
				"binary_col binary(4) not null default 'xy', varbinary_col varbinary(8) not null default 'xy', " +
				"blob_col blob not null default ('blob-seed'), float_col float not null default 1.5, " +
				"double_col double not null default 2.5, bit_col bit(4) not null default b'1010', " +
				"year_col year not null default 2024, text_col text not null default ('text-seed'), " +
				"nullable_text text default ('nullable-text-seed'), " +
				"nullable_blob blob default ('nullable-blob-seed'), " +
				"nullable_expr uuid default (uuid()), " +
				"expr_col uuid not null default (uuid()), " +
				"priority enum('low','medium','high') not null default 'medium', " +
				"flags set('a','b') not null default 'a')",
			"create table " + db + ".source_t2 (id int primary key, qty int not null default 9)",
			"insert into " + db + ".source_t(id) values (1)",
			"insert into " + db + ".source_t2 values (1, 9)",
			"create view " + db + ".v_source_t as select * from " + db + ".source_t",
			"create view " + db + ".v_alias as select qty as amount from " + db + ".source_t",
			"create view " + db + ".v_explicit(amount) as select qty from " + db + ".source_t",
			"create view " + db + ".v_derived as select amount from (select qty as amount from " + db + ".source_t) d",
			"create view " + db + ".v_cte as with d as (select qty as amount from " + db + ".source_t) select amount from d",
			"create view " + db + ".v_view as select amount from " + db + ".v_alias",
			"create view " + db + ".v_join as select l.qty as left_qty, r.qty as right_qty from " + db + ".source_t l join " + db + ".source_t2 r on l.id = r.id",
			"create view " + db + ".v_constant as select 7 as qty",
			"create view " + db + ".v_function as select abs(qty) as qty from " + db + ".source_t",
			"create view " + db + ".v_arithmetic as select qty + 0 as qty from " + db + ".source_t",
			"create view " + db + ".v_aggregate as select max(qty) as qty from " + db + ".source_t",
			"create view " + db + ".v_union as select qty from " + db + ".source_t union select qty from " + db + ".source_t",
			"create view " + db + ".v_union_all as select qty from " + db + ".source_t union all select qty from " + db + ".source_t",
			"create view " + db + ".v_recursive as with recursive d(qty) as (select qty from " + db + ".source_t union all select qty from d where false) select qty from d",
			"create table " + db + ".ctas_view as select id, qty, nullable_col, null_col, str_col, amount, " +
				"date_col, datetime_col, time_col, timestamp_col, binary_col, varbinary_col, blob_col, " +
				"float_col, double_col, bit_col, year_col, text_col, nullable_text, nullable_blob, nullable_expr, " +
				"expr_col, priority, flags from " + db + ".v_source_t",
		} {
			execSQLRequire(t, ctx, dbConn, stmt)
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, dbConn, "drop database if exists "+db)
		}()

		for _, test := range []struct {
			viewName  string
			column    string
			wantValue string
		}{
			{viewName: "v_source_t", column: "qty", wantValue: "7"},
			{viewName: "v_alias", column: "amount", wantValue: "7"},
			{viewName: "v_explicit", column: "amount", wantValue: "7"},
			{viewName: "v_derived", column: "amount", wantValue: "7"},
			{viewName: "v_cte", column: "amount", wantValue: "7"},
			{viewName: "v_view", column: "amount", wantValue: "7"},
			{viewName: "v_join", column: "left_qty", wantValue: "7"},
			{viewName: "v_join", column: "right_qty", wantValue: "9"},
			{viewName: "v_source_t", column: "null_col", wantValue: "null"},
			{viewName: "v_source_t", column: "str_col", wantValue: "'seed'"},
			{viewName: "v_source_t", column: "amount", wantValue: "1.25"},
			{viewName: "v_source_t", column: "expr_col", wantValue: "(uuid())"},
			{viewName: "v_source_t", column: "priority", wantValue: "'medium'"},
			{viewName: "v_source_t", column: "flags", wantValue: "'a'"},
		} {
			var got sql.NullString
			require.NoError(t, dbConn.QueryRowContext(ctx,
				"select column_default from information_schema.columns "+
					"where table_schema = ? and table_name = ? and column_name = ?",
				db, test.viewName, test.column).Scan(&got), test.viewName+"."+test.column)
			require.True(t, got.Valid, test.viewName+"."+test.column)
			require.Equal(t, test.wantValue, got.String, test.viewName+"."+test.column)
		}

		for _, viewName := range []string{
			"v_constant", "v_function", "v_arithmetic", "v_aggregate",
			"v_union", "v_union_all", "v_recursive",
		} {
			var got sql.NullString
			require.NoError(t, dbConn.QueryRowContext(ctx,
				"select column_default from information_schema.columns "+
					"where table_schema = ? and table_name = ? and column_name = 'qty'",
				db, viewName).Scan(&got), viewName)
			require.False(t, got.Valid, viewName)
		}

		var nullableDefault sql.NullString
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select column_default from information_schema.columns "+
				"where table_schema = ? and table_name = 'v_source_t' and column_name = 'nullable_col'",
			db).Scan(&nullableDefault))
		require.False(t, nullableDefault.Valid)

		require.Equal(t, "7", descColumnDefault(t, ctx, dbConn, db+".v_source_t", "qty").String)
		var viewName, viewSQL, charset, collation string
		require.NoError(t, dbConn.QueryRowContext(ctx, "show create view "+db+".v_source_t").
			Scan(&viewName, &viewSQL, &charset, &collation))
		require.Contains(t, strings.ToLower(viewSQL), "create view")
		require.NotContains(t, strings.ToLower(viewSQL), "default 7")

		for _, test := range []struct {
			column   string
			wantInfo sql.NullString
			wantDesc sql.NullString
		}{
			{column: "qty", wantInfo: sql.NullString{String: "0", Valid: true}, wantDesc: sql.NullString{String: "0", Valid: true}},
			{column: "nullable_col"},
			{column: "null_col"},
			{column: "str_col", wantInfo: sql.NullString{String: "''", Valid: true}, wantDesc: sql.NullString{String: "", Valid: true}},
			{column: "amount", wantInfo: sql.NullString{String: "0.00", Valid: true}, wantDesc: sql.NullString{String: "0.00", Valid: true}},
			{column: "priority", wantInfo: sql.NullString{String: "'low'", Valid: true}, wantDesc: sql.NullString{String: "low", Valid: true}},
			{column: "flags", wantInfo: sql.NullString{String: "''", Valid: true}, wantDesc: sql.NullString{String: "", Valid: true}},
			{column: "date_col"},
			{column: "datetime_col"},
			{column: "time_col", wantInfo: sql.NullString{String: "'00:00:00'", Valid: true}, wantDesc: sql.NullString{String: "00:00:00", Valid: true}},
			{column: "timestamp_col"},
			{column: "binary_col", wantInfo: sql.NullString{String: "''", Valid: true}, wantDesc: sql.NullString{String: "", Valid: true}},
			{column: "varbinary_col", wantInfo: sql.NullString{String: "''", Valid: true}, wantDesc: sql.NullString{String: "", Valid: true}},
			{column: "blob_col", wantInfo: sql.NullString{String: "('blob-seed')", Valid: true}, wantDesc: sql.NullString{String: "('blob-seed')", Valid: true}},
			{column: "float_col", wantInfo: sql.NullString{String: "0", Valid: true}, wantDesc: sql.NullString{String: "0", Valid: true}},
			{column: "double_col", wantInfo: sql.NullString{String: "0", Valid: true}, wantDesc: sql.NullString{String: "0", Valid: true}},
			{column: "bit_col", wantInfo: sql.NullString{String: "0", Valid: true}, wantDesc: sql.NullString{String: "0", Valid: true}},
			{column: "year_col", wantInfo: sql.NullString{String: "'0000'", Valid: true}, wantDesc: sql.NullString{String: "0000", Valid: true}},
			{column: "text_col", wantInfo: sql.NullString{String: "('text-seed')", Valid: true}, wantDesc: sql.NullString{String: "('text-seed')", Valid: true}},
			{column: "nullable_text", wantInfo: sql.NullString{String: "('nullable-text-seed')", Valid: true}, wantDesc: sql.NullString{String: "('nullable-text-seed')", Valid: true}},
			{column: "nullable_blob", wantInfo: sql.NullString{String: "('nullable-blob-seed')", Valid: true}, wantDesc: sql.NullString{String: "('nullable-blob-seed')", Valid: true}},
			{column: "nullable_expr", wantInfo: sql.NullString{String: "(uuid())", Valid: true}, wantDesc: sql.NullString{String: "(uuid())", Valid: true}},
			{column: "expr_col", wantInfo: sql.NullString{String: "(uuid())", Valid: true}, wantDesc: sql.NullString{String: "(uuid())", Valid: true}},
		} {
			var infoDefault sql.NullString
			require.NoError(t, dbConn.QueryRowContext(ctx,
				"select column_default from information_schema.columns "+
					"where table_schema = ? and table_name = 'ctas_view' and column_name = ?",
				db, test.column).Scan(&infoDefault), test.column)
			require.Equal(t, test.wantInfo, infoDefault, test.column)
			require.Equal(t, test.wantDesc,
				descColumnDefault(t, ctx, dbConn, db+".ctas_view", test.column), test.column)
		}

		var tableName, createTableSQL string
		require.NoError(t, dbConn.QueryRowContext(ctx, "show create table "+db+".ctas_view").
			Scan(&tableName, &createTableSQL))
		require.Contains(t, createTableSQL, "DEFAULT 0")
		require.NotContains(t, createTableSQL, "DEFAULT 7")
		require.Contains(t, createTableSQL, "DEFAULT ''")
		require.Contains(t, createTableSQL, "DEFAULT 0.00")
		require.Contains(t, createTableSQL, "DEFAULT 'low'")
		require.Contains(t, strings.ToLower(createTableSQL), "`time_col` time not null default '00:00:00'")
		require.Contains(t, strings.ToLower(createTableSQL), "`year_col` year not null default '0000'")
		require.Contains(t, strings.ToLower(createTableSQL), "`text_col` text not null default ('text-seed')")
		require.Contains(t, strings.ToLower(createTableSQL), "`nullable_text` text default ('nullable-text-seed')")
		require.Contains(t, strings.ToLower(createTableSQL), "`nullable_blob` blob default ('nullable-blob-seed')")
		require.Contains(t, strings.ToLower(createTableSQL), "`nullable_expr` uuid default (uuid())")
		require.Contains(t, strings.ToLower(createTableSQL), "`expr_col` uuid not null default (uuid())")

		execSQLRequire(t, ctx, dbConn, "insert into "+db+".ctas_view"+
			"(id,date_col,datetime_col,timestamp_col) values "+
			"(2,'2025-02-03','2025-02-03 04:05:06','2025-02-03 04:05:06')")
		for _, insertSQL := range []string{
			"insert into " + db + ".ctas_view(id,datetime_col,timestamp_col) values " +
				"(3,'2025-02-03 04:05:06','2025-02-03 04:05:06')",
			"insert into " + db + ".ctas_view(id,date_col,timestamp_col) values " +
				"(4,'2025-02-03','2025-02-03 04:05:06')",
			"insert into " + db + ".ctas_view(id,date_col,datetime_col) values " +
				"(5,'2025-02-03','2025-02-03 04:05:06')",
		} {
			_, insertErr := dbConn.ExecContext(ctx, insertSQL)
			require.Error(t, insertErr, insertSQL)
		}
		var insertedQty int
		var insertedNullable, insertedNull sql.NullInt64
		var insertedString, insertedAmount, insertedPriority, insertedFlags string
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select qty, nullable_col, null_col, str_col, amount, priority, flags "+
				"from "+db+".ctas_view where id = 2").
			Scan(&insertedQty, &insertedNullable, &insertedNull, &insertedString,
				&insertedAmount, &insertedPriority, &insertedFlags))
		require.Equal(t, 0, insertedQty)
		require.False(t, insertedNullable.Valid)
		require.False(t, insertedNull.Valid)
		require.Empty(t, insertedString)
		require.Equal(t, "0.00", insertedAmount)
		require.Equal(t, "low", insertedPriority)
		require.Empty(t, insertedFlags)
		var insertedTime string
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select time_col from "+db+".ctas_view where id = 2").Scan(&insertedTime))
		require.Equal(t, "00:00:00", insertedTime)
		var binaryHex, varbinaryHex, insertedBlob string
		var insertedFloat, insertedDouble float64
		var insertedBit uint64
		var nullableExprIsNull, exprIsNull bool
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select hex(binary_col), hex(varbinary_col), blob_col, float_col, double_col, bit_col + 0, "+
				"nullable_expr is null, expr_col is null "+
				"from "+db+".ctas_view where id = 2").
			Scan(&binaryHex, &varbinaryHex, &insertedBlob, &insertedFloat, &insertedDouble, &insertedBit,
				&nullableExprIsNull, &exprIsNull))
		require.Equal(t, "00000000", binaryHex)
		require.Empty(t, varbinaryHex)
		require.Equal(t, "blob-seed", insertedBlob)
		require.Zero(t, insertedFloat)
		require.Zero(t, insertedDouble)
		require.Zero(t, insertedBit)
		require.False(t, nullableExprIsNull)
		require.False(t, exprIsNull)
		var insertedYear int
		var insertedText string
		var insertedNullableText, insertedNullableBlob sql.NullString
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select year_col, text_col, nullable_text, nullable_blob from "+db+".ctas_view where id = 2").
			Scan(&insertedYear, &insertedText, &insertedNullableText, &insertedNullableBlob))
		require.Zero(t, insertedYear)
		require.Equal(t, "text-seed", insertedText)
		require.Equal(t, sql.NullString{String: "nullable-text-seed", Valid: true}, insertedNullableText)
		require.Equal(t, sql.NullString{String: "nullable-blob-seed", Valid: true}, insertedNullableBlob)
	})
}

func descColumnDefault(
	t *testing.T, ctx context.Context, dbConn *sql.DB, tableName, columnName string,
) sql.NullString {
	t.Helper()
	rows, err := dbConn.QueryContext(ctx, "desc "+tableName)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var field, typ, nullable, key, extra, comment string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
		if field == columnName {
			return defaultValue
		}
	}
	require.NoError(t, rows.Err())
	require.FailNow(t, "column not found in DESC", tableName+"."+columnName)
	return sql.NullString{}
}
