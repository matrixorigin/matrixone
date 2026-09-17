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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func assertPreparedTimeResult(
	t *testing.T,
	rows *sql.Rows,
	want string,
	wantNull bool,
	wantPrecision int64,
	wantScale int64,
) {
	t.Helper()
	defer func() {
		require.NoError(t, rows.Close())
	}()

	columns, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Equal(t, "result", columns[0].Name())
	require.Equal(t, "DECIMAL", columns[0].DatabaseTypeName())
	precision, scale, ok := columns[0].DecimalSize()
	require.True(t, ok)
	require.Equal(t, wantPrecision, precision)
	require.Equal(t, wantScale, scale)

	require.True(t, rows.Next())
	var value sql.NullString
	require.NoError(t, rows.Scan(&value))
	require.Equal(t, wantNull, !value.Valid)
	if !wantNull {
		require.Equal(t, want, value.String)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func assertPreparedTimeDoubleResult(t *testing.T, rows *sql.Rows, want float64) {
	t.Helper()
	defer func() {
		require.NoError(t, rows.Close())
	}()

	columns, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Equal(t, "result", columns[0].Name())
	require.Equal(t, "DOUBLE", columns[0].DatabaseTypeName())
	require.True(t, rows.Next())
	var value sql.NullFloat64
	require.NoError(t, rows.Scan(&value))
	require.True(t, value.Valid)
	require.InDelta(t, want, value.Float64, 1e-9)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

// TestPreparedTimeArithmeticOverMySQLProtocol covers both SQL
// PREPARE/EXECUTE and a real database/sql COM_STMT_PREPARE/COM_STMT_EXECUTE
// statement. The one-CN fixture is shared with the package's other embedded
// tests; all executions reuse one connection and one prepared statement.
func TestPreparedTimeArithmeticOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		deallocate := func(statement string) {
			t.Helper()
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, statement)
		}
		queryText := func(statement, want string, wantNull bool, scale int64) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			assertPreparedTimeResult(t, rows, want, wantNull, 38, scale)
		}
		queryDecimal64Text := func(statement, want string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			assertPreparedTimeResult(t, rows, want, false, 18, 0)
		}

		// The SQL path transports values through session user variables. Reusing
		// one server-side prepared statement makes each SourceType transition
		// observable, including NULL followed by a concrete value.
		exec("prepare issue28963_time_sql from 'select cast(''00:00:01'' as time(0)) * ? as result'")
		defer func() {
			deallocate("deallocate prepare issue28963_time_sql")
		}()
		for _, tc := range []struct {
			assignment string
			want       string
			wantNull   bool
			scale      int64
		}{
			{assignment: "cast(10 as signed)", want: "10", scale: 0},
			{assignment: "cast(1.2345678901234 as decimal(14,13))", want: "1.2345678901234", scale: 13},
			{assignment: "cast(1.25 as decimal(3,2))", want: "1.25", scale: 2},
			{assignment: "null", wantNull: true, scale: 0},
			{assignment: "cast(10 as signed)", want: "10", scale: 0},
		} {
			exec("set @issue28963_time_value = " + tc.assignment)
			queryText("execute issue28963_time_sql using @issue28963_time_value",
				tc.want, tc.wantNull, tc.scale)
		}

		for _, tc := range []struct {
			name string
			op   string
			want string
		}{
			{name: "add", op: "+", want: "11"},
			{name: "subtract", op: "-", want: "-9"},
			{name: "mod", op: "%", want: "1"},
		} {
			statementName := "issue28963_time_sql_" + tc.name
			exec("prepare " + statementName + " from 'select cast(''00:00:01'' as time(0)) " + tc.op + " ? as result'")
			exec("set @issue28963_time_value = cast(10 as signed)")
			queryDecimal64Text("execute "+statementName+" using @issue28963_time_value", tc.want)
			exec("deallocate prepare " + statementName)
		}

		exec("prepare issue28963_time_sql_fractional from 'select cast(''03:04:05.123456'' as time(6)) * ? as result'")
		defer func() {
			deallocate("deallocate prepare issue28963_time_sql_fractional")
		}()
		exec("set @issue28963_time_value = cast(10 as signed)")
		queryText("execute issue28963_time_sql_fractional using @issue28963_time_value",
			"304051.234560", false, 6)
		exec("set @issue28963_time_value = cast(1.25 as decimal(3,2))")
		queryText("execute issue28963_time_sql_fractional using @issue28963_time_value",
			"38006.40432000", false, 8)

		// Keep ordinary expressions as controls for both result domains.
		queryText("select cast('00:00:01' as time(0)) * cast(1.2345678901234 as decimal(14,13)) as result",
			"1.2345678901234", false, 13)
		queryText("select cast('03:04:05.123456' as time(6)) * cast(1.25 as decimal(3,2)) as result",
			"38006.40432000", false, 8)

		// database/sql with interpolateParams=false uses the binary protocol.
		// The same *sql.Stmt is deliberately reused across integer, DOUBLE,
		// NULL, and integer values again; the last execution catches stale
		// execute-time metadata or a cached decimal scale. A NULL binary marker
		// has no numeric source type, so TIME(6) arithmetic falls back to the
		// prepared decimal envelope and reports scale 12 (6+6).
		const binarySQLStatement = "select cast('03:04:05.123456' as time(6)) * ? as result"
		binaryStmt, err := conn.PrepareContext(ctx, binarySQLStatement)
		require.NoError(t, err)
		defer binaryStmt.Close()
		for _, tc := range []struct {
			name     string
			value    any
			want     string
			wantNull bool
			scale    int64
			isDouble bool
		}{
			{name: "integer", value: int64(10), want: "304051.234560", scale: 6},
			{name: "double", value: float64(1.25), isDouble: true},
			{name: "null", value: nil, wantNull: true, scale: 12},
			{name: "integer after double", value: int64(10), want: "304051.234560", scale: 6},
		} {
			t.Run("binary/"+tc.name, func(t *testing.T) {
				rows, err := binaryStmt.QueryContext(ctx, tc.value)
				require.NoError(t, err)
				if tc.isDouble {
					assertPreparedTimeDoubleResult(t, rows, 38006.40432)
				} else {
					assertPreparedTimeResult(t, rows, tc.want, tc.wantNull, 38, tc.scale)
				}
			})
		}

		for _, tc := range []struct {
			name string
			op   string
			want string
		}{
			{name: "add", op: "+", want: "11"},
			{name: "subtract", op: "-", want: "-9"},
			{name: "mod", op: "%", want: "1"},
		} {
			t.Run("binary/"+tc.name, func(t *testing.T) {
				statement := "select cast('00:00:01' as time(0)) " + tc.op + " ? as result"
				stmt, err := conn.PrepareContext(ctx, statement)
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.QueryContext(ctx, int64(10))
				require.NoError(t, err)
				assertPreparedTimeResult(t, rows, tc.want, false, 18, 0)
			})
		}

		for _, tc := range []struct {
			name          string
			op            string
			want          string
			nullPrecision int64
		}{
			{name: "add", op: "+", want: "4", nullPrecision: 65},
			{name: "subtract", op: "-", want: "2", nullPrecision: 65},
			{name: "mod", op: "%", want: "0", nullPrecision: 38},
		} {
			t.Run("binary/nested/"+tc.name, func(t *testing.T) {
				statement := "select cast('00:00:01' as time(0)) " + tc.op +
					" (? " + tc.op + " ?) as result"
				ordinaryRows, err := conn.QueryContext(ctx,
					"select cast('00:00:01' as time(0)) "+tc.op+" (1 "+tc.op+" 2) as result")
				require.NoError(t, err)
				assertPreparedTimeResult(t, ordinaryRows, tc.want, false, 18, 0)
				stmt, err := conn.PrepareContext(ctx, statement)
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.QueryContext(ctx, int64(1), int64(2))
				require.NoError(t, err)
				assertPreparedTimeResult(t, rows, tc.want, false, 18, 0)

				nullRows, err := stmt.QueryContext(ctx, nil, int64(2))
				require.NoError(t, err)
				// A NULL binary marker has no runtime numeric category. Keep the
				// operation's prepare-time decimal envelope for the unresolved domain.
				assertPreparedTimeResult(t, nullRows, "", true, tc.nullPrecision, 0)
			})
		}

		t.Run("binary/explicit-decimal", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx,
				"select cast(cast('00:00:01' as time(0)) as decimal(10,2)) + ? as result")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct {
				value int64
				want  string
			}{
				{value: 10, want: "11.00"},
				{value: 9223372036854775807, want: "9223372036854775808.00"},
			} {
				rows, err := stmt.QueryContext(ctx, tc.value)
				require.NoError(t, err)
				assertPreparedTimeResult(t, rows, tc.want, false, 38, 2)
			}
		})

		t.Run("binary/nested/add-overflow", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx,
				"select cast('00:00:01' as time(0)) + (? + ?) as result")
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx, int64(9223372036854775807), int64(1))
			if err == nil {
				if rows.Next() {
					var value any
					err = rows.Scan(&value)
				}
				if err == nil {
					err = rows.Err()
				}
				_ = rows.Close()
			}
			require.Error(t, err)
		})
	})
}
