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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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
		for _, temporal := range []struct {
			literal, want string
			scale         int64
		}{{"00:00:01", "11", 0}, {"03:04:05.123456", "30415.123456", 6}} {
			for _, binary := range []bool{false, true} {
				t.Run(fmt.Sprintf("abs-boundary/time%d/binary=%t", temporal.scale, binary), func(t *testing.T) {
					expression := fmt.Sprintf("select abs(cast('%s' as time(%d)) + ?) as result", temporal.literal, temporal.scale)
					var query func(any) (*sql.Rows, error)
					if binary {
						stmt, err := conn.PrepareContext(ctx, expression)
						require.NoError(t, err)
						defer stmt.Close()
						query = func(value any) (*sql.Rows, error) { return stmt.QueryContext(ctx, value) }
					} else {
						exec("prepare fallback_time_abs from '" + strings.ReplaceAll(expression, "'", "''") + "'")
						defer deallocate("deallocate prepare fallback_time_abs")
						query = func(value any) (*sql.Rows, error) {
							assignment := "NULL"
							if value != nil {
								assignment = fmt.Sprintf("cast(%d as signed)", value)
							}
							exec("set @fallback_time_abs_value = " + assignment)
							return conn.QueryContext(ctx, "execute fallback_time_abs using @fallback_time_abs_value")
						}
					}
					// Query may expose an execution error immediately or through Rows.Err.
					queryError := func(query func() (*sql.Rows, error)) error {
						rows, err := query()
						if err != nil {
							return err
						}
						defer rows.Close()
						require.False(t, rows.Next(), "overflow must not produce a row")
						return rows.Err()
					}
					for _, value := range []any{int64(10), nil, int64(10), int64(9223372036854775807), int64(10)} {
						if value == int64(9223372036854775807) {
							wantErr := queryError(func() (*sql.Rows, error) {
								return conn.QueryContext(ctx,
									strings.ReplaceAll(expression, "?", "cast(9223372036854775807 as signed)"))
							})
							gotErr := queryError(func() (*sql.Rows, error) { return query(value) })
							var wantMySQL, gotMySQL *mysql.MySQLError
							require.True(t, errors.As(wantErr, &wantMySQL), "%v", wantErr)
							require.True(t, errors.As(gotErr, &gotMySQL), "%v", gotErr)
							require.Equal(t, wantMySQL.Number, gotMySQL.Number)
							continue
						}
						func() {
							rows, err := query(value)
							require.NoError(t, err)
							if value != nil {
								assertPreparedTimeResult(t, rows, temporal.want, false, 18, temporal.scale)
								return
							}
							defer rows.Close()
							require.True(t, rows.Next())
							var result sql.NullString
							require.NoError(t, rows.Scan(&result))
							require.False(t, result.Valid)
							require.False(t, rows.Next())
							require.NoError(t, rows.Err())
						}()
					}
				})
			}
		}

		// Recovering NULL's logical type must retain SELECT's aggregate and
		// window binding capabilities without evaluating either expression twice.
		exec("set @null_aggregate = sum(null), @null_window = sum(null) over ()")
		var aggregateNull, windowNull int
		require.NoError(t, conn.QueryRowContext(ctx, "select isnull(@null_aggregate), isnull(@null_window)").Scan(&aggregateNull, &windowNull))
		require.Equal(t, 1, aggregateNull)
		require.Equal(t, 1, windowNull)

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
			{assignment: "(null)", wantNull: true, scale: 0},
			{assignment: "@issue28963_time_value", wantNull: true, scale: 0},
			{assignment: "cast(null as decimal(3,2))", wantNull: true, scale: 2},
			{assignment: "cast(10 as signed)", want: "10", scale: 0},
		} {
			exec("set @issue28963_time_value = " + tc.assignment)
			queryText("execute issue28963_time_sql using @issue28963_time_value",
				tc.want, tc.wantNull, tc.scale)
		}

		// Arithmetic converts typed string NULL at its consumer, preserving the
		// source domain and recovering on the same statement after NULL.
		for _, op := range []string{"*", "/", "div"} {
			func() {
				exec("prepare typed_null_time from 'select cast(''00:00:01'' as time(0)) " + op + " ? as result'")
				defer deallocate("deallocate prepare typed_null_time")
				for _, tc := range []struct {
					assignment string
					value      float64
					null       bool
				}{
					{assignment: "cast('0.5' as char)", value: 0.5},
					{assignment: "cast(null as char)", null: true},
					{assignment: "cast(null as binary)", null: true},
					{assignment: "cast('2.5' as char)", value: 2.5},
				} {
					exec("set @typed_null_value = " + tc.assignment)
					func() {
						rows, err := conn.QueryContext(ctx, "execute typed_null_time using @typed_null_value")
						require.NoError(t, err)
						defer rows.Close()
						columns, err := rows.ColumnTypes()
						require.NoError(t, err)
						wantType := "DOUBLE"
						if op == "div" {
							wantType = "BIGINT"
						}
						require.Equal(t, wantType, columns[0].DatabaseTypeName())
						require.True(t, rows.Next())
						var value sql.NullFloat64
						require.NoError(t, rows.Scan(&value))
						require.Equal(t, !tc.null, value.Valid)
						if !tc.null {
							want := tc.value
							if op == "/" {
								want = 1 / tc.value
							}
							if op == "div" {
								want = float64(int64(1 / tc.value))
							}
							require.Equal(t, want, value.Float64)
						}
						require.False(t, rows.Next())
						require.NoError(t, rows.Err())
					}()
				}
			}()
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
		// has no numeric source type, so its TIME(6) peer supplies the decimal
		// operand domain and multiplication reports scale 12 (6+6).
		for _, expression := range []string{
			"cast('03:04:05.123456' as time(6)) * cast(1.25 as double)",
			"cast(1.25 as double) * cast('03:04:05.123456' as time(6))",
		} {
			rows, err := conn.QueryContext(ctx, "select "+expression+" as result")
			require.NoError(t, err)
			assertPreparedTimeDoubleResult(t, rows, 38006.40432)
		}
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
				defer rows.Close()
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

		t.Run("binary/fractional-time-add", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx,
				"select cast('03:04:05.123456' as time(6)) + ? as result")
			require.NoError(t, err)
			defer stmt.Close()

			rows, err := stmt.QueryContext(ctx, int64(10))
			require.NoError(t, err)
			assertPreparedTimeResult(t, rows, "30415.123456", false, 18, 6)

			rows, err = stmt.QueryContext(ctx, int64(9223372036854775807))
			if err == nil {
				defer rows.Close()
				if rows.Next() {
					var value any
					err = rows.Scan(&value)
				}
				if err == nil {
					err = rows.Err()
				}
			}
			require.Error(t, err)
		})

		for _, tc := range []struct {
			name string
			op   string
			want string
		}{
			{name: "add", op: "+", want: "4"},
			{name: "subtract", op: "-", want: "2"},
			{name: "mod", op: "%", want: "0"},
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

				// The concrete integer sibling supplies NULL's numeric domain.
				// Compare metadata with the ordinary typed expression instead of
				// retaining the unresolved PREPARE template's wider envelope.
				control, err := conn.QueryContext(ctx, "select cast('00:00:01' as time(0)) "+tc.op+
					" (cast(null as signed) "+tc.op+" cast(2 as signed)) as result")
				require.NoError(t, err)
				var precision, scale int64
				func() {
					defer control.Close()
					columns, err := control.ColumnTypes()
					require.NoError(t, err)
					var ok bool
					precision, scale, ok = columns[0].DecimalSize()
					require.True(t, ok)
					require.Equal(t, "DECIMAL", columns[0].DatabaseTypeName())
					require.True(t, control.Next())
					var value sql.NullString
					require.NoError(t, control.Scan(&value))
					require.False(t, value.Valid)
					require.False(t, control.Next())
					require.NoError(t, control.Err())
				}()
				nullRows, err := stmt.QueryContext(ctx, nil, int64(2))
				require.NoError(t, err)
				assertPreparedTimeResult(t, nullRows, "", true, precision, scale)
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
				defer rows.Close()
				if rows.Next() {
					var value any
					err = rows.Scan(&value)
				}
				if err == nil {
					err = rows.Err()
				}
			}
			require.Error(t, err)
		})
	})
}
