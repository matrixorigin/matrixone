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
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

type issue28989TemporalTypeConn struct {
	net.Conn

	mu             sync.Mutex
	rewriteType    defines.MysqlType
	rewritePayload []byte
	rewritten      bool
}

func (c *issue28989TemporalTypeConn) rewriteNext(mysqlType defines.MysqlType, payload []byte) {
	c.mu.Lock()
	c.rewriteType = mysqlType
	c.rewritePayload = append(c.rewritePayload[:0], payload...)
	c.rewritten = false
	c.mu.Unlock()
}

func (c *issue28989TemporalTypeConn) wasRewritten() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rewritten
}

func (c *issue28989TemporalTypeConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	originalLen := len(data)
	if c.rewritePayload != nil {
		if modified, ok := issue28989RewriteStringParamAsTemporal(
			data, c.rewriteType, c.rewritePayload); ok {
			data = modified
			c.rewritePayload = nil
			c.rewritten = true
		}
	}
	written, err := c.Conn.Write(data)
	if err == nil && written == len(data) {
		return originalLen, nil
	}
	return written, err
}

// issue28989RewriteStringParamAsTemporal emits one real binary temporal value,
// rather than changing only the descriptor of text bytes.
func issue28989RewriteStringParamAsTemporal(
	data []byte,
	mysqlType defines.MysqlType,
	payload []byte,
) ([]byte, bool) {
	const (
		packetHeaderSize = 4
		stmtExecute      = 0x17
		executeHeaderLen = 1 + 4 + 1 + 4
	)
	if len(data) < packetHeaderSize+executeHeaderLen+1+1+2 || data[packetHeaderSize] != stmtExecute {
		return data, false
	}
	payloadLen := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	packetEnd := packetHeaderSize + payloadLen
	if packetEnd > len(data) {
		return data, false
	}
	newTypesFlagPos := packetHeaderSize + executeHeaderLen + 1
	typePos := newTypesFlagPos + 1
	valuePos := typePos + 2
	if valuePos >= packetEnd || data[newTypesFlagPos] != 1 ||
		(data[typePos] != byte(defines.MYSQL_TYPE_VAR_STRING) && data[typePos] != byte(defines.MYSQL_TYPE_STRING)) {
		return data, false
	}
	modified := append([]byte(nil), data[:valuePos]...)
	modified[typePos] = byte(mysqlType)
	modified = append(modified, payload...)
	newPayloadLen := len(modified) - packetHeaderSize
	modified[0] = byte(newPayloadLen)
	modified[1] = byte(newPayloadLen >> 8)
	modified[2] = byte(newPayloadLen >> 16)
	return modified, true
}

type issue27907ODBCTypeConn struct {
	net.Conn

	mu                sync.Mutex
	rewriteParamCount int
	rewritten         bool
}

func (c *issue27907ODBCTypeConn) rewriteNextExecute(paramCount int) {
	c.mu.Lock()
	c.rewriteParamCount = paramCount
	c.rewritten = false
	c.mu.Unlock()
}

func (c *issue27907ODBCTypeConn) wasRewritten() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rewritten
}

func (c *issue27907ODBCTypeConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	if c.rewriteParamCount > 0 {
		modified := append([]byte(nil), data...)
		if issue27907RewriteFirstParamAsBlob(modified, c.rewriteParamCount) {
			data = modified
			c.rewriteParamCount = 0
			c.rewritten = true
		}
	}
	c.mu.Unlock()
	return c.Conn.Write(data)
}

// issue27907RewriteFirstParamAsBlob reproduces Connector/ODBC's parameter
// descriptors while retaining the driver's length-encoded string payloads.
func issue27907RewriteFirstParamAsBlob(data []byte, paramCount int) bool {
	const (
		packetHeaderSize = 4
		stmtExecute      = 0x17
		executeHeaderLen = 1 + 4 + 1 + 4
	)
	if paramCount <= 0 {
		return false
	}
	for pos := 0; pos+packetHeaderSize <= len(data); {
		payloadLen := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16
		end := pos + packetHeaderSize + payloadLen
		if end > len(data) {
			return false
		}
		payload := data[pos+packetHeaderSize : end]
		nullBitmapLen := (paramCount + 7) / 8
		newTypesFlagPos := executeHeaderLen + nullBitmapLen
		typePos := newTypesFlagPos + 1
		if len(payload) > typePos+1 && payload[0] == stmtExecute &&
			payload[newTypesFlagPos] == 1 &&
			(payload[typePos] == byte(defines.MYSQL_TYPE_VAR_STRING) ||
				payload[typePos] == byte(defines.MYSQL_TYPE_STRING)) {
			payload[typePos] = byte(defines.MYSQL_TYPE_BLOB)
			return true
		}
		pos = end
	}
	return false
}

func TestIssue25408PreparedPaginationParameters(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_25408_pagination"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db, "create table "+dbName+".page(id int)")
		execSQLRequire(t, ctx, db, "insert into "+dbName+".page values (1),(2),(3)")

		t.Run("integer source domains across protocols and writes", func(t *testing.T) {
			conn, connErr := db.Conn(ctx)
			require.NoError(t, connErr)
			defer conn.Close()
			_, connErr = conn.ExecContext(ctx, "create table "+dbName+".integer_sources(v varchar(20))")
			require.NoError(t, connErr)
			_, connErr = conn.ExecContext(ctx, "create table "+dbName+".integer_peer(pad bigint, v double)")
			require.NoError(t, connErr)
			_, connErr = conn.ExecContext(ctx, "insert into "+dbName+".integer_peer values (42,1.5)")
			require.NoError(t, connErr)
			t.Run("binary cached source domain transitions", func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, `select substring_index("a.b.c.d",".",coalesce(?,0e0))`)
				require.NoError(t, err)
				defer stmt.Close()
				for _, value := range []struct {
					input any
					want  string
				}{
					{nil, ""}, {float64(1.5), "a.b"}, {"1.5", "a"},
					{nil, ""}, {float64(1.5), "a.b"}, {"1.5", "a"},
				} {
					var got string
					require.NoError(t, stmt.QueryRowContext(ctx, value.input).Scan(&got))
					require.Equal(t, value.want, got)
				}
			})
			for _, tc := range []struct {
				name, source, assignment string
				want                     any
				args                     []any
			}{
				{"explicit char peer", "coalesce(?,cast(0e0 as char))", "set @integer_a=1.5e0", "a", []any{float64(1.5)}},
				{"parenthesized char peer", "coalesce(?,(cast(0e0 as char)))", "set @integer_a=1.5e0", "a", []any{float64(1.5)}},
				{"parenthesized char marker", "coalesce((cast(? as char)),0e0)", "set @integer_a=1.5e0", "a", []any{float64(1.5)}},
				{"derived null fallback", "(select coalesce(?,x) from (select 1.5e0 as x) d)", "set @integer_a=NULL", "a.b", []any{nil}},
				{"scan null fallback", "(select coalesce(?,v) from " + dbName + ".integer_peer)", "set @integer_a=NULL", "a.b", []any{nil}},
				{"subquery peer", "coalesce(?,(select 1.5e0 where true))", "set @integer_a=NULL", "a.b", []any{nil}},
				{"union text", "(select 0e0 where false union all select ?)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"group marker", "(select ? group by 1)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"group null", "(select ? group by 1)", "set @integer_a=NULL", nil, []any{nil}},
				{"union null", "(select ? union all select null limit 1)", "set @integer_a=NULL", nil, []any{nil}},
				{"group coalesce null", "(select coalesce(?,null) group by 1)", "set @integer_a=NULL", nil, []any{nil}},
				{"null peer", "coalesce(?,null)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"null peer ifnull", "ifnull(?,null)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"null peer case", "coalesce(case when true then ? else null end,null)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"null peer nullif", "coalesce(nullif(?,0),null)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"null peer text", "coalesce(?,null)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"bit or text", "(select bit_or(?) from " + dbName + ".integer_peer)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"bit and text", "(select bit_and(?) from " + dbName + ".integer_peer)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"bit xor text", "(select bit_xor(?) from " + dbName + ".integer_peer)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"bit or double", "(select bit_or(?) from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"bit or null peer", "(select bit_or(coalesce(?,null)) from " + dbName + ".integer_peer)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"outer coalesce subquery", "coalesce((select ? where true),2.5e0)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"derived union marker", "(select 0e0 where false union all select x from (select ? x) d)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"recursive cte marker", "(with recursive r(n) as (select ? union all select n from r where false) select n from r)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"bit count integer", "bit_count(?)", "set @integer_a=2", "a", []any{int64(2)}},
				{"bit count text", "bit_count(?)", `set @integer_a="2"`, "a.b.c", []any{"2"}},
				{"max source", "(select max(coalesce(?,0e0)) from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"min source", "(select min(coalesce(?,0e0)) from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"window source", "(select first_value(coalesce(?,0e0)) over () from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"text", "coalesce(?,0e0)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"text ifnull", "ifnull(?,0e0)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"double", "coalesce(?,0e0)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"explicit float peer", "coalesce(?,cast(0 as double))", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"mixed", "coalesce(?,?)", `set @integer_a="1.5", @integer_b=1.5e0`, "a", []any{"1.5", float64(1.5)}},
				{"null", "coalesce(?,?)", "set @integer_a=NULL, @integer_b=1.5e0", "a.b", []any{nil, float64(1.5)}},
				{"project", "(select coalesce(?,0e0) where true)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"project text", "(select coalesce(?,0e0) where true)", `set @integer_a="1.5"`, "a", []any{"1.5"}},
				{"project null", "(select coalesce(?,?) where true)", "set @integer_a=NULL, @integer_b=1.5e0", "a.b", []any{nil, float64(1.5)}},
				{"union distinct right", "(select 0e0 where false union select ?)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"column peer", "(select coalesce(?,x) from (select 0e0 as x) d)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"scan column peer", "(select coalesce(?,v) from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"unfolded cast peer", "(select coalesce(?,cast(v as double)) from " + dbName + ".integer_peer)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
				{"union right", "(select 0e0 where false union all select ?)", "set @integer_a=1.5e0", "a.b", []any{float64(1.5)}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr := `substring_index("a.b.c.d",".",` + tc.source + `)`
					for _, binary := range []bool{false, true} {
						for _, write := range []bool{false, true} {
							t.Run(fmt.Sprintf("binary=%v/write=%v", binary, write), func(t *testing.T) {
								query := "select " + expr
								if write {
									_, err := conn.ExecContext(ctx, "delete from "+dbName+".integer_sources")
									require.NoError(t, err)
									query = "insert into " + dbName + ".integer_sources values (" + expr + ")"
									if strings.Contains(tc.source, "select") {
										// Scalar subqueries use INSERT SELECT; the VALUES
										// planner does not support this subquery shape.
										query = "insert into " + dbName + ".integer_sources select " + expr
									}
								}
								var got sql.NullString
								if binary {
									stmt, err := conn.PrepareContext(ctx, query)
									require.NoError(t, err)
									defer stmt.Close()
									if write {
										_, err = stmt.ExecContext(ctx, tc.args...)
									} else {
										err = stmt.QueryRowContext(ctx, tc.args...).Scan(&got)
									}
									require.NoError(t, err)
								} else {
									_, err := conn.ExecContext(ctx, tc.assignment)
									require.NoError(t, err)
									_, err = conn.ExecContext(ctx, "prepare integer_domain from '"+query+"'")
									require.NoError(t, err)
									defer func() { _, err := conn.ExecContext(ctx, "deallocate prepare integer_domain"); require.NoError(t, err) }()
									execute := "execute integer_domain using @integer_a"
									if len(tc.args) == 2 {
										execute += ",@integer_b"
									}
									if write {
										_, err = conn.ExecContext(ctx, execute)
									} else {
										err = conn.QueryRowContext(ctx, execute).Scan(&got)
									}
									require.NoError(t, err)
								}
								if write {
									require.NoError(t, conn.QueryRowContext(ctx, "select v from "+dbName+".integer_sources").Scan(&got))
								}
								if tc.want == nil {
									require.False(t, got.Valid)
								} else {
									require.True(t, got.Valid)
									require.Equal(t, tc.want, got.String)
								}
							})
						}
					}
				})
			}
		})

		type scalarObservation struct {
			value        string
			databaseType string
		}
		observeScalar := func(t *testing.T, rows *sql.Rows) scalarObservation {
			t.Helper()

			columnTypes, typeErr := rows.ColumnTypes()
			require.NoError(t, typeErr)
			require.Len(t, columnTypes, 1)
			require.True(t, rows.Next())
			var value string
			require.NoError(t, rows.Scan(&value))
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			return scalarObservation{
				value:        value,
				databaseType: columnTypes[0].DatabaseTypeName(),
			}
		}

		t.Run("SQL PREPARE runtime numeric type reuse", func(t *testing.T) {
			execSQLRequire(t, ctx, db,
				"prepare issue25408_runtime from 'select ? + 1 as plus_one'")
			defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_runtime")

			for _, execution := range []struct {
				assignment       string
				wantValue        string
				wantDatabaseType string
			}{
				{
					assignment: "set @issue25408_runtime = '2'", wantValue: "3", wantDatabaseType: "DOUBLE",
				},
				{
					assignment: "set @issue25408_runtime = 2.5", wantValue: "3.5", wantDatabaseType: "DECIMAL",
				},
				{
					assignment: "set @issue25408_runtime = 3.5", wantValue: "4.5", wantDatabaseType: "DECIMAL",
				},
				{
					assignment: "set @issue25408_runtime = -2", wantValue: "-1", wantDatabaseType: "BIGINT",
				},
			} {
				execSQLRequire(t, ctx, db, execution.assignment)
				preparedRows, preparedErr := db.QueryContext(
					ctx, "execute issue25408_runtime using @issue25408_runtime")
				require.NoError(t, preparedErr)
				defer preparedRows.Close()
				prepared := observeScalar(t, preparedRows)
				require.NoError(t, preparedRows.Err())
				require.Equal(t, execution.wantValue, prepared.value)
				require.Equal(t, execution.wantDatabaseType, prepared.databaseType,
					"prepared execution must use the current variable's numeric category")
			}
		})

		t.Run("SQL PREPARE division binds source before provisional cast", func(t *testing.T) {
			execSQLRequire(t, ctx, db,
				"prepare issue25408_divide from 'select ? / 2 as quotient'")
			defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_divide")

			for _, execution := range []struct {
				assignment       string
				wantValue        string
				wantDatabaseType string
			}{
				{assignment: "set @issue25408_divide = 2.5", wantValue: "1.2500000", wantDatabaseType: "DECIMAL"},
				{assignment: "set @issue25408_divide = 9007199254740993.5", wantValue: "4503599627370496.7500000",
					wantDatabaseType: "DECIMAL"},
				{assignment: "set @issue25408_divide = 3.5", wantValue: "1.7500000", wantDatabaseType: "DECIMAL"},
			} {
				execSQLRequire(t, ctx, db, execution.assignment)
				preparedRows, preparedErr := db.QueryContext(
					ctx, "execute issue25408_divide using @issue25408_divide")
				require.NoError(t, preparedErr)
				defer preparedRows.Close()
				prepared := observeScalar(t, preparedRows)
				require.NoError(t, preparedRows.Err())
				require.Equal(t, execution.wantValue, prepared.value,
					"prepared division must use the current value before evaluating its provisional cast")
				require.Equal(t, execution.wantDatabaseType, prepared.databaseType)
			}
		})

		t.Run("SQL PREPARE nested numeric consumers retain decimal domain", func(t *testing.T) {
			for _, test := range []struct {
				name             string
				expression       string
				directExpression string
				wantValue        string
				wantDatabaseType string
			}{
				{name: "exact integer peer", expression: "(? / 2) + 1", directExpression: "(@issue25408_nested / 2) + 1", wantValue: "4503599627370497.7500000", wantDatabaseType: "DECIMAL"},
				{name: "scientific integral float peer", expression: "(? / 2) + 1e0", directExpression: "(@issue25408_nested / 2) + 1e0", wantValue: "4.503599627370498e+15", wantDatabaseType: "DOUBLE"},
				{name: "scientific fractional float peer", expression: "(? / 2) + 1e-1", directExpression: "(@issue25408_nested / 2) + 1e-1", wantValue: "4.503599627370497e+15", wantDatabaseType: "DOUBLE"},
				{name: "explicit double peer", expression: "(? / 2) + cast(1 as double)", directExpression: "(@issue25408_nested / 2) + cast(1 as double)", wantValue: "4.503599627370498e+15", wantDatabaseType: "DOUBLE"},
				{name: "abs", expression: "abs(? / 2)", directExpression: "abs(@issue25408_nested / 2)", wantValue: "4503599627370496.7500000", wantDatabaseType: "DECIMAL"},
				{name: "multiplication", expression: "(? / 2) * 3", directExpression: "(@issue25408_nested / 2) * 3", wantValue: "13510798882111490.2500000", wantDatabaseType: "DECIMAL"},
			} {
				t.Run(test.name, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "set @issue25408_nested = 9007199254740993.5")
					directRows, directErr := db.QueryContext(ctx, "select "+test.directExpression+" as result")
					require.NoError(t, directErr)
					defer directRows.Close()
					direct := observeScalar(t, directRows)
					require.NoError(t, directRows.Err())
					require.Equal(t, test.wantValue, direct.value)
					require.Equal(t, test.wantDatabaseType, direct.databaseType)

					execSQLRequire(t, ctx, db, "prepare issue25408_nested from 'select "+test.expression+" as result'")
					defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_nested")
					rows, queryErr := db.QueryContext(ctx, "execute issue25408_nested using @issue25408_nested")
					require.NoError(t, queryErr)
					defer rows.Close()
					observed := observeScalar(t, rows)
					require.NoError(t, rows.Err())
					require.Equal(t, direct, observed)
				})
			}
		})

		t.Run("SQL PREPARE ABS preserves numeric peer provenance", func(t *testing.T) {
			for _, expression := range []struct {
				name          string
				prepared      string
				direct        string
				floatBoundary bool
			}{
				{name: "exact integer peer", prepared: "abs(? + 1)", direct: "abs(@issue25408_abs + 1)"},
				{name: "scientific integral float peer", prepared: "abs(? + 1e0)", direct: "abs(@issue25408_abs + 1e0)", floatBoundary: true},
				{name: "scientific fractional float peer", prepared: "abs(? + 1e-1)", direct: "abs(@issue25408_abs + 1e-1)", floatBoundary: true},
				{name: "explicit double peer", prepared: "abs(? + cast(1 as double))", direct: "abs(@issue25408_abs + cast(1 as double))", floatBoundary: true},
			} {
				t.Run(expression.name, func(t *testing.T) {
					execSQLRequire(t, ctx, db, "prepare issue25408_abs from 'select "+expression.prepared+" as result'")
					defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_abs")
					assignments := []string{"9007199254740993.5"}
					if expression.floatBoundary {
						assignments = append(assignments, "2", "'2'")
					}
					for _, assignment := range assignments {
						execSQLRequire(t, ctx, db, "set @issue25408_abs = "+assignment)
						directRows, directErr := db.QueryContext(ctx, "select "+expression.direct+" as result")
						require.NoError(t, directErr)
						defer directRows.Close()
						direct := observeScalar(t, directRows)
						require.NoError(t, directRows.Err())

						preparedRows, preparedErr := db.QueryContext(ctx, "execute issue25408_abs using @issue25408_abs")
						require.NoError(t, preparedErr)
						defer preparedRows.Close()
						prepared := observeScalar(t, preparedRows)
						require.NoError(t, preparedRows.Err())
						require.Equal(t, direct, prepared, "runtime assignment %s", assignment)
					}
				})
			}
		})

		t.Run("COM_STMT runtime numeric type reuse", func(t *testing.T) {
			stmt, prepareErr := db.PrepareContext(ctx, "select ? + 1 as plus_one")
			require.NoError(t, prepareErr)
			defer stmt.Close()

			for _, execution := range []struct {
				value            any
				wantValue        string
				wantDatabaseType string
			}{
				{value: int64(2), wantValue: "3", wantDatabaseType: "BIGINT"},
				{value: float64(2.5), wantValue: "3.5", wantDatabaseType: "DOUBLE"},
				{value: int64(-2), wantValue: "-1", wantDatabaseType: "BIGINT"},
			} {
				preparedRows, preparedErr := stmt.QueryContext(ctx, execution.value)
				require.NoError(t, preparedErr)
				defer preparedRows.Close()
				prepared := observeScalar(t, preparedRows)
				require.NoError(t, preparedRows.Err())
				require.Equal(t, execution.wantValue, prepared.value)
				require.Equal(t, execution.wantDatabaseType, prepared.databaseType,
					"binary prepared execution must use the current parameter's numeric category")
			}
		})

		assertRows := func(t *testing.T, query string, want ...int) {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, query)
			require.NoError(t, queryErr)
			defer rows.Close()
			var actual []int
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				actual = append(actual, value)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, actual)
		}
		assertMySQLError := func(t *testing.T, err error, number uint16) {
			t.Helper()
			require.Error(t, err)
			mysqlErr, ok := err.(*mysqlDriver.MySQLError)
			require.True(t, ok)
			require.Equal(t, number, mysqlErr.Number)
		}

		t.Run("SQL PREPARE reuse", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "prepare issue25408_page from 'select id from "+dbName+".page order by id limit ? offset ?'")
			defer execSQLMaybe(t, context.Background(), db, "deallocate prepare issue25408_page")

			execSQLRequire(t, ctx, db, "set @lim=2,@off=1")
			assertRows(t, "execute issue25408_page using @lim,@off", 2, 3)
			execSQLRequire(t, ctx, db, "set @lim=null,@off=1")
			assertRows(t, "execute issue25408_page using @lim,@off")
			execSQLRequire(t, ctx, db, "set @lim=2,@off=null")
			assertRows(t, "execute issue25408_page using @lim,@off", 1, 2)
			execSQLRequire(t, ctx, db, "set @lim=true,@off=0")
			assertRows(t, "execute issue25408_page using @lim,@off", 1)

			execSQLRequire(t, ctx, db, "set @lim='1',@off=0")
			_, err = db.ExecContext(ctx, "execute issue25408_page using @lim,@off")
			assertMySQLError(t, err, 1210)
			execSQLRequire(t, ctx, db, "set @lim=-1,@off=0")
			_, err = db.ExecContext(ctx, "execute issue25408_page using @lim,@off")
			assertMySQLError(t, err, 1690)
		})

		for index, paginationSQL := range []string{
			"select id from " + dbName + ".page order by id limit ? offset ?",
			"select id from " + dbName + ".page order by id limit ?, ?",
		} {
			t.Run(fmt.Sprintf("SQL PREPARE error priority %d", index), func(t *testing.T) {
				name := fmt.Sprintf("issue25408_priority_%d", index)
				execSQLRequire(t, ctx, db, "prepare "+name+" from '"+paginationSQL+"'")
				defer execSQLMaybe(t, context.Background(), db, "deallocate prepare "+name)
				execSQLRequire(t, ctx, db, "set @first='1',@second=-1")
				_, executeErr := db.ExecContext(ctx, "execute "+name+" using @first,@second")
				assertMySQLError(t, executeErr, 1210)
			})
		}

		t.Run("COM_STMT and CTAS", func(t *testing.T) {
			stmt, prepareErr := db.PrepareContext(ctx,
				"select id from "+dbName+".page order by id limit ? offset ?")
			require.NoError(t, prepareErr)
			defer stmt.Close()

			func() {
				rows, queryErr := stmt.QueryContext(ctx, int64(1), int64(1))
				require.NoError(t, queryErr)
				defer rows.Close()
				require.True(t, rows.Next())
				var id int
				require.NoError(t, rows.Scan(&id))
				require.Equal(t, 2, id)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
			// Connector/ODBC sends integer bindings as MYSQL_TYPE_STRING.
			// A numeric Go string uses the same COM_STMT_EXECUTE wire type.
			func() {
				rows, queryErr := stmt.QueryContext(ctx, "1", "1")
				require.NoError(t, queryErr)
				defer rows.Close()
				require.True(t, rows.Next())
				var id int
				require.NoError(t, rows.Scan(&id))
				require.Equal(t, 2, id)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
			_, err = stmt.ExecContext(ctx, "1.0", int64(0))
			assertMySQLError(t, err, 1210)
			_, err = stmt.ExecContext(ctx, int64(-1), int64(0))
			assertMySQLError(t, err, 1690)

			ctas, prepareErr := db.PrepareContext(ctx,
				"create table "+dbName+".bad_page as select 1 limit ?")
			require.NoError(t, prepareErr)
			defer ctas.Close()
			_, err = ctas.ExecContext(ctx, "1.0")
			assertMySQLError(t, err, 1210)
		})

		t.Run("COM_STMT temporal descriptor remains temporal", func(t *testing.T) {
			var connMu sync.Mutex
			var wireConn *issue28989TemporalTypeConn
			mysqlDriver.RegisterDialContext("issue28989temporal", func(ctx context.Context, addr string) (net.Conn, error) {
				conn, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if dialErr != nil {
					return nil, dialErr
				}
				wrapped := &issue28989TemporalTypeConn{Conn: conn}
				connMu.Lock()
				wireConn = wrapped
				connMu.Unlock()
				return wrapped, nil
			})
			defer mysqlDriver.DeregisterDialContext("issue28989temporal")
			cn, cnErr := c.GetCNService(0)
			require.NoError(t, cnErr)
			temporalDB, openErr := sql.Open("mysql", fmt.Sprintf(
				"dump:111@issue28989temporal(127.0.0.1:%d)/?interpolateParams=false",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, openErr)
			temporalDB.SetMaxOpenConns(1)
			temporalDB.SetMaxIdleConns(1)
			defer temporalDB.Close()

			stmt, prepareErr := temporalDB.PrepareContext(ctx,
				`select substring_index("a.b.c",".",?)`)
			require.NoError(t, prepareErr)
			defer stmt.Close()
			var textResult string
			require.NoError(t, stmt.QueryRowContext(ctx, "2024-01-02").Scan(&textResult))
			require.Equal(t, "a.b.c", textResult)

			connMu.Lock()
			capturedConn := wireConn
			connMu.Unlock()
			require.NotNil(t, capturedConn)
			// Binary DATE uses a length byte followed by little-endian year,
			// month and day.
			capturedConn.rewriteNext(defines.MYSQL_TYPE_DATE, []byte{4, 0xe8, 0x07, 1, 2})
			var dateResult string
			dateErr := stmt.QueryRowContext(ctx, "2024-01-02").Scan(&dateResult)
			require.Error(t, dateErr)
			require.NotErrorIs(t, dateErr, net.ErrClosed)
			require.True(t, capturedConn.wasRewritten(), "test did not emit MYSQL_TYPE_DATE")
			// Prove the packet was valid and the cached statement remains reusable;
			// a malformed rewrite could otherwise satisfy only the error assertion.
			require.NoError(t, stmt.QueryRowContext(ctx, "2024-01-02").Scan(&textResult))
			require.Equal(t, "a.b.c", textResult)
		})

		t.Run("COM_STMT temporal values preserve wire semantics", func(t *testing.T) {
			var connMu sync.Mutex
			var wireConn *issue28989TemporalTypeConn
			mysqlDriver.RegisterDialContext("issue28989temporalvalues", func(ctx context.Context, addr string) (net.Conn, error) {
				conn, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if dialErr != nil {
					return nil, dialErr
				}
				wrapped := &issue28989TemporalTypeConn{Conn: conn}
				connMu.Lock()
				wireConn = wrapped
				connMu.Unlock()
				return wrapped, nil
			})
			defer mysqlDriver.DeregisterDialContext("issue28989temporalvalues")
			cn, cnErr := c.GetCNService(0)
			require.NoError(t, cnErr)
			temporalDB, openErr := sql.Open("mysql", fmt.Sprintf(
				"dump:111@issue28989temporalvalues(127.0.0.1:%d)/issue_25408_pagination?interpolateParams=false",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, openErr)
			temporalDB.SetMaxOpenConns(1)
			temporalDB.SetMaxIdleConns(1)
			defer temporalDB.Close()

			stmt, prepareErr := temporalDB.PrepareContext(ctx, "select cast(? as char)")
			require.NoError(t, prepareErr)
			defer stmt.Close()
			for _, test := range []struct {
				name      string
				mysqlType defines.MysqlType
				payload   []byte
				want      string
			}{
				{name: "zero time", mysqlType: defines.MYSQL_TYPE_TIME, payload: []byte{0}, want: "00:00:00"},
				{name: "day time", mysqlType: defines.MYSQL_TYPE_TIME, payload: []byte{8, 0, 1, 0, 0, 0, 2, 3, 4}, want: "26:03:04"},
				{name: "negative fractional time", mysqlType: defines.MYSQL_TYPE_TIME, payload: []byte{12, 1, 0, 0, 0, 0, 11, 22, 33, 0x1f, 0xa1, 0x07, 0}, want: "-11:22:33.499999"},
				{name: "half second time", mysqlType: defines.MYSQL_TYPE_TIME, payload: []byte{12, 0, 0, 0, 0, 0, 11, 22, 33, 0x20, 0xa1, 0x07, 0}, want: "11:22:33.500000"},
				{name: "datetime fraction", mysqlType: defines.MYSQL_TYPE_DATETIME, payload: []byte{11, 0xe8, 0x07, 1, 2, 23, 59, 59, 0x1f, 0xa1, 0x07, 0}, want: "2024-01-02 23:59:59.499999"},
				{name: "timestamp half second", mysqlType: defines.MYSQL_TYPE_TIMESTAMP, payload: []byte{11, 0xe8, 0x07, 1, 2, 23, 59, 59, 0x20, 0xa1, 0x07, 0}, want: "2024-01-02 23:59:59.500000"},
			} {
				t.Run(test.name, func(t *testing.T) {
					connMu.Lock()
					capturedConn := wireConn
					connMu.Unlock()
					require.NotNil(t, capturedConn)
					capturedConn.rewriteNext(test.mysqlType, test.payload)
					var got string
					require.NoError(t, stmt.QueryRowContext(ctx, "placeholder").Scan(&got))
					require.Equal(t, test.want, got)
					require.True(t, capturedConn.wasRewritten())
				})
			}

			_, err := temporalDB.ExecContext(ctx, "create table issue28989_time(v time(6))")
			require.NoError(t, err)
			defer temporalDB.ExecContext(ctx, "drop table issue28989_time")
			insertStmt, prepareErr := temporalDB.PrepareContext(ctx, "insert into issue28989_time values (?)")
			require.NoError(t, prepareErr)
			defer insertStmt.Close()
			for _, payload := range [][]byte{
				{12, 0, 0, 0, 0, 0, 11, 22, 33, 0x1f, 0xa1, 0x07, 0},
				{12, 0, 0, 0, 0, 0, 11, 22, 33, 0x20, 0xa1, 0x07, 0},
			} {
				wireConn.rewriteNext(defines.MYSQL_TYPE_TIME, payload)
				_, err = insertStmt.ExecContext(ctx, "placeholder")
				require.NoError(t, err)
			}
			rows, queryErr := temporalDB.QueryContext(ctx, "select cast(v as char) from issue28989_time order by v")
			require.NoError(t, queryErr)
			defer rows.Close()
			var stored []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				stored = append(stored, value)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"11:22:33.499999", "11:22:33.500000"}, stored)

			for _, test := range []struct {
				name      string
				mysqlType defines.MysqlType
				column    string
				payload   []byte
				want      string
			}{
				{
					name: "datetime", mysqlType: defines.MYSQL_TYPE_DATETIME, column: "datetime(6)",
					payload: []byte{11, 0xe8, 0x07, 1, 2, 23, 59, 59, 0x20, 0xa1, 0x07, 0},
					want:    "2024-01-02 23:59:59.500000",
				},
				{
					name: "timestamp", mysqlType: defines.MYSQL_TYPE_TIMESTAMP, column: "timestamp(6)",
					payload: []byte{11, 0xe8, 0x07, 1, 2, 23, 59, 59, 0x1f, 0xa1, 0x07, 0},
					want:    "2024-01-02 23:59:59.499999",
				},
			} {
				t.Run(test.name+" insert", func(t *testing.T) {
					table := "issue28989_" + test.name
					_, err = temporalDB.ExecContext(ctx, fmt.Sprintf("create table %s(v %s)", table, test.column))
					require.NoError(t, err)
					defer temporalDB.ExecContext(ctx, "drop table "+table)
					prepared, prepareErr := temporalDB.PrepareContext(ctx, "insert into "+table+" values (?)")
					require.NoError(t, prepareErr)
					defer prepared.Close()
					wireConn.rewriteNext(test.mysqlType, test.payload)
					_, err = prepared.ExecContext(ctx, "placeholder")
					require.NoError(t, err)
					var got string
					require.NoError(t, temporalDB.QueryRowContext(ctx,
						"select cast(v as char) from "+table).Scan(&got))
					require.Equal(t, test.want, got)
				})
			}
		})

		t.Run("Connector ODBC HAVING and pagination", func(t *testing.T) {
			var connMu sync.Mutex
			var wireConn *issue27907ODBCTypeConn
			mysqlDriver.RegisterDialContext("issue27907odbc", func(ctx context.Context, addr string) (net.Conn, error) {
				conn, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if dialErr != nil {
					return nil, dialErr
				}
				wrapped := &issue27907ODBCTypeConn{Conn: conn}
				connMu.Lock()
				wireConn = wrapped
				connMu.Unlock()
				return wrapped, nil
			})
			defer mysqlDriver.DeregisterDialContext("issue27907odbc")
			cn, cnErr := c.GetCNService(0)
			require.NoError(t, cnErr)
			odbcDB, openErr := sql.Open("mysql", fmt.Sprintf(
				"dump:111@issue27907odbc(127.0.0.1:%d)/?interpolateParams=false",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, openErr)
			odbcDB.SetMaxOpenConns(1)
			odbcDB.SetMaxIdleConns(1)
			defer odbcDB.Close()

			for _, test := range []struct {
				name string
				sql  string
				args []any
			}{
				{
					name: "limit",
					sql: "select id, sum(id) from " + dbName + ".page group by id " +
						"having sum(id) > ? order by id limit ?",
					args: []any{"1", "1"},
				},
				{
					name: "limit offset",
					sql: "select id, sum(id) from " + dbName + ".page group by id " +
						"having sum(id) > ? order by id limit ? offset ?",
					args: []any{"0", "1", "1"},
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					stmt, prepareErr := odbcDB.PrepareContext(ctx, test.sql)
					require.NoError(t, prepareErr)
					defer stmt.Close()

					connMu.Lock()
					capturedConn := wireConn
					connMu.Unlock()
					require.NotNil(t, capturedConn)
					capturedConn.rewriteNextExecute(len(test.args))

					var id, total int
					require.NoError(t, stmt.QueryRowContext(ctx, test.args...).Scan(&id, &total))
					require.Equal(t, 2, id)
					require.Equal(t, 2, total)
					require.True(t, capturedConn.wasRewritten(),
						"test did not send MYSQL_TYPE_BLOB for the HAVING parameter")
				})
			}
		})

		for index, paginationSQL := range []string{
			"select id from " + dbName + ".page order by id limit ? offset ?",
			"select id from " + dbName + ".page order by id limit ?, ?",
		} {
			t.Run(fmt.Sprintf("COM_STMT error priority %d", index), func(t *testing.T) {
				stmt, prepareErr := db.PrepareContext(ctx, paginationSQL)
				require.NoError(t, prepareErr)
				defer stmt.Close()
				_, executeErr := stmt.ExecContext(ctx, "1.0", int64(-1))
				assertMySQLError(t, executeErr, 1210)
			})
		}
	})
}
