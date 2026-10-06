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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// One existing one-CN fixture exercises the frontend, both prepared protocols,
// persistence and metadata. Each closure uses only its distinguishing rows.
func TestPreparedNumericTemporalContracts(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "prepared_numeric_temporal_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(t *testing.T, q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q, args...)
			require.NoError(t, err, q)
		}
		exec(t, "drop database if exists "+schema)
		exec(t, "create database "+schema)
		exec(t, "use "+schema)
		scalar := func(t *testing.T, q string, args ...any) string {
			t.Helper()
			var s string
			require.NoError(t, conn.QueryRowContext(ctx, q, args...).Scan(&s), q)
			return s
		}
		t.Run("NULL assignment history does not change SQL EXECUTE conversion", func(t *testing.T) {
			exec(t, "create table null_history_result(v varchar(8))")
			for _, history := range []struct {
				name, variable, assignment string
			}{
				{"fresh", "@null_history_fresh", ""},
				{"text", "@null_history_text", "'abc'"},
				{"numeric", "@null_history_numeric", "1"},
				{"json", "@null_history_json", "cast(null as json)"},
				{"date", "@null_history_date", "cast(null as date)"},
			} {
				t.Run(history.name, func(t *testing.T) {
					if history.assignment != "" {
						exec(t, "set "+history.variable+"="+history.assignment)
					}
					exec(t, "set "+history.variable+"=NULL")
					var matched sql.NullBool
					err := conn.QueryRowContext(ctx, "select regexp_like(cast("+history.variable+" as binary),'a')").Scan(&matched)
					if history.name == "numeric" {
						require.ErrorContains(t, err, "Character set 'binary'")
					} else {
						require.NoError(t, err)
						require.False(t, matched.Valid)
					}
					exec(t, "prepare null_history_select from 'select substring_index(\"a.b\",\".\",coalesce(?,1.5))'")
					defer exec(t, "deallocate prepare null_history_select")
					require.Equal(t, "a.b", scalar(t, "execute null_history_select using "+history.variable))
					exec(t, "delete from null_history_result")
					exec(t, "prepare null_history_insert from 'insert into null_history_result select substring_index(\"a.b\",\".\",coalesce(?,1.5))'")
					defer exec(t, "deallocate prepare null_history_insert")
					exec(t, "execute null_history_insert using "+history.variable)
					require.Equal(t, "a.b", scalar(t, "select v from null_history_result"))
				})
			}
		})
		t.Run("guarded integer ranges", func(t *testing.T) {
			exec(t, "create table range_keys(id int primary key,k int,v int,key k_1(k),key kv_1(k,v))")
			exec(t, "insert into range_keys values(1,-2147483648,0),(2,-1,0),(3,null,0),(4,0,0),(5,1,0),(6,1,0),(7,8,0),(8,2147483647,0)")
			for _, path := range []struct{ name, projection, table, column string }{
				{"covering", "id", "range_keys force index(k_1)", "k"},
				{"backfill", "id+v", "range_keys force index(k_1)", "k"},
				{"composite", "id", "range_keys force index(kv_1)", "k"},
				{"explicit cast", "id", "range_keys", "cast(k as signed)"},
				{"base scan", "id", "range_keys ignore index(k_1,kv_1)", "k"},
			} {
				t.Run(path.name, func(t *testing.T) {
					q := fmt.Sprintf("select %s from %s where %s between ? and ? or %s between ? and ? order by id", path.projection, path.table, path.column, path.column)
					explained, err := conn.QueryContext(ctx, "explain "+q, int64(0), int64(1), int64(8), int64(8))
					require.NoError(t, err)
					defer explained.Close()
					var planText strings.Builder
					for explained.Next() {
						var line string
						require.NoError(t, explained.Scan(&line))
						planText.WriteString(line)
						planText.WriteByte('\n')
					}
					require.NoError(t, explained.Err())
					if strings.Contains(path.table, "force index") {
						require.Contains(t, planText.String(), "Index Table Scan")
						if path.name == "backfill" {
							require.Contains(t, planText.String(), "Join Type: INDEX")
							require.Contains(t, planText.String(), "Table Scan on "+schema+".range_keys")
						}
					} else {
						require.NotContains(t, planText.String(), "Index Table Scan")
					}
					p, err := conn.PrepareContext(ctx, q)
					require.NoError(t, err)
					defer p.Close()
					for _, tc := range []struct {
						name   string
						bounds []any
						want   []int64
					}{
						{"closed endpoints", []any{int64(-2147483648), int64(-2147483648), int64(2147483647), int64(2147483647)}, []int64{1, 8}},
						{"overlap and duplicate keys", []any{int64(0), int64(1), int64(1), int64(8)}, []int64{4, 5, 6, 7}},
						{"reversed", []any{int64(8), int64(1), int64(9), int64(10)}, []int64{}},
						{"lower outside domain", []any{int64(-2147483649), int64(-1), int64(2147483648), int64(2147483650)}, []int64{1, 2}},
						{"upper outside domain", []any{int64(0), int64(2147483648), int64(1), int64(1)}, []int64{4, 5, 6, 7, 8}},
						{"safe recovery", []any{int64(1), int64(1), int64(8), int64(8)}, []int64{5, 6, 7}},
						{"NULL bound", []any{nil, int64(1), int64(8), int64(8)}, []int64{7}},
						{"unsigned", []any{uint64(1), ^uint64(0), ^uint64(0), ^uint64(0)}, []int64{5, 6, 7, 8}},
						{"fractional", []any{float64(0.5), float64(1.5), float64(8), float64(8)}, []int64{5, 6, 7}},
						{"text fractions", []any{"0.5", "1.5", int64(8), int64(8)}, []int64{5, 6, 7}},
						{"recovery after category changes", []any{int64(0), int64(0), int64(8), int64(8)}, []int64{4, 7}},
					} {
						got, err := readPreparedContractIDs(p.QueryContext(ctx, tc.bounds...))
						if tc.name == "unsigned" && path.name != "explicit cast" {
							// The existing unsigned comparison domain rejects negative
							// column values. Signed admission must not hide that error.
							require.ErrorContains(t, err, "data out of range")
							continue
						}
						require.NoError(t, err, tc.name)
						require.Equal(t, tc.want, got, tc.name)
					}
				})
			}
			for _, tc := range []struct {
				name, predicate string
				want            []int64
				warn            bool
			}{
				{"active warning", "k between ? and ?", []int64{5, 6}, true},
				{"inactive warning", "case when false then k between ? and ? else false end", []int64{}, false},
				{"empty warning", "id<0 and k between ? and ?", []int64{}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					p, err := conn.PrepareContext(ctx, "select id from range_keys where "+tc.predicate+" order by id")
					require.NoError(t, err)
					defer p.Close()
					got, err := readPreparedContractIDs(p.QueryContext(ctx, "1tail", int64(2)))
					require.NoError(t, err, tc.name)
					require.Equal(t, tc.want, got, tc.name)
					count, err := strconv.Atoi(scalar(t, "select @@warning_count"))
					require.NoError(t, err)
					wantWarnings := 0
					if tc.warn {
						wantWarnings = 1
					}
					require.Equal(t, wantWarnings, count, tc.name)
				})
			}
		})
		t.Run("persisted signed DML read budget", func(t *testing.T) {
			// Same fixture, connection and tiny layout for public results and
			// fresh-execution work. EXPLAIN does not prove cached reader work.
			for pass := range 2 {
				t.Run(fmt.Sprintf("reuse-%d", pass), func(t *testing.T) {
					const table = "narrow_keys"
					exec(t, "create table "+table+"(id int primary key, v int)")
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, err := conn.ExecContext(cleanup, "drop table "+table)
						require.NoError(t, err)
						var count int
						require.NoError(t, conn.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, table).Scan(&count))
						require.Zero(t, count, "teardown must not be hidden by the next pass")
					}()
					inspect := func(qctx context.Context, command string) string {
						t.Helper()
						var response string
						require.NoError(t, conn.QueryRowContext(qctx, "select mo_ctl('dn','inspect',?)", command).Scan(&response))
						require.NotContains(t, response, "run err:")
						return response
					}
					target := schema + "." + table
					require.Contains(t, inspect(ctx, "merge show -t "+target), "auto merge: true")
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						require.Contains(t, inspect(cleanup, "merge switch on -t "+target), "merge enabled for table")
						require.Contains(t, inspect(cleanup, "merge show -t "+target), "auto merge: true")
					}()
					require.Contains(t, inspect(ctx, "merge switch off -t "+target), "merge disabled for table")
					paused := inspect(ctx, "merge show -t "+target)
					require.Contains(t, paused, "auto merge: false")
					require.Contains(t, paused, "merge tasks in queue: 0")
					for _, key := range []int{1, 1001, 2001} {
						exec(t, fmt.Sprintf("insert into %s values (%d,0)", table, key))
						exec(t, "select mo_ctl('dn','flush','"+target+"')")
						service := cn.RawService().(cnservice.Service)
						frontier, _ := service.GetClock().Now()
						_, err := service.GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
						require.NoError(t, err)
					}
					var rows, blocks, objects int64
					require.NoError(t, conn.QueryRowContext(ctx, "select table_cnt,block_number,accurate_object_number from table_stats('"+target+"','refresh','full') g").Scan(&rows, &blocks, &objects))
					require.Equal(t, []int64{3, 3, 3}, []int64{rows, blocks, objects})
					state := func(tx *sql.Tx, expected string) {
						t.Helper()
						var actual string
						require.NoError(t, tx.QueryRowContext(ctx, "select group_concat(concat(id,':',v) order by id) from "+table).Scan(&actual))
						require.Equal(t, expected, actual)
					}
					for _, statement := range []string{"update narrow_keys set v=? where id=?", "delete from narrow_keys where id=?"} {
						func() {
							stmt, err := conn.PrepareContext(ctx, statement)
							require.NoError(t, err)
							defer stmt.Close()
							update := strings.HasPrefix(statement, "update")
							for _, key := range []any{int64(1), int64(1001), int64(2147483648), int64(2001), nil, int64(1)} {
								func() {
									tx, err := conn.BeginTx(ctx, nil)
									require.NoError(t, err)
									defer tx.Rollback()
									args := []any{key}
									if update {
										args = []any{int64(7), key}
									}
									bound := tx.StmtContext(ctx, stmt)
									defer bound.Close()
									result, err := bound.ExecContext(ctx, args...)
									require.NoError(t, err)
									affected, err := result.RowsAffected()
									require.NoError(t, err)
									want := []string{"1:0", "1001:0", "2001:0"}
									var matched int64
									for i, id := range []int64{1, 1001, 2001} {
										if key == id {
											matched = 1
											if update {
												want[i] = fmt.Sprintf("%d:7", id)
											} else {
												want = append(want[:i], want[i+1:]...)
											}
											break
										}
									}
									require.Equal(t, matched, affected)
									state(tx, strings.Join(want, ","))
								}()
							}
							if update {
								func() {
									tx, err := conn.BeginTx(ctx, nil)
									require.NoError(t, err)
									defer tx.Rollback()
									bound := tx.StmtContext(ctx, stmt)
									defer bound.Close()
									_, err = bound.ExecContext(ctx, int64(2147483648), int64(1))
									var sqlErr *mysql.MySQLError
									require.ErrorAs(t, err, &sqlErr)
									require.Equal(t, uint16(1690), sqlErr.Number)
								}()
								func() {
									tx, err := conn.BeginTx(ctx, nil)
									require.NoError(t, err)
									defer tx.Rollback()
									state(tx, "1:0,1001:0,2001:0")
									bound := tx.StmtContext(ctx, stmt)
									defer bound.Close()
									_, err = bound.ExecContext(ctx, int64(9), int64(1001))
									require.NoError(t, err)
									state(tx, "1:0,1001:9,2001:0")
								}()
							}
						}()
					}
					// These binary prepared EXPLAIN executions intentionally do not
					// use the runtime cache. Compare logical input, never wall time.
					scanMetrics := regexp.MustCompile(`inputBlocks=(\d+) inputRows=(\d+)`)
					for _, write := range []string{"update narrow_keys set v=7", "delete from narrow_keys"} {
						for _, predicate := range []string{"id=1", "id=?", "cast(id as signed)=?"} {
							func() {
								stmt, err := conn.PrepareContext(ctx, "explain analyze "+write+" where "+predicate)
								require.NoError(t, err)
								defer stmt.Close()
								tx, err := conn.BeginTx(ctx, nil)
								require.NoError(t, err)
								defer tx.Rollback()
								var args []any
								if strings.Contains(predicate, "?") {
									args = []any{int64(1)}
								}
								bound := tx.StmtContext(ctx, stmt)
								defer bound.Close()
								rows, queryErr := bound.QueryContext(ctx, args...)
								text := readPreparedExplainRows(t, rows, queryErr)
								require.Contains(t, text, "Table Scan on "+target)
								metrics := scanMetrics.FindAllStringSubmatch(text, -1)
								require.Len(t, metrics, 1, text)
								inputBlocks, err := strconv.ParseInt(metrics[0][1], 10, 64)
								require.NoError(t, err)
								inputRows, err := strconv.ParseInt(metrics[0][2], 10, 64)
								require.NoError(t, err)
								t.Logf("write=%s predicate=%s blocks=%d rows=%d", write, predicate, inputBlocks, inputRows)
								want := int64(1)
								if strings.HasPrefix(predicate, "cast") {
									want = 3
								}
								require.Equal(t, []int64{want, want}, []int64{inputBlocks, inputRows}, text)
								if strings.HasPrefix(write, "update") {
									state(tx, "1:7,1001:0,2001:0")
								} else {
									state(tx, "1001:0,2001:0")
								}
							}()
						}
					}
				})
			}
		})
		t.Run("ROUND TRUNCATE output cast", func(t *testing.T) {
			type binding struct {
				name, sqlValue string
				value          any
				want           [2]string
			}
			text := binding{"text", "'1.46'", "1.46", [2]string{"1.5", "1.4"}}
			negative := binding{"negative text", "'-1.46'", "-1.46", [2]string{"-1.5", "-1.4"}}
			exec(t, "create table output_cast_keys(c bigint primary key)")
			exec(t, "insert into output_cast_keys values(-9999),(9999),(54321)")
			typedReuse := []binding{
				{"integer", "3", int64(3), [2]string{"3.0", "3.0"}},
				text,
				{"double", "cast(1.46 as double)", float64(1.46), [2]string{"1.5", "1.4"}},
				negative,
				{"NULL", "NULL", nil, [2]string{"NULL", "NULL"}},
				{"text recovery", "'1.46'", "1.46", [2]string{"1.5", "1.4"}},
			}
			// Output scale must not be pushed through TRUNCATE into its input.
			// Conversely an explicit input scale is part of the user's semantics:
			// rounding 1.46 to DECIMAL(20,1) before TRUNCATE legitimately yields 1.5.
			for _, protocol := range []string{"binary", "SQL"} {
				t.Run(protocol, func(t *testing.T) {
					for _, tc := range []struct {
						name, input, output string
						precision           int
						bindings            []binding
					}{
						{"text first", "?", "decimal(20,1)", 1, []binding{text}},
						{"typed reuse", "?", "decimal(20,1)", 1, typedReuse},
						{"scalar input", "(select ?)", "decimal(20,1)", 1, []binding{text, negative}},
						{"outer double", "?", "double", 1, []binding{text, negative}},
						{"outer scale 2", "?", "decimal(20,2)", 1, []binding{
							{"text", "'1.46'", "1.46", [2]string{"1.50", "1.40"}},
							{"negative text", "'-1.46'", "-1.46", [2]string{"-1.50", "-1.40"}},
						}},
						{"explicit input scale 1", "cast(? as decimal(20,1))", "decimal(20,1)", 1, []binding{
							{"text", "'1.46'", "1.46", [2]string{"1.5", "1.5"}},
							{"negative text", "'-1.46'", "-1.46", [2]string{"-1.5", "-1.5"}},
						}},
						{"explicit input scale 2", "cast(? as decimal(20,2))", "decimal(20,1)", 1, []binding{text}},
						{"explicit input double", "cast(? as double)", "decimal(20,1)", 1, []binding{text}},
						{"double rounding", "?", "decimal(20,1)", 2, []binding{
							{"text", "'1.449'", "1.449", [2]string{"1.5", "1.4"}},
							{"negative text", "'-1.449'", "-1.449", [2]string{"-1.5", "-1.4"}},
						}},
					} {
						t.Run(tc.name, func(t *testing.T) {
							q := fmt.Sprintf("select cast(round(%s,%d) as %s),cast(truncate(%s,%d) as %s)", tc.input, tc.precision, tc.output, tc.input, tc.precision, tc.output)
							var queryRow func(*testing.T, binding) *sql.Row
							if protocol == "binary" {
								stmt, err := conn.PrepareContext(ctx, q)
								require.NoError(t, err)
								defer stmt.Close()
								queryRow = func(_ *testing.T, b binding) *sql.Row {
									return stmt.QueryRowContext(ctx, b.value, b.value)
								}
							} else {
								exec(t, "prepare output_cast_contract from '"+q+"'")
								defer func() {
									cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
									defer stop()
									_, err := conn.ExecContext(cleanup, "deallocate prepare output_cast_contract")
									require.NoError(t, err)
									_, err = conn.ExecContext(cleanup, "set @output_cast_value=NULL")
									require.NoError(t, err)
								}()
								queryRow = func(t *testing.T, b binding) *sql.Row {
									exec(t, "set @output_cast_value="+b.sqlValue)
									return conn.QueryRowContext(ctx, "execute output_cast_contract using @output_cast_value,@output_cast_value")
								}
							}
							for _, b := range tc.bindings {
								t.Run(b.name, func(t *testing.T) {
									// QueryRow.Scan closes its rows on both success and failure;
									// no result can outlive the next binding on this connection.
									var round, truncate sql.NullString
									require.NoError(t, queryRow(t, b).Scan(&round, &truncate))
									got := [2]string{"NULL", "NULL"}
									for i, value := range []sql.NullString{round, truncate} {
										if value.Valid {
											got[i] = value.String
										}
									}
									require.Equal(t, b.want, got, q)
								})
							}
							if tc.name == "typed reuse" {
								t.Run("recovery after invalid text", func(t *testing.T) {
									invalid := binding{sqlValue: "'not-a-number'", value: "not-a-number"}
									var round, truncate sql.NullString
									err := queryRow(t, invalid).Scan(&round, &truncate)
									var sqlErr *mysql.MySQLError
									require.ErrorAs(t, err, &sqlErr, "invalid numeric text must be rejected by the server")
									// Reuse the same handle after the server error; neither the
									// failed value nor its conversion may poison the next execution.
									require.NoError(t, queryRow(t, text).Scan(&round, &truncate))
									require.True(t, round.Valid && truncate.Valid)
									require.Equal(t, text.want, [2]string{round.String, truncate.String})
								})
							}
						})
					}
					t.Run("explicit result narrowing predicate", func(t *testing.T) {
						// Key lowering may use a proven result, but must still execute
						// the explicit DECIMAL cast after ROUND (including saturation).
						const q = "select c from output_cast_keys where c=cast(round(?,0) as decimal(4,0))"
						var query func(*testing.T, string) (*sql.Rows, error)
						if protocol == "binary" {
							stmt, err := conn.PrepareContext(ctx, q)
							require.NoError(t, err)
							defer stmt.Close()
							query = func(_ *testing.T, value string) (*sql.Rows, error) {
								return stmt.QueryContext(ctx, value)
							}
						} else {
							exec(t, "prepare output_cast_predicate from '"+q+"'")
							defer func() {
								cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
								defer stop()
								_, err := conn.ExecContext(cleanup, "deallocate prepare output_cast_predicate")
								require.NoError(t, err)
								_, err = conn.ExecContext(cleanup, "set @output_cast_predicate_value=NULL")
								require.NoError(t, err)
							}()
							query = func(t *testing.T, value string) (*sql.Rows, error) {
								exec(t, "set @output_cast_predicate_value='"+value+"'")
								return conn.QueryContext(ctx, "execute output_cast_predicate using @output_cast_predicate_value")
							}
						}
						for _, tc := range []struct {
							value string
							want  int64
						}{{"54321.0", 9999}, {"-54321.0", -9999}, {"54321.0", 9999}} {
							got, err := readPreparedContractIDs(query(t, tc.value))
							require.NoError(t, err)
							require.Equal(t, []int64{tc.want}, got)
						}
					})
				})
			}
		})
		t.Run("decimal scientific values and persistence", func(t *testing.T) {
			exec(t, "create table source(v varchar(128))")
			exec(t, "insert into source values ('1E-2'),('-1E-2'),('0E2')")
			for _, typ := range []string{"decimal(5,2)", "decimal(20,8)", "decimal(38,18)", "decimal(65,10)"} {
				// Exact independent value oracle; equality with e also checks spelling invariance.
				require.Equal(t, "1", scalar(t, "select cast('1E-2' as "+typ+")=cast('0.01' as "+typ+")"))
				func() {
					p, err := conn.PrepareContext(ctx, "select cast(? as "+typ+")=cast('0.01' as "+typ+")")
					require.NoError(t, err)
					defer p.Close()
					var same int
					require.NoError(t, p.QueryRowContext(ctx, "1E-2").Scan(&same))
					require.Equal(t, 1, same)
				}()
			}
			require.Equal(t, "1.23", scalar(t, "select cast(? as decimal(5,2))", "0."+strings.Repeat("0", 60)+"123E61"))
			exec(t, "create table wide_source(v varchar(128))")
			const wide = "123456789012345678901"
			padded := wide + strings.Repeat("0", 80) + "E-80"
			exec(t, "insert into wide_source values (?)", padded)
			for _, typ := range []string{"decimal(38,0)", "decimal(65,0)"} {
				require.Equal(t, wide, scalar(t, "select cast('"+padded+"' as "+typ+")"))
				require.Equal(t, wide, scalar(t, "select cast(v as "+typ+") from wide_source"))
				func() {
					p, err := conn.PrepareContext(ctx, "select cast(? as "+typ+")")
					require.NoError(t, err)
					defer p.Close()
					var value string
					require.NoError(t, p.QueryRowContext(ctx, padded).Scan(&value))
					require.Equal(t, wide, value)
					exec(t, "prepare wide_cast from 'select cast(? as "+typ+")'")
					defer exec(t, "deallocate prepare wide_cast")
					exec(t, "set @wide=?", padded)
					require.Equal(t, wide, scalar(t, "execute wide_cast using @wide"))
				}()
			}
			exec(t, "create table converted(v decimal(5,2))")
			exec(t, "insert into converted select cast(v as decimal(5,2)) from source")
			require.Equal(t, "-0.01,0.00,0.01", scalar(t, "select group_concat(v order by v) from converted"))
			exec(t, "prepare d from 'select cast(? as decimal(5,2))'")
			defer exec(t, "deallocate prepare d")
			for _, tc := range []struct{ input, want string }{{"1E-2", "0.01"}, {"0E2", "0.00"}, {"1E10", "999.99"}, {"-1E10", "-999.99"}} {
				exec(t, "set @d=?", tc.input)
				require.Equal(t, tc.want, scalar(t, "execute d using @d"))
			}
		})
		t.Run("signed prepared identity and DML", func(t *testing.T) {

			exec(t, "create table keys_t(id bigint primary key,n int)")
			exec(t, "insert into keys_t values(9007199254740992,0),(9007199254740993,0),(9007199254740994,0)")
			target := int64(9007199254740993)
			for _, tc := range []struct {
				op   string
				want []int64
			}{{"=", []int64{target}}, {"<>", []int64{target - 1, target + 1}}, {"<", []int64{target - 1}}, {"<=", []int64{target - 1, target}}, {">", []int64{target + 1}}, {">=", []int64{target, target + 1}}} {
				func() {
					p, err := conn.PrepareContext(ctx, "select id from keys_t where id"+tc.op+"? order by id")
					require.NoError(t, err)
					defer p.Close()
					got, err := readPreparedContractIDs(p.QueryContext(ctx, "9007199254740993"))
					require.NoError(t, err)
					require.Equal(t, tc.want, got)
				}()
			}
			p, err := conn.PrepareContext(ctx, "select id from keys_t where id=? order by id")
			require.NoError(t, err)
			defer p.Close()
			for _, v := range []any{"9007199254740993", target, nil, "9007199254740993E0", "9007199254740993" + strings.Repeat("0", 60) + "E-60", "0." + strings.Repeat("0", 60) + "9007199254740993E76", "9007199254740993"} {
				want := []int64{target}
				if v == nil {
					want = []int64{}
				}
				got, err := readPreparedContractIDs(p.QueryContext(ctx, v))
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			for _, tc := range []struct {
				value string
				want  []int64
			}{
				{"9007199254740993.5", []int64{target + 1}},
				{"9223372036854775808", []int64{}},
				{"notnumber", []int64{}},
				{"9007199254740993tail", []int64{target - 1, target}},
				{"9007199254740993", []int64{target}},
			} {
				got, err := readPreparedContractIDs(p.QueryContext(ctx, tc.value))
				require.NoError(t, err)
				require.Equal(t, tc.want, got, tc.value)
			}
			for _, expression := range []string{"cast(? as decimal(38,0))", "abs(cast(? as decimal(38,0)))", "cast(? as decimal(65,0))+0"} {
				func() {
					p, err := conn.PrepareContext(ctx, "select id from keys_t where id="+expression+" order by id")
					require.NoError(t, err)
					defer p.Close()
					for _, tc := range []struct {
						value any
						want  []int64
					}{
						{"9007199254740993", []int64{target}},
						{"9007199254740993.5", []int64{target + 1}},
						{nil, []int64{}},
						{"9223372036854775808", []int64{}},
						{"9007199254740993", []int64{target}},
					} {
						got, err := readPreparedContractIDs(p.QueryContext(ctx, tc.value))
						require.NoError(t, err)
						require.Equal(t, tc.want, got, expression)
					}
				}()
			}
			for _, projection := range []string{
				"select v from (select cast(? as decimal(38,1)) as v) y",
				"select abs(v) as v from (select cast(? as decimal(38,1)) as v) y",
			} {
				func() {
					p, err := conn.PrepareContext(ctx, "select k.id from keys_t k join ("+projection+") x on k.id=x.v order by k.id")
					require.NoError(t, err)
					defer p.Close()
					for _, tc := range []struct {
						value any
						want  []int64
					}{
						{"9007199254740993", []int64{target}},
						{"9007199254740993.5", []int64{}},
						{nil, []int64{}},
						{"9223372036854775808", []int64{}},
						{"9007199254740993", []int64{target}},
					} {
						got, err := readPreparedContractIDs(p.QueryContext(ctx, tc.value))
						require.NoError(t, err)
						require.Equal(t, tc.want, got, projection)
					}
					_, err = readPreparedContractIDs(p.QueryContext(ctx, "not-a-number"))
					require.ErrorContains(t, err, "invalid decimal string")
					got, err := readPreparedContractIDs(p.QueryContext(ctx, "9007199254740993"))
					require.NoError(t, err)
					require.Equal(t, []int64{target}, got)
				}()
			}
			// Preserve the existing literal comparison path independently of marker binding.
			got, err := readPreparedContractIDs(conn.QueryContext(ctx, "select id from keys_t where id='9007199254740993' order by id"))
			require.NoError(t, err)
			require.Equal(t, []int64{target}, got)
			got, err = readPreparedContractIDs(conn.QueryContext(ctx, "select id from keys_t where id in (?) order by id", "9007199254740993"))
			require.NoError(t, err)
			require.Equal(t, []int64{target}, got)
			got, err = readPreparedContractIDs(conn.QueryContext(ctx, "select id from keys_t where ?=id order by id", "9007199254740993"))
			require.NoError(t, err)
			require.Equal(t, []int64{target}, got)
			exec(t, "prepare s from 'select id from keys_t where id=? order by id'")
			defer exec(t, "deallocate prepare s")
			exec(t, "set @v='9007199254740993'")
			got, err = readPreparedContractIDs(conn.QueryContext(ctx, "execute s using @v"))
			require.NoError(t, err)
			require.Equal(t, []int64{target}, got)
			exec(t, "prepare u from 'update keys_t set n=n+1 where id=?'")
			defer exec(t, "deallocate prepare u")
			exec(t, "execute u using @v")
			require.Equal(t, "9007199254740993", scalar(t, "select group_concat(id order by id) from keys_t where n=1"))
			exec(t, "select mo_ctl('dn','flush','"+schema+".keys_t')")
			got, err = readPreparedContractIDs(p.QueryContext(ctx, "9007199254740993"))
			require.NoError(t, err)
			require.Equal(t, []int64{target}, got)
			d, err := conn.PrepareContext(ctx, "delete from keys_t where id=?")
			require.NoError(t, err)
			defer d.Close()
			_, err = d.ExecContext(ctx, "9007199254740993")
			require.NoError(t, err)
			got, err = readPreparedContractIDs(conn.QueryContext(ctx, "select id from keys_t order by id"))
			require.NoError(t, err)
			require.Equal(t, []int64{target - 1, target + 1}, got)
		})
		t.Run("last day DATE metadata and schema", func(t *testing.T) {

			rows, queryErr := conn.QueryContext(ctx, "select last_day('2024-02-10'),last_day(cast('2024-02-10' as date)),last_day(cast('2024-02-10 13:00:00' as datetime))")
			checkPreparedLastDay(t, rows, queryErr)
			p, err := conn.PrepareContext(ctx, "select last_day(?)")
			require.NoError(t, err)
			defer p.Close()
			rows, queryErr = p.QueryContext(ctx, "2024-02-10")
			checkPreparedLastDay(t, rows, queryErr)
			exec(t, "prepare l from 'select last_day(?)'")
			defer exec(t, "deallocate prepare l")
			exec(t, "set @l='2024-02-10'")
			rows, queryErr = conn.QueryContext(ctx, "execute l using @l")
			checkPreparedLastDay(t, rows, queryErr)
			exec(t, "create table calendar as select last_day(v) as d from (select 'not-a-date' as v union all select '0000-00-00' union all select null) as calendar_source")
			exec(t, "create view calendar_view as select last_day('2024-02-10') as d")
			for _, name := range []string{"calendar", "calendar_view"} {
				require.Equal(t, "date", scalar(t, "select data_type from information_schema.columns where table_schema='"+schema+"' and table_name='"+name+"' and column_name='d'"))
			}
			require.Equal(t, "3", scalar(t, "select count(*) from calendar where d is null"))
			exec(t, "create table valid_calendar as select last_day('2024-02-10') as d")
			require.Equal(t, "2024-02-29", scalar(t, "select d from valid_calendar"))
		})
		t.Run("EXPLAIN preserves underlying SELECT binding", func(t *testing.T) {
			exec(t, "create table explain_t(id int primary key)")
			exec(t, "insert into explain_t values(1),(2)")

			for _, wrapper := range []string{"explain ", "explain analyze ", "explain phyplan "} {
				func() {
					exec(t, "prepare prof from '"+wrapper+"select sum(id) from explain_t where id=?'")
					defer exec(t, "deallocate prepare prof")
					for _, v := range []any{int64(1), "1", int64(2)} {
						exec(t, "set @p=?", v)
						rows, queryErr := conn.QueryContext(ctx, "execute prof using @p")
						text := readPreparedExplain(t, rows, queryErr)
						if wrapper != "explain phyplan " {
							require.Contains(t, text, "Filter Cond:")
						}
					}
					p, err := conn.PrepareContext(ctx, wrapper+"select sum(id) from explain_t where id=?")
					require.NoError(t, err)
					defer p.Close()
					rows, queryErr := p.QueryContext(ctx, int64(1))
					readPreparedExplain(t, rows, queryErr)
				}()
			}

		})
	})
}

// readPreparedContractIDs owns each query result through scan, terminal error and close.
func readPreparedContractIDs(rows *sql.Rows, queryErr error) ([]int64, error) {
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func checkPreparedLastDay(t *testing.T, rows *sql.Rows, err error) {
	t.Helper()
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	require.NoError(t, err)
	for _, col := range cols {
		require.Equal(t, "DATE", col.DatabaseTypeName())
	}
	require.True(t, rows.Next())
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	require.NoError(t, rows.Scan(dest...))
	for _, v := range vals {
		require.True(t, v.Valid)
		require.Equal(t, "2024-02-29", v.String)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func readPreparedExplain(t *testing.T, rows *sql.Rows, err error) string {
	t.Helper()
	text := readPreparedExplainRows(t, rows, err)
	require.NotContains(t, strings.ToLower(text), "cast(explain_t.id as bigint)")
	require.NotContains(t, text, "Cast expression may prevent index usage")
	return text
}

func readPreparedExplainRows(t *testing.T, rows *sql.Rows, err error) string {
	t.Helper()
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, lines)
	return strings.Join(lines, "\n")
}
