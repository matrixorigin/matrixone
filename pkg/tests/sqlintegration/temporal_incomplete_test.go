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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIncompleteTemporalStrings(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("set sql_mode=''")
		const schema = "incomplete_temporal_30234"
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("create table " + schema + ".dates(id int primary key,v varchar(16))")
		exec("insert into " + schema + ".dates values(1,'2024-00-00'),(2,'2024-02-29'),(3,NULL)")
		warningsFor := func(t *testing.T, want int, input string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			n := 0
			for rows.Next() {
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				require.Equal(t, "Warning", level)
				require.Equal(t, 1292, code)
				require.Contains(t, message, input)
				n++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, n)
		}
		warnings := func(t *testing.T, want int) { warningsFor(t, want, "2024-00-00") }
		check := func(t *testing.T, q string, want []sql.NullString) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			var got []sql.NullString
			for rows.Next() {
				var v sql.NullString
				require.NoError(t, rows.Scan(&v))
				got = append(got, v)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got, q)
		}
		value := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
		t.Run("time_comparison_policy", func(t *testing.T) {
			exec("set sql_mode='STRICT_TRANS_TABLES'")
			defer exec("set sql_mode=''")
			exec("create table " + schema + ".times(t time(2))")
			exec("insert into " + schema + ".times values('12:00:00.24')")
			stmt, err := conn.PrepareContext(ctx, "select t = ? from "+schema+".times")
			require.NoError(t, err)
			defer stmt.Close()
			for _, input := range []string{"12:00:00.24", "not-a-time", "12:00:00.24"} {
				var got sql.NullInt64
				err := stmt.QueryRowContext(ctx, input).Scan(&got)
				if input == "not-a-time" {
					require.ErrorContains(t, err, "invalid time value not-a-time")
				} else {
					require.NoError(t, err)
					require.Equal(t, sql.NullInt64{Int64: 1, Valid: true}, got)
				}
			}
			exec("prepare temporal_time_compare from 'select t = ? from " + schema + ".times'")
			defer exec("deallocate prepare temporal_time_compare")
			exec("set @temporal_time_input='not-a-time'")
			var got sql.NullInt64
			require.ErrorContains(t, conn.QueryRowContext(ctx, "execute temporal_time_compare using @temporal_time_input").Scan(&got), "invalid time value not-a-time")
			check(t, "select cast('not-a-time' as time)", []sql.NullString{{}})
			warningsFor(t, 1, "not-a-time")
		})
		t.Run("gapfill_constant_bounds", func(t *testing.T) {
			exec("create table " + schema + ".bounded_readings(device varchar(8), event_ts datetime, value int)")
			exec("insert into " + schema + ".bounded_readings values ('d1','2026-01-01 00:02:10',20),('d1','2026-01-01 00:04:20',40),('d2','2026-01-01 00:01:30',7)")
			for _, partitioned := range []bool{true, false} {
				func() {
					query := "select _wstart, sum(value) from " + schema + ".bounded_readings where event_ts >= '2026-01-02 00:00:00' and event_ts < '2026-01-02 00:03:00' interval(event_ts, 1, minute) gapfill(partition) fill(null) order by _wstart"
					wantRows := 3
					if partitioned {
						query = "select _wstart, sum(value) from " + schema + ".bounded_readings where event_ts >= '2026-01-01 00:00:00' and event_ts < '2026-01-01 00:06:00' group by device interval(event_ts, 1, minute) gapfill(partition) fill(value, 0) order by device, _wstart"
						wantRows = 12
					}
					rows, err := conn.QueryContext(ctx, query)
					require.NoError(t, err)
					defer rows.Close()
					count := 0
					for rows.Next() {
						var start string
						var sum sql.NullInt64
						require.NoError(t, rows.Scan(&start, &sum))
						day, minute := 2, count
						if partitioned {
							day, minute = 1, count%6
						}
						require.Equal(t, fmt.Sprintf("2026-01-%02d 00:%02d:00", day, minute), start)
						require.Equal(t, partitioned, sum.Valid)
						count++
					}
					require.NoError(t, rows.Err())
					require.Equal(t, wantRows, count)
				}()
			}
		})
		t.Run("date_format_invalid_conversion", func(t *testing.T) {
			inputs := []string{"123456", "19981321000000", "19981033000000", "0000-01-01", "0000-02-28", "2026-02-30"}
			stmt, err := conn.PrepareContext(ctx, "select date_format(?,'%Y-%m-%d')")
			require.NoError(t, err)
			defer stmt.Close()
			for _, input := range inputs {
				check(t, "select date_format('"+input+"','%Y-%m-%d')", []sql.NullString{{}})
				warningsFor(t, 1, input)
				for _, next := range []string{input, "2024-02-29", "2024-00-00"} {
					var got sql.NullString
					require.NoError(t, stmt.QueryRowContext(ctx, next).Scan(&got))
					want := sql.NullString{}
					if next != input {
						want = value(next)
					}
					require.Equal(t, want, got)
					count := 0
					if next == input {
						count = 1
					}
					warningsFor(t, count, input)
				}
			}
			exec("insert into " + schema + ".dates values(4,'2026-02-30'),(5,'2024-02-29')")
			defer exec("delete from " + schema + ".dates where id>=4")
			check(t, "select date_format(v,'%Y-%m-%d') from "+schema+".dates order by id", []sql.NullString{value("2024-00-00"), value("2024-02-29"), {}, {}, value("2024-02-29")})
			warningsFor(t, 1, "2026-02-30")
		})

		for _, tc := range []struct {
			name, expression, complete string
			format                     bool
		}{
			{"date_format", "date_format($date,'%Y-%m-%d')", "2024-02-29", true},
			{"to_days", "to_days($date)", "739310", false},
			{"to_seconds", "to_seconds($date)", "63876384000", false},
			{"dayofweek", "dayofweek($date)", "5", false},
			{"week", "week($date,0)", "8", false},
			{"last_day", "last_day($date)", "2024-02-29", false},
			{"date_add", "date_add($date,interval 1 day)", "2024-03-01", false},
			{"date_add_text_interval", "date_add($date,interval '1.5' day)", "2024-03-01", false},
			{"date_sub_text_interval", "date_sub($date,interval '1.5' day)", "2024-02-28", false},
			{"datediff", "datediff($date,'2024-01-01')", "59", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				partial := sql.NullString{}
				warn := 1
				if tc.format {
					partial = value("2024-00-00")
					warn = 0
				}
				q := func(operand string) string { return "select " + strings.ReplaceAll(tc.expression, "$date", operand) }
				calendarTotal := tc.name == "to_days" || tc.name == "to_seconds"
				if calendarTotal {
					check(t, q("'2024-02-30'"), []sql.NullString{{}})
					warnings(t, 0)
					exec("insert into " + schema + ".dates values(4,'2024-02-30'),(5,'2024-02-29')")
					defer exec("delete from " + schema + ".dates where id>=4")
					check(t, q("v")+" from "+schema+".dates order by id", []sql.NullString{{}, value(tc.complete), {}, {}, value(tc.complete)})
					warnings(t, 1)
				}
				check(t, q("'2024-00-00'"), []sql.NullString{partial})
				warnings(t, warn)
				check(t, q("v")+" from "+schema+".dates where id<=3 order by id", []sql.NullString{partial, value(tc.complete), {}})
				warnings(t, warn)
				func() {
					stmt, err := conn.PrepareContext(ctx, q("?"))
					require.NoError(t, err)
					defer stmt.Close()
					inputs := []any{"2024-00-00", "2024-02-29", nil, "2024-00-00"}
					if calendarTotal {
						inputs = append(inputs, "2024-02-30", "2024-02-29", "2024-00-00", "2024-02-30")
					}
					for _, input := range inputs {
						var got sql.NullString
						require.NoError(t, stmt.QueryRowContext(ctx, input).Scan(&got))
						want := partial
						n := warn
						if input == nil || input == "2024-02-30" {
							want = sql.NullString{}
							n = 0
						} else if input == "2024-02-29" {
							want = value(tc.complete)
							n = 0
						}
						require.Equal(t, want, got)
						warnings(t, n)
					}
				}()
				exec("prepare incomplete_date from '" + strings.ReplaceAll(q("?"), "'", "''") + "'")
				defer exec("deallocate prepare incomplete_date")
				inputs := []string{"'2024-00-00'", "'2024-02-29'", "NULL", "'2024-00-00'"}
				if calendarTotal {
					inputs = append(inputs, "'2024-02-30'", "'2024-02-29'", "'2024-00-00'", "'2024-02-30'")
				}
				for _, input := range inputs {
					exec("set @incomplete_date=" + input)
					want := partial
					n := warn
					if input == "NULL" || input == "'2024-02-30'" {
						want = sql.NullString{}
						n = 0
					} else if input == "'2024-02-29'" {
						want = value(tc.complete)
						n = 0
					}
					check(t, "execute incomplete_date using @incomplete_date", []sql.NullString{want})
					warnings(t, n)
				}
			})
		}
		for _, name := range []string{"date_add", "date_sub"} {
			t.Run("prepared_null_interval/"+name, func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, "select "+name+"(?,interval ? day)")
				require.NoError(t, err)
				defer stmt.Close()
				for _, interval := range []any{nil, int64(1), nil} {
					var got sql.NullString
					require.NoError(t, stmt.QueryRowContext(ctx, "2024-00-00", interval).Scan(&got))
					require.False(t, got.Valid)
					count := 0
					if interval != nil {
						count = 1
					}
					warnings(t, count)
				}
			})
		}
		// CTAS must admit NULL synthesized from a non-NULL string value.
		exec("create table " + schema + ".required_dates(id int primary key,v varchar(16) not null)")
		exec("insert into " + schema + ".required_dates values(1,'2024-00-00'),(2,'2024-02-29')")
		for _, tc := range []struct{ name, expression, complete string }{
			{"to_days", "to_days(v)", "739310"},
			{"to_seconds", "to_seconds(v)", "63876384000"},
			{"datediff_strings", "datediff(v,'2024-01-01')", "59"},
			{"datediff_string_date", "datediff(v,cast('2024-01-01' as date))", "59"},
			{"datediff_date_string", "datediff(cast('2024-01-01' as date),v)", "-59"},
			{"dayofweek", "dayofweek(v)", "5"},
			{"date_format_month", "date_format(v,'%M')", "February"},
		} {
			t.Run("not_null_ctas/"+tc.name, func(t *testing.T) {
				table := schema + ".materialized_" + tc.name
				_, err := conn.ExecContext(ctx, "create table "+table+" as select id,"+tc.expression+" as n from "+schema+".required_dates")
				require.NoError(t, err)
				check(t, "select n from "+table+" order by id", []sql.NullString{{}, value(tc.complete)})
				check(t, "select is_nullable from information_schema.columns where table_schema='"+schema+"' and table_name='materialized_"+tc.name+"' and column_name='n'", []sql.NullString{value("YES")})
			})
		}
		// CTAS initially stores full expression results even when the declared
		// width is too small. A later ordinary write must preserve them too.
		exec("create table " + schema + ".week_dates(id int primary key,v varchar(16) not null)")
		exec("insert into " + schema + ".week_dates values(1,'0000-01-00'),(2,'2024-00-30'),(3,'2020-00-00')")
		for _, tc := range []struct {
			name, format, sourceID, want, width string
		}{
			{"week_year_sunday", "%X", "1", "4294967295", "10"},
			{"week_year_monday", "%x", "1", "4294967295", "10"},
			{"week_sunday", "%U", "2", "613566756", "9"},
			{"week_year_week_sunday", "%V", "2", "613566756", "9"},
			{"iso_week_boundary", "%v", "3", "613566753", "9"},
		} {
			t.Run("format_width_ctas/"+tc.name, func(t *testing.T) {
				tableName := "formatted_" + tc.name
				table := schema + "." + tableName
				query := "select date_format(v,'" + tc.format + "') as n from " + schema + ".week_dates where id=" + tc.sourceID
				check(t, query, []sql.NullString{value(tc.want)})
				_, err := conn.ExecContext(ctx, "create table "+table+" as "+query)
				require.NoError(t, err)
				check(t, "select n from "+table, []sql.NullString{value(tc.want)})
				insert := "insert into " + table + " values('" + tc.want + "')"
				if tc.name == "iso_week_boundary" {
					insert = "insert into " + table + " " + query
				}
				_, err = conn.ExecContext(ctx, insert)
				require.NoError(t, err)
				check(t, "select n from "+table+" order by n", []sql.NullString{value(tc.want), value(tc.want)})
				check(t, "select character_maximum_length from information_schema.columns where table_schema='"+schema+"' and table_name='"+tableName+"' and column_name='n'", []sql.NullString{value(tc.width)})
			})
		}
		check(t, "select date_format('2024-02-29','%X|%x')", []sql.NullString{value("2024|2024")})
		check(t, "select date_format('0000-00-00','%Y-%m-%d')", []sql.NullString{value("0000-00-00")})
		warnings(t, 0)
		check(t, "select concat(year('2024-00-00'),'/',month('2024-00-00'),'/',dayofmonth('2024-00-00'))", []sql.NullString{value("2024/0/0")})
		warnings(t, 0)
		check(t, "select case when false then to_days('2024-00-00') else 7 end", []sql.NullString{value("7")})
		warnings(t, 0)
		check(t, "select datediff(NULL,'2024-00-00')", []sql.NullString{{}})
		warnings(t, 0)
	})
}
