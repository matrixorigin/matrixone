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
		warnings := func(t *testing.T, want int) {
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
				require.Contains(t, message, "2024-00-00")
				n++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, n)
		}
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
				check(t, q("'2024-00-00'"), []sql.NullString{partial})
				warnings(t, warn)
				check(t, q("v")+" from "+schema+".dates order by id", []sql.NullString{partial, value(tc.complete), {}})
				warnings(t, warn)
				func() {
					stmt, err := conn.PrepareContext(ctx, q("?"))
					require.NoError(t, err)
					defer stmt.Close()
					for _, input := range []any{"2024-00-00", "2024-02-29", nil, "2024-00-00"} {
						var got sql.NullString
						require.NoError(t, stmt.QueryRowContext(ctx, input).Scan(&got))
						want := partial
						n := warn
						if input == nil {
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
				for _, input := range []string{"'2024-00-00'", "'2024-02-29'", "NULL", "'2024-00-00'"} {
					exec("set @incomplete_date=" + input)
					want := partial
					n := warn
					if input == "NULL" {
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
