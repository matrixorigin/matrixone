// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

// One existing single-CN fixture, four data rows. Values, executable metadata,
// and persisted schema are independent oracles; no generated expected output.
func TestIntegerArithmeticWidth(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "integer_arithmetic_width"
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("drop database if exists " + schema)
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("use " + schema)
		exec("create table t(id int primary key,b tinyint,s smallint,m mediumint,i int,u tinyint unsigned)")
		exec("insert into t values(1,100,30000,5000000,2147483647,200),(2,-128,NULL,NULL,NULL,NULL)")
		check := func(t *testing.T, rows *sql.Rows, wantTypes []string, want [][]string) {
			t.Helper()
			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			if wantTypes != nil {
				require.Len(t, cols, len(wantTypes))
				for i, col := range cols {
					require.Equal(t, wantTypes[i], col.DatabaseTypeName(), col.Name())
				}
			}
			var got [][]string
			for rows.Next() {
				values := make([]sql.NullString, len(cols))
				dest := make([]any, len(cols))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(cols))
				for i, v := range values {
					row[i] = "NULL"
					if v.Valid {
						row[i] = v.String
					}
				}
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
		}
		query := func(t *testing.T, q string, types []string, want [][]string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			check(t, rows, types, want)
			require.NoError(t, rows.Err())
		}
		t.Run("vectors null and aggregates", func(t *testing.T) {
			projection := "b*b,s*s,m*m,i*i,b+b,u*u"
			want := [][]string{{"10000", "900000000", "25000000000000", "4611686014132420609", "200", "40000"}, {"16384", "NULL", "NULL", "NULL", "-256", "NULL"}}
			names := []string{"BIGINT", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "UNSIGNED BIGINT"}
			query(t, "select "+projection+" from t order by id", names, want)
			stmt, err := conn.PrepareContext(ctx, "select "+projection+" from t order by id")
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx)
			require.NoError(t, err)
			defer rows.Close()
			check(t, rows, names, want)
			require.NoError(t, rows.Err())
			query(t, "select sum(b*b) from t", nil, [][]string{{"26384"}})
			query(t, "select id from t where b*b=10000 order by id", nil, [][]string{{"1"}})
			query(t, "select id from t where b+b=-256 order by id", nil, [][]string{{"2"}})
			query(t, "select v from (select b*b v from t where id=1 union all select b*b v from t where id=2) x order by v", []string{"BIGINT"}, [][]string{{"10000"}, {"16384"}})

			query(t, "select (b+b)*(b+b) from t order by id", []string{"BIGINT"}, [][]string{{"40000"}, {"65536"}})
		})
		t.Run("mixed signed columns", func(t *testing.T) {
			exec("create table mixed(id int primary key,s bigint,u bigint unsigned)")
			exec("insert into mixed values(1,-1,18446744073709551615),(2,-9223372036854775808,9223372036854775808),(3,-1,0),(4,-1,1)")
			unsigned := []string{"UNSIGNED BIGINT", "UNSIGNED BIGINT"}
			query(t, "select s+u,u+s from mixed where id=1", unsigned, [][]string{{"18446744073709551614", "18446744073709551614"}})
			query(t, "select s+u,u+s from mixed where id=2", unsigned, [][]string{{"0", "0"}})
			query(t, "select s*u,u*s from mixed where id=3", unsigned, [][]string{{"0", "0"}})
			for _, expr := range []string{"s*u", "u*s"} {
				var value string
				err := conn.QueryRowContext(ctx, "select "+expr+" from mixed where id=4").Scan(&value)
				require.ErrorContains(t, err, "1690")
				query(t, "select s+u from mixed where id=1", unsigned[:1], [][]string{{"18446744073709551614"}})
			}
		})
		t.Run("top level set outputs", func(t *testing.T) {
			for _, tc := range []struct {
				op   string
				want [][]string
			}{
				{"union all", [][]string{{"10000", "1"}, {"10000", "1"}, {"16384", "1"}}},
				{"union", [][]string{{"10000", "1"}, {"16384", "1"}}},
				{"intersect", [][]string{{"10000", "1"}}},
				{"except", [][]string{{"16384", "1"}}},
			} {
				q := "select b*b as x,1 as y from t " + tc.op + " select 10000 as other,1 as ignored order by x"
				for _, prepared := range []bool{false, true} {
					func() {
						var stmt *sql.Stmt
						var rows *sql.Rows
						var err error
						if prepared {
							stmt, err = conn.PrepareContext(ctx, q)
							require.NoError(t, err)
							defer stmt.Close()
							rows, err = stmt.QueryContext(ctx)
						} else {
							rows, err = conn.QueryContext(ctx, q)
						}
						require.NoError(t, err, q)
						defer rows.Close()
						columns, err := rows.Columns()
						require.NoError(t, err)
						require.Equal(t, []string{"x", "y"}, columns)
						check(t, rows, []string{"BIGINT", "BIGINT"}, tc.want)
						require.NoError(t, rows.Err())
					}()
				}
			}
			stmt, err := conn.PrepareContext(ctx, "select ?*b as x,1 as y from t where id=1 union all select ?*b,1 from t where id=2 order by x")
			require.NoError(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx, int64(2), int64(3))
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"x", "y"}, columns)
			check(t, rows, []string{"BIGINT", "BIGINT"}, [][]string{{"-384", "1"}, {"200", "1"}})
			require.NoError(t, rows.Err())
		})
		t.Run("grouping rewrite preserves CTAS roots", func(t *testing.T) {
			for groupIndex, group := range []string{"%s", "%s with rollup", "cube(%s)", "grouping sets ((%s),())"} {
				for windowIndex, window := range []struct{ projection, order string }{
					{"", ""}, {",row_number() over () as rn", ""}, {"", " order by row_number() over ()"},
				} {
					for expressionIndex, tc := range []struct{ expr, from, typ, negative, positive string }{
						{"b*b*b", "t", "int", "-2097152", "1000000"},
						{"b*b*b+1", "t", "int", "-2097151", "1000001"},
						{"case when b>0 then b*b*b else b*b*b+1 end", "t", "int", "-2097151", "1000000"},
						{"x", "(select b*b*b as x from t) s", "bigint", "-2097152", "1000000"},
					} {
						table := fmt.Sprintf("grouped_%d_%d_%d", groupIndex, windowIndex, expressionIndex)
						key := "b"
						if tc.expr == "x" {
							key = "x"
						}
						by := fmt.Sprintf(group, key)
						exec("create table " + table + " as select " + tc.expr + " as x" + window.projection + " from " + tc.from + " group by " + by + window.order)
						var typ string
						require.NoError(t, conn.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=? and column_name='x'", schema, table).Scan(&typ))
						require.Equal(t, tc.typ, typ, table)
						want := [][]string{{tc.negative}, {tc.positive}}
						if groupIndex > 0 {
							want = append([][]string{{"NULL"}}, want...)
						}
						wireType := "INT"
						if tc.typ == "bigint" {
							wireType = "BIGINT"
						}
						query(t, "select x from "+table+" order by x", []string{wireType}, want)
						if window.projection != "" {
							count := len(want)
							query(t, "select count(distinct rn),min(rn),max(rn),sum(rn) from "+table, nil, [][]string{{fmt.Sprint(count), "1", fmt.Sprint(count), fmt.Sprint(count * (count + 1) / 2)}})
						}
					}
				}
			}
			source := "select b*b*b as x,row_number() over () as rn from t group by b with rollup"
			exec("create view window_grouped as " + source)
			for index, selectSQL := range []string{
				"select x from (" + source + ") __mo_rollup_window",
				"with grouped as (" + source + ") select x from grouped",
				"select x from window_grouped",
			} {
				table := fmt.Sprintf("grouped_real_ref_%d", index)
				exec("create table " + table + " as " + selectSQL)
				var typ string
				require.NoError(t, conn.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=? and column_name='x'", schema, table).Scan(&typ))
				require.Equal(t, "bigint", typ, selectSQL)
				query(t, "select x from "+table+" order by x", []string{"BIGINT"}, [][]string{{"NULL"}, {"-2097152"}, {"1000000"}})
			}
		})

		exec("create table c(b tinyint,m mediumint,u mediumint unsigned)")
		exec("insert into c values(1,100,100),(0,200,200)")
		t.Run("CASE alias and reference schemas", func(t *testing.T) {
			for n, tc := range []struct{ expr, direct, ref string }{
				{"case when b>0 then m+1 else m+m end", "int", "bigint"},
				{"case when b>0 then u+1 else u+u end", "int unsigned", "int unsigned"},
				{"case when b>0 then m+1 else null end", "int", "bigint"},
				{"case when b>0 then m+1 else cast(null as signed) end", "bigint", "bigint"},
				{"m+1", "int", "bigint"},
			} {
				for _, reference := range []bool{false, true} {
					table := fmt.Sprintf("d%d_%v", n, reference)
					selectSQL := "select " + tc.expr + " as x from c"
					want := tc.direct
					if reference {
						selectSQL = "select x from (" + selectSQL + ") r"
						want = tc.ref
					}
					exec("create table " + table + " as " + selectSQL)
					var typ string
					require.NoError(t, conn.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema=? and table_name=?", schema, table).Scan(&typ))
					require.Equal(t, want, typ, selectSQL)
				}
			}
			exec("create table square as select b*b bb,i*i ii from t")
			query(t, "select bb,ii from square order by bb", []string{"INT", "BIGINT"}, [][]string{{"10000", "4611686014132420609"}, {"16384", "NULL"}})
		})
		t.Run("view and CTE preserve execution precision", func(t *testing.T) {
			exec("create view v as select b*b bb,m+1 mp from c")
			query(t, "select bb,mp from v order by mp", []string{"BIGINT", "BIGINT"}, [][]string{{"1", "101"}, {"0", "201"}})
			query(t, "with x as(select b*b bb,m+1 mp from c) select bb*bb,mp*mp from x order by mp", []string{"BIGINT", "BIGINT"}, [][]string{{"1", "10201"}, {"0", "40401"}})
			exec("alter table c add unrelated int")
			query(t, "select bb,mp from v order by mp", []string{"BIGINT", "BIGINT"}, [][]string{{"1", "101"}, {"0", "201"}})
			k := strings.Repeat("0", 69) + "2"
			exec("create view long_v as select " + k + "+1 x")
			query(t, "select x+1 from long_v", []string{"BIGINT"}, [][]string{{"4"}})
		})
		t.Run("prepared string transition drops integer metadata", func(t *testing.T) {
			stmt, err := conn.PrepareContext(ctx, "select ?*b from t where id=1")
			require.NoError(t, err)
			defer stmt.Close()
			for _, value := range []any{int64(2), "2.5", nil, int64(2)} {
				func() {
					rows, err := stmt.QueryContext(ctx, value)
					require.NoError(t, err)
					defer rows.Close()
					cols, err := rows.ColumnTypes()
					require.NoError(t, err)
					require.True(t, rows.Next())
					var got sql.NullFloat64
					require.NoError(t, rows.Scan(&got))
					if value == nil {
						require.False(t, got.Valid)
					} else if value == "2.5" {
						require.Equal(t, float64(250), got.Float64)
					} else {
						require.Equal(t, float64(200), got.Float64)
					}
					if _, integer := value.(int64); integer {
						require.Equal(t, "BIGINT", cols[0].DatabaseTypeName())
					} else {
						require.NotContains(t, cols[0].DatabaseTypeName(), "INT")
					}
					require.False(t, rows.Next())
					require.NoError(t, rows.Err())
				}()
			}
		})
		t.Run("prepared domain and recovery", func(t *testing.T) {
			for _, op := range []string{"+", "*"} {
				func() {
					stmt, err := conn.PrepareContext(ctx, "select ? "+op+" b from t where id=1")
					require.NoError(t, err)
					defer stmt.Close()
					expected := "102"
					if op == "*" {
						expected = "200"
					}
					unsigned := false
					for _, value := range []any{nil, int64(2), int64(9223372036854775807), int64(2), uint64(2), nil, uint64(18446744073709551615), int64(2)} {
						func() {
							if value != nil {
								_, unsigned = value.(uint64)
							}
							t.Logf("op=%s source=%T value=%v", op, value, value)
							rows, err := stmt.QueryContext(ctx, value)
							if value == int64(9223372036854775807) || value == uint64(18446744073709551615) {
								require.ErrorContains(t, err, "1690")
								return
							}
							require.NoError(t, err)
							defer rows.Close()
							want := expected
							if value == nil {
								want = "NULL"
							}
							typ := "BIGINT"
							if unsigned {
								typ = "UNSIGNED BIGINT"
							}
							check(t, rows, []string{typ}, [][]string{{want}})
							require.NoError(t, rows.Err())
						}()
					}
				}()
			}
		})
	})
}
