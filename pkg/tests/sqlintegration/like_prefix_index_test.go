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

func TestLikePrefixIndex(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists like_prefix_index")
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		exec("create database like_prefix_index")
		exec("use like_prefix_index")
		exec("create table t(id int primary key, name varchar(40), payload varchar(80), key ix(name),key ix_multi(name,payload))")
		for i, value := range []any{"abc", "abcd", "abc_def", "abc%def", "abd", "ab", "", "中文", "中文尾", "ab\x00c", nil, "\x7f", "\x7fa", "\x80", "\xc2\x80"} {
			_, err := conn.ExecContext(ctx, "insert into t values(?,?,?)", i, value, "payload")
			require.NoError(t, err)
		}
		explain := func(query string) string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, "explain "+query)
			require.NoError(t, err)
			defer rows.Close()
			var out strings.Builder
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				out.WriteString(line)
			}
			require.NoError(t, rows.Err())
			return out.String()
		}
		exec("create table t_unique(id int primary key,name varchar(40),payload varchar(80),unique key ux(name))")
		exec("insert into t_unique select * from t")
		exec("analyze table t_unique(name) fullscan")
		exec("create table residual_rows(id int primary key,name varchar(40),amount int,other int,payload varchar(40),key ix(name,amount))")
		exec("insert into residual_rows values(1,'rare',7,7,'p'),(2,'z2',1,2,'p'),(3,'z3',3,3,'p'),(4,'z4',4,5,'p'),(5,'z5',6,6,'p')")
		exec("analyze table residual_rows(name,amount) fullscan")
		for _, table := range []string{"candidate_errors", "residual_errors"} {
			exec("create table " + table + "(id int primary key,name varchar(40),payload varchar(30),key ix(name))")
			exec("insert into " + table + " values(1,'zzz','p'),(2,'xy','p')")
			if table == "residual_errors" {
				exec("insert into " + table + " values(3,'abc9','p')")
			}
			exec("analyze table " + table + "(name) fullscan")
		}
		exec("create table error_rows(name varchar(40),amount varchar(40),payload varchar(40),key ix(name,amount))")
		for _, name := range []string{"abc\x00x", "\x80"} {
			_, err := conn.ExecContext(ctx, "insert into error_rows values(?,'a','p')", name)
			require.NoError(t, err)
		}

		equalityQuery := "select name from t where name like 'abc%' and name='abc'"
		require.NotContains(t, explain(equalityQuery), "Join Type: INDEX", "a new prefix must not displace the existing covering equality")
		require.Contains(t, explain(equalityQuery), "Index Table Scan")
		query := "select count(*) from t where name like 'abcd%'"
		require.NotContains(t, explain(query), "Index Table Scan", "unknown frequency keeps automatic access conservative")
		exec("analyze table t(name) fullscan")
		require.NotContains(t, explain(equalityQuery), "Join Type: INDEX")
		require.Contains(t, explain(equalityQuery), "Index Table Scan")
		require.Contains(t, explain(query), "Index Table Scan")
		require.NotContains(t, explain("select count(*) from t where name like 'a%'"), "Index Table Scan", "a measured broad prefix must keep the scan")
		// Cross one storage block so automatic non-covering access can save reads.
		// Keep this in the existing fixture; tiny tables cannot prove that choice.
		exec("create table prefix_backfill(id int primary key,name varchar(40),amount int,payload int,key ix(name,amount))")
		exec("insert into prefix_backfill select result,concat(if(result%100=0,'abc','zzz'),result),result,result from generate_series(1,10000)g")
		exec("analyze table prefix_backfill(name,amount) fullscan")
		prefixQuery := "select sum(id+payload) from prefix_backfill where name like 'abc%' and amount=100"
		prefixPlan := explain(prefixQuery)
		require.Contains(t, prefixPlan, "Join Type: INDEX")
		require.Contains(t, prefixPlan, "prefix_in_range(")
		var prefixSum int64
		require.NoError(t, conn.QueryRowContext(ctx, prefixQuery).Scan(&prefixSum))
		require.Equal(t, int64(200), prefixSum)
		// Original issue scale and inclusive 30% threshold, collected through SQL.
		exec("create table issue_rows(id int primary key,name varchar(20),key ix(name))")
		exec("insert into issue_rows select result,case result%10 when 0 then 'apple' when 1 then 'apply' when 2 then 'apricot' when 3 then 'banana' when 4 then 'band' when 5 then 'bank' when 6 then 'zebra' when 7 then 'zoo' when 8 then 'orange' else 'pear' end from generate_series(0,499) g")
		exec("analyze table issue_rows(name) fullscan")
		require.Contains(t, explain("select count(*) from issue_rows where name like 'ap%'"), "Index Table Scan")
		var count int64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from issue_rows where name like 'ap%'").Scan(&count))
		require.Equal(t, int64(150), count)

		ids := func(q string) []int64 {
			t.Helper()
			result, err := readPreparedContractIDs(conn.QueryContext(ctx, q))
			require.NoError(t, err, q)
			return result
		}
		patterns := []struct {
			sql      string
			expected []int64
		}{
			{"'abc%'", []int64{0, 1, 2, 3}}, {"'abc%def'", []int64{2, 3}}, {"'abc_def'", []int64{2, 3}},
			{`'abc\\_%'`, []int64{2}}, {`'abc\\%%'`, []int64{3}}, {"'中文%'", []int64{7, 8}},
			{"'%def'", []int64{2, 3}}, {"'ab\\0%'", []int64{9}}, {"'abc'", []int64{0}}, {"'\x7f%'", []int64{11, 12}}, {"NULL", []int64{}},
		}
		for _, phase := range []string{"memory", "persisted"} {
			t.Run(phase, func(t *testing.T) {
				if phase == "persisted" {
					for _, table := range []string{"t", "t_unique", "residual_rows", "error_rows", "candidate_errors", "residual_errors"} {
						exec("select mo_ctl('dn','flush','like_prefix_index." + table + "')")
						indexes := func() []string {
							rows, err := conn.QueryContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='like_prefix_index' and relname=?) and index_table_name is not null", table)
							require.NoError(t, err)
							defer rows.Close()
							var indexes []string
							for rows.Next() {
								var index string
								require.NoError(t, rows.Scan(&index))
								if index != "" {
									indexes = append(indexes, index)
								}
							}
							require.NoError(t, rows.Err())
							return indexes
						}()
						for _, index := range indexes {
							exec("select mo_ctl('dn','flush','like_prefix_index." + index + "')")
						}
					}
					exec("analyze table t(name) fullscan")
					require.Contains(t, explain(query), "Index Table Scan")
				}
				nulQuery := "select id from t where name like 'ab\\0%' order by id"
				require.Contains(t, explain(nulQuery), "Index Table Scan")
				require.Contains(t, explain(nulQuery), "prefix_in_range")
				gapQuery := "select id from t force index(ix) where name like '\x7f%' order by id"
				gapPlan := explain(gapQuery)
				require.Contains(t, gapPlan, "Index Table Scan")
				require.Contains(t, gapPlan, "prefix_eq", "Unicode-successor gaps require an exact prefix residual")
				require.Equal(t, []int64{11, 12}, ids(gapQuery))

				require.Equal(t, []int64{9}, ids(nulQuery))
				require.Equal(t, []int64{9}, ids("select id from t force index(ix_multi) where name like 'ab\\0%' order by id"))
				for _, pattern := range patterns {
					t.Run(pattern.sql, func(t *testing.T) {
						// A computed subject bypasses the direct-column LIKE rewrite, providing
						// an independent truth oracle rather than sharing prefix extraction.
						control := ids("select id from t ignore index(ix) where concat(name,'') like " + pattern.sql + " order by id")
						require.Equal(t, pattern.expected, control)
						for _, projection := range []string{"id", "id+length(payload)-7"} {
							require.Equal(t, control, ids("select "+projection+" from t force index(ix) where name like "+pattern.sql+" order by id"))
						}
					})
				}
				t.Run("prefix with selective IN backfill", func(t *testing.T) {
					for _, predicate := range []string{"name like 'abc%' and name in ('abc','abcd')", "name in ('abc','abcd') and name like 'abc%'"} {
						q := "select id+length(payload)-7 from t ignore index(ix_multi) where " + predicate + " order by id"
						plan := explain(q)
						require.Contains(t, plan, "Join Type: INDEX")
						require.Contains(t, plan, "prefix_in(", "the driving lookup must be IN, not the broad prefix")
						require.Equal(t, []int64{0, 1}, ids(q))
					}
					require.Equal(t, []int64{0, 1}, ids("select id+length(payload)-7 from t ignore index(ix,ix_multi) where concat(name,'') like 'abc%' and name in ('abc','abcd') order by id"))
				})
				t.Run("alternative preserves error domains", func(t *testing.T) {
					check := func(query string, wantError bool) {
						t.Helper()
						rows, err := conn.QueryContext(ctx, query)
						if rows != nil {
							defer rows.Close()
						}
						if err == nil {
							require.False(t, rows.Next(), query)
							err = rows.Err()
						}
						if wantError {
							require.ErrorContains(t, err, "invalid regexp", query)
						} else {
							require.NoError(t, err, query)
						}
					}
					for _, hint := range []string{"", " force index(ix)", " ignore index(ix)"} {
						check("select payload from candidate_errors"+hint+" where name like 'abc%' and name in (regexp_replace('x','[','y'),'abc1')", !strings.Contains(hint, "ignore"))
						// Preserve the baseline error domain for each hint: an ordinary
						// IN lookup can avoid a throwing residual on excluded rows.
						check("select payload from residual_errors"+hint+" where name like 'abc%' and name in ('zzz','xy') and payload=regexp_replace('x','[','y')", strings.Contains(hint, "ignore"))
					}
					for _, scoped := range []struct{ hint, suffix string }{
						{" force index for order by(ix)", " order by name"},
						{" force index for group by(ix)", " group by name"},
					} {
						check("select name from candidate_errors"+scoped.hint+" where name like 'abc%' and name in (regexp_replace('x','[','y'),'abc1')"+scoped.suffix, false)
					}
				})

				for _, tc := range []struct {
					pattern string
					want    []int64
					gap     bool
				}{{"'abc%'", []int64{0, 1, 2, 3}, false}, {"'ab\\0%'", []int64{9}, false}, {"'\x7f%'", []int64{11, 12}, true}, {"'中文%'", []int64{7, 8}, false}} {
					q := "select count(*) from t_unique force index(ux) where name like " + tc.pattern
					plan := explain(q)
					require.Contains(t, plan, "Index Table Scan")
					require.Contains(t, plan, "in_range")
					if tc.gap {
						require.Contains(t, plan, "prefix_eq")
					} else {
						require.NotContains(t, plan, "prefix_eq")
					}
					var actual, control int64
					require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&actual))
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from t_unique ignore index(ux) where concat(name,'') like "+tc.pattern).Scan(&control))
					require.Equal(t, int64(len(tc.want)), actual)
					require.Equal(t, actual, control)
				}

				for _, bound := range []string{"name='rare'", "name between 'rar' and 'ras'"} {
					q := "select id+length(payload)-1 from residual_rows where " + bound + " and amount=other order by id"
					require.Contains(t, explain(q), "Join Type: INDEX", "automatic sparse noncovering access remains executable")
					require.Equal(t, []int64{1}, ids(q))
					require.Equal(t, []int64{1}, ids("select id+length(payload)-1 from residual_rows ignore index(ix) where "+bound+" and amount=other order by id"))
				}

				for _, predicate := range []string{
					"name='abc' and amount like 'a%' escape 'xx'",
					"name like '\x7f%' and amount=regexp_replace('x','[','y')",
				} {
					for _, projection := range []string{"count(payload)", "count(amount)"} {
						for _, hint := range []string{" force index(ix)", " ignore index(ix)"} {
							q := "select " + projection + " from error_rows" + hint + " where " + predicate
							if strings.Contains(hint, "force") {
								if strings.HasPrefix(predicate, "name=") {
									require.Contains(t, explain(q), "Index Table Scan")
								} else {
									require.NotContains(t, explain(q), "Index Table Scan", "throwing residual excludes new prefix access")
								}
							}
							var count int64
							require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&count), q)
							require.Zero(t, count)
						}
					}
				}

				require.Equal(t, []int64{3}, ids("select id from t where name like 'abc%' order by name,id limit 1 offset 1"))
			})
		}
		stmt, err := conn.PrepareContext(ctx, "select id from t where name like ? order by id")
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct {
			pattern  any
			expected []int64
		}{
			{"abc%", []int64{0, 1, 2, 3}}, {"%def", []int64{2, 3}}, {nil, []int64{}}, {`abc\_%`, []int64{2}}, {"中文%", []int64{7, 8}}, {"abd%", []int64{4}},
		} {
			got, err := readPreparedContractIDs(stmt.QueryContext(ctx, tc.pattern))
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		}

		t.Run("prepared prefix and IN reuse", func(t *testing.T) {
			statement, err := conn.PrepareContext(ctx, "select id+length(payload)-7 from t ignore index(ix_multi) where name like 'abc%' and name in (?,?) order by id")
			require.NoError(t, err)
			defer statement.Close()
			for _, tc := range []struct {
				left, right any
				want        []int64
			}{
				{"abc", "abcd", []int64{0, 1}}, {"abc", "abd", []int64{0}}, {nil, "abcd", []int64{1}}, {"zzz", "中文", []int64{}},
			} {
				got, err := readPreparedContractIDs(statement.QueryContext(ctx, tc.left, tc.right))
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})

		exec("begin")
		exec("update t set name='abcd_new' where id=4")
		require.NotContains(t, explain(query), "Index Table Scan", "workspace changes cannot use the published population sample")
		require.Equal(t, []int64{1, 4}, ids("select id from t where name like 'abcd%' order by id"))
		exec("rollback")
		require.Equal(t, []int64{1}, ids("select id from t where name like 'abcd%' order by id"))

	})
}
