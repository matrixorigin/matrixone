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
		for i, value := range []any{"abc", "abcd", "abc_def", "abc%def", "abd", "ab", "", "中文", "中文尾", "ab\x00c", nil} {
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
		equalityQuery := "select name from t where name like 'abc%' and name='abc'"
		require.NotContains(t, explain(equalityQuery), "Join Type: INDEX", "a new prefix must not displace the existing covering equality")
		require.Contains(t, explain(equalityQuery), "Index Table Scan")
		query := "select count(*) from t where name like 'abcd%'"
		require.NotContains(t, explain(query), "Index Table Scan", "unknown frequency keeps automatic access conservative")
		exec("analyze table t(name) fullscan")
		require.NotContains(t, explain(equalityQuery), "Join Type: INDEX")
		require.Contains(t, explain(equalityQuery), "Index Table Scan")
		require.Contains(t, explain(query), "Index Table Scan")
		require.NotContains(t, explain("select count(*) from t where name like 'abc%'"), "Index Table Scan", "a measured broad prefix must keep the scan")
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
			{"'%def'", []int64{2, 3}}, {"'ab\\0%'", []int64{9}}, {"'abc'", []int64{0}}, {"NULL", []int64{}},
		}
		for _, phase := range []string{"memory", "persisted"} {
			t.Run(phase, func(t *testing.T) {
				if phase == "persisted" {
					var indexTable string
					require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='ix' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='like_prefix_index' and relname='t') limit 1").Scan(&indexTable))
					exec("select mo_ctl('dn','flush','like_prefix_index.t')")
					exec("select mo_ctl('dn','flush','like_prefix_index." + indexTable + "')")
					var multiIndex string
					require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='ix_multi' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='like_prefix_index' and relname='t') limit 1").Scan(&multiIndex))
					exec("select mo_ctl('dn','flush','like_prefix_index." + multiIndex + "')")
					exec("analyze table t(name) fullscan")
					require.Contains(t, explain(query), "Index Table Scan")
				}
				nulQuery := "select id from t where name like 'ab\\0%' order by id"
				require.Contains(t, explain(nulQuery), "Index Table Scan")
				require.Contains(t, explain(nulQuery), "prefix_in_range")
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

		exec("begin")
		exec("update t set name='abcd_new' where id=4")
		require.NotContains(t, explain(query), "Index Table Scan", "workspace changes cannot use the published population sample")
		require.Equal(t, []int64{1, 4}, ids("select id from t where name like 'abcd%' order by id"))
		exec("rollback")
		require.Equal(t, []int64{1}, ids("select id from t where name like 'abcd%' order by id"))

	})
}
