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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPreparedZeroLimitReuse(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists prepared_zero_limit")
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		query := func(q string, args ...any) int {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q, args...)
			require.NoError(t, err, q)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			require.NotEmpty(t, cols, "metadata retained for empty results")
			n := 0
			for rows.Next() {
				n++
			}
			require.NoError(t, rows.Err())
			return n
		}
		for round := 0; round < 2; round++ {
			exec("create database prepared_zero_limit")
			exec("use prepared_zero_limit")
			exec("create table t(k int primary key,v int)")
			exec("insert into t values(1,10),(2,20),(3,30)")
			queries := []string{"select a.k,b.v from t a join t b on a.k=b.k order by a.k limit ?", "select v,count(*) from t group by v order by v limit ?", "with recursive r(n) as (select 1 union all select n+1 from r where n<3) select n from r limit ?"}
			for _, q := range append([]string(nil), queries...) {
				queries = append(queries, "("+q+")", "(("+q+"))")
			}
			for _, q := range queries {
				func() {
					stmt, err := conn.PrepareContext(ctx, q)
					require.NoError(t, err)
					defer stmt.Close()
					for _, limit := range []int{2, 0, 1, 0, 2} {
						rows, err := stmt.QueryContext(ctx, limit)
						require.NoError(t, err)
						func() {
							defer rows.Close()
							cols, err := rows.Columns()
							require.NoError(t, err)
							require.NotEmpty(t, cols)
							n := 0
							for rows.Next() {
								n++
							}
							require.NoError(t, rows.Err())
							require.Equal(t, limit, n, q)
						}()
					}
				}()
				exec("prepare z from '" + q + "'")
				func() {
					defer conn.ExecContext(context.Background(), "deallocate prepare z")
					for _, limit := range []int{2, 0, 1, 0, 2} {
						exec(fmt.Sprintf("set @lim=%d", limit))
						require.Equal(t, limit, query("execute z using @lim"), q)
					}
				}()
			}
			require.Zero(t, query("select a.k,b.v from t a join t b on a.k=b.k limit 0"))
			for _, q := range []string{"select sql_calc_found_rows k from t order by k limit ?", "(select sql_calc_found_rows k from t order by k limit ?)"} {
				exec("prepare counted from '" + q + "'")
				exec("set @lim=0")
				require.Zero(t, query("execute counted using @lim"))
				var found int
				require.NoError(t, conn.QueryRowContext(ctx, "select found_rows()").Scan(&found))
				require.Equal(t, 3, found)
				exec("deallocate prepare counted")
			}
			exec("prepare z from 'select k from t order by k limit ?'")
			exec("set @lim=-1")
			_, err = conn.ExecContext(ctx, "execute z using @lim")
			require.Error(t, err)
			exec("set @lim=1")
			require.Equal(t, 1, query("execute z using @lim"))
			exec("deallocate prepare z")
			exec("drop database prepared_zero_limit")
			var left int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where datname='prepared_zero_limit'").Scan(&left))
			require.Zero(t, left, "cleanup before same-instance rerun")
		}
	})
}
