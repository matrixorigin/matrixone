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
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue27610PreExecutionRollback(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		schema := fmt.Sprintf("issue27610_%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		defer conn.ExecContext(ctx, "drop database "+schema)
		for _, mode := range []int{0, 1} {
			for _, kind := range []string{"syntax", "prepare"} {
				t.Run(fmt.Sprintf("%s_mode%d", kind, mode), func(t *testing.T) {
					table := fmt.Sprintf("%s.%s_%d", schema, kind, mode)
					exec := func(query string) {
						t.Helper()
						_, err := conn.ExecContext(ctx, query)
						require.NoError(t, err)
					}
					exec("create table " + table + " (id int primary key)")
					exec(fmt.Sprintf("set mo_rollback_txn_on_error=%d", mode))
					exec("begin")
					exec("insert into " + table + " values (1)")
					exec("insert into " + table + " values (2)")
					if kind == "syntax" {
						_, err = conn.ExecContext(ctx, "selec 1")
					} else {
						var stmt *sql.Stmt
						stmt, err = conn.PrepareContext(ctx, "/*+ {invalid} */ select 1")
						if stmt != nil {
							defer stmt.Close()
						}
					}
					var serverError *mysql.MySQLError
					require.ErrorAs(t, err, &serverError)
					// An observer sees this write before COMMIT only if the
					// failed request ended the explicit transaction.
					exec("insert into " + table + " values (3)")
					var visible int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where id=3").Scan(&visible))
					exec("commit")
					rows, err := db.QueryContext(ctx, "select id from "+table+" order by id")
					require.NoError(t, err)
					defer rows.Close()
					var ids []int
					for rows.Next() {
						var id int
						require.NoError(t, rows.Scan(&id))
						ids = append(ids, id)
					}
					require.NoError(t, rows.Err())
					t.Logf("observer marker=%d committed ids=%v", visible, ids)
					require.Equal(t, mode, visible)
					if mode == 0 {
						require.Equal(t, []int{1, 2, 3}, ids)
					} else {
						require.Equal(t, []int{3}, ids)
					}
				})
			}
		}
	})
}
