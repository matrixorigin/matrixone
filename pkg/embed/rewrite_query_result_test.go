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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRewritePolicySavedResult(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		adminDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer adminDB.Close()
		admin, err := adminDB.Conn(ctx)
		require.NoError(t, err)
		defer admin.Close()
		// Reuse the same names on the same CN: cleanup must work without restart.
		for round := 0; round < 2; round++ {
			t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
				exec := func(query string) {
					_, err := admin.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				cleanup := func(query string) {
					t.Cleanup(func() {
						cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
						defer done()
						_, err := admin.ExecContext(cleanupCtx, query)
						assert.NoError(t, err, query)
					})
				}
				exec("create database rewrite_saved")
				cleanup("drop database rewrite_saved")
				exec("create table rewrite_saved.t(id int, secret int)")
				exec("insert into rewrite_saved.t values (1,10),(2,20)")
				exec("create role rewrite_saved_reader")
				cleanup("drop role rewrite_saved_reader")
				exec("grant connect on account * to rewrite_saved_reader")
				exec("grant select on table rewrite_saved.t to rewrite_saved_reader")
				exec("create user rewrite_saved_user identified by '111' default role rewrite_saved_reader")
				cleanup("drop user rewrite_saved_user")
				connect := func(t *testing.T) *sql.Conn {
					db, err := sql.Open("mysql", fmt.Sprintf("rewrite_saved_user:111@tcp(127.0.0.1:%d)/", port))
					require.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, db.Close()) })
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, conn.Close()) })
					_, err = conn.ExecContext(ctx, "set save_query_result=on")
					require.NoError(t, err)
					_, err = conn.ExecContext(ctx, "set enable_remap_hint=0")
					require.NoError(t, err)
					return conn
				}
				read := func(t *testing.T, conn *sql.Conn, query string, columns []string, want [][]int) {
					rows, err := conn.QueryContext(ctx, query)
					require.NoError(t, err, query)
					defer rows.Close()
					gotColumns, err := rows.Columns()
					require.NoError(t, err)
					require.Equal(t, columns, gotColumns)
					var got [][]int
					for rows.Next() {
						row := make([]int, len(columns))
						args := make([]any, len(row))
						for i := range row {
							args[i] = &row[i]
						}
						require.NoError(t, rows.Scan(args...))
						got = append(got, row)
					}
					require.NoError(t, rows.Err())
					require.Equal(t, want, got)
				}
				var savedID string
				for i, tc := range []struct {
					name, rule string
					columns    []string
					rows       [][]int
				}{
					{"none", "", []string{"id", "secret"}, [][]int{{1, 10}, {2, 20}}},
					{"row", "select * from rewrite_saved.t where id=1", []string{"id", "secret"}, [][]int{{1, 10}}},
					{"column", "select id from rewrite_saved.t", []string{"id"}, [][]int{{1}, {2}}},
					{"combined", "select id from rewrite_saved.t where id=1", []string{"id"}, [][]int{{1}}},
				} {
					if i > 1 {
						exec("alter role rewrite_saved_reader drop rule on table rewrite_saved.t")
					}
					if tc.rule != "" {
						exec(fmt.Sprintf("alter role rewrite_saved_reader add rule %q on table rewrite_saved.t", tc.rule))
					}
					t.Run(tc.name, func(t *testing.T) {
						// Fresh sessions isolate rewrite correctness from policy-cache invalidation.
						conn := connect(t)
						markers := []string{"/* save_result */"}
						if tc.name == "combined" {
							markers = append(markers, "", "/* cloud_nonuser */", "/* save_result */")
						}
						for _, marker := range markers {
							read(t, conn, marker+" select * from rewrite_saved.t order by id", tc.columns, tc.rows)
							var id string
							require.NoError(t, conn.QueryRowContext(ctx, "select last_query_id()").Scan(&id))
							query := fmt.Sprintf("select * from result_scan('%s') as saved order by id", id)
							if marker != "/* save_result */" {
								var value int
								err := conn.QueryRowContext(ctx, query).Scan(&value)
								var mysqlErr *mysql.MySQLError
								require.ErrorAs(t, err, &mysqlErr)
								require.EqualValues(t, 20440, mysqlErr.Number)
								continue
							}
							read(t, conn, query, tc.columns, tc.rows)
							var count int
							require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("select count(*) from meta_scan('%s') as meta", id)).Scan(&count))
							require.Equal(t, 1, count)
							savedID = id
						}
						if tc.name == "column" || tc.name == "combined" {
							var secret int
							err := conn.QueryRowContext(ctx, "/* save_result */ select secret from rewrite_saved.t").Scan(&secret)
							require.ErrorContains(t, err, "secret")
						}
					})
				}
				exec("revoke select on table rewrite_saved.t from rewrite_saved_reader")
				t.Run("revoked", func(t *testing.T) {
					conn := connect(t)
					for _, query := range []string{"select * from rewrite_saved.t", fmt.Sprintf("select * from result_scan('%s') as saved", savedID)} {
						var value int
						err := conn.QueryRowContext(ctx, query).Scan(&value)
						require.ErrorContains(t, err, "privilege")
					}
				})
			})
		}
	})
}
