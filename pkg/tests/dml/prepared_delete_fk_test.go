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

package dml

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

func TestPreparedDeleteForeignKeyChecks(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		for _, protocol := range []string{"text", "binary", "direct"} {
			t.Run(protocol, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
				defer cancel()
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				exec := func(query string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				count := func(query string) int {
					t.Helper()
					var n int
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&n), query)
					return n
				}

				exec("create database prepared_delete_fk")
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					_, err := db.ExecContext(cleanup, "drop table if exists prepared_delete_fk.child")
					require.NoError(t, err)
					_, err = db.ExecContext(cleanup, "drop database prepared_delete_fk")
					require.NoError(t, err)
				}()
				exec("use prepared_delete_fk")
				exec("create table parent (id int primary key)")
				exec("create table child (id int primary key, parent_id int, foreign key (parent_id) references parent(id) on delete restrict)")
				exec("insert into parent values (1), (2)")
				exec("insert into child values (10, 1)")
				exec("set foreign_key_checks = 0")

				const deleteParent = "delete from parent where id = 1"
				var execute func() (sql.Result, error)
				switch protocol {
				case "text":
					exec("prepare delete_parent from '" + deleteParent + "'")
					defer exec("deallocate prepare delete_parent")
					execute = func() (sql.Result, error) {
						return conn.ExecContext(ctx, "execute delete_parent")
					}
				case "binary":
					stmt, err := conn.PrepareContext(ctx, deleteParent)
					require.NoError(t, err)
					defer func() { require.NoError(t, stmt.Close()) }()
					execute = func() (sql.Result, error) {
						return stmt.ExecContext(ctx)
					}
				case "direct":
					execute = func() (sql.Result, error) {
						return conn.ExecContext(ctx, deleteParent)
					}
				}

				for _, checks := range []int{0, 1, 0, 1} {
					exec(fmt.Sprintf("set foreign_key_checks = %d", checks))
					result, err := execute()
					if checks == 1 {
						var mysqlErr *mysql.MySQLError
						require.ErrorAs(t, err, &mysqlErr)
						require.Equal(t, uint16(20101), mysqlErr.Number)
						require.Contains(t, mysqlErr.Message, "Cannot delete or update a parent row: a foreign key constraint fails")
						require.Equal(t, 1, count("select count(*) from parent where id = 1"))
					} else {
						require.NoError(t, err)
						rows, err := result.RowsAffected()
						require.NoError(t, err)
						require.EqualValues(t, 1, rows)
						require.Zero(t, count("select count(*) from parent where id = 1"))
						exec("insert into parent values (1)")
					}
					require.Equal(t, 1, count("select count(*) from parent where id = 2"))
					require.Equal(t, 1, count("select count(*) from child where id = 10 and parent_id = 1"))
				}
			})
		}
	})
}
