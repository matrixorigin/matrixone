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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue30375CTASSequence(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		for cnIndex := range 2 {
			t.Run(fmt.Sprintf("CN%d", cnIndex), func(t *testing.T) {
				cn, err := c.GetCNService(cnIndex)
				require.NoError(t, err)
				db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer db.Close()
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
				name := testutils.GetDatabaseName(t)
				exec("create database `" + name + "`")
				defer func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := conn.ExecContext(cleanupCtx, "drop database `"+name+"`")
					require.NoError(t, err)
				}()
				exec("use `" + name + "`")
				exec("create table src(k int primary key)")
				exec("insert into src values (1),(2)")
				exec("create table ordinary as select k,k+1 as v from src")
				for _, tc := range []struct{ name, query string }{
					{"projection", "create table cp as select k,nextval('s') as ticket from src order by k"},
					{"default", "create table cp(ticket bigint default nextval('s')) as select k from src order by k"},
					{"view", "create table cp as select k,ticket from v order by k"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						exec := func(query string) {
							t.Helper()
							_, err := conn.ExecContext(ctx, query)
							require.NoError(t, err, query)
						}
						exec("create sequence s as bigint increment by 1 start with 1")
						defer exec("drop sequence s")
						if tc.name == "view" {
							exec("create view v as select k,nextval('s') as ticket from src")
							defer exec("drop view v")
						}
						_, err := conn.ExecContext(ctx, tc.query)
						require.NoError(t, err, tc.query)
						defer exec("drop table cp")
						var rows, low, high, distinct, keyLow, keyHigh, keys int
						require.NoError(t, conn.QueryRowContext(ctx, "select count(*),min(ticket),max(ticket),count(distinct ticket),min(k),max(k),count(distinct k) from cp").Scan(&rows, &low, &high, &distinct, &keyLow, &keyHigh, &keys))
						require.Equal(t, []int{2, 1, 2, 2, 1, 2, 2}, []int{rows, low, high, distinct, keyLow, keyHigh, keys})
						var last int
						var called bool
						require.NoError(t, conn.QueryRowContext(ctx, "select last_seq_num,is_called from s").Scan(&last, &called))
						require.Equal(t, 2, last)
						require.True(t, called)
						var current, sessionLast int
						require.NoError(t, conn.QueryRowContext(ctx, "select currval('s'),lastval()").Scan(&current, &sessionLast))
						require.Equal(t, []int{2, 2}, []int{current, sessionLast})
						require.NoError(t, conn.QueryRowContext(ctx, "select nextval('s')").Scan(&last))
						require.Equal(t, 3, last)
					})
				}
				t.Run("empty and inherited session state", func(t *testing.T) {
					exec := func(query string) {
						t.Helper()
						_, err := conn.ExecContext(ctx, query)
						require.NoError(t, err, query)
					}
					exec("create sequence inherited start with 10")
					var value int
					require.NoError(t, conn.QueryRowContext(ctx, "select nextval('inherited')").Scan(&value))
					require.Equal(t, 10, value)
					exec("create table inherited_copy as select k,currval('inherited') as cv,lastval() as lv from src")
					var count int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from inherited_copy where k in (1,2) and cv=10 and lv=10").Scan(&count))
					require.Equal(t, 2, count)
					exec("create table empty_copy as select k,nextval('inherited') as ticket from src where k<0")
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from empty_copy").Scan(&count))
					require.Zero(t, count)
					var last int
					require.NoError(t, conn.QueryRowContext(ctx, "select last_seq_num from inherited").Scan(&last))
					require.Equal(t, 10, last)
					require.NoError(t, conn.QueryRowContext(ctx, "select currval('inherited'),lastval()").Scan(&value, &last))
					require.Equal(t, []int{10, 10}, []int{value, last})
				})
				t.Run("allocation failure rolls back CTAS", func(t *testing.T) {
					exec := func(query string) {
						t.Helper()
						_, err := conn.ExecContext(ctx, query)
						require.NoError(t, err, query)
					}
					exec("create sequence bounded minvalue 1 maxvalue 2 start with 1 no cycle")
					var value int
					require.NoError(t, conn.QueryRowContext(ctx, "select nextval('bounded')").Scan(&value))
					require.Equal(t, 1, value)
					_, err := conn.ExecContext(ctx, "create table failed_copy as select k,nextval('bounded') as ticket from src")
					var sqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &sqlErr)
					require.Contains(t, sqlErr.Message, "Reached maximum value of sequence")
					var count, last int
					require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=? and table_name='failed_copy'", name).Scan(&count))
					require.Zero(t, count)
					require.NoError(t, conn.QueryRowContext(ctx, "select last_seq_num from bounded").Scan(&last))
					require.Equal(t, 1, last)
					require.NoError(t, conn.QueryRowContext(ctx, "select currval('bounded'),lastval()").Scan(&value, &last))
					require.Equal(t, []int{1, 1}, []int{value, last})
					require.NoError(t, conn.QueryRowContext(ctx, "select nextval('bounded')").Scan(&value))
					require.Equal(t, 2, value, "failed CTAS must release sequence admission and consume no value")
					exec("create table failed_copy as select k from src")
				})
			})
		}
	})
}
