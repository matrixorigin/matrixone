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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Reuse the shared SQL fixture. Only the orphan catalog precondition is seeded
// internally: all SHOWs and healthy publication/subscription setup use MySQL.
// This does not attempt to reproduce the separate DROP ACCOUNT lifecycle bug.
func TestShowPublicationsMissingAccountsSQL(t *testing.T) {
	RunBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer sys.Close()
		internal := cn.RawService().(cnservice.Service).GetSQLExecutor()
		internalSQL := func(ctx context.Context, statement string) error {
			result, err := internal.Exec(ctx, statement, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
			if err == nil {
				result.Close()
			}
			return err
		}
		const account = "showpub_live"
		const database = "showpub_source"
		const orphanID = 2000000000
		const orphanPredicate = "sub_account_id=2000000000 and pub_account_id=0 and pub_name in ('showpub_mixed','showpub_orphan')"
		for pass := 0; pass < 2; pass++ {
			if !t.Run(fmt.Sprintf("pass_%d", pass), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
				defer cancel()
				exec := func(statement string) {
					t.Helper()
					_, err := sys.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				count := func(ctx context.Context, statement string) int {
					t.Helper()
					var n int
					require.NoError(t, sys.QueryRowContext(ctx, statement).Scan(&n))
					return n
				}
				// Assert the prior pass's cleanup; entry DROP IF EXISTS would mask it.
				require.Zero(t, count(ctx, "select count(*) from mo_catalog.mo_account where account_name='showpub_live' or account_id=2000000000"))
				require.Zero(t, count(ctx, "select count(*) from mo_catalog.mo_pubs where account_id=0 and pub_name in ('showpub_mixed','showpub_orphan')"))
				require.Zero(t, count(ctx, "select count(*) from mo_catalog.mo_database where account_id=0 and datname='showpub_source'"))
				defer func() {
					cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
					defer stop()
					if err := internalSQL(cleanup, "delete from mo_catalog.mo_subs where "+orphanPredicate); err != nil {
						t.Errorf("cleanup orphan subscriptions: %v", err)
					}
					for _, statement := range []string{
						"drop account if exists " + account,
						"drop publication if exists showpub_mixed",
						"drop publication if exists showpub_orphan",
						"drop database if exists " + database,
					} {
						_, err := sys.ExecContext(cleanup, statement)
						if err != nil {
							t.Errorf("cleanup %q: %v", statement, err)
						}
					}
					require.Zero(t, count(cleanup, "select count(*) from mo_catalog.mo_subs where pub_account_id=0 and pub_name in ('showpub_mixed','showpub_orphan')"))
					require.Zero(t, count(cleanup, "select count(*) from mo_catalog.mo_account where account_name='showpub_live'"))
					require.Zero(t, count(cleanup, "select count(*) from mo_catalog.mo_pubs where account_id=0 and pub_name in ('showpub_mixed','showpub_orphan')"))
					require.Zero(t, count(cleanup, "select count(*) from mo_catalog.mo_database where account_id=0 and datname='showpub_source'"))
				}()
				exec("create account " + account + " admin_name 'admin' identified by '111'")
				exec("create database " + database)
				exec("create table " + database + ".t (id int primary key)")
				exec("insert into " + database + ".t values (7)")
				for _, pub := range []string{"showpub_mixed", "showpub_orphan"} {
					exec("create publication " + pub + " database " + database + " account " + account)
				}
				subscriber, err := sql.Open("mysql", fmt.Sprintf("%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", account, cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer subscriber.Close()
				_, err = subscriber.ExecContext(ctx, "create database live_sub from sys publication showpub_mixed")
				require.NoError(t, err)
				show := func(statement string) map[string][8]sql.NullString {
					t.Helper()
					rows, err := sys.QueryContext(ctx, statement)
					require.NoError(t, err)
					defer rows.Close()
					columns, err := rows.Columns()
					require.NoError(t, err)
					require.Equal(t, []string{"publication", "database", "tables", "sub_account", "subscribed_accounts", "create_time", "update_time", "comments"}, columns)
					result := make(map[string][8]sql.NullString)
					for rows.Next() {
						var values [8]sql.NullString
						require.NoError(t, rows.Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7]))
						if strings.HasPrefix(values[0].String, "showpub_") {
							result[values[0].String] = values
						}
					}
					require.NoError(t, rows.Err())
					return result
				}
				before := show("show publications like 'showpub_%'")
				require.Len(t, before, 2)
				require.Equal(t, account, before["showpub_mixed"][4].String)
				require.Equal(t, "", before["showpub_orphan"][4].String)
				for _, pub := range []string{"showpub_mixed", "showpub_orphan"} {
					// The stored name matches a live account, but this ID does not.
					// Looking up by name would incorrectly inherit its subscription.
					statement := fmt.Sprintf("insert into mo_catalog.mo_subs (sub_account_id,sub_account_name,sub_name,sub_time,pub_account_id,pub_account_name,pub_name,pub_database,pub_tables,pub_time,pub_comment,status) values (%d,'%s','%s','2026-01-01 00:00:00',0,'sys','%s','%s','*','2026-01-01 00:00:00','',1)", orphanID, account, "lost_"+pub, pub, database)
					require.NoError(t, internalSQL(ctx, statement))
				}
				require.Equal(t, 2, count(ctx, "select count(*) from mo_catalog.mo_subs where "+orphanPredicate))
				require.Equal(t, before, show("show publications"))
				require.Equal(t, before, show("show publications like 'showpub_%'"))
				// SHOW is read-only, and the healthy subscriber still reads its data.
				require.Equal(t, 2, count(ctx, "select count(*) from mo_catalog.mo_subs where "+orphanPredicate))
				var value int
				require.NoError(t, subscriber.QueryRowContext(ctx, "select id from live_sub.t").Scan(&value))
				require.Equal(t, 7, value)
			}) {
				return
			}
		}
	})
}
