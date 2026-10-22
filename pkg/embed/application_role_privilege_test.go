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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exercise #30626 through an actual role login, keeping its existing session
// across grants/revokes. DATABASE DDL and TABLE db.* DML are distinct scopes.
func TestLeastPrivilegeApplicationRole(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		var previous string
		require.NoError(t, admin.QueryRowContext(ctx, "select @@global.enable_privilege_cache").Scan(&previous))
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := admin.ExecContext(cleanupCtx, "set global enable_privilege_cache="+previous)
			require.NoError(t, err)
		}()
		for _, cached := range []bool{true, false} {
			t.Run(fmt.Sprintf("privilege-cache=%t", cached), func(t *testing.T) {
				execAdmin := func(query string) {
					t.Helper()
					_, err := admin.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				execAdmin(fmt.Sprintf("set global enable_privilege_cache=%t", cached))
				const database = "application_grant_30626"
				const other = "application_grant_30626_other"
				const role = "application_role_30626"
				const username = "application_user_30626"
				execAdmin("create database " + database)
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					for _, query := range []string{
						"drop database if exists " + database, "drop database if exists " + other,
						"drop user if exists " + username, "drop role if exists " + role,
					} {
						_, err := admin.ExecContext(cleanupCtx, query)
						require.NoError(t, err, query)
					}
				})
				execAdmin("create database " + other)
				execAdmin("create role " + role)
				execAdmin("create user " + username + " identified by 'fixture-only'")
				execAdmin("grant " + role + " to " + username)
				execAdmin("grant all privileges on database " + database + " to " + role)
				execAdmin("create table " + database + ".root_made(USER_ID varchar(36) primary key)")
				execAdmin("create table " + other + ".denied(USER_ID varchar(36) primary key)")
				execAdmin("insert into " + database + ".root_made values ('root')")
				execAdmin("insert into " + other + ".denied values ('other')")

				user, err := sql.Open("mysql", fmt.Sprintf("sys#%s#%s:fixture-only@tcp(127.0.0.1:%d)/%s",
					username, role, cn.GetServiceConfig().CN.Frontend.Port, database))
				require.NoError(t, err)
				defer user.Close()
				conn, err := user.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				execUser := func(query string, wantAffected int64) {
					t.Helper()
					result, err := conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
					if wantAffected >= 0 {
						n, err := result.RowsAffected()
						require.NoError(t, err)
						require.Equal(t, wantAffected, n, query)
					}
				}
				deny := func(query string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, query)
					require.ErrorContains(t, err, "do not have privilege", query)
				}
				deny("use " + database)
				execUser("create table credential(USER_ID varchar(36) primary key)", -1)
				execUser("insert into credential values ('x')", 1)
				execUser("update credential set USER_ID='y' where USER_ID='x'", 1)
				execUser("delete from credential where USER_ID='y'", 1)
				var count int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from credential").Scan(&count))
				require.Zero(t, count)
				deny("delete from root_made where USER_ID='root'")
				deny("update root_made set USER_ID='changed'")

				execAdmin("grant update,delete on table " + database + ".credential to " + role)
				execAdmin("grant update,delete on table " + database + ".root_made to " + role)
				execUser("insert into credential values ('x')", 1)
				execUser("update credential set USER_ID='y' where USER_ID='x'", 1)
				execUser("delete from credential where USER_ID='y'", 1)
				execUser("update root_made set USER_ID='changed' where USER_ID='root'", 1)
				execUser("delete from root_made where USER_ID='changed'", 1)

				execAdmin("grant connect on account * to " + role)
				execUser("use "+database, -1)
				execAdmin("grant select,insert,update,delete on table " + database + ".* to " + role)
				execUser("insert into root_made values ('wildcard')", 1)
				execUser("update root_made set USER_ID='changed' where USER_ID='wildcard'", 1)
				execUser("delete from root_made where USER_ID='changed'", 1)
				deny("delete from " + other + ".denied where USER_ID='other'")
				deny("update " + other + ".denied set USER_ID='changed'")
				deny("grant select on table " + database + ".root_made to public")
				execAdmin("insert into " + database + ".root_made values ('retained')")
				execAdmin("revoke update,delete on table " + database + ".* from " + role)
				execAdmin("revoke update,delete on table " + database + ".root_made from " + role)
				deny("delete from root_made where USER_ID='retained'")
				deny("update root_made set USER_ID='changed' where USER_ID='retained'")
				require.NoError(t, admin.QueryRowContext(ctx,
					"select count(*) from "+database+".root_made where USER_ID='retained'").Scan(&count))
				require.Equal(t, 1, count)
				require.NoError(t, admin.QueryRowContext(ctx,
					"select count(*) from "+other+".denied where USER_ID='other'").Scan(&count))
				require.Equal(t, 1, count)
			})
		}
	})
}
