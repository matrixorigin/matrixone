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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue29133RoleRulesTrackRemoteChanges(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		// Keep client cleanup inside the shared fixture callback admission.
		t.Run("scenario", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
			defer cancel()
			var tenantPools []*sql.DB
			open := func(index int, user string) *sql.Conn {
				t.Helper()
				cn, err := c.GetCNService(index)
				require.NoError(t, err)
				db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				if user != "dump" {
					tenantPools = append(tenantPools, db)
				}
				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, conn.Close()) })
				return conn
			}
			admin := open(0, "dump")
			mustExec(t, ctx, admin, "create account rule_refresh admin_name 'admin' identified by '111'")
			// Register before tenant resources so their LIFO cleanup completes before
			// DROP ACCOUNT kills sessions. The admin connection remains usable.
			t.Cleanup(func() {
				for _, db := range tenantPools {
					assert.Zero(t, db.Stats().OpenConnections, "tenant pool still open before DROP ACCOUNT")
				}
				cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := admin.ExecContext(cleanup, "drop account rule_refresh")
				stop()
				assert.NoError(t, err)
				cleanup, stop = context.WithTimeout(context.Background(), 30*time.Second)
				defer stop()
				var remaining int
				err = admin.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_account where account_name='rule_refresh'").Scan(&remaining)
				assert.NoError(t, err)
				assert.Zero(t, remaining)
			})
			owner := open(0, "rule_refresh#admin#accountadmin")
			for _, statement := range []string{
				"create database app", "create table app.t(id int primary key, tenant int)",
				"insert into app.t values (1,1),(2,2)", "create role reader",
				"create user learner identified by '111' default role reader",
				"grant reader to learner", "grant connect on account * to reader",
				"grant select on table app.t to reader",
			} {
				mustExec(t, ctx, owner, statement)
			}
			mustExec(t, ctx, admin, "select mo_ctl('cn','synccommit','')")
			reader := open(1, "rule_refresh#learner#reader")
			binaryReader := open(1, "rule_refresh#learner#reader")
			sqlReader := open(1, "rule_refresh#learner#reader")
			const query = "select id from app.t order by id"
			checkRows := func(query func() (*sql.Rows, error), expected []int) {
				t.Helper()
				rows, err := query()
				require.NoError(t, err)
				defer rows.Close()
				var got []int
				for rows.Next() {
					var id int
					require.NoError(t, rows.Scan(&id))
					got = append(got, id)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, expected, got)
			}
			check := func(conn *sql.Conn, expected []int) {
				t.Helper()
				checkRows(func() (*sql.Rows, error) { return conn.QueryContext(ctx, query) }, expected)
			}
			prepare := func() *sql.Stmt {
				t.Helper()
				stmt, err := binaryReader.PrepareContext(ctx, query)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, stmt.Close()) })
				return stmt
			}
			checkStmt := func(stmt *sql.Stmt, expected []int) {
				t.Helper()
				checkRows(func() (*sql.Rows, error) { return stmt.QueryContext(ctx) }, expected)
			}
			checkSQL := func(expected []int) {
				t.Helper()
				checkRows(func() (*sql.Rows, error) { return sqlReader.QueryContext(ctx, "execute saved") }, expected)
			}
			needReprepare := func(query func() (*sql.Rows, error)) {
				t.Helper()
				rows, err := query()
				if rows != nil {
					defer rows.Close()
					require.NoError(t, rows.Err())
				}
				var mysqlErr *mysql.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.EqualValues(t, 1615, mysqlErr.Number)
			}
			syncCatalog := func() { mustExec(t, ctx, admin, "select mo_ctl('cn','synccommit','')") }
			// Authorization reads must remain fresh even while user data reads retain
			// an older repeatable-read transaction snapshot.
			mustExec(t, ctx, reader, "set session transaction isolation level repeatable read")
			mustExec(t, ctx, reader, "set enable_remap_hint=0")
			mustExec(t, ctx, reader, "begin")
			transactionOpen := true
			t.Cleanup(func() {
				if transactionOpen {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					_, err := reader.ExecContext(cleanup, "rollback")
					assert.NoError(t, err)
				}
			})
			check(reader, []int{1, 2})
			binary := prepare()
			checkStmt(binary, []int{1, 2})
			mustExec(t, ctx, sqlReader, "prepare saved from '"+query+"'")
			checkSQL([]int{1, 2})
			// Unrelated DDL changes the broad catalog proof without changing policy.
			mustExec(t, ctx, owner, "create table app.unrelated(id int)")
			syncCatalog()
			checkStmt(binary, []int{1, 2})
			checkSQL([]int{1, 2})

			for _, transition := range []struct {
				name, ddl string
				expected  []int
			}{
				{"add", "alter role reader add rule 'select id, tenant from app.t where tenant=1' on table app.t", []int{1}},
				{"replace", "alter role reader add rule 'select id, tenant from app.t where tenant=2' on table app.t", []int{2}},
				{"drop", "alter role reader drop rule on table app.t", []int{1, 2}},
			} {
				t.Logf("remote policy transition: %s", transition.name)
				mustExec(t, ctx, owner, transition.ddl)
				syncCatalog()
				// The first binary request after remote commit must refresh too, without
				// relying on a preceding text command on that connection.
				needReprepare(func() (*sql.Rows, error) { return binary.QueryContext(ctx) })
				needReprepare(func() (*sql.Rows, error) { return sqlReader.QueryContext(ctx, "execute saved") })
				check(open(1, "rule_refresh#learner#reader"), transition.expected)
				check(reader, transition.expected)
				check(reader, transition.expected)
				require.NoError(t, binary.Close())
				binary = prepare()
				checkStmt(binary, transition.expected)
				mustExec(t, ctx, sqlReader, "deallocate prepare saved")
				mustExec(t, ctx, sqlReader, "prepare saved from '"+query+"'")
				checkSQL(transition.expected)
			}
			var isolation string
			require.NoError(t, reader.QueryRowContext(ctx, "select @@session.transaction_isolation").Scan(&isolation))
			require.Equal(t, "REPEATABLE-READ", isolation)
			// A failed proof may fall back to a fresh rule read, but failure of the
			// mandatory rule table itself must reject every protocol, even warm ones.
			t.Run("rule_catalog_fault", func(t *testing.T) {
				func() {
					if fault.Enable() {
						defer fault.Disable()
					}
					remove, err := objectio.InjectLogging(objectio.FJ_CNSubscribeTableFail, "mo_catalog", "mo_role_rule", 0, true)
					require.NoError(t, err)
					defer remove()
					for _, request := range []func() (*sql.Rows, error){
						func() (*sql.Rows, error) { return reader.QueryContext(ctx, query) },
						func() (*sql.Rows, error) { return sqlReader.QueryContext(ctx, "execute saved") },
						func() (*sql.Rows, error) { return binary.QueryContext(ctx) },
					} {
						func() {
							rows, err := request()
							if rows != nil {
								defer rows.Close()
								require.NoError(t, rows.Err())
							}
							require.ErrorContains(t, err, "injected subscribe table err")
						}()
					}
				}()
				check(reader, []int{1, 2})
				needReprepare(func() (*sql.Rows, error) { return binary.QueryContext(ctx) })
				needReprepare(func() (*sql.Rows, error) { return sqlReader.QueryContext(ctx, "execute saved") })
			})
			mustExec(t, ctx, reader, "rollback")
			transactionOpen = false
		})
	})
}
