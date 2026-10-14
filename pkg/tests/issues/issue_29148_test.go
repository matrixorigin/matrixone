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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

func TestIssue29148PreparedOwnerPolicyLifecycle(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		sourceCN, err := c.GetCNService(0)
		require.NoError(t, err)
		targetCN, err := c.GetCNService(1)
		require.NoError(t, err)
		require.NotEqual(t, sourceCN.ServiceID(), targetCN.ServiceID())

		for _, tc := range []struct {
			name    string
			migrate bool
		}{
			{name: "string"},
			{name: "variable"},
			{name: "typed"},
			{name: "sql_migration", migrate: true},
			{name: "procedure_migration", migrate: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dbName := "issue29148_owner_" + tc.name
				roleName, userName := dbName+"_role", dbName+"_user"
				adminDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", sourceCN.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, adminDB.Close()) })
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					execSQLMaybe(t, cleanup, adminDB, "drop user if exists "+userName)
					execSQLMaybe(t, cleanup, adminDB, "drop role if exists "+roleName)
					execSQLMaybe(t, cleanup, adminDB, "drop database if exists "+dbName)
				})
				execSQLRequire(t, ctx, adminDB, "create database "+dbName)
				execSQLRequire(t, ctx, adminDB, "create table "+dbName+".t(id int, tenant int)")
				execSQLRequire(t, ctx, adminDB, "insert into "+dbName+".t values (1,1),(2,2)")
				execSQLRequire(t, ctx, adminDB, "create role "+roleName)
				execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)
				execSQLRequire(t, ctx, adminDB, "grant select on table "+dbName+".t to "+roleName)
				// CALL currently uses the database CREATE VIEW privilege contract.
				execSQLRequire(t, ctx, adminDB, "grant create view on database "+dbName+" to "+roleName)
				execSQLRequire(t, ctx, adminDB, "create user "+userName+" identified by '123456' default role "+roleName)

				innerSQL := "select id from " + dbName + ".t order by id"
				body := "prepare owner_handle from ''" + innerSQL + "'';"
				switch tc.name {
				case "variable":
					body = "set @owner_sql = ''" + innerSQL + "''; prepare owner_handle from @owner_sql;"
				case "typed":
					body = "prepare owner_handle from " + innerSQL + ";"
				}
				execSQLRequire(t, ctx, adminDB, "create procedure "+dbName+".make_handle() 'begin "+body+" end'")
				openUser := func(cn embed.ServiceOperator) *sql.Conn {
					t.Helper()
					db, err := sql.Open("mysql", fmt.Sprintf("%s:123456@tcp(127.0.0.1:%d)/%s", userName, cn.GetServiceConfig().CN.Frontend.Port, dbName))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					conn, err := db.Conn(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, conn.Close()) })
					_, err = conn.ExecContext(ctx, "set enable_remap_hint = 0")
					require.NoError(t, err)
					return conn
				}
				conn := openUser(sourceCN)
				prepare := "call " + dbName + ".make_handle()"
				if tc.name == "sql_migration" {
					prepare = "prepare owner_handle from '" + innerSQL + "'"
				}
				_, err = conn.ExecContext(ctx, prepare)
				require.NoError(t, err)
				assertRows := func(statement string, want []int) {
					t.Helper()
					rows, err := conn.QueryContext(ctx, statement)
					require.NoError(t, err, statement)
					defer rows.Close()
					var got []int
					for rows.Next() {
						var id int
						require.NoError(t, rows.Scan(&id))
						got = append(got, id)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Equal(t, want, got, statement)
				}
				assertRows("execute owner_handle", []int{1, 2})

				if tc.migrate {
					target := openUser(targetCN)
					migrateIssue29148PreparedConnection(t, ctx, sourceCN, targetCN, conn, target)
					conn = target
					// No PREPARE on the target: this handle must have come from RPC replay.
					assertRows("execute owner_handle", []int{1, 2})
				}
				_, err = conn.ExecContext(ctx, "set role issue29148_missing_owner_role")
				require.Error(t, err)
				assertRows("execute owner_handle", []int{1, 2})
				execSQLRequire(t, ctx, adminDB, "alter role "+roleName+" add rule \"select id from "+dbName+".t where tenant = 1\" on table "+dbName+".t")
				_, err = conn.ExecContext(ctx, "set role issue29148_missing_owner_role")
				require.Error(t, err)
				assertRows(innerSQL, []int{1})
				_, err = conn.ExecContext(ctx, "execute owner_handle")
				requireNeedReprepare(t, err)
				_, err = conn.ExecContext(ctx, "deallocate prepare owner_handle")
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, prepare)
				require.NoError(t, err)
				assertRows("execute owner_handle", []int{1})
				_, err = conn.ExecContext(ctx, "deallocate prepare owner_handle")
				require.NoError(t, err)
			})
		}
	})
}

func migrateIssue29148PreparedConnection(t *testing.T, ctx context.Context, sourceCN, targetCN embed.ServiceOperator, source, target *sql.Conn) {
	t.Helper()
	var sourceID, targetID uint32
	require.NoError(t, source.QueryRowContext(ctx, "select connection_id()").Scan(&sourceID))
	require.NoError(t, target.QueryRowContext(ctx, "select connection_id()").Scan(&targetID))
	inventory, err := clusterservice.GetMOClusterWithContext(ctx, sourceCN.ServiceID())
	require.NoError(t, err)
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	require.NoError(t, refresher.Refresh(ctx))
	address := func(cn embed.ServiceOperator) string {
		t.Helper()
		var value string
		// Migration is an internal control-plane RPC. Both frontend connections
		// are already authenticated; use the live listener inventory directly.
		require.NoError(t, clusterservice.GetCNServiceRawWithContext(ctx, inventory,
			clusterservice.NewServiceIDSelector(cn.ServiceID()), func(service metadata.CNService) bool {
				value = service.QueryAddress
				return false
			}))
		require.NotEmpty(t, value)
		return value
	}
	sourceAddress, targetAddress := address(sourceCN), address(targetCN)
	require.NotEqual(t, sourceAddress, targetAddress)
	client, err := queryclient.NewQueryClient(sourceCN.ServiceID(), sourceCN.GetServiceConfig().CN.RPC)
	require.NoError(t, err)
	defer client.Close()
	request := client.NewRequest(query.CmdMethod_MigrateConnFrom)
	request.MigrateConnFromRequest = &query.MigrateConnFromRequest{
		ConnID: sourceID, TempTableMigrationSupported: true, LastInsertIDMigrationSupported: true,
	}
	response, err := client.SendMessage(ctx, sourceAddress, request)
	require.NoError(t, err, "export must accept a procedure-created empty-policy handle")
	defer client.Release(response)
	snapshot := response.MigrateConnFromResponse
	require.NotNil(t, snapshot)
	require.Len(t, snapshot.PrepareStmts, 1)
	require.Equal(t, "owner_handle", snapshot.PrepareStmts[0].Name)
	require.True(t, snapshot.PreparedStmtLongDataChecked)
	require.True(t, snapshot.PreparedStmtCursorsChecked)
	request = client.NewRequest(query.CmdMethod_MigrateConnTo)
	request.MigrateConnToRequest = &query.MigrateConnToRequest{
		ConnID: targetID, DB: snapshot.DB, PrepareStmts: snapshot.PrepareStmts,
		LastAffectedRows: snapshot.LastAffectedRows, LastInsertID: snapshot.LastInsertID,
		LastInsertIDExported: snapshot.LastInsertIDExported, FoundRows: snapshot.FoundRows,
		UserDefinedVars: snapshot.UserDefinedVars, UserDefinedVarsExported: snapshot.UserDefinedVarsExported,
		UserDefinedVarsReplayable: snapshot.UserDefinedVarsReplayable,
		SystemVariables:           snapshot.SystemVariables, SystemVariablesExported: snapshot.SystemVariablesExported,
		SystemVariablesReplayable: snapshot.SystemVariablesReplayable, TempTables: snapshot.TempTables,
	}
	response, err = client.SendMessage(ctx, targetAddress, request)
	require.NoError(t, err)
	defer client.Release(response)
	require.NotNil(t, response.MigrateConnToResponse)
	require.True(t, response.MigrateConnToResponse.Success)
	t.Logf("prepared handle migrated from CN %s connection %d to CN %s connection %d", sourceCN.ServiceID(), sourceID, targetCN.ServiceID(), targetID)
}

func TestIssue29148PreparedRowRuleProtocolLifecycle(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port

		const (
			dbName    = "issue29148_protocol_db"
			tableName = "t"
			roleName  = "issue29148_protocol_role"
			emptyRole = "issue29148_empty_role"
			userName  = "issue29148_protocol_user"
			password  = "123456"
		)

		adminDB, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", port))
		require.NoError(t, err)
		defer adminDB.Close()
		execSQLRequire(t, ctx, adminDB, "set role moadmin")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, adminDB, "drop user if exists "+userName)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop role if exists "+emptyRole)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop role if exists "+roleName)
			execSQLMaybe(t, cleanupCtx, adminDB, "drop database if exists "+dbName)
		}()

		execSQLRequire(t, ctx, adminDB, "drop user if exists "+userName)
		execSQLRequire(t, ctx, adminDB, "drop role if exists "+roleName)
		execSQLRequire(t, ctx, adminDB, "drop database if exists "+dbName)
		execSQLRequire(t, ctx, adminDB, "create database "+dbName)
		execSQLRequire(t, ctx, adminDB,
			"create table "+dbName+"."+tableName+" (id int, amount int, tenant int)")
		execSQLRequire(t, ctx, adminDB,
			"insert into "+dbName+"."+tableName+" values (1, 100, 1), (2, 200, 2)")
		execSQLRequire(t, ctx, adminDB, "create role "+roleName)
		execSQLRequire(t, ctx, adminDB, "create role "+emptyRole)
		execSQLRequire(t, ctx, adminDB,
			"alter role "+roleName+" add rule \"select id, amount from "+dbName+"."+tableName+" where tenant = 1\" on table "+dbName+"."+tableName)
		execSQLRequire(t, ctx, adminDB,
			"create user "+userName+" identified by '"+password+"' default role "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant "+roleName+" to "+userName)
		execSQLRequire(t, ctx, adminDB, "grant "+emptyRole+" to "+userName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+emptyRole)
		execSQLRequire(t, ctx, adminDB,
			"grant select on table "+dbName+"."+tableName+" to "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"grant select on table "+dbName+"."+tableName+" to "+emptyRole)

		userDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s:%s@tcp(127.0.0.1:%d)/?multiStatements=true&interpolateParams=false&maxAllowedPacket=1048576",
			userName, password, port))
		require.NoError(t, err)
		userDB.SetMaxOpenConns(1)
		userDB.SetMaxIdleConns(1)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()

		execOnUser := func(statement string) {
			t.Helper()
			_, execErr := userConn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		var id, amount int
		execOnUser("set enable_remap_hint = 0")
		execOnUser("set role " + roleName)
		execOnUser("prepare issue29148_disabled from 'select id, amount from " + dbName + "." + tableName + " order by id'")
		rows, err := userConn.QueryContext(ctx, "execute issue29148_disabled")
		require.NoError(t, err)
		defer rows.Close()
		var disabledRows [][2]int
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]))
			disabledRows = append(disabledRows, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, [][2]int{{1, 100}}, disabledRows,
			"mandatory role rules still filter rows when enable_remap_hint is off")

		// Changing the optional rewrite switch removes prepared handles. Keep
		// this one open and prove the client cannot execute it afterward.
		execOnUser("set enable_remap_hint = 1")
		_, err = userConn.ExecContext(ctx, "execute issue29148_disabled")
		require.Error(t, err, "a handle removed by the switch change must not execute")

		// This role has no row rules, so the request starts with a genuinely
		// disabled policy snapshot. Enabling the switch before PREPARE in the same
		// COM_QUERY must prevent that stale handle from executing in a later one.
		execOnUser("set role " + emptyRole)
		execOnUser("set enable_remap_hint = 0")
		execOnUser("set enable_remap_hint = 1; prepare issue29148_disabled_snapshot from 'select id, amount from " + dbName + "." + tableName + " order by id'")
		_, err = userConn.ExecContext(ctx, "execute issue29148_disabled_snapshot")
		requireNeedReprepare(t, err)
		execOnUser("deallocate prepare issue29148_disabled_snapshot")

		// A role switch can activate mandatory rules while the captured request
		// policy and optional switch are both disabled. Publication must inspect
		// the current role before keeping that old snapshot on a prepared handle.
		execOnUser("set enable_remap_hint = 0")
		execOnUser("set role " + emptyRole)
		execOnUser("prepare issue29148_empty_role_handle from 'select id, amount from " + dbName + "." + tableName + " order by id'")
		execOnUser("set role " + emptyRole)
		rows, err = userConn.QueryContext(ctx, "execute issue29148_empty_role_handle")
		require.NoError(t, err)
		defer rows.Close()
		var emptyRoleRows [][2]int
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]))
			emptyRoleRows = append(emptyRoleRows, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, [][2]int{{1, 100}, {2, 200}}, emptyRoleRows,
			"a disabled handle remains valid across a role switch with no row rules")

		// Failed SET ROLE also clears the cache. It must preserve the harmless
		// empty-policy case, but cannot leave that same handle executable once
		// the active role acquires a mandatory rule.
		_, err = userConn.ExecContext(ctx, "set role issue29148_nonexistent_role")
		require.Error(t, err)
		func() {
			rows, err := userConn.QueryContext(ctx, "execute issue29148_empty_role_handle")
			require.NoError(t, err)
			defer rows.Close()
			var got [][2]int
			for rows.Next() {
				var row [2]int
				require.NoError(t, rows.Scan(&row[0], &row[1]))
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][2]int{{1, 100}, {2, 200}}, got)
		}()

		func() {
			stmt, err := userConn.PrepareContext(ctx,
				"select id, amount from "+dbName+"."+tableName+" order by id")
			require.NoError(t, err)
			defer stmt.Close()
			execSQLRequire(t, ctx, adminDB,
				"alter role "+emptyRole+" add rule \"select id, amount from "+dbName+"."+tableName+" where tenant = 1\" on table "+dbName+"."+tableName)
			_, err = userConn.ExecContext(ctx, "set role issue29148_nonexistent_role")
			require.Error(t, err)

			// Binary EXECUTE arrives before any direct query can reload policy.
			// Use Exec so an unfixed server reports success instead of leaking an
			// unread Rows resource when the negative assertion fails.
			_, err = stmt.ExecContext(ctx)
			requireNeedReprepare(t, err)
			require.NoError(t, userConn.QueryRowContext(ctx,
				"select id, amount from "+dbName+"."+tableName+" order by id").Scan(&id, &amount))
			require.Equal(t, 1, id)
			require.Equal(t, 100, amount)
			_, err = userConn.ExecContext(ctx, "execute issue29148_empty_role_handle")
			requireNeedReprepare(t, err)
			fresh, err := userConn.PrepareContext(ctx,
				"select id, amount from "+dbName+"."+tableName+" order by id")
			require.NoError(t, err)
			defer fresh.Close()
			require.NoError(t, fresh.QueryRowContext(ctx).Scan(&id, &amount))
			require.Equal(t, 1, id)
			require.Equal(t, 100, amount)
		}()
		execOnUser("deallocate prepare issue29148_empty_role_handle")
		execSQLRequire(t, ctx, adminDB,
			"alter role "+emptyRole+" drop rule on table "+dbName+"."+tableName)
		execOnUser("set role " + emptyRole)
		// Independently exercise COM_QUERY's cache reload before SQL EXECUTE.
		execOnUser("prepare issue29148_failed_switch from 'select id, amount from " + dbName + "." + tableName + " order by id'")
		execSQLRequire(t, ctx, adminDB,
			"alter role "+emptyRole+" add rule \"select id, amount from "+dbName+"."+tableName+" where tenant = 1\" on table "+dbName+"."+tableName)
		_, err = userConn.ExecContext(ctx, "set role issue29148_nonexistent_role")
		require.Error(t, err)
		require.NoError(t, userConn.QueryRowContext(ctx,
			"select id, amount from "+dbName+"."+tableName+" order by id").Scan(&id, &amount))
		require.Equal(t, 1, id)
		require.Equal(t, 100, amount)
		_, err = userConn.ExecContext(ctx, "execute issue29148_failed_switch")
		requireNeedReprepare(t, err)
		execOnUser("deallocate prepare issue29148_failed_switch")
		execSQLRequire(t, ctx, adminDB,
			"alter role "+emptyRole+" drop rule on table "+dbName+"."+tableName)
		execOnUser("set role " + emptyRole)
		execOnUser("prepare issue29148_empty_role_handle from 'select id, amount from " + dbName + "." + tableName + " order by id'")

		execOnUser("set role " + roleName)
		_, err = userConn.ExecContext(ctx, "execute issue29148_empty_role_handle")
		requireNeedReprepare(t, err)
		execOnUser("deallocate prepare issue29148_empty_role_handle")

		execOnUser("set role " + emptyRole)
		execOnUser("set role " + roleName + "; prepare issue29148_new_role_rule from 'select id, amount from " + dbName + "." + tableName + " order by id'")
		_, err = userConn.ExecContext(ctx, "execute issue29148_new_role_rule")
		requireNeedReprepare(t, err)
		execOnUser("deallocate prepare issue29148_new_role_rule")

		execOnUser("set enable_remap_hint = 1")
		execOnUser("set role " + roleName)

		// Load the original role policy into this session before the administrator
		// changes it. The following SET ROLE and PREPARE are deliberately sent as
		// one COM_QUERY; EXECUTE happens in a later request.
		require.NoError(t, userConn.QueryRowContext(ctx,
			"select id, amount from "+dbName+"."+tableName+" order by id").Scan(&id, &amount))
		require.Equal(t, 1, id)
		require.Equal(t, 100, amount)

		execSQLRequire(t, ctx, adminDB,
			"alter role "+roleName+" add rule \"select id, amount from "+dbName+"."+tableName+" where tenant = 2\" on table "+dbName+"."+tableName)
		execOnUser("set role " + roleName + "; prepare issue29148_multi from 'select id, amount from " + dbName + "." + tableName + " order by id'")

		_, err = userConn.ExecContext(ctx, "execute issue29148_multi")
		requireNeedReprepare(t, err)
		execOnUser("deallocate prepare issue29148_multi")

		// Prepare through the binary protocol while the policy is current, then
		// refresh the same session again. A large parameter makes the Go MySQL
		// driver emit COM_STMT_SEND_LONG_DATA before COM_STMT_EXECUTE.
		func() {
			stmt, err := userConn.PrepareContext(ctx,
				"select id, amount from "+dbName+"."+tableName+" where amount > ? order by id")
			require.NoError(t, err)
			defer stmt.Close()

			adminRule := "alter role " + roleName + " add rule \"select id, amount from " + dbName + "." + tableName + " where tenant = 1\" on table " + dbName + "." + tableName
			execSQLRequire(t, ctx, adminDB, adminRule)
			execOnUser("set role " + roleName)

			_, err = stmt.ExecContext(ctx, []byte(strings.Repeat("x", 700*1024)))
			requireNeedReprepare(t, err)
		}()

		// The execute error must be the only response consumed for the failed
		// request. A normal command immediately afterward proves that the
		// no-response long-data command did not desynchronize the connection.
		var one int
		require.NoError(t, userConn.QueryRowContext(ctx, "select 1").Scan(&one))
		require.Equal(t, 1, one)

		reprepared, err := userConn.PrepareContext(ctx,
			"select id, amount from "+dbName+"."+tableName+" order by id")
		require.NoError(t, err)
		defer reprepared.Close()
		require.NoError(t, reprepared.QueryRowContext(ctx).Scan(&id, &amount))
		require.Equal(t, 1, id)
		require.Equal(t, 100, amount)
	})
}

func requireNeedReprepare(t *testing.T, err error) {
	t.Helper()
	var mysqlErr *mysqlDriver.MySQLError
	require.Error(t, err)
	require.True(t, errors.As(err, &mysqlErr), "expected a MySQL protocol error, got %T: %v", err, err)
	require.Equal(t, moerr.ER_NEED_REPREPARE, mysqlErr.Number)
	require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
}
