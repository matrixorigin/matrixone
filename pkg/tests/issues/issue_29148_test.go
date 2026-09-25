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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

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
