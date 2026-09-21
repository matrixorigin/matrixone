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
		execSQLRequire(t, ctx, adminDB,
			"alter role "+roleName+" add rule \"select id, amount from "+dbName+"."+tableName+" where tenant = 1\" on table "+dbName+"."+tableName)
		execSQLRequire(t, ctx, adminDB,
			"create user "+userName+" identified by '"+password+"' default role "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant "+roleName+" to "+userName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"grant select on table "+dbName+"."+tableName+" to "+roleName)

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
		execOnUser("set enable_remap_hint = 1")

		// Load the original role policy into this session before the administrator
		// changes it. The following SET ROLE and PREPARE are deliberately sent as
		// one COM_QUERY; EXECUTE happens in a later request.
		var id, amount int
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
		stmt, err := userConn.PrepareContext(ctx,
			"select id, amount from "+dbName+"."+tableName+" where amount > ? order by id")
		require.NoError(t, err)
		adminRule := "alter role " + roleName + " add rule \"select id, amount from " + dbName + "." + tableName + " where tenant = 1\" on table " + dbName + "." + tableName
		execSQLRequire(t, ctx, adminDB, adminRule)
		execOnUser("set role " + roleName)

		_, err = stmt.ExecContext(ctx, []byte(strings.Repeat("x", 700*1024)))
		requireNeedReprepare(t, err)
		require.NoError(t, stmt.Close())

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
