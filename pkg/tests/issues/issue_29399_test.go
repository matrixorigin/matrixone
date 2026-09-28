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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue29399AccountPITRRebindsPrivileges(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()

		const (
			accountName  = "issue_29399_pitr"
			pitrName     = "issue_29399_account_pitr"
			databaseName = "issue_29399_pitr_db"
		)
		execSQLMaybe(t, ctx, sysDB, "drop account if exists `"+accountName+"`")
		execSQLRequire(t, ctx, sysDB,
			"create account `"+accountName+"` admin_name 'admin' identified by '111'")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+accountName+"`")
		}()

		adminDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port,
		))
		require.NoError(t, err)
		defer adminDB.Close()
		execSQLRequire(t, ctx, adminDB, "create pitr "+pitrName+" for account range 1 'h'")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, adminDB, "drop pitr if exists "+pitrName)
		}()

		execSQLRequire(t, ctx, adminDB, "create database `"+databaseName+"`")
		execSQLRequire(t, ctx, adminDB,
			"create table `"+databaseName+"`.orders (id int primary key)")
		execSQLRequire(t, ctx, adminDB, "insert into `"+databaseName+"`.orders values (1)")
		execSQLRequire(t, ctx, adminDB, "create role pitr_reader")
		execSQLRequire(t, ctx, adminDB,
			"create user pitr_user identified by '111' default role pitr_reader")
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to pitr_reader")
		execSQLRequire(t, ctx, adminDB,
			"grant select on table `"+databaseName+"`.orders to pitr_reader")
		execSQLRequire(t, ctx, adminDB, "grant pitr_reader to pitr_user")

		// PITR timestamps have second precision. Waiting through one server-side
		// second makes the chosen timestamp strictly newer than all setup commits;
		// this is part of the SQL timestamp contract, not scheduler coordination.
		var slept int
		require.NoError(t, adminDB.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var restoreAt string
		// The PITR parser accepts second-precision timestamps.  Derive the
		// boundary by truncating a microsecond timestamp instead of casting the
		// session's default-FSP CURRENT_TIMESTAMP: the latter is rounded to the
		// nearest second and can occasionally point one second into the future.
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&restoreAt))

		execSQLRequire(t, ctx, adminDB,
			"revoke select on table `"+databaseName+"`.orders from pitr_reader")
		execSQLRequire(t, ctx, adminDB,
			"restore from pitr "+pitrName+" '"+restoreAt+"'")

		var restoredLogicalID, restoredPrivilegeID uint64
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'orders'",
			databaseName,
		).Scan(&restoredLogicalID))
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select obj_id from mo_catalog.mo_role_privs where role_name = 'pitr_reader' "+
				"and obj_type = 'table' and privilege_level = 'd.t' and privilege_name = 'select'",
		).Scan(&restoredPrivilegeID))
		require.Equal(t, restoredLogicalID, restoredPrivilegeID)

		readerDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#pitr_user#pitr_reader:111@tcp(127.0.0.1:%d)/", accountName, port,
		))
		require.NoError(t, err)
		defer readerDB.Close()
		var count int
		require.NoError(t, readerDB.QueryRowContext(ctx,
			"select count(*) from `"+databaseName+"`.orders").Scan(&count))
		require.Equal(t, 1, count)
	})
}
