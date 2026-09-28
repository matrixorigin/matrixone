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

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue29399AccountRestoreRollsBackInvalidPrivileges(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		openDB := func(user string) *sql.DB {
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sysDB := openDB("dump")
		const (
			source       = "issue_29399_atomic_source"
			target       = "issue_29399_atomic_target"
			goodSnapshot = "issue_29399_atomic_good"
			badSnapshot  = "issue_29399_atomic_bad"
		)
		// Register before setup so a partially constructed fixture is cleaned too.
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, statement := range []string{
				"drop snapshot if exists " + badSnapshot,
				"drop snapshot if exists " + goodSnapshot,
				"drop account if exists " + target,
				"drop account if exists " + source,
			} {
				if _, err := sysDB.ExecContext(cleanupCtx, statement); err != nil {
					t.Errorf("cleanup %q: %v", statement, err)
				}
			}
		}()
		admins := make(map[string]*sql.DB)
		for i, name := range []string{source, target} {
			execSQLRequire(t, ctx, sysDB, "create account "+name+" admin_name 'admin' identified by '111'")
			admin := openDB(name + "#admin#accountadmin")
			admins[name] = admin
			execSQLRequire(t, ctx, admin, "create database app")
			execSQLRequire(t, ctx, admin, "create table app.t(id int primary key)")
			execSQLRequire(t, ctx, admin, fmt.Sprintf("insert into app.t values (%d)", i+1))
			execSQLRequire(t, ctx, admin, "create role reader")
			execSQLRequire(t, ctx, admin, "create user u1 identified by '111' default role reader")
			execSQLRequire(t, ctx, admin, "grant connect on account * to reader")
			execSQLRequire(t, ctx, admin, "grant select on table app.t to reader")
			execSQLRequire(t, ctx, admin, "grant reader to u1")
		}
		execSQLRequire(t, ctx, sysDB, "create snapshot "+goodSnapshot+" for account "+source)

		// Client SQL cannot edit system catalogs. Use the scoped internal executor
		// only to manufacture a corrupt snapshot; the restore uses the real SQL path.
		var sourceID uint32
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = ?", source).Scan(&sourceID))
		internal := cn.RawService().(cnservice.Service).GetSQLExecutor()
		setLevel := func(level string) {
			result, err := internal.Exec(defines.AttachAccountId(ctx, sourceID),
				"update mo_catalog.mo_role_privs set privilege_level = '"+level+"' "+
					"where role_name = 'reader' and obj_type = 'table'",
				executor.Options{}.WithAccountID(sourceID))
			require.NoError(t, err)
			result.Close()
			var got string
			require.NoError(t, admins[source].QueryRowContext(ctx,
				"select privilege_level from mo_catalog.mo_role_privs where role_name = 'reader' and obj_type = 'table'").Scan(&got))
			require.Equal(t, level, got)
		}
		setLevel("invalid")
		execSQLRequire(t, ctx, sysDB, "create snapshot "+badSnapshot+" for account "+source)
		setLevel("d.t")
		execSQLRequire(t, ctx, admins[source], "update app.t set id = 99")

		for _, destination := range []string{target, source} {
			t.Run(destination, func(t *testing.T) {
				admin := admins[destination]
				var beforeID, beforeGrant uint64
				var beforeValue int
				require.NoError(t, admin.QueryRowContext(ctx, "select id from app.t").Scan(&beforeValue))
				require.Equal(t, map[string]int{source: 99, target: 2}[destination], beforeValue,
					"restoring one account changed the other account")
				require.NoError(t, admin.QueryRowContext(ctx,
					"select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'app' and relname = 't'").Scan(&beforeID))
				require.NoError(t, admin.QueryRowContext(ctx,
					"select obj_id from mo_catalog.mo_role_privs where role_name = 'reader' and obj_type = 'table'").Scan(&beforeGrant))
				restoreSQL := func(snapshot string) string {
					return "restore account " + source + " {snapshot = '" + snapshot + "'} to account " + destination
				}
				_, err := sysDB.ExecContext(ctx, restoreSQL(badSnapshot))
				require.ErrorContains(t, err, "nonzero table or view privilege has an invalid level")
				var afterID, afterGrant uint64
				require.NoError(t, admin.QueryRowContext(ctx,
					"select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'app' and relname = 't'").Scan(&afterID))
				require.NoError(t, admin.QueryRowContext(ctx,
					"select obj_id from mo_catalog.mo_role_privs where role_name = 'reader' and obj_type = 'table'").Scan(&afterGrant))
				require.Equal(t, beforeID, afterID, "DDL was not rolled back")
				require.Equal(t, beforeGrant, afterGrant, "grants were not rolled back")

				// Fresh authenticated readers test durable state rather than cached plans.
				reader := openDB(destination + "#u1#reader")
				var afterValue int
				require.NoError(t, reader.QueryRowContext(ctx, "select id from app.t").Scan(&afterValue))
				require.Equal(t, beforeValue, afterValue)
				_, err = reader.ExecContext(ctx, "delete from app.t")
				require.ErrorContains(t, err, "do not have privilege")

				execSQLRequire(t, ctx, sysDB, restoreSQL(goodSnapshot))
				freshReader := openDB(destination + "#u1#reader")
				require.NoError(t, freshReader.QueryRowContext(ctx, "select id from app.t").Scan(&afterValue))
				require.Equal(t, 1, afterValue)
				_, err = freshReader.ExecContext(ctx, "delete from app.t")
				require.ErrorContains(t, err, "do not have privilege")
			})
		}
	})
}

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
		_, err = adminDB.ExecContext(ctx, "restore from pitr "+pitrName+" '2000-01-01 00:00:00'")
		require.ErrorContains(t, err, "is less than the pitr valid time")
		var grantCount, rowCount int
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_role_privs where role_name = 'pitr_reader' "+
				"and obj_type = 'table' and privilege_name = 'select'").Scan(&grantCount))
		require.Zero(t, grantCount, "rejected PITR must not resurrect a revoked grant")
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select count(*) from `"+databaseName+"`.orders").Scan(&rowCount))
		require.Equal(t, 1, rowCount)
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
		_, err = readerDB.ExecContext(ctx, "delete from `"+databaseName+"`.orders")
		require.ErrorContains(t, err, "do not have privilege")
	})
}
