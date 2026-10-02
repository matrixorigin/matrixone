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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue29399SQLTaskDefinerAuthorization(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		open := func(user string) *sql.DB {
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys := open("dump")
		const account = "issue_29399_task_definer"
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			execSQLRequire(t, cleanup, sys, "drop account if exists "+account)
		}()
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		var accountID uint32
		require.NoError(t, sys.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		admin := open(account + "#admin#accountadmin")
		for _, statement := range []string{
			"create database app", "create table app.t(id int)", "insert into app.t values(1)",
			"create role reader", "grant connect on account * to reader", "grant reader to admin",
			"create user ordinary identified by '111' default role reader", "grant reader to ordinary",
			"grant select on table app.t to reader",
		} {
			execSQLRequire(t, ctx, admin, statement)
		}
		const revoke = "revoke select on table app.t from reader"
		const grant = "grant select on table app.t to reader"
		grantCount := func(want int) {
			var got int
			require.NoError(t, admin.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_role_privs where role_name='reader' and obj_type='table' and privilege_name='select'").Scan(&got))
			require.Equal(t, want, got)
		}
		execSQLRequire(t, ctx, admin, revoke)
		grantCount(0)
		execSQLRequire(t, ctx, admin, grant)
		execSQLRequire(t, ctx, admin, "create task task_insert as begin insert into app.t values(2); end;")
		execSQLRequire(t, ctx, admin, "execute task task_insert")
		var count int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from app.t").Scan(&count))
		require.Equal(t, 2, count)
		t.Run("accountadmin task changes the actual grant", func(t *testing.T) {
			execSQLRequire(t, ctx, admin, "create task task_revoke as begin "+revoke+"; end;")
			execSQLRequire(t, ctx, admin, "execute task task_revoke")
			grantCount(0)
			execSQLRequire(t, ctx, admin, grant)
		})
		t.Run("SET ROLE supersedes the login role", func(t *testing.T) {
			conn, err := open(account + "#admin#reader").Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			_, err = conn.ExecContext(ctx, "set role accountadmin")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create task task_selected_role as begin "+revoke+"; end;")
			require.NoError(t, err)
			var storedAccount, storedRole uint32
			var creator string
			require.NoError(t, sys.QueryRowContext(ctx, "select account_id, creator, creator_role_id from mo_task.sql_task where task_name='task_selected_role'").Scan(&storedAccount, &creator, &storedRole))
			require.Equal(t, accountID, storedAccount)
			require.Equal(t, uint32(2), storedRole)
			require.Equal(t, account+"#admin#reader", creator, "login role must remain distinct from selected role")
			_, err = conn.ExecContext(ctx, "execute task task_selected_role")
			require.NoError(t, err)
			grantCount(0)
			execSQLRequire(t, ctx, admin, grant)
		})
		var adminID, ordinaryID, readerID uint32
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='admin'").Scan(&adminID))
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='ordinary'").Scan(&ordinaryID))
		require.NoError(t, admin.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='reader'").Scan(&readerID))
		internal := frontend.NewInternalExecutor(cn.ServiceID())
		for _, tc := range []struct {
			name, creator string
			user, role    uint32
			allowed       bool
		}{
			{"bare username with explicit IDs", "admin", adminID, 2, true},
			{"ordinary role", account + "#ordinary#reader", ordinaryID, readerID, false},
			{"login administrator with selected ordinary role", account + "#admin#accountadmin", adminID, readerID, false},
			{"administrator role with mismatched username and user ID", account + "#ordinary#accountadmin", adminID, 2, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				task := taskservice.SQLTask{AccountID: accountID, Creator: tc.creator, CreatorUserID: tc.user, CreatorRoleID: tc.role}
				err := internal.Exec(defines.AttachAccount(ctx, accountID, tc.user, tc.role), revoke, taskservice.DefinerOpts(task))
				if tc.allowed {
					require.NoError(t, err)
					grantCount(0)
					execSQLRequire(t, ctx, admin, grant)
				} else {
					require.ErrorContains(t, err, "do not have privilege")
					grantCount(1)
				}
			})
		}
		t.Run("current membership remains required", func(t *testing.T) {
			task := taskservice.SQLTask{AccountID: accountID, Creator: account + "#ordinary#reader", CreatorUserID: ordinaryID, CreatorRoleID: readerID}
			definerCtx := defines.AttachAccount(ctx, accountID, ordinaryID, readerID)
			opts := taskservice.DefinerOpts(task)
			result := internal.Query(definerCtx, "select count(*) from app.t", opts)
			require.NoError(t, result.Error())
			got, err := result.GetUint64(ctx, 0, 0)
			require.NoError(t, err)
			require.Equal(t, uint64(2), got)
			execSQLRequire(t, ctx, admin, "revoke reader from ordinary")
			require.ErrorContains(t, internal.Query(definerCtx, "select count(*) from app.t", opts).Error(), "do not have privilege")
		})
	})
}

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
				reader := openDB(destination + "#u1#reader")
				persistent, err := reader.Conn(ctx)
				require.NoError(t, err)
				defer persistent.Close()
				var warmedValue int
				require.NoError(t, persistent.QueryRowContext(ctx, "select id from app.t").Scan(&warmedValue))
				require.Equal(t, beforeValue, warmedValue)
				_, err = sysDB.ExecContext(ctx, restoreSQL(badSnapshot))
				require.ErrorContains(t, err, "nonzero table or view privilege has an invalid level")
				// Both object recreation and the real grant DELETE occurred;
				// actual server cancellation must still roll all of them back.
				cancelRestoreAtGrantDeletion(t, ctx, sysDB, restoreSQL(goodSnapshot))

				var afterID, afterGrant uint64
				require.NoError(t, admin.QueryRowContext(ctx,
					"select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'app' and relname = 't'").Scan(&afterID))
				require.NoError(t, admin.QueryRowContext(ctx,
					"select obj_id from mo_catalog.mo_role_privs where role_name = 'reader' and obj_type = 'table'").Scan(&afterGrant))
				require.Equal(t, beforeID, afterID, "DDL was not rolled back")
				require.Equal(t, beforeGrant, afterGrant, "grants were not rolled back")

				// Failed/canceled restore must preserve both durable state and the
				// pre-existing authenticated identity.
				var afterValue int
				require.NoError(t, persistent.QueryRowContext(ctx, "select id from app.t").Scan(&afterValue))
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

		// Keep the reader on another CN to cover remote revocation and restore.
		readerCN, err := c.GetCNService(1)
		require.NoError(t, err)
		readerDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#pitr_user#pitr_reader:111@tcp(127.0.0.1:%d)/", accountName, readerCN.GetServiceConfig().CN.Frontend.Port,
		))
		require.NoError(t, err)
		defer readerDB.Close()
		var count int
		require.NoError(t, readerDB.QueryRowContext(ctx,
			"select count(*) from `"+databaseName+"`.orders").Scan(&count))
		require.Equal(t, 1, count)
		_, err = readerDB.ExecContext(ctx, "delete from `"+databaseName+"`.orders")
		require.ErrorContains(t, err, "do not have privilege")

		// Binary prepared execution must reauthorize on the same connection,
		// including after catalog recreation. Preparing is not a durable grant.
		reader, err := readerDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		execSQLRequire(t, ctx, adminDB, "grant delete on table `"+databaseName+"`.orders to pitr_reader")
		prepared, err := reader.PrepareContext(ctx, "delete from `"+databaseName+"`.orders where id = ?")
		require.NoError(t, err)
		defer prepared.Close()
		_, err = prepared.ExecContext(ctx, -1)
		require.NoError(t, err)
		execSQLRequire(t, ctx, adminDB, "revoke delete on table `"+databaseName+"`.orders from pitr_reader")
		_, err = prepared.ExecContext(ctx, -1)
		require.ErrorContains(t, err, "do not have privilege")

		for _, scope := range []string{"database `" + databaseName + "` table orders", "database `" + databaseName + "`"} {
			for range 2 {
				execSQLRequire(t, ctx, adminDB, "restore "+scope+" from pitr "+pitrName+" '"+restoreAt+"'")
				require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from `"+databaseName+"`.orders").Scan(&count))
				require.Equal(t, 1, count)
				_, err = prepared.ExecContext(ctx, -1)
				require.ErrorContains(t, err, "do not have privilege", "partial PITR resurrected a revoked grant")
			}
		}
		execSQLRequire(t, ctx, adminDB, "grant delete on table `"+databaseName+"`.orders to pitr_reader")
		_, err = prepared.ExecContext(ctx, -1)
		require.NoError(t, err)
		execSQLRequire(t, ctx, adminDB, "restore from pitr "+pitrName+" '"+restoreAt+"'")
		_, err = prepared.ExecContext(ctx, -1)
		require.ErrorContains(t, err, "do not have privilege", "account PITR left a stale prepared privilege")
	})
}

func TestIssue29399AuthorizationScopeAndRevokedRole(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		cn1, err := c.GetCNService(1)
		require.NoError(t, err)
		open := func(user string, port int64) *sql.DB {
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys := open("dump", cn0.GetServiceConfig().CN.Frontend.Port)
		const account = "issue_29399_scope"
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := sys.ExecContext(cleanup, "drop account if exists "+account)
			require.NoError(t, err)
		}()
		admin := open(account+"#admin", cn0.GetServiceConfig().CN.Frontend.Port)
		for _, stmt := range []string{
			"create database allowed", "create database denied",
			"create table allowed.t(id int)", "create table denied.t(id int)",
			"insert into allowed.t values(1)", "insert into denied.t values(2)",
			"create role reader", "create user u identified by '111'",
			"use allowed", "grant select on table * to reader", "grant reader to u",
			"grant create table on database allowed to reader", "grant select on table allowed.t to reader with grant option", "create role delegate",
		} {
			execSQLRequire(t, ctx, admin, stmt)
		}
		user := open(account+"#u#reader", cn1.GetServiceConfig().CN.Frontend.Port)
		conn, err := user.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from allowed.t").Scan(&count))
		t.Run("database wildcard must not cover another database", func(t *testing.T) {
			for _, query := range []string{
				"select count(*) from allowed.t a join denied.t b on a.id = b.id",
				"select count(*) from denied.t b join allowed.t a on a.id = b.id",
			} {
				err := conn.QueryRowContext(ctx, query).Scan(&count)
				require.ErrorContains(t, err, "do not have privilege", query)
			}
		})
		prepared, err := conn.PrepareContext(ctx, "select count(*) from allowed.t where id = ?")
		require.NoError(t, err)
		defer prepared.Close()
		require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&count))
		_, err = conn.ExecContext(ctx, "create table allowed.owned(id int)")
		require.NoError(t, err)
		execSQLRequire(t, ctx, admin, "insert into allowed.owned values(7)")
		_, err = conn.ExecContext(ctx, "prepare text_scope from 'select count(*) from allowed.t'")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "execute text_scope").Scan(&count))
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from allowed.t").Scan(&count))
		execSQLRequire(t, ctx, admin, "revoke reader from u")
		t.Run("revoked primary role", func(t *testing.T) {
			err := conn.QueryRowContext(ctx, "select count(*) from allowed.t").Scan(&count)
			require.ErrorContains(t, err, "do not have privilege")
		})
		t.Run("revoked primary role binary prepared", func(t *testing.T) {
			err := prepared.QueryRowContext(ctx, 1).Scan(&count)
			require.ErrorContains(t, err, "do not have privilege")
		})

		for _, query := range []string{"execute text_scope", "drop table allowed.owned", "grant select on table allowed.t to delegate"} {
			t.Run("revoked role: "+query, func(t *testing.T) {
				_, err := conn.ExecContext(ctx, query)
				require.ErrorContains(t, err, "do not have privilege")
			})
		}
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "set role reader")
		require.ErrorContains(t, err, "has not be granted")
		_, err = conn.ExecContext(ctx, "set role public")
		require.NoError(t, err, "a revoked primary role must not block role recovery")
		require.ErrorContains(t, prepared.QueryRowContext(ctx, 1).Scan(&count), "do not have privilege")
		execSQLRequire(t, ctx, admin, "grant reader to u")
		_, err = conn.ExecContext(ctx, "set role reader")
		require.NoError(t, err)
		require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&count))
		require.Equal(t, 1, count)
		t.Run("account restore removes current membership", func(t *testing.T) {
			const snapshot = "issue_29399_scope_membership"
			execSQLRequire(t, ctx, admin, "revoke reader from u")
			execSQLRequire(t, ctx, sys, "create snapshot "+snapshot+" for account "+account)
			defer func() { _, err := sys.ExecContext(ctx, "drop snapshot if exists "+snapshot); require.NoError(t, err) }()
			execSQLRequire(t, ctx, admin, "grant reader to u")
			require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&count))
			execSQLRequire(t, ctx, sys, "restore account "+account+" {snapshot='"+snapshot+"'}")
			require.ErrorContains(t, prepared.QueryRowContext(ctx, 1).Scan(&count), "do not have privilege")
			execSQLRequire(t, ctx, admin, "grant reader to u")
			require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&count))
		})
		t.Run("dropped user cannot retain public privileges", func(t *testing.T) {
			execSQLRequire(t, ctx, admin, "grant select on table allowed.t to public")
			execSQLRequire(t, ctx, admin, "create user public_u identified by '111'")
			public, err := open(account+"#public_u#public", cn1.GetServiceConfig().CN.Frontend.Port).Conn(ctx)
			require.NoError(t, err)
			defer public.Close()
			require.NoError(t, public.QueryRowContext(ctx, "select id from allowed.t").Scan(&count))
			execSQLRequire(t, ctx, admin, "drop user public_u")
			require.ErrorContains(t, public.QueryRowContext(ctx, "select id from allowed.t").Scan(&count), "do not have privilege")
		})
		t.Run("cross account restore cannot reuse authenticated numeric identities", func(t *testing.T) {
			const source, target, snapshot = "issue_29399_identity_source", "issue_29399_identity_target", "issue_29399_identity"
			defer func() {
				cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
				defer done()
				for _, q := range []string{"drop snapshot if exists " + snapshot, "drop account if exists " + target, "drop account if exists " + source} {
					_, err := sys.ExecContext(cleanup, q)
					require.NoError(t, err)
				}
			}()
			var old []*sql.Conn
			var oldAdmin *sql.Conn
			cn2, err := c.GetCNService(2)
			require.NoError(t, err)
			var sourceUserID, targetUserID, sourceRoleID, targetRoleID uint32
			for i, name := range []string{source, target} {
				adminName, roleName, userName := "source_admin", "secret_reader", "source_user"
				if i == 1 {
					adminName, roleName, userName = "target_admin", "guest", "target_user"
				}
				execSQLRequire(t, ctx, sys, "create account "+name+" admin_name '"+adminName+"' identified by '111'")
				a := open(name+"#"+adminName, cn0.GetServiceConfig().CN.Frontend.Port)
				for _, q := range []string{"create database app", "create table app.secret(id int)", "create table app.visible(id int)", "insert into app.secret values(42)", "insert into app.visible values(1)", "create role " + roleName, "create user " + userName + " identified by '111'", "grant connect on account * to " + roleName, "grant " + roleName + " to " + userName, "grant select on table app.visible to " + roleName} {
					execSQLRequire(t, ctx, a, q)
				}
				if i == 0 {
					execSQLRequire(t, ctx, a, "grant select on table app.secret to "+roleName)
					require.NoError(t, a.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name = ?", userName).Scan(&sourceUserID))
					require.NoError(t, a.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name = ?", roleName).Scan(&sourceRoleID))
				} else {
					require.NoError(t, a.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name = ?", userName).Scan(&targetUserID))
					require.NoError(t, a.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name = ?", roleName).Scan(&targetRoleID))
					for _, port := range []int64{cn1.GetServiceConfig().CN.Frontend.Port, cn2.GetServiceConfig().CN.Frontend.Port} {
						conn, err := open(name+"#"+userName+"#"+roleName, port).Conn(ctx)
						require.NoError(t, err)
						defer conn.Close()
						old = append(old, conn)
					}
					oldAdmin, err = a.Conn(ctx)
					require.NoError(t, err)
					defer oldAdmin.Close()
					require.NoError(t, oldAdmin.QueryRowContext(ctx, "select id from app.secret").Scan(&count))
				}
			}
			require.Equal(t, sourceUserID, targetUserID, "the witness must collide numeric user identities")
			require.Equal(t, sourceRoleID, targetRoleID, "the witness must collide numeric role identities")
			var prepared []*sql.Stmt
			for _, conn := range old {
				require.ErrorContains(t, conn.QueryRowContext(ctx, "select id from app.secret").Scan(&count), "do not have privilege")
				binary, err := conn.PrepareContext(ctx, "select id from app.visible")
				require.NoError(t, err)
				defer binary.Close()
				prepared = append(prepared, binary)
				require.NoError(t, binary.QueryRowContext(ctx).Scan(&count))
				_, err = conn.ExecContext(ctx, "prepare identity_text from 'select id from app.visible'")
				require.NoError(t, err)
			}
			execSQLRequire(t, ctx, sys, "create snapshot "+snapshot+" for account "+source)
			restore := "restore account " + source + " {snapshot='" + snapshot + "'} to account " + target
			cancelRestoreAtGrantDeletion(t, ctx, sys, restore)
			for i, conn := range old {
				require.NoError(t, prepared[i].QueryRowContext(ctx).Scan(&count))
				require.NoError(t, conn.QueryRowContext(ctx, "execute identity_text").Scan(&count))
				require.ErrorContains(t, conn.QueryRowContext(ctx, "select id from app.secret").Scan(&count), "do not have privilege")
			}
			_, err = oldAdmin.ExecContext(ctx, "grant select on table app.visible to guest")
			require.NoError(t, err, "rollback invalidated the surviving administrator")
			// These native statements use cached administrator/owner identity,
			// rather than an ordinary table privilege. Rollback must retain it.
			native := []string{
				"create stage retained_admin_probe url='s3://bucket/'",
				"alter stage retained_admin_probe set url='s3://bucket2/'",
				"set global sql_mode=''",
				"set @auth_native_probe=1, global sql_mode=''",
				"alter database app set mysql_compatibility_mode='0.8.0'",
				"set role accountadmin",
			}
			for _, q := range native {
				_, err = oldAdmin.ExecContext(ctx, q)
				require.NoError(t, err, "rollback invalidated native command: %s", q)
			}
			_, err = oldAdmin.ExecContext(ctx, "drop stage retained_admin_probe")
			require.NoError(t, err)
			native = append(native, "drop stage retained_admin_probe")
			_, err = oldAdmin.ExecContext(ctx, "set @auth_native_probe=0")
			require.NoError(t, err)
			execSQLRequire(t, ctx, sys, restore)
			for _, q := range native {
				_, err = oldAdmin.ExecContext(ctx, q)
				assert.ErrorContains(t, err, "do not have privilege", "replaced administrator ran native command: %s", q)
			}
			var localEffect int
			require.NoError(t, oldAdmin.QueryRowContext(ctx, "select @auth_native_probe").Scan(&localEffect))
			require.Zero(t, localEffect, "denied mixed SET partially executed its local assignment")
			freshAdmin := open(target+"#source_admin#accountadmin", cn1.GetServiceConfig().CN.Frontend.Port)
			require.NoError(t, freshAdmin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_stages where stage_name='retained_admin_probe'").Scan(&count))
			require.Zero(t, count, "denied CREATE STAGE mutated the restored catalog")
			for i, conn := range old {
				require.ErrorContains(t, conn.QueryRowContext(ctx, "select id from app.secret").Scan(&count), "do not have privilege")
				require.ErrorContains(t, conn.QueryRowContext(ctx, "select id from app.visible").Scan(&count), "do not have privilege")
				require.ErrorContains(t, prepared[i].QueryRowContext(ctx).Scan(&count), "do not have privilege")
				require.ErrorContains(t, conn.QueryRowContext(ctx, "execute identity_text").Scan(&count), "do not have privilege")
			}
			require.ErrorContains(t, oldAdmin.QueryRowContext(ctx, "select id from app.secret").Scan(&count), "do not have privilege")
			_, err = oldAdmin.ExecContext(ctx, "grant delete on table app.secret to secret_reader")
			require.ErrorContains(t, err, "do not have privilege")
			_, err = oldAdmin.ExecContext(ctx, "revoke select on table app.secret from secret_reader")
			require.ErrorContains(t, err, "do not have privilege")
			fresh := open(target+"#source_user#secret_reader", cn1.GetServiceConfig().CN.Frontend.Port)
			require.NoError(t, fresh.QueryRowContext(ctx, "select id from app.secret").Scan(&count))
			require.Equal(t, 42, count)
		})
		t.Run("recreated creator is not historical owner", func(t *testing.T) {
			var err error
			execSQLRequire(t, ctx, admin, "grant create database on account * to reader")
			_, err = conn.ExecContext(ctx, "create database historical_parent")
			require.NoError(t, err)
			execSQLRequire(t, ctx, admin, "create table historical_parent.t(id int)")
			execSQLRequire(t, ctx, admin, "insert into historical_parent.t values(9)")

			// Bulk restore omits external tables: their missing creators cannot
			// reject reconstruction of the eligible admin-owned objects.
			execSQLRequire(t, ctx, admin, "create database external_parent")
			execSQLRequire(t, ctx, admin, "grant create table on database external_parent to reader")
			csv := filepath.Join(t.TempDir(), "external.csv")
			require.NoError(t, os.WriteFile(csv, []byte("1\n"), 0600))
			_, err = conn.ExecContext(ctx, "create external table external_parent.ext(id int) infile{'filepath'='"+csv+"'} fields terminated by ','")
			require.NoError(t, err)
			execSQLRequire(t, ctx, admin, "create table external_parent.t(id int)")
			execSQLRequire(t, ctx, admin, "insert into external_parent.t values(9)")

			const snapshot = "issue_29399_scope_owner"
			execSQLRequire(t, ctx, admin, "create snapshot "+snapshot+" for account")
			defer func() {
				cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
				defer done()
				_, err := admin.ExecContext(cleanup, "drop snapshot if exists "+snapshot)
				require.NoError(t, err)
			}()

			var owner, restoredOwner uint32
			require.NoError(t, admin.QueryRowContext(ctx, "select owner from mo_catalog.mo_tables where reldatabase = 'allowed' and relname = 'owned'").Scan(&owner))
			execSQLRequire(t, ctx, admin, "alter role reader rename to surviving_reader")
			execSQLRequire(t, ctx, admin, "create role reader")
			execSQLRequire(t, ctx, admin, "restore table allowed.owned {snapshot='"+snapshot+"'}")
			require.NoError(t, admin.QueryRowContext(ctx, "select owner from mo_catalog.mo_tables where reldatabase = 'allowed' and relname = 'owned'").Scan(&restoredOwner))
			require.Equal(t, owner, restoredOwner)
			execSQLRequire(t, ctx, admin, "create user replacement identified by '111'")
			execSQLRequire(t, ctx, admin, "grant reader to replacement")
			namesake := open(account+"#replacement#reader", cn1.GetServiceConfig().CN.Frontend.Port)
			_, err = namesake.ExecContext(ctx, "drop table allowed.owned")
			require.ErrorContains(t, err, "do not have privilege")
			execSQLRequire(t, ctx, admin, "drop user u")
			require.ErrorContains(t, conn.QueryRowContext(ctx, "select count(*) from allowed.t").Scan(&count), "do not have privilege")
			execSQLRequire(t, ctx, admin, "update external_parent.t set id = 12")
			execSQLRequire(t, ctx, admin, "restore database external_parent {snapshot='"+snapshot+"'}")
			require.NoError(t, admin.QueryRowContext(ctx, "select id from external_parent.t").Scan(&count))
			require.Equal(t, 9, count)
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='external_parent' and relname='ext'").Scan(&count))
			require.Zero(t, count)

			// Only the table is reconstructed; an existing parent's deleted
			// creator must not reject restoration of an admin-owned child.
			execSQLRequire(t, ctx, admin, "update historical_parent.t set id = 12")
			execSQLRequire(t, ctx, admin, "restore table historical_parent.t {snapshot='"+snapshot+"'}")
			require.NoError(t, admin.QueryRowContext(ctx, "select id from historical_parent.t").Scan(&count))
			require.Equal(t, 9, count)

			execSQLRequire(t, ctx, admin, "create user u identified by '111'")
			var original, current uint64
			const identity = "select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'allowed' and relname = 'owned'"
			require.NoError(t, admin.QueryRowContext(ctx, identity).Scan(&original))
			_, err = admin.ExecContext(ctx, "restore table allowed.owned {snapshot='"+snapshot+"'}")
			require.ErrorContains(t, err, "creator no longer exists")
			require.NoError(t, admin.QueryRowContext(ctx, identity).Scan(&current))
			require.Equal(t, original, current)
			require.NoError(t, admin.QueryRowContext(ctx, "select id from allowed.owned").Scan(&count))
			require.Equal(t, 7, count)
		})
	})
}
