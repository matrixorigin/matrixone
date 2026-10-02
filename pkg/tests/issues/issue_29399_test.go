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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue29399TableRestoreRetainsDatabaseOwnership(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
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
		const account = "issue_29399_owner_scope"
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		admin := open(account + "#admin#accountadmin")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			execSQLRequire(t, cleanup, admin, "drop snapshot if exists issue_29399_owner_scope_s")
			execSQLRequire(t, cleanup, admin, "drop pitr if exists issue_29399_owner_scope_p")
			execSQLRequire(t, cleanup, sys, "drop account if exists "+account)
		}()
		for _, stmt := range []string{
			"create role builder", "create user db_creator identified by '111' default role builder",
			"grant builder to db_creator", "grant connect, create database on account * to builder",
			"grant create table, create view on database * to builder",
			"grant select on table *.* to builder",
			"create pitr issue_29399_owner_scope_p for account range 1 'h'",
		} {
			execSQLRequire(t, ctx, admin, stmt)
		}
		creator := open(account + "#db_creator#builder")
		execSQLRequire(t, ctx, creator, "create database app")
		execSQLRequire(t, ctx, creator, "create table app.owned(id int)")
		for _, stmt := range []string{
			"create table app.t(id int)", "insert into app.t values (1)",
			"insert into app.owned values (3)",
			"create database recreated", "create table recreated.t(id int)", "insert into recreated.t values (7)",
			"create database role_owned", "create table role_owned.t(id int)", "insert into role_owned.t values (5)",
			"create role external_owner", "grant external_owner to admin",
			"grant connect on account * to external_owner", "grant create table, create view on database role_owned to external_owner",
			"grant select on table *.* to external_owner",
		} {
			execSQLRequire(t, ctx, admin, stmt)
		}
		execSQLRequire(t, ctx, creator, "create external table recreated.ext(id int) infile{'filepath'='/tmp/issue29399-unused.csv','format'='csv'} fields terminated by ','")
		externalOwner := open(account + "#admin#external_owner")
		execSQLRequire(t, ctx, externalOwner, "create external table role_owned.ext(id int) infile{'filepath'='/tmp/issue29399-unused.csv','format'='csv'} fields terminated by ','")
		// The stored MATCH views become unservable only after their index is
		// removed. They must be skipped without pinning their deleted principals.
		for _, stmt := range []string{
			"set experimental_fulltext_index=1", "set ft_relevancy_algorithm='TF-IDF'",
			"create table recreated.docs(id int primary key, body text)",
			"insert into recreated.docs values (1, 'apple banana'), (2, 'apple cherry'), (3, 'banana cherry')",
			"create fulltext index ft_body on recreated.docs(body)",
		} {
			execSQLRequire(t, ctx, admin, stmt)
		}
		for _, db := range []*sql.DB{creator, externalOwner} {
			execSQLRequire(t, ctx, db, "set experimental_fulltext_index=1")
			execSQLRequire(t, ctx, db, "set ft_relevancy_algorithm='TF-IDF'")
		}
		execSQLRequire(t, ctx, creator, "create view recreated.v_ft as select id from recreated.docs where match(body) against('apple')")
		execSQLRequire(t, ctx, externalOwner, "create view role_owned.v_ft as select id from recreated.docs where match(body) against('apple')")
		execSQLRequire(t, ctx, creator, "create view app.owned_view as select 3 as id")
		var matches int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from recreated.v_ft").Scan(&matches))
		require.Equal(t, 2, matches)
		execSQLRequire(t, ctx, admin, "drop index ft_body on recreated.docs")
		execSQLRequire(t, ctx, admin, "create snapshot issue_29399_owner_scope_s for account")
		// PITR accepts second-resolution timestamps. This delay crosses that
		// public precision boundary; it does not schedule competing operations.
		var slept int
		require.NoError(t, admin.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var at string
		require.NoError(t, admin.QueryRowContext(ctx, "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&at))
		var id, owner, user uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select dat_id, owner, creator from mo_catalog.mo_database where datname='app'").Scan(&id, &owner, &user))
		var userCatalog, roleCatalog uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_user'").Scan(&userCatalog))
		require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_role'").Scan(&roleCatalog))
		execSQLRequire(t, ctx, admin, "drop user db_creator")
		execSQLRequire(t, ctx, admin, "drop role external_owner")
		// Each database isolates one missing principal of an external table:
		// recreated has a missing creator; role_owned has a missing owner only.
		for _, target := range []struct {
			database string
			value    int
		}{{"recreated", 7}, {"role_owned", 5}} {
			database := target.database
			execSQLRequire(t, ctx, admin, "drop table "+database+".ext")
			execSQLRequire(t, ctx, admin, "create table "+database+".ext(id int)")
			execSQLRequire(t, ctx, admin, "insert into "+database+".ext values(42)")
			execSQLRequire(t, ctx, admin, "update "+database+".t set id=9")
			var before uint64
			identitySQL := "select rel_id from mo_catalog.mo_tables where reldatabase='" + database + "' and relname='ext'"
			require.NoError(t, admin.QueryRowContext(ctx, identitySQL).Scan(&before))
			for _, restore := range []string{
				"restore table " + database + ".ext {snapshot='issue_29399_owner_scope_s'}",
				"restore database " + database + " table ext from pitr issue_29399_owner_scope_p '" + at + "'",
			} {
				_, err := admin.ExecContext(ctx, restore)
				require.ErrorContains(t, err, "external table "+database+".ext cannot be restored")
				var after uint64
				var value int
				require.NoError(t, admin.QueryRowContext(ctx, identitySQL).Scan(&after))
				require.Equal(t, before, after)
				require.NoError(t, admin.QueryRowContext(ctx, "select id from "+database+".ext").Scan(&value))
				require.Equal(t, 42, value)
				require.NoError(t, admin.QueryRowContext(ctx, "select id from "+database+".t").Scan(&value))
				require.Equal(t, 9, value)
			}
			execSQLRequire(t, ctx, admin, "drop view "+database+".v_ft")
			execSQLRequire(t, ctx, admin, "create view "+database+".v_ft as select 42 as id")
			viewIdentitySQL := "select rel_id, rel_createsql from mo_catalog.mo_tables where reldatabase='" + database + "' and relname='v_ft'"
			var viewID uint64
			var viewSQL string
			require.NoError(t, admin.QueryRowContext(ctx, viewIdentitySQL).Scan(&viewID, &viewSQL))
			for _, restore := range []string{
				"restore table " + database + ".v_ft {snapshot='issue_29399_owner_scope_s'}",
				"restore database " + database + " table v_ft from pitr issue_29399_owner_scope_p '" + at + "'",
			} {
				execSQLRequire(t, ctx, admin, restore)
				var currentID uint64
				var currentSQL string
				var value int
				require.NoError(t, admin.QueryRowContext(ctx, viewIdentitySQL).Scan(&currentID, &currentSQL))
				require.Equal(t, viewID, currentID)
				require.Equal(t, viewSQL, currentSQL)
				require.NoError(t, admin.QueryRowContext(ctx, "select id from "+database+".v_ft").Scan(&value))
				require.Equal(t, 42, value)
			}
			for _, restore := range []string{
				"restore database " + database + " {snapshot='issue_29399_owner_scope_s'}",
				"restore database " + database + " from pitr issue_29399_owner_scope_p '" + at + "'",
			} {
				execSQLRequire(t, ctx, admin, "update "+database+".t set id=9")
				execSQLRequire(t, ctx, admin, restore)
				var value, externalTables int
				require.NoError(t, admin.QueryRowContext(ctx, "select id from "+database+".t").Scan(&value))
				require.Equal(t, target.value, value)
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+database+"' and relname in ('ext','v_ft')").Scan(&externalTables))
				require.Zero(t, externalTables)
			}
		}
		var currentUserCatalog, currentRoleCatalog uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_user'").Scan(&currentUserCatalog))
		require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_role'").Scan(&currentRoleCatalog))
		require.Equal(t, userCatalog, currentUserCatalog)
		require.Equal(t, roleCatalog, currentRoleCatalog)

		for _, restore := range []string{
			"restore table app.t {snapshot='issue_29399_owner_scope_s'}",
			"restore database app table t from pitr issue_29399_owner_scope_p '" + at + "'",
		} {
			execSQLRequire(t, ctx, admin, "update app.t set id=2")
			execSQLRequire(t, ctx, admin, restore)
			var value int
			require.NoError(t, admin.QueryRowContext(ctx, "select id from app.t").Scan(&value))
			require.Equal(t, 1, value)
			var currentID, currentOwner, currentUser uint64
			require.NoError(t, admin.QueryRowContext(ctx, "select dat_id, owner, creator from mo_catalog.mo_database where datname='app'").Scan(&currentID, &currentOwner, &currentUser))
			require.Equal(t, []uint64{id, owner, user}, []uint64{currentID, currentOwner, currentUser})
		}
		// Retaining the DB does not exempt a recreated table's own principal.
		execSQLRequire(t, ctx, admin, "update app.owned set id=9")
		var beforeTable uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='app' and relname='owned'").Scan(&beforeTable))
		for _, restore := range []string{
			"restore table app.owned {snapshot='issue_29399_owner_scope_s'}",
			"restore database app table owned from pitr issue_29399_owner_scope_p '" + at + "'",
		} {
			_, err = admin.ExecContext(ctx, restore)
			require.ErrorContains(t, err, "creator no longer exists")
			var afterTable uint64
			var value int
			require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='app' and relname='owned'").Scan(&afterTable))
			require.Equal(t, beforeTable, afterTable, "rejected restore must fail before DROP")
			require.NoError(t, admin.QueryRowContext(ctx, "select id from app.owned").Scan(&value))
			require.Equal(t, 9, value)
		}
		// A servable view still requires its historical creator before DROP.
		execSQLRequire(t, ctx, admin, "drop view app.owned_view")
		execSQLRequire(t, ctx, admin, "create view app.owned_view as select 42 as id")
		var viewBefore uint64
		const viewIDSQL = "select rel_id from mo_catalog.mo_tables where reldatabase='app' and relname='owned_view'"
		require.NoError(t, admin.QueryRowContext(ctx, viewIDSQL).Scan(&viewBefore))
		for _, restore := range []string{
			"restore table app.owned_view {snapshot='issue_29399_owner_scope_s'}",
			"restore database app table owned_view from pitr issue_29399_owner_scope_p '" + at + "'",
		} {
			_, err = admin.ExecContext(ctx, restore)
			require.ErrorContains(t, err, "creator no longer exists")
			var after uint64
			var value int
			require.NoError(t, admin.QueryRowContext(ctx, viewIDSQL).Scan(&after))
			require.Equal(t, viewBefore, after)
			require.NoError(t, admin.QueryRowContext(ctx, "select id from app.owned_view").Scan(&value))
			require.Equal(t, 42, value)
		}
		// A genuinely missing database still needs its historical identities.
		execSQLRequire(t, ctx, admin, "drop database app")
		_, err = admin.ExecContext(ctx, "restore table app.t {snapshot='issue_29399_owner_scope_s'}")
		require.ErrorContains(t, err, "creator no longer exists")
		var databases int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database where datname='app'").Scan(&databases))
		require.Zero(t, databases)
		execSQLRequire(t, ctx, admin, "drop database recreated")
		execSQLRequire(t, ctx, admin, "restore table recreated.t {snapshot='issue_29399_owner_scope_s'}")
		var value int
		require.NoError(t, admin.QueryRowContext(ctx, "select id from recreated.t").Scan(&value))
		require.Equal(t, 7, value)
		var correctOwners int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_database d join mo_catalog.mo_user u on d.creator=u.user_id join mo_catalog.mo_role r on d.owner=r.role_id where d.datname='recreated' and u.user_name='admin' and r.role_name='accountadmin'").Scan(&correctOwners))
		require.Equal(t, 1, correctOwners)
	})
}

func TestIssue29399PartialRestoreSerializesPrivilegeRemoval(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		openDB := func(user string) *sql.DB {
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sysDB := openDB("dump")
		execSQLRequire(t, ctx, sysDB, "create account issue_29399_race admin_name 'admin' identified by '111'")
		defer execSQLMaybe(t, context.Background(), sysDB, "drop account if exists issue_29399_race")
		admin := openDB("issue_29399_race#admin#accountadmin")
		for _, statement := range []string{
			"create database app", "create table app.t(id int)", "insert into app.t values (1)",
			"create role reader", "grant select on table app.t to reader",
			"create pitr issue_29399_race_pitr for account range 1 'h'",
			"create snapshot issue_29399_race_snapshot for account issue_29399_race",
		} {
			execSQLRequire(t, ctx, admin, statement)
		}
		defer execSQLMaybe(t, context.Background(), admin, "drop pitr if exists issue_29399_race_pitr")
		defer execSQLMaybe(t, context.Background(), admin, "drop snapshot if exists issue_29399_race_snapshot")
		var slept int
		require.NoError(t, admin.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var restoreAt string
		require.NoError(t, admin.QueryRowContext(ctx, "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&restoreAt))
		var databaseCatalogID uint64
		require.NoError(t, admin.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname = 'mo_database'").Scan(&databaseCatalogID))
		var privilegeCatalogID uint64
		require.NoError(t, admin.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname = 'mo_role_privs'").Scan(&privilegeCatalogID))
		for _, statement := range []string{
			"restore table app.t {snapshot = 'issue_29399_race_snapshot'}",
			"restore database app {snapshot = 'issue_29399_race_snapshot'}",
			"restore database app table t from pitr issue_29399_race_pitr '" + restoreAt + "'",
			"restore database app from pitr issue_29399_race_pitr '" + restoreAt + "'",
		} {
			for _, mutation := range []string{"revoke select on table app.t from reader", "drop role reader"} {
				t.Run(statement+"/"+mutation, func(t *testing.T) {
					execSQLRequire(t, ctx, admin, "create role if not exists reader")
					execSQLRequire(t, ctx, admin, "drop table app.t")
					execSQLRequire(t, ctx, admin, "create table app.t(id int)")
					execSQLRequire(t, ctx, admin, "insert into app.t values (2)")
					execSQLRequire(t, ctx, admin, "grant select on table app.t to reader")
					// Private restore/removal transactions must retain their locking
					// contract even when ordinary transactions are optimistic SI.
					restoreMode := setIssue27718TxnConfig([]embed.ServiceOperator{cn}, pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI)
					defer restoreMode()
					captured, release, queued := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var captureOnce, releaseOnce, queueOnce sync.Once
					releaseRestore := func() { releaseOnce.Do(func() { close(release) }) }
					defer releaseRestore()
					restoreCapture := frontend.SetPartialRestorePrivilegesCapturedHookForTest(func() {
						captureOnce.Do(func() { close(captured) })
						<-release
					})
					defer restoreCapture()
					gateID := databaseCatalogID
					if mutation == "drop role reader" {
						gateID = privilegeCatalogID
					}
					restoreWaiter := lockservice.SetWaiterEnqueuedHookForTest(func(tableID uint64, waiter []byte, holders [][]byte) {
						if tableID == gateID && len(waiter) != 0 && len(holders) != 0 {
							queueOnce.Do(func() { close(queued) })
						}
					})
					defer restoreWaiter()
					restored := make(chan error, 1)
					go func() { _, err := admin.ExecContext(ctx, statement); restored <- err }()
					select {
					case <-captured:
					case err := <-restored:
						t.Fatalf("restore returned before capture: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					revoked := make(chan error, 1)
					go func() { _, err := admin.ExecContext(ctx, mutation); revoked <- err }()
					select {
					case <-queued:
					case err := <-revoked:
						releaseRestore()
						require.NoError(t, <-restored)
						t.Fatalf("privilege removal bypassed restore locks: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					releaseRestore()
					require.NoError(t, <-restored)
					require.NoError(t, <-revoked)
					restoreMode()
					var grants, value int
					require.NoError(t, admin.QueryRowContext(ctx,
						"select count(*) from mo_catalog.mo_role_privs where role_name = 'reader' and obj_type = 'table' and privilege_name = 'select'").Scan(&grants))
					require.Zero(t, grants, "restore resurrected removed privileges")
					if mutation == "drop role reader" {
						var roles int
						require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_role where role_name = 'reader'").Scan(&roles))
						require.Zero(t, roles)
					}
					require.NoError(t, admin.QueryRowContext(ctx, "select id from app.t").Scan(&value))
					require.Equal(t, 1, value)
				})
			}
		}
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

// Principal names are mutable, and account rollback can rewind principal IDs.
// Exercise both boundaries through authenticated SQL, including actual ownership
// authority and preservation of the replacement object on rejected restores.
func TestIssue29399PartialRestorePrincipalIdentity(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
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
		const account = "issue_29399_identity"
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		admin := open(account + "#admin#accountadmin")
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			for _, snapshot := range []string{"issue_29399_identity_floor", "issue_29399_identity_s"} {
				execSQLMaybe(t, clean, sys, "drop snapshot if exists "+snapshot)
			}
			execSQLMaybe(t, clean, admin, "drop pitr if exists issue_29399_identity_p")
			execSQLMaybe(t, clean, sys, "drop account if exists "+account)
		}()
		for _, q := range []string{
			"create snapshot issue_29399_identity_floor for account",
			"create role maker", "create user builder identified by '111' default role maker",
			"grant maker to builder", "grant connect,create database on account * to maker",
			"grant create table on database * to maker",
			"create pitr issue_29399_identity_p for account range 1 'h'",
		} {
			execSQLRequire(t, ctx, admin, q)
		}
		builder := open(account + "#builder#maker")
		for _, q := range []string{"create database app", "create table app.t(id int)", "create table app.disposable(id int)"} {
			execSQLRequire(t, ctx, builder, q)
		}
		execSQLRequire(t, ctx, admin, "insert into app.t values(1)")
		execSQLRequire(t, ctx, admin, "create snapshot issue_29399_identity_s for account")
		var slept int
		require.NoError(t, admin.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var at string
		require.NoError(t, admin.QueryRowContext(ctx, "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&at))
		statements := []string{
			"restore table app.t {snapshot='issue_29399_identity_s'}",
			"restore database app {snapshot='issue_29399_identity_s'}",
			"restore database app table t from pitr issue_29399_identity_p '" + at + "'",
			"restore database app from pitr issue_29399_identity_p '" + at + "'",
		}
		var originalRole, originalUser uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='maker'").Scan(&originalRole))
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='builder'").Scan(&originalUser))
		execSQLRequire(t, ctx, admin, statements[0]) // unchanged-name control
		execSQLRequire(t, ctx, admin, "alter role maker rename to maker2")
		checkOwner := func() {
			var value int
			var user, owner uint64
			require.NoError(t, admin.QueryRowContext(ctx, "select id from app.t").Scan(&value))
			require.Equal(t, 1, value)
			require.NoError(t, admin.QueryRowContext(ctx, "select creator,owner from mo_catalog.mo_tables where reldatabase='app' and relname='t'").Scan(&user, &owner))
			require.Equal(t, []uint64{originalUser, originalRole}, []uint64{user, owner})
		}
		for _, statement := range statements {
			execSQLRequire(t, ctx, admin, "update app.t set id=9")
			execSQLRequire(t, ctx, admin, statement)
			checkOwner()
		}
		for _, q := range []string{"create role maker", "create user newcomer identified by '111' default role maker", "grant maker to newcomer", "grant connect on account * to maker"} {
			execSQLRequire(t, ctx, admin, q)
		}
		newcomer := open(account + "#newcomer#maker")
		for _, statement := range statements {
			_, err := newcomer.ExecContext(ctx, "drop table app.t")
			require.Error(t, err)
			execSQLRequire(t, ctx, admin, "update app.t set id=9")
			execSQLRequire(t, ctx, admin, statement)
			checkOwner()
			_, err = newcomer.ExecContext(ctx, "drop table app.t")
			require.Error(t, err, "vacated name must not acquire ownership")
		}
		renamedOwner := open(account + "#builder#maker2")
		execSQLRequire(t, ctx, renamedOwner, "drop table app.disposable")
		checkRejected := func(statement, message string) {
			var before, after uint64
			var value int
			require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='app' and relname='t'").Scan(&before))
			_, err := admin.ExecContext(ctx, statement)
			require.ErrorContains(t, err, message)
			require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='app' and relname='t'").Scan(&after))
			require.Equal(t, before, after, "rejected restore must precede DROP")
			require.NoError(t, admin.QueryRowContext(ctx, "select id from app.t").Scan(&value))
			require.Equal(t, 9, value)
		}
		execSQLRequire(t, ctx, admin, "update app.t set id=9")
		execSQLRequire(t, ctx, admin, "drop role maker2")
		for _, statement := range statements {
			checkRejected(statement, "owner role no longer exists")
		}
		execSQLRequire(t, ctx, admin, "drop user builder")
		execSQLRequire(t, ctx, admin, "create user builder identified by '111'")
		for _, statement := range statements {
			checkRejected(statement, "creator no longer exists")
		}

		// A full rollback changes the catalog generation and really reuses ID 3.
		execSQLRequire(t, ctx, admin, "restore account "+account+" {snapshot='issue_29399_identity_floor'}")
		for _, q := range []string{"create role replacement", "create user replacement_user identified by '111' default role replacement", "grant replacement to replacement_user", "grant connect on account * to replacement", "create database app", "create table app.t(id int)", "insert into app.t values(9)"} {
			execSQLRequire(t, ctx, admin, q)
		}
		var reusedRole, reusedUser uint64
		require.NoError(t, admin.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='replacement'").Scan(&reusedRole))
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='replacement_user'").Scan(&reusedUser))
		require.Equal(t, originalRole, reusedRole)
		require.Equal(t, originalUser, reusedUser)
		checkRejected(statements[0], "catalog was rebuilt")
		checkRejected(statements[1], "catalog was rebuilt")
		replacement := open(account + "#replacement_user#replacement")
		_, err = replacement.ExecContext(ctx, "drop table app.t")
		require.Error(t, err)
	})
}

func TestIssue29399PartialRestorePinsPrincipals(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
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
		const account = "issue_29399_principal_pins"
		execSQLRequire(t, ctx, sys, "create account "+account+" admin_name 'admin' identified by '111'")
		admin := open(account + "#admin#accountadmin")
		defer func() {
			clean, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			execSQLMaybe(t, clean, sys, "drop snapshot if exists issue_29399_pins_s")
			execSQLMaybe(t, clean, admin, "drop pitr if exists issue_29399_pins_p")
			execSQLMaybe(t, clean, sys, "drop account if exists "+account)
		}()
		execSQLRequire(t, ctx, admin, "create database app")
		execSQLRequire(t, ctx, admin, "create pitr issue_29399_pins_p for account range 1 'h'")
		for i := 0; i < 2; i++ {
			role, user := fmt.Sprintf("pin_role_%d", i), fmt.Sprintf("pin_user_%d", i)
			for _, q := range []string{"create role " + role, "create user " + user + " identified by '111' default role " + role,
				"grant " + role + " to " + user, "grant connect on account * to " + role, "grant create table on database app to " + role} {
				execSQLRequire(t, ctx, admin, q)
			}
			creator := open(account + "#" + user + "#" + role)
			execSQLRequire(t, ctx, creator, fmt.Sprintf("create table app.t%d(id int)", i))
			execSQLRequire(t, ctx, admin, fmt.Sprintf("insert into app.t%d values(1)", i))
		}
		execSQLRequire(t, ctx, admin, "create snapshot issue_29399_pins_s for account")
		var slept int
		require.NoError(t, admin.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var at string
		require.NoError(t, admin.QueryRowContext(ctx, "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')").Scan(&at))
		for i, tc := range []struct{ catalog, mutation, restore string }{
			{"mo_user", "drop user pin_user_0", "restore table app.t0 {snapshot='issue_29399_pins_s'}"},
			{"mo_role", "drop role pin_role_1", "restore database app table t1 from pitr issue_29399_pins_p '" + at + "'"},
		} {
			t.Run(tc.mutation, func(t *testing.T) {
				// The current table belongs to admin, with no scoped grant for the
				// historical owner. Only principal-row pins can block this deletion.
				name := fmt.Sprintf("app.t%d", i)
				execSQLRequire(t, ctx, admin, "drop table "+name)
				execSQLRequire(t, ctx, admin, "create table "+name+"(id int)")
				execSQLRequire(t, ctx, admin, "insert into "+name+" values(9)")
				var gateID uint64
				require.NoError(t, admin.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='"+tc.catalog+"'").Scan(&gateID))
				restoreMode := setIssue27718TxnConfig([]embed.ServiceOperator{cn}, pbtxn.TxnMode_Optimistic, pbtxn.TxnIsolation_SI)
				defer restoreMode()
				captured, release, queued := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var captureOnce, releaseOnce, queueOnce sync.Once
				releaseRestore := func() { releaseOnce.Do(func() { close(release) }) }
				defer releaseRestore()
				restoreCapture := frontend.SetPartialRestorePrivilegesCapturedHookForTest(func() {
					captureOnce.Do(func() { close(captured) })
					<-release
				})
				defer restoreCapture()
				restoreWaiter := lockservice.SetWaiterEnqueuedHookForTest(func(tableID uint64, waiter []byte, holders [][]byte) {
					if tableID == gateID && len(waiter) != 0 && len(holders) != 0 {
						queueOnce.Do(func() { close(queued) })
					}
				})
				defer restoreWaiter()
				restored := make(chan error, 1)
				go func() { _, err := admin.ExecContext(ctx, tc.restore); restored <- err }()
				select {
				case <-captured:
				case err := <-restored:
					t.Fatalf("restore returned before capture: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				deleted := make(chan error, 1)
				go func() { _, err := admin.ExecContext(ctx, tc.mutation); deleted <- err }()
				select {
				case <-queued:
				case err := <-deleted:
					releaseRestore()
					require.NoError(t, <-restored)
					t.Fatalf("deletion bypassed principal pin: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				releaseRestore()
				require.NoError(t, <-restored)
				require.NoError(t, <-deleted)
				restoreMode()
				var value, principals int
				require.NoError(t, admin.QueryRowContext(ctx, "select id from "+name).Scan(&value))
				require.Equal(t, 1, value)
				principalName := "user_name='pin_user_0'"
				if tc.catalog == "mo_role" {
					principalName = "role_name='pin_role_1'"
				}
				require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from mo_catalog."+tc.catalog+" where "+principalName).Scan(&principals))
				require.Zero(t, principals)
			})
		}
	})
}
