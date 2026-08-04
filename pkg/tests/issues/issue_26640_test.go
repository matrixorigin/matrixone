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

func TestIssue26640CrossAccountRestoreRebindsPrivileges(t *testing.T) {
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
			sourceAccount = "issue_26640_source"
			targetAccount = "issue_26640_target"
			snapshotName  = "issue_26640_snapshot"
			databaseName  = "issue_26640_db"
		)
		cleanup := func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop snapshot if exists "+snapshotName)
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+sourceAccount+"`")
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+targetAccount+"`")
		}
		cleanup()
		defer cleanup()

		execSQLRequire(t, ctx, sysDB,
			"create account `"+sourceAccount+"` admin_name 'admin' identified by '111'")
		execSQLRequire(t, ctx, sysDB,
			"create account `"+targetAccount+"` admin_name 'admin' identified by '111'")

		sourceDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", sourceAccount, port,
		))
		require.NoError(t, err)
		defer sourceDB.Close()
		execSQLRequire(t, ctx, sourceDB, "create database `"+databaseName+"`")
		execSQLRequire(t, ctx, sourceDB,
			"create table `"+databaseName+"`.orders (id int primary key, note varchar(32))")
		execSQLRequire(t, ctx, sourceDB,
			"insert into `"+databaseName+"`.orders values (1, 'snapshotted')")
		execSQLRequire(t, ctx, sourceDB,
			"create view `"+databaseName+"`.order_view as select id, note from `"+databaseName+"`.orders")
		execSQLRequire(t, ctx, sourceDB, "create role snapshot_reader")
		execSQLRequire(t, ctx, sourceDB,
			"create user source_reader identified by '111' default role snapshot_reader")
		execSQLRequire(t, ctx, sourceDB, "grant connect on account * to snapshot_reader")
		execSQLRequire(t, ctx, sourceDB,
			"grant select on table `"+databaseName+"`.orders to snapshot_reader")
		execSQLRequire(t, ctx, sourceDB,
			"grant select on view `"+databaseName+"`.order_view to snapshot_reader")
		execSQLRequire(t, ctx, sourceDB,
			"grant insert on table `"+databaseName+"`.* to snapshot_reader")
		execSQLRequire(t, ctx, sourceDB,
			"grant create table on database `"+databaseName+"` to snapshot_reader")
		execSQLRequire(t, ctx, sourceDB, "grant snapshot_reader to source_reader")
		sourceReaderDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#source_reader#snapshot_reader:111@tcp(127.0.0.1:%d)/", sourceAccount, port,
		))
		require.NoError(t, err)
		defer sourceReaderDB.Close()
		var count int
		require.NoError(t, sourceReaderDB.QueryRowContext(ctx,
			"select count(*) from `"+databaseName+"`.order_view").Scan(&count))
		require.Equal(t, 1, count)

		var sourceLogicalID uint64
		require.NoError(t, sourceDB.QueryRowContext(ctx,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'orders'",
			databaseName,
		).Scan(&sourceLogicalID))

		execSQLRequire(t, ctx, sysDB,
			"create snapshot "+snapshotName+" for account `"+sourceAccount+"`")
		execSQLRequire(t, ctx, sysDB,
			"restore account `"+sourceAccount+"`{snapshot='"+snapshotName+"'} to account `"+targetAccount+"`")

		targetAdminDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", targetAccount, port,
		))
		require.NoError(t, err)
		defer targetAdminDB.Close()
		var targetPhysicalID, targetLogicalID, restoredPrivilegeID uint64
		require.NoError(t, targetAdminDB.QueryRowContext(ctx,
			"select rel_id, rel_logical_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'orders'",
			databaseName,
		).Scan(&targetPhysicalID, &targetLogicalID))
		require.NotEqual(t, sourceLogicalID, targetLogicalID)
		require.NoError(t, targetAdminDB.QueryRowContext(ctx,
			"select obj_id from mo_catalog.mo_role_privs where role_name = 'snapshot_reader' "+
				"and obj_type = 'table' and privilege_level = 'd.t' and privilege_name = 'select'",
		).Scan(&restoredPrivilegeID))
		// Direct table grants are authorization identities, so they follow the
		// target logical ID. They must not use either account's physical rel_id.
		require.Equal(t, targetLogicalID, restoredPrivilegeID)
		var targetViewLogicalID, restoredViewPrivilegeID uint64
		require.NoError(t, targetAdminDB.QueryRowContext(ctx,
			"select rel_logical_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'order_view'",
			databaseName,
		).Scan(&targetViewLogicalID))
		require.NoError(t, targetAdminDB.QueryRowContext(ctx,
			"select obj_id from mo_catalog.mo_role_privs where role_name = 'snapshot_reader' "+
				"and obj_type = 'view' and privilege_level = 'd.t' and privilege_name = 'select'",
		).Scan(&restoredViewPrivilegeID))
		require.Equal(t, targetViewLogicalID, restoredViewPrivilegeID)

		// TRUNCATE replaces the physical table incarnation while preserving its
		// authorization identity. A restored grant must therefore bind to the
		// logical ID, not merely to the target's current physical ID.
		execSQLRequire(t, ctx, targetAdminDB, "truncate table `"+databaseName+"`.orders")
		execSQLRequire(t, ctx, targetAdminDB,
			"insert into `"+databaseName+"`.orders values (1, 'after truncate')")
		var truncatedPhysicalID, truncatedLogicalID uint64
		require.NoError(t, targetAdminDB.QueryRowContext(ctx,
			"select rel_id, rel_logical_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'orders'",
			databaseName,
		).Scan(&truncatedPhysicalID, &truncatedLogicalID))
		require.NotEqual(t, targetPhysicalID, truncatedPhysicalID)
		require.Equal(t, targetLogicalID, truncatedLogicalID)

		readerDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#source_reader#snapshot_reader:111@tcp(127.0.0.1:%d)/", targetAccount, port,
		))
		require.NoError(t, err)
		defer readerDB.Close()
		var role string
		require.NoError(t, readerDB.QueryRowContext(ctx, "select current_role()").Scan(&role))
		require.Equal(t, "snapshot_reader", role)
		require.NoError(t, readerDB.QueryRowContext(ctx,
			"select count(*) from `"+databaseName+"`.orders").Scan(&count))
		require.Equal(t, 1, count)
		execSQLRequire(t, ctx, readerDB,
			"insert into `"+databaseName+"`.orders values (2, 'database wildcard grant')")
		execSQLRequire(t, ctx, readerDB,
			"create table `"+databaseName+"`.created_by_restored_role (id int)")
	})
}

func TestIssue26640AccountPITRUsesCatalogRestoreHandlers(t *testing.T) {
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
			accountName  = "issue_26640_pitr"
			pitrName     = "issue_26640_account_pitr"
			databaseName = "issue_26640_pitr_db"
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
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select cast(current_timestamp as char)").Scan(&restoreAt))

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

func TestIssue26640ClusterRestoreRebindsSubscriptionPrivileges(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()

		const (
			publisherAccount  = "issue_26640_publisher"
			subscriberAccount = "issue_26640_subscriber"
			snapshotName      = "issue_26640_cluster_snapshot"
			publishedDB       = "issue_26640_published"
			subscriptionDB    = "issue_26640_subscription"
			publicationName   = "issue_26640_publication"
		)
		cleanup := func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop snapshot if exists "+snapshotName)
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+publisherAccount+"`")
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+subscriberAccount+"`")
		}
		cleanup()
		defer cleanup()

		execSQLRequire(t, ctx, sysDB,
			"create account `"+publisherAccount+"` admin_name 'admin' identified by '111'")
		execSQLRequire(t, ctx, sysDB,
			"create account `"+subscriberAccount+"` admin_name 'admin' identified by '111'")

		publisherDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", publisherAccount, port,
		))
		require.NoError(t, err)
		defer publisherDB.Close()
		execSQLRequire(t, ctx, publisherDB, "create database `"+publishedDB+"`")
		execSQLRequire(t, ctx, publisherDB,
			"create table `"+publishedDB+"`.orders (id int primary key)")
		execSQLRequire(t, ctx, publisherDB,
			"insert into `"+publishedDB+"`.orders values (1)")
		execSQLRequire(t, ctx, publisherDB,
			"create publication `"+publicationName+"` database `"+publishedDB+"` account `"+subscriberAccount+"`")

		subscriberDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", subscriberAccount, port,
		))
		require.NoError(t, err)
		execSQLRequire(t, ctx, subscriberDB,
			"create database `"+subscriptionDB+"` from `"+publisherAccount+"` publication `"+publicationName+"`")
		execSQLRequire(t, ctx, subscriberDB, "create role subscription_reader")
		execSQLRequire(t, ctx, subscriberDB,
			"create user subscription_user identified by '111' default role subscription_reader")
		execSQLRequire(t, ctx, subscriberDB, "grant connect on account * to subscription_reader")
		execSQLRequire(t, ctx, subscriberDB,
			"grant select on table `"+subscriptionDB+"`.* to subscription_reader")
		execSQLRequire(t, ctx, subscriberDB, "grant subscription_reader to subscription_user")
		require.NoError(t, subscriberDB.Close())
		var sourceSubscriberID uint64
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = ?", subscriberAccount,
		).Scan(&sourceSubscriberID))

		execSQLRequire(t, ctx, sysDB, "create snapshot "+snapshotName+" for cluster")
		// Re-creating the subscriber during cluster restore gives it a new account
		// ID and also forces its subscription database into the deferred phase.
		execSQLRequire(t, ctx, sysDB, "drop account `"+subscriberAccount+"`")
		execSQLRequire(t, ctx, sysDB, "restore cluster{snapshot='"+snapshotName+"'}")
		var targetSubscriberID uint64
		require.NoError(t, sysDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = ?", subscriberAccount,
		).Scan(&targetSubscriberID))
		require.NotEqual(t, sourceSubscriberID, targetSubscriberID)

		restoredAdminDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", subscriberAccount, port,
		))
		require.NoError(t, err)
		defer restoredAdminDB.Close()
		var databaseID, privilegeObjectID uint64
		require.NoError(t, restoredAdminDB.QueryRowContext(ctx,
			"select dat_id from mo_catalog.mo_database where datname = ?", subscriptionDB,
		).Scan(&databaseID))
		require.NoError(t, restoredAdminDB.QueryRowContext(ctx,
			"select obj_id from mo_catalog.mo_role_privs where role_name = 'subscription_reader' "+
				"and obj_type = 'table' and privilege_level = 'd.*' and privilege_name = 'select'",
		).Scan(&privilegeObjectID))
		require.Equal(t, databaseID, privilegeObjectID)

		readerDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#subscription_user#subscription_reader:111@tcp(127.0.0.1:%d)/", subscriberAccount, port,
		))
		require.NoError(t, err)
		defer readerDB.Close()
		var count int
		require.NoError(t, readerDB.QueryRowContext(ctx,
			"select count(*) from `"+subscriptionDB+"`.orders").Scan(&count))
		require.Equal(t, 1, count)
	})
}
