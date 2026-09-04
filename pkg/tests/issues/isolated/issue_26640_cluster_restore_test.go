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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// A cluster restore replaces cluster-wide catalog tables. Keep this regression
// in the isolated package so a failed restore cannot poison shared issue tests.
func TestIssue26640ClusterRestoreRebindsSubscriptionPrivileges(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(2),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() != metadata.ServiceType_CN {
				return
			}
			service.Adjust(func(config *embed.ServiceConfig) {
				config.CN.LockService.MaxFixedSliceSize = 10001
				config.CN.LockService.MaxLockRowCount = 10000
				config.CN.Frontend.SkipCheckUser = false
				config.CN.Frontend.Iceberg.Enable = true
				config.CN.Frontend.Iceberg.EnableWrite = true
				config.CN.Frontend.Iceberg.EnableDelete = true
				config.CN.Frontend.Iceberg.EnableDML = true
				config.CN.Frontend.Iceberg.EnableMaintenance = true
			})
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port
	sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
	require.NoError(t, err)
	defer sysDB.Close()
	require.NoError(t, waitSystemBootstrap(ctx, sysDB))

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
	var sourcePublisherID, sourceSubscriberID uint64
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", publisherAccount,
	).Scan(&sourcePublisherID))
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", subscriberAccount,
	).Scan(&sourceSubscriberID))

	execSQLRequire(t, ctx, sysDB, "create snapshot "+snapshotName+" for cluster")
	// Re-create both sides of the publication. Publication metadata carries
	// the historical publisher ID, while the subscription database is restored
	// in a deferred phase after the publication has been reconstructed.
	execSQLRequire(t, ctx, sysDB, "drop account `"+subscriberAccount+"`")
	execSQLRequire(t, ctx, sysDB, "drop account `"+publisherAccount+"`")
	execSQLRequire(t, ctx, sysDB, "restore cluster{snapshot='"+snapshotName+"'}")
	var targetPublisherID, targetSubscriberID uint64
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", publisherAccount,
	).Scan(&targetPublisherID))
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", subscriberAccount,
	).Scan(&targetSubscriberID))
	require.NotEqual(t, sourcePublisherID, targetPublisherID)
	require.NotEqual(t, sourceSubscriberID, targetSubscriberID)

	var restoredPublicationAccountID, restoredPublicationDatabaseID, targetPublishedDatabaseID uint64
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id, database_id from mo_catalog.mo_pubs "+
			"where account_name = ? and pub_name = ?",
		publisherAccount,
		publicationName,
	).Scan(&restoredPublicationAccountID, &restoredPublicationDatabaseID))
	require.Equal(t, targetPublisherID, restoredPublicationAccountID)
	restoredPublisherDB, err := sql.Open("mysql", fmt.Sprintf(
		"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", publisherAccount, port,
	))
	require.NoError(t, err)
	defer restoredPublisherDB.Close()
	require.NoError(t, restoredPublisherDB.QueryRowContext(ctx,
		"select dat_id from mo_catalog.mo_database where datname = ?", publishedDB,
	).Scan(&targetPublishedDatabaseID))
	require.Equal(t, targetPublishedDatabaseID, restoredPublicationDatabaseID)

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
}
