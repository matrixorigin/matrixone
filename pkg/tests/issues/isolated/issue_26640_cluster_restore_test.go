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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// A cluster restore replaces cluster-wide catalog tables. Keep this regression
// in the isolated package so a failed restore cannot poison shared issue tests.
func TestIssue26640ClusterRestoreRebindsSubscriptionPrivileges(t *testing.T) {
	releaseSharedSingleCNCluster(t)
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
	runIssue28742CanceledRestoreWithMetadataProbe(t, ctx, cluster, sysDB, snapshotName,
		publisherAccount, subscriberAccount)
	runIssue28742RestoreWithMetadataProbe(t, ctx, cluster, sysDB, snapshotName)
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

func runIssue28742CanceledRestoreWithMetadataProbe(
	t *testing.T,
	parent context.Context,
	cluster embed.Cluster,
	sysDB *sql.DB,
	snapshotName string,
	publisherAccount string,
	subscriberAccount string,
) {
	t.Helper()
	const restoreGate = "restore-before-view-metadata-lifecycle"
	const restoreGateWaiters = restoreGate + "-waiters"

	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	cn1Service, ok := cn1.RawService().(cnservice.Service)
	require.True(t, ok, "CN service does not expose the SQL executor")
	probeExecutor := cn1Service.GetSQLExecutor()
	require.NotNil(t, probeExecutor)
	services := issue28742LockServices(cluster)
	require.NotEmpty(t, services)

	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}
	require.NoError(t, fault.AddFaultPoint(
		parent, restoreGate, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(
		parent, restoreGateWaiters, ":::", "getwaiters", 0, restoreGate, false))

	restoreCtx, cancelRestore := context.WithTimeout(parent, 90*time.Second)
	probeCtx, cancelProbe := context.WithCancel(restoreCtx)
	defer cancelProbe()
	defer cancelRestore()
	restoreDone := make(chan error, 1)
	probeDone := make(chan error, 1)
	restoreStarted := false
	probeStarted := false
	defer func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), restoreGateWaiters)
		_, _ = fault.RemoveFaultPoint(context.Background(), restoreGate)
		cancelProbe()
		cancelRestore()
		if restoreStarted {
			select {
			case <-restoreDone:
			case <-time.After(30 * time.Second):
				t.Errorf("canceled restore goroutine did not exit during cleanup")
			}
		}
		if probeStarted {
			select {
			case <-probeDone:
			case <-time.After(30 * time.Second):
				t.Errorf("canceled view-metadata fence goroutine did not exit during cleanup")
			}
		}
	}()

	go func() {
		_, restoreErr := sysDB.ExecContext(restoreCtx,
			"restore cluster{snapshot='"+snapshotName+"'}")
		restoreDone <- restoreErr
	}()
	restoreStarted = true
	require.Eventually(t, func() bool {
		waiters, _, exists := fault.TriggerFault(restoreGateWaiters)
		return exists && waiters == 1
	}, 30*time.Second, 10*time.Millisecond,
		"restore did not reach the cancellation barrier")

	go func() {
		probeDone <- compile.RequireViewMetadataRevalidation(probeCtx, probeExecutor)
	}()
	probeStarted = true
	require.Eventually(t, func() bool {
		return issue28742HasFeatureRegistryMetadataWaiter(services)
	}, 20*time.Second, 10*time.Millisecond,
		"feature-registry metadata probe did not wait before cancellation")

	cancelProbe()
	select {
	case err = <-probeDone:
		probeStarted = false
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "canceled view-metadata fence did not return")
	}

	cancelRestore()
	_, err = fault.RemoveFaultPoint(parent, restoreGate)
	require.NoError(t, err)
	select {
	case err = <-restoreDone:
		restoreStarted = false
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "canceled restore did not return")
	}

	freshCtx, freshCancel := context.WithTimeout(parent, 15*time.Second)
	defer freshCancel()
	err = probeExecutor.ExecTxn(freshCtx, func(txn executor.TxnExecutor) error {
		for _, sql := range []string{
			catalog.FeatureRegistryCatalogGateSQL,
			catalog.SnapshotLifecycleGateSQL,
		} {
			result, execErr := txn.Exec(sql, executor.StatementOption{})
			if execErr != nil {
				return execErr
			}
			result.Close()
		}
		return nil
	}, executor.Options{}.WithAccountID(catalog.System_Account))
	require.NoError(t, err)

	var remaining int
	require.NoError(t, sysDB.QueryRowContext(parent,
		"select count(*) from mo_catalog.mo_account where account_name in (?, ?)",
		publisherAccount, subscriberAccount).Scan(&remaining))
	require.Zero(t, remaining)
}

func runIssue28742RestoreWithMetadataProbe(
	t *testing.T,
	parent context.Context,
	cluster embed.Cluster,
	sysDB *sql.DB,
	snapshotName string,
) {
	t.Helper()
	const restoreGate = "restore-before-view-metadata-lifecycle"
	const restoreGateWaiters = restoreGate + "-waiters"

	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	cn1Service, ok := cn1.RawService().(cnservice.Service)
	require.True(t, ok, "CN service does not expose the SQL executor")
	probeExecutor := cn1Service.GetSQLExecutor()
	require.NotNil(t, probeExecutor)

	services := issue28742LockServices(cluster)
	require.NotEmpty(t, services)

	faultEnabledHere := fault.Enable()
	if faultEnabledHere {
		defer fault.Disable()
	}
	require.NoError(t, fault.AddFaultPoint(
		parent, restoreGate, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(
		parent, restoreGateWaiters, ":::", "getwaiters", 0, restoreGate, false))

	ctx, cancel := context.WithTimeout(parent, 180*time.Second)
	defer cancel()
	restoreDone := make(chan error, 1)
	probeDone := make(chan error, 1)
	restoreStarted := false
	probeStarted := false
	restoreConsumed := false
	probeConsumed := false
	defer func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), restoreGateWaiters)
		_, _ = fault.RemoveFaultPoint(context.Background(), restoreGate)
		cancel()
		if restoreStarted && !restoreConsumed {
			select {
			case <-restoreDone:
			case <-time.After(30 * time.Second):
				t.Errorf("restore goroutine did not exit during cleanup")
			}
		}
		if probeStarted && !probeConsumed {
			select {
			case <-probeDone:
			case <-time.After(30 * time.Second):
				t.Errorf("view-metadata fence goroutine did not exit during cleanup")
			}
		}
	}()

	go func() {
		_, restoreErr := sysDB.ExecContext(ctx,
			"restore cluster{snapshot='"+snapshotName+"'}")
		restoreDone <- restoreErr
	}()
	restoreStarted = true

	require.Eventually(t, func() bool {
		waiters, _, exists := fault.TriggerFault(restoreGateWaiters)
		return exists && waiters == 1
	}, 30*time.Second, 10*time.Millisecond,
		"restore did not reach the lifecycle coordination barrier")

	probeCtx, cancelProbe := context.WithTimeout(ctx, 120*time.Second)
	defer cancelProbe()
	go func() {
		probeDone <- compile.RequireViewMetadataRevalidation(probeCtx, probeExecutor)
	}()
	probeStarted = true

	// The fixed path owns the feature-registry identity exclusively before it
	// pauses at restoreGate. The probe therefore queues on that same metadata
	// key instead of holding a shared key while waiting on SNAPSHOT. On the old
	// path this condition cannot become true until the restore is released,
	// which makes the regression discriminate the original lock order.
	require.Eventually(t, func() bool {
		return issue28742HasFeatureRegistryMetadataWaiter(services)
	}, 15*time.Second, 10*time.Millisecond,
		"feature-registry metadata probe did not wait behind restore admission")

	_, err = fault.RemoveFaultPoint(parent, restoreGate)
	require.NoError(t, err)

	select {
	case err = <-restoreDone:
		restoreConsumed = true
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatalf("restore did not return: %v", ctx.Err())
	}
	select {
	case err = <-probeDone:
		probeConsumed = true
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatalf("feature-registry metadata probe did not return: %v", ctx.Err())
	}

	var generation uint64
	require.NoError(t, sysDB.QueryRowContext(parent,
		"select dependency_generation from mo_catalog.mo_view_dependencies "+
			"where account_id=0 and target_relation_id=0 and dependency_ordinal=0",
	).Scan(&generation))
	require.Greater(t, generation, uint64(0))
}

func issue28742LockServices(cluster embed.Cluster) []lockservice.LockService {
	var services []lockservice.LockService
	cluster.ForeachServices(func(service embed.ServiceOperator) bool {
		if service.ServiceType() == metadata.ServiceType_CN {
			services = append(services, lockservice.GetLockServiceByServiceID(service.ServiceID()))
		}
		return true
	})
	return services
}

func issue28742HasFeatureRegistryMetadataWaiter(services []lockservice.LockService) bool {
	for _, service := range services {
		found := false
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID != catalog.MO_TABLES_ID || !issue28742HasKey(keys, []byte("mo_feature_registry")) {
				return true
			}
			lock.IterWaiters(func(pblock.WaitTxn) bool {
				found = true
				return false
			})
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func issue28742HasKey(keys [][]byte, needle []byte) bool {
	for _, key := range keys {
		if bytes.Contains(key, needle) {
			return true
		}
	}
	return false
}
