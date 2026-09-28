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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_8"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// This fixture owns its cluster: it rewrites upgrade progress and restarts all
// services on the same storage. A shared cluster or a direct handler call cannot
// prove that persisted progress reaches SQL admission after process-local state
// has been discarded. This models the catalog boundary, not an old binary.
func TestV408ViewMetadataUpgradeResumesAfterRestart(t *testing.T) {
	// Release the package's shared fixture before acquiring a dedicated cluster.
	// The lifecycle helper leaves it reusable for -count and shuffled ordering.
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(svc embed.ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.AutomaticUpgrade = false
				cfg.CN.Frontend.SkipCheckUser = false
			})
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	sqlExecutor := testutils.GetSQLExecutor(cn)
	opts := viewMetadataUpgradeExecutorOptions()
	exec := func(statement string) {
		t.Helper()
		res, err := sqlExecutor.Exec(ctx, statement, opts)
		require.NoError(t, err, statement)
		res.Close()
	}
	exec("create database view_upgrade_recovery")
	exec("create table view_upgrade_recovery.sentinel (id int primary key)")
	exec("insert into view_upgrade_recovery.sentinel values (42)")

	final := v4_0_8.Handler.Metadata()
	previous := v4_0_7.Handler.Metadata()
	steps := []versions.VersionUpgrade{
		{
			FromVersion: "4.0.6", ToVersion: previous.Version,
			FinalVersion: final.Version, FinalVersionOffset: final.VersionOffset,
			State: versions.StateReady, UpgradeOrder: 0,
			UpgradeCluster: previous.UpgradeCluster, UpgradeTenant: previous.UpgradeTenant,
		},
		{
			FromVersion: previous.Version, ToVersion: final.Version,
			FinalVersion: final.Version, FinalVersionOffset: final.VersionOffset,
			State: versions.StateCreated, UpgradeOrder: 1,
			UpgradeCluster: final.UpgradeCluster, UpgradeTenant: final.UpgradeTenant,
		},
	}
	// Initial automatic upgrade is disabled, so no background worker can consume
	// these persisted steps before the deliberate restart.
	require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		if err := versions.UpdateVersionState(final.Version, final.VersionOffset, versions.StateCreated, txn); err != nil {
			return err
		}
		return versions.AddVersionUpgrades(steps, txn)
	}, opts))
	deleteViewMetadataCatalogTables(t, ctx, cn.RawService().(cnservice.Service),
		catalog.MO_VIEW_DEPENDENCIES, catalog.MO_VIEW_REFRESH)

	assertPending := func() {
		t.Helper()
		requireViewMetadataCatalogState(t, ctx, sqlExecutor, map[string]bool{
			catalog.MO_VIEW_DEPENDENCIES: false,
			catalog.MO_VIEW_REFRESH:      false,
		})
		var state int32
		var exists bool
		var got []versions.VersionUpgrade
		require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			var err error
			state, exists, err = versions.GetVersionState(final.Version, final.VersionOffset, txn, false)
			if err != nil {
				return err
			}
			got, err = versions.GetUpgradeVersions(final.Version, final.VersionOffset, txn, false, false)
			return err
		}, opts))
		// Assertions run after ExecTxn releases the transaction, including when
		// a failed assertion terminates this test with FailNow.
		require.True(t, exists)
		require.Equal(t, versions.StateCreated, state)
		require.Len(t, got, 2)
		require.Equal(t, versions.StateReady, got[0].State)
		require.Equal(t, versions.StateCreated, got[1].State)
	}

	// Fail the second DDL after the first has executed in a real transaction.
	injected := &failViewMetadataRefreshCreateTxn{}
	err = sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		injected.TxnExecutor = txn
		return v4_0_8.Handler.HandleClusterUpgrade(ctx, injected)
	}, opts)
	require.ErrorIs(t, err, errInjectedViewMetadataUpgrade)
	require.True(t, injected.failed)
	assertPending()

	// A later attempt executes both DDLs and the progress update, but is
	// interrupted before commit. Neither table nor progress may survive.
	err = sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		if err := v4_0_8.Handler.HandleClusterUpgrade(ctx, txn); err != nil {
			return err
		}
		if err := versions.UpdateVersionUpgradeState(steps[1], versions.StateUpgradingTenant, txn); err != nil {
			return err
		}
		return context.Canceled
	}, opts)
	require.ErrorIs(t, err, context.Canceled)
	assertPending()

	// Also retain one already-created catalog table. Recovery must create only
	// the missing table rather than replacing existing metadata.
	exec(catalog.MoViewDependenciesDDL)
	dependencyID := func() uint64 {
		res, err := sqlExecutor.Exec(ctx,
			"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_view_dependencies'", opts)
		require.NoError(t, err)
		defer res.Close()
		require.Len(t, res.Batches, 1)
		ids := executor.GetFixedRows[uint64](res.Batches[0].Vecs[0])
		require.Len(t, ids, 1)
		return ids[0]
	}()
	require.NotZero(t, dependencyID)

	require.NoError(t, cluster.Close())
	cn.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
	var refreshID uint64
	for restart := 0; restart < 2; restart++ {
		require.NoError(t, cluster.Start())
		func() {
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=30s&writeTimeout=30s",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			var value int
			require.NoError(t, db.QueryRowContext(ctx, "select id from view_upgrade_recovery.sentinel").Scan(&value))
			require.Equal(t, 42, value, "SQL admission and existing user data must survive recovery")
			// Ingress readiness does not mean the asynchronous tenant phase has
			// finished. Wait for durable upgrade completion before restarting again.
			require.Eventually(t, func() bool {
				var state int32
				err := db.QueryRowContext(ctx,
					"select state from mo_catalog.mo_version where version = ? and version_offset = ?",
					final.Version, final.VersionOffset).Scan(&state)
				return err == nil && state == versions.StateReady
			}, time.Minute, 100*time.Millisecond)
			var gotDependencyID, gotRefreshID uint64
			require.NoError(t, db.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_view_dependencies'").Scan(&gotDependencyID))
			require.NoError(t, db.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'mo_catalog' and relname = 'mo_view_refresh'").Scan(&gotRefreshID))
			require.Equal(t, dependencyID, gotDependencyID)
			if restart == 0 {
				refreshID = gotRefreshID
			} else {
				require.Equal(t, refreshID, gotRefreshID, "a completed upgrade must not recreate catalog tables")
			}
			var stepCount, readyCount int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*), sum(state = ?) from mo_catalog.mo_upgrade where final_version = ? and final_version_offset = ?",
				versions.StateReady, final.Version, final.VersionOffset).Scan(&stepCount, &readyCount))
			require.Equal(t, 2, stepCount, "restart must reuse persisted steps rather than queue duplicates")
			require.Equal(t, stepCount, readyCount)
		}()
		require.NoError(t, cluster.Close())
	}
}
