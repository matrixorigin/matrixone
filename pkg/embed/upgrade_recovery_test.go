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

package embed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_10"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_11"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_12"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_13"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const recoveryAdminUser = "upgrade_recovery_admin"

// The catalog and CN generations are destructive test inputs, so this fixture
// cannot share the package's reusable SQL cluster. Three CNs compete for the
// same real transactions; only one user tenant and one sentinel row are needed.
func TestCrossTargetUpgradeRecovery(t *testing.T) {
	// Earlier package tests may leave an idle reusable fixture. This test owns
	// destructive catalog generations and must use exclusive cluster admission.
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	for _, tc := range []struct {
		name   string
		final  bootstrap.VersionHandle
		manual bool
	}{
		{name: "4.0.12", final: v4_0_12.Handler},
		{name: "4.0.13", final: v4_0_13.Handler},
		{name: "manual-4.0.13", final: v4_0_13.Handler, manual: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			final := tc.final
			var retainedHandler bootstrap.VersionHandle = v4_0_11.Handler
			cnCount, retainedSteps, pendingState := 3, 1, int32(versions.StateCreated)
			if tc.manual {
				// One CN proves recovery does not borrow a worker from a peer.
				cnCount, retainedSteps, pendingState = 1, 2, versions.StateUpgradingTenant
				retainedHandler = v4_0_12.Handler
				// Use the supported bootstrap admin for the recovery command:
				// ordinary logins try to upgrade their tenant before accepting SQL.
				t.Setenv("mo_admin_user", recoveryAdminUser)
				t.Setenv("mo_admin_password", "111")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			started := time.Now()
			c, err := NewCluster(WithTesting(), WithCNCount(cnCount), WithPreStart(func(svc ServiceOperator) {
				adjustBasicClusterService(svc)
				if svc.ServiceType() == metadata.ServiceType_CN {
					svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
					svc.(*operator).testingCNOptions = recoveryCNOptions([]bootstrap.VersionHandle{v4_0_10.Handler})
				}
			}))
			if c != nil {
				t.Cleanup(func() {
					require.NoError(t, c.Close())
					require.NoError(t, os.RemoveAll(c.(*cluster).options.dataPath))
				})
			}
			require.NoError(t, err)
			require.NoError(t, c.Start())
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			func() {
				db := recoverySQLClient(t, cn)
				defer db.Close()
				for _, statement := range []string{
					"create database upgrade_recovery",
					"create table upgrade_recovery.sentinel(id int primary key, value varchar(20))",
					"insert into upgrade_recovery.sentinel values (1, 'preserved')",
					"create account recovery_tenant ADMIN_NAME 'root' IDENTIFIED BY '111'",
				} {
					_, err := db.ExecContext(ctx, statement)
					require.NoError(t, err)
				}
			}()
			// Fresh bootstrap uses current definitions. Restore just the schema
			// boundary repaired by the real retained handler, not upgrade states.
			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			for _, statement := range []string{
				"alter table mo_catalog.mo_cdc_watermark drop column target_identity",
				"alter table mo_catalog.mo_cdc_watermark drop column pending_source_table_id",
			} {
				res, err := exec.Exec(ctx, statement, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
				require.NoError(t, err)
				res.Close()
			}
			require.NoError(t, c.Close())

			interrupted := &interruptedUpgradeHandler{VersionHandle: retainedHandler, entered: make(chan struct{}), tenant: tc.manual}
			retainedHandles := []bootstrap.VersionHandle{v4_0_10.Handler}
			if tc.manual {
				retainedHandles = append(retainedHandles, v4_0_11.Handler)
			}
			setRecoveryCNHandles(c, append(retainedHandles, interrupted), true)
			require.NoError(t, c.Start())
			select {
			case <-interrupted.entered:
			case <-ctx.Done():
				t.Fatal("retained upgrade did not reach the injected interruption", ctx.Err())
			}
			retained, steps := readRecoveryCatalog(t, ctx, cn, retainedHandler.Metadata())
			require.Equal(t, versions.StateCreated, retained.State)
			require.Len(t, steps, retainedSteps)
			require.Equal(t, pendingState, steps[len(steps)-1].State)
			retainedStepIDs := make([]uint64, len(steps))
			for i, step := range steps {
				retainedStepIDs[i] = step.ID
			}
			var retainedTasks []uint64
			if tc.manual {
				require.Equal(t, final.Metadata().VersionOffset, retained.VersionOffset)
				require.Equal(t, int32(2), steps[len(steps)-1].TotalTenant)
				require.Zero(t, steps[len(steps)-1].ReadyTenant)
				retainedTasks = recoveryTenantTaskIDs(t, ctx, cn, steps[len(steps)-1].ID, versions.No)
				require.NotEmpty(t, retainedTasks) // CN configuration determines the account range batch size.
			}
			require.NoError(t, c.Close())

			handles := []bootstrap.VersionHandle{v4_0_10.Handler, v4_0_11.Handler, v4_0_12.Handler}
			if final.Metadata().Version != v4_0_12.Handler.Metadata().Version {
				handles = append(handles, final)
			}
			setRecoveryCNHandles(c, handles, !tc.manual)
			// No version/task records are manually rewritten for recovery.
			require.NoError(t, c.Start())
			if tc.manual {
				func() {
					db := recoverySQLClientWithUser(t, cn, recoveryAdminUser)
					defer db.Close()
					var newerTargets int
					require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_version where version = ?", final.Metadata().Version).Scan(&newerTargets))
					require.Zero(t, newerTargets, "automatic upgrade must remain disabled")
					_, err := db.ExecContext(ctx, "upgrade account 'recovery_tenant' with retry 1")
					require.ErrorContains(t, err, "Please try again later", "do not accept a single-tenant retry with no consumer for the retained tasks")
				}()
			}
			checkRecovered := func() {
				t.Helper()
				for i := 0; i < cnCount; i++ {
					cn, err := c.GetCNService(i)
					require.NoError(t, err)
					checkRecoverySQL(t, ctx, cn, final.Metadata())
				}
				retained, steps = readRecoveryCatalog(t, ctx, cn, retainedHandler.Metadata())
				require.Equal(t, versions.StateReady, retained.State)
				require.Len(t, steps, retainedSteps)
				for i, step := range steps {
					require.Equal(t, retainedStepIDs[i], step.ID, "recovery must reuse the retained route")
					require.Equal(t, versions.StateReady, step.State)
					require.Equal(t, step.TotalTenant, step.ReadyTenant)
				}
				if tc.manual {
					require.Equal(t, retainedTasks, recoveryTenantTaskIDs(t, ctx, cn, steps[len(steps)-1].ID, versions.Yes))
				}
				_, current := readRecoveryCatalog(t, ctx, cn, final.Metadata())
				require.Len(t, current, len(handles)-1-retainedSteps)
				for i, step := range current {
					require.Equal(t, int32(i), step.UpgradeOrder)
					require.Equal(t, versions.StateReady, step.State)
					require.Equal(t, int32(2), step.TotalTenant)
					require.Equal(t, step.TotalTenant, step.ReadyTenant)
				}
			}
			checkRecovered()
			if tc.manual {
				func() {
					db := recoverySQLClientWithUser(t, cn, recoveryAdminUser)
					defer db.Close()
					_, err := db.ExecContext(ctx, "upgrade account 'recovery_tenant' with retry 1")
					require.NoError(t, err)
				}()
			}
			require.NoError(t, c.Close())
			require.NoError(t, c.Start())
			checkRecovered()
			t.Logf("%d-CN interrupted upgrade and restart (manual=%t): %s", cnCount, tc.manual, time.Since(started))
		})
	}
}

type interruptedUpgradeHandler struct {
	bootstrap.VersionHandle
	entered chan struct{}
	once    sync.Once
	tenant  bool
}

func (h *interruptedUpgradeHandler) HandleClusterUpgrade(ctx context.Context, txn executor.TxnExecutor) error {
	if h.tenant {
		return h.VersionHandle.HandleClusterUpgrade(ctx, txn)
	}
	return h.interrupt()
}

func (h *interruptedUpgradeHandler) HandleTenantUpgrade(ctx context.Context, id int32, txn executor.TxnExecutor) error {
	if !h.tenant {
		return h.VersionHandle.HandleTenantUpgrade(ctx, id, txn)
	}
	return h.interrupt()
}

func (h *interruptedUpgradeHandler) interrupt() error {
	h.once.Do(func() { close(h.entered) })
	// A retryable failure leaves the transaction rolled back and the already
	// committed route intact. It never blocks service shutdown behind a barrier.
	return errors.New("test interruption before retained upgrade DDL")
}

func recoveryCNOptions(handles []bootstrap.VersionHandle) []cnservice.Option {
	return []cnservice.Option{cnservice.WithBootstrapOptions(
		bootstrap.WithUpgradeHandles(handles),
		bootstrap.WithCheckUpgradeDuration(100*time.Millisecond),
		bootstrap.WithCheckUpgradeTenantDuration(100*time.Millisecond),
		bootstrap.WithCheckUpgradeTenantWorkers(1),
		bootstrap.WithUpgradeTenantBatch(1),
	)}
}

func setRecoveryCNHandles(c Cluster, handles []bootstrap.VersionHandle, automatic bool) {
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			op := svc.(*operator)
			op.Lock()
			op.testingCNOptions = recoveryCNOptions(handles)
			op.Unlock()
			svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.AutomaticUpgrade = automatic })
		}
		return true
	})
}

func recoverySQLClient(t *testing.T, cn ServiceOperator) *sql.DB {
	t.Helper()
	return recoverySQLClientWithUser(t, cn, "dump")
}

func recoverySQLClientWithUser(t *testing.T, cn ServiceOperator, user string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	return db
}

func readRecoveryCatalog(t *testing.T, ctx context.Context, cn ServiceOperator, target versions.Version) (versions.Version, []versions.VersionUpgrade) {
	t.Helper()
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	var steps []versions.VersionUpgrade
	err := exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		state, exists, err := versions.GetVersionState(target.Version, target.VersionOffset, txn, false)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("missing persisted target %s offset %d", target.Version, target.VersionOffset)
		}
		target.State = state
		steps, err = versions.GetUpgradeVersions(target.Version, target.VersionOffset, txn, false, false)
		return err
	}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
	require.NoError(t, err)
	return target, steps
}

func recoveryTenantTaskIDs(t *testing.T, ctx context.Context, cn ServiceOperator, upgradeID uint64, ready int32) []uint64 {
	t.Helper()
	db := recoverySQLClientWithUser(t, cn, recoveryAdminUser)
	defer db.Close()
	rows, err := db.QueryContext(ctx, "select id from mo_catalog.mo_upgrade_tenant where upgrade_id = ? and ready = ? order by id", upgradeID, ready)
	require.NoError(t, err)
	defer rows.Close()
	var ids []uint64
	for rows.Next() {
		var id uint64
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func checkRecoverySQL(t *testing.T, ctx context.Context, cn ServiceOperator, final versions.Version) {
	t.Helper()
	db := recoverySQLClient(t, cn)
	defer db.Close()
	// SQL readiness alone is weaker than committed upgrade completion. Poll the
	// real catalog condition with a bounded context, not a scheduling delay.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var state int32
		err := db.QueryRowContext(ctx, "select state from mo_catalog.mo_version where version = ? and version_offset = ?",
			final.Version, final.VersionOffset).Scan(&state)
		require.NoError(c, err)
		require.Equal(c, versions.StateReady, state)
	}, time.Minute, 100*time.Millisecond)
	var value string
	require.NoError(t, db.QueryRowContext(ctx, "select value from upgrade_recovery.sentinel where id=1").Scan(&value))
	require.Equal(t, "preserved", value)
	for _, query := range []string{
		"select count(*) from mo_catalog.mo_columns where att_database='mo_catalog' and att_relname='mo_cdc_watermark' and attname in ('pending_source_table_id','target_identity')",
		"select count(*) from mo_catalog.mo_account where create_version = '" + final.Version + "'",
	} {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&count))
		require.Equal(t, 2, count)
	}
	var duplicates int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select upgrade_id, from_account_id, to_account_id from mo_catalog.mo_upgrade_tenant group by upgrade_id, from_account_id, to_account_id having count(*) > 1) d").Scan(&duplicates))
	require.Zero(t, duplicates)
}
