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

// The catalog and CN generations are destructive test inputs, so this fixture
// cannot share the package's reusable SQL cluster. Three CNs compete for the
// same real transactions; only one user tenant and one sentinel row are needed.
func TestCrossTargetUpgradeRecovery(t *testing.T) {
	// Earlier package tests may leave an idle reusable fixture. This test owns
	// destructive catalog generations and must use exclusive cluster admission.
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	for _, final := range []bootstrap.VersionHandle{v4_0_12.Handler, v4_0_13.Handler} {
		t.Run(final.Metadata().Version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			started := time.Now()
			c, err := NewCluster(WithTesting(), WithCNCount(3), WithPreStart(func(svc ServiceOperator) {
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

			interrupted := &interruptedUpgradeHandler{VersionHandle: v4_0_11.Handler, entered: make(chan struct{})}
			setRecoveryCNHandles(c, []bootstrap.VersionHandle{v4_0_10.Handler, interrupted})
			require.NoError(t, c.Start())
			select {
			case <-interrupted.entered:
			case <-ctx.Done():
				t.Fatal("retained upgrade did not reach the injected interruption", ctx.Err())
			}
			retained, steps := readRecoveryCatalog(t, ctx, cn, v4_0_11.Handler.Metadata())
			require.Equal(t, versions.StateCreated, retained.State)
			require.Len(t, steps, 1)
			require.Equal(t, versions.StateCreated, steps[0].State)
			retainedStepID := steps[0].ID
			require.NoError(t, c.Close())

			handles := []bootstrap.VersionHandle{v4_0_10.Handler, v4_0_11.Handler, v4_0_12.Handler}
			if final.Metadata().Version != v4_0_12.Handler.Metadata().Version {
				handles = append(handles, final)
			}
			setRecoveryCNHandles(c, handles)
			// Start uses concurrent CN startup against the persisted interrupted
			// target. No version/task records are manually rewritten for recovery.
			require.NoError(t, c.Start())
			checkRecovered := func() {
				t.Helper()
				for i := 0; i < 3; i++ {
					cn, err := c.GetCNService(i)
					require.NoError(t, err)
					checkRecoverySQL(t, ctx, cn, final.Metadata())
				}
				retained, steps = readRecoveryCatalog(t, ctx, cn, v4_0_11.Handler.Metadata())
				require.Equal(t, versions.StateReady, retained.State)
				require.Len(t, steps, 1)
				require.Equal(t, retainedStepID, steps[0].ID, "recovery must reuse the retained route")
				require.Equal(t, versions.StateReady, steps[0].State)
				_, current := readRecoveryCatalog(t, ctx, cn, final.Metadata())
				require.Len(t, current, len(handles)-2)
				for i, step := range current {
					require.Equal(t, int32(i), step.UpgradeOrder)
					require.Equal(t, versions.StateReady, step.State)
					require.Equal(t, int32(2), step.TotalTenant)
					require.Equal(t, step.TotalTenant, step.ReadyTenant)
				}
			}
			checkRecovered()
			require.NoError(t, c.Close())
			require.NoError(t, c.Start())
			checkRecovered()
			t.Logf("three-CN interrupted upgrade and restart: %s", time.Since(started))
		})
	}
}

type interruptedUpgradeHandler struct {
	bootstrap.VersionHandle
	entered chan struct{}
	once    sync.Once
}

func (h *interruptedUpgradeHandler) HandleClusterUpgrade(context.Context, executor.TxnExecutor) error {
	h.once.Do(func() { close(h.entered) })
	// A retryable failure leaves the transaction rolled back and the already
	// committed route intact. It never blocks service shutdown behind a barrier.
	return errors.New("test interruption before retained cluster DDL")
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

func setRecoveryCNHandles(c Cluster, handles []bootstrap.VersionHandle) {
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			op := svc.(*operator)
			op.Lock()
			op.testingCNOptions = recoveryCNOptions(handles)
			op.Unlock()
		}
		return true
	})
}

func recoverySQLClient(t *testing.T, cn ServiceOperator) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
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
