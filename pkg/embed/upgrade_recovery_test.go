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
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_4"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_5"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Recovery has one five-minute workload budget across all cluster generations.
// Startup, including runner admission, does not consume that budget. Call only
// after the preceding generation has been stopped and its SQL calls have returned.
func newRecoveryClusterStarter(
	parent context.Context,
	startCluster func() error,
	now func() time.Time,
) (start func() (context.Context, error), cleanup func()) {
	return newRecoveryClusterStarterWithBudget(parent, startCluster, now, 5*time.Minute)
}

// newRecoveryClusterStarterWithBudget is newRecoveryClusterStarter with an
// explicit workload budget: recovery scenarios that wait on background
// cadences, like storage GC, need a wider allowance than five minutes.
func newRecoveryClusterStarterWithBudget(
	parent context.Context,
	startCluster func() error,
	now func() time.Time,
	budget time.Duration,
) (start func() (context.Context, error), cleanup func()) {
	deadline := now().Add(budget)
	var current context.Context
	var cancel context.CancelFunc
	cleanup = func() {
		if cancel != nil {
			cancel()
		}
	}
	start = func() (context.Context, error) {
		if err := parent.Err(); err != nil {
			return nil, err
		}
		if current != nil {
			if err := current.Err(); err != nil {
				return nil, err
			}
		}
		if !now().Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		cleanup()
		current = nil
		begin := now()
		err := startCluster()
		deadline = deadline.Add(now().Sub(begin))
		if err != nil {
			return nil, err
		}
		if err := parent.Err(); err != nil {
			return nil, err
		}
		current, cancel = context.WithDeadline(parent, deadline)
		if err := current.Err(); err != nil {
			cleanup()
			return nil, err
		}
		return current, nil
	}
	return start, cleanup
}

// Exercise an interrupted published 4.1-to-4.2 route with two CNs, then
// complete the consolidated 4.0.7 step. Startup must be cancellable while
// ordinary SQL is fenced and must reuse the committed upgrade route.
func TestCrossTargetUpgradeRecovery(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	c, err := NewCluster(WithTesting(), WithTestAdmissionContext(ctx), WithCNCount(2), WithPreStart(func(svc ServiceOperator) {
		adjustBasicClusterService(svc)
		if svc.ServiceType() == metadata.ServiceType_CN {
			svc.Adjust(func(cfg *ServiceConfig) { cfg.CN.AutomaticUpgrade = true })
			svc.(*operator).testingCNOptions = recoveryCNOptions([]bootstrap.VersionHandle{v4_0_4.Handler})
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
	frontendPort := cn.GetServiceConfig().CN.Frontend.Port
	db := recoverySQLClient(t, cn)
	for _, statement := range []string{
		"create database upgrade_recovery",
		"create table upgrade_recovery.sentinel(id int primary key, value varchar(20))",
		"insert into upgrade_recovery.sentinel values (1, 'preserved')",
		"create account recovery_tenant ADMIN_NAME 'root' IDENTIFIED BY '111'",
	} {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	for _, statement := range []string{
		"alter table mo_catalog.mo_cdc_watermark drop column target_identity",
		"alter table mo_catalog.mo_cdc_watermark drop column pending_source_table_id",
	} {
		result, err := exec.Exec(ctx, statement, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
		require.NoError(t, err)
		result.Close()
	}
	require.NoError(t, c.Stop())

	interrupted := &interruptedUpgradeHandler{VersionHandle: v4_0_6.Handler, entered: make(chan struct{})}
	setRecoveryCNHandles(c, []bootstrap.VersionHandle{v4_0_4.Handler, v4_0_5.Handler, interrupted}, true)
	started := make(chan error, 1)
	go func() { started <- c.Start() }()
	select {
	case <-interrupted.entered:
	case err := <-started:
		t.Fatalf("startup failed before the retained step: %v", err)
	case <-ctx.Done():
		t.Fatal("retained step was not reached", ctx.Err())
	}
	// Use the saved frontend address rather than the cluster lock while Start is blocked.
	pingCtx, pingCancel := context.WithTimeout(ctx, time.Second)
	db, err = sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", frontendPort))
	require.NoError(t, err)
	require.Error(t, db.PingContext(pingCtx), "unfinished catalog must not admit SQL")
	pingCancel()
	require.NoError(t, db.Close())
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()
	select {
	case err := <-started:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("stop did not cancel the upgrade startup", ctx.Err())
	}
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("cluster did not finish stopping", ctx.Err())
	}

	setRecoveryCNHandles(c, []bootstrap.VersionHandle{v4_0_4.Handler, v4_0_5.Handler, v4_0_6.Handler}, true)
	require.NoError(t, c.Start())
	cn, err = c.GetCNService(0)
	require.NoError(t, err)
	retained, steps := readRecoveryCatalog(t, ctx, cn, v4_0_6.Handler.Metadata())
	require.Equal(t, versions.StateReady, retained.State)
	require.Len(t, steps, 2)
	retainedIDs := make([]uint64, len(steps))
	for i, step := range steps {
		retainedIDs[i] = step.ID
		require.Equal(t, versions.StateReady, step.State)
		require.Equal(t, step.TotalTenant, step.ReadyTenant)
	}
	require.NoError(t, c.Stop())

	setRecoveryCNHandles(c, []bootstrap.VersionHandle{v4_0_4.Handler, v4_0_5.Handler, v4_0_6.Handler, v4_0_7.Handler}, true)
	for generation := 0; generation < 2; generation++ {
		require.NoError(t, c.Start())
		for i := 0; i < 2; i++ {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			checkRecoverySQL(t, ctx, cn, v4_0_7.Handler.Metadata())
		}
		cn, err = c.GetCNService(0)
		require.NoError(t, err)
		retained, steps = readRecoveryCatalog(t, ctx, cn, v4_0_6.Handler.Metadata())
		require.Equal(t, versions.StateReady, retained.State)
		require.Len(t, steps, len(retainedIDs))
		for i, step := range steps {
			require.Equal(t, retainedIDs[i], step.ID, "published route must not be replaced")
		}
		final, current := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
		require.Equal(t, versions.StateReady, final.State)
		require.Len(t, current, 1)
		require.Equal(t, versions.StateReady, current[0].State)
		require.Equal(t, int32(2), current[0].TotalTenant)
		require.Equal(t, current[0].TotalTenant, current[0].ReadyTenant)
		require.NoError(t, c.Stop())
	}
}

func TestRecoveryClusterStartupBudget(t *testing.T) {
	t.Run("aggregate workload across restarts", func(t *testing.T) {
		base := time.Now().Add(time.Hour)
		now := base
		starts := 0
		start, cleanup := newRecoveryClusterStarter(t.Context(), func() error {
			starts++
			now = now.Add(10 * time.Minute)
			return nil
		}, func() time.Time { return now })
		defer cleanup()
		first, err := start()
		require.NoError(t, err)
		deadline, ok := first.Deadline()
		require.True(t, ok)
		require.Equal(t, base.Add(15*time.Minute), deadline)

		now = now.Add(time.Minute) // One minute of actual workload consumes budget.
		second, err := start()
		require.NoError(t, err)
		require.ErrorIs(t, first.Err(), context.Canceled)
		deadline, ok = second.Deadline()
		require.True(t, ok)
		require.Equal(t, base.Add(25*time.Minute), deadline)
		require.Equal(t, 4*time.Minute, deadline.Sub(now))

		now = deadline.Add(time.Minute)
		ctx, err := start()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, ctx)
		require.Equal(t, 2, starts, "exhausted workload must not start another generation")
		cleanup()
		cleanup()
		require.ErrorIs(t, second.Err(), context.Canceled)
	})

	for _, phase := range []string{"before startup", "during startup", "after startup"} {
		t.Run("parent canceled "+phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			starts := 0
			start, cleanup := newRecoveryClusterStarter(parent, func() error {
				starts++
				if phase == "during startup" {
					cancel()
				}
				return nil
			}, time.Now)
			defer cleanup()
			if phase == "before startup" {
				cancel()
			}
			ctx, err := start()
			if phase == "after startup" {
				require.NoError(t, err)
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				ctx, err = start()
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, ctx)
			expectedStarts := 1
			if phase == "before startup" {
				expectedStarts = 0
			}
			require.Equal(t, expectedStarts, starts)
		})
	}

	t.Run("startup error", func(t *testing.T) {
		failure := errors.New("startup failed")
		base := time.Now().Add(time.Hour)
		now := base
		starts := 0
		start, cleanup := newRecoveryClusterStarter(t.Context(), func() error {
			starts++
			now = now.Add(10 * time.Minute)
			if starts == 1 {
				return failure
			}
			return nil
		}, func() time.Time { return now })
		defer cleanup()
		ctx, err := start()
		require.ErrorIs(t, err, failure)
		require.Nil(t, ctx)
		ctx, err = start()
		require.NoError(t, err, "a failed startup must not consume workload budget")
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, 5*time.Minute, deadline.Sub(now))
		require.Equal(t, 2, starts)
	})

	t.Run("parent deadline and workload cancellation", func(t *testing.T) {
		deadline := time.Now().Add(time.Minute)
		parent, cancel := context.WithDeadline(t.Context(), deadline)
		defer cancel()
		starts := 0
		start, cleanup := newRecoveryClusterStarter(parent, func() error { starts++; return nil }, time.Now)
		defer cleanup()
		ctx, err := start()
		require.NoError(t, err)
		actual, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, deadline, actual, "workload budget cannot extend the parent deadline")
		cleanup()
		ctx, err = start()
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ctx)
		require.Equal(t, 1, starts)
	})
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

func recoveryCNOptions(handles []bootstrap.VersionHandle, extra ...bootstrap.Option) []cnservice.Option {
	return []cnservice.Option{
		cnservice.WithBootstrapOptions(
			bootstrap.WithUpgradeHandles(handles),
			bootstrap.WithCheckUpgradeDuration(100*time.Millisecond),
			bootstrap.WithCheckUpgradeTenantDuration(100*time.Millisecond),
			bootstrap.WithCheckUpgradeTenantWorkers(1),
			bootstrap.WithUpgradeTenantBatch(1),
		),
		cnservice.WithBootstrapOptions(extra...),
	}
}

func setRecoveryCNHandles(c Cluster, handles []bootstrap.VersionHandle, automatic bool, extra ...bootstrap.Option) {
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			op := svc.(*operator)
			op.Lock()
			op.testingCNOptions = recoveryCNOptions(handles, extra...)
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
