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

package bootstrap

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	molog "github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMaintainOrphanObjectPrivilegesFiniteAccountRound(t *testing.T) {
	lookup := 0
	var selected []int32
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
				lookup++
				switch lookup {
				case 1:
					require.Contains(t, sql, "account_id >= 10")
					require.Contains(t, sql, "account_id <= 12")
					selected = append(selected, 10)
					return buildMaintenanceAccountRows(10), nil
				case 2:
					require.Contains(t, sql, "account_id >= 11")
					selected = append(selected, 12)
					return buildMaintenanceAccountRows(12), nil
				case 3:
					require.Contains(t, sql, "account_id >= 13")
					return executor.Result{}, nil
				case 4:
					require.Contains(t, sql, "account_id >= 0")
					require.Contains(t, sql, "account_id < 10")
					selected = append(selected, 0)
					return buildMaintenanceAccountRows(0), nil
				case 5:
					require.Contains(t, sql, "account_id >= 1")
					selected = append(selected, 5)
					return buildMaintenanceAccountRows(5), nil
				case 6:
					require.Contains(t, sql, "account_id >= 6")
					require.Contains(t, sql, "account_id < 10")
					return executor.Result{}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected lookup %d", lookup)
				}
			case strings.Contains(sql, "from mo_catalog.mo_role_privs"):
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceStart = func() string { return "0102" }
	service.upgrade.orphanPrivilegeMaintenanceState = orphanPrivilegeMaintenanceState{
		restartSeed:      1,
		roundInitialized: true,
		roundHighWater:   12,
		roundStart:       10,
		accountCursor:    10,
	}

	for range 5 {
		require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	}
	require.Equal(t, []int32{10, 12, 0, 5}, selected)
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.False(t, state.roundInitialized)
	require.Equal(t, uint64(1), state.round)
	require.False(t, state.tenantSelected)
}

func TestMaintainOrphanObjectPrivilegesFinishesTenantHighWaterBeforeAdvancing(t *testing.T) {
	lookupCount := 0
	candidatePage := make([]v4_0_6.OrphanPrivilegeKey, 1000)
	for i := range candidatePage {
		candidatePage[i] = v4_0_6.OrphanPrivilegeKey{
			RoleID: int32(i), ObjectType: "account", PrivilegeLevel: "*",
		}
	}
	highWater := v4_0_6.OrphanPrivilegeKey{
		RoleID: 2000, ObjectType: "account", PrivilegeLevel: "*",
	}
	candidateReads := 0
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
				lookupCount++
				return buildMaintenanceAccountRows(10), nil
			case strings.Contains(sql, "order by __mo_cpkey_col desc"):
				return buildMaintenancePrivilegeRows(t, highWater), nil
			case strings.HasPrefix(sql,
				"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "):
				candidateReads++
				if candidateReads == 1 {
					return buildMaintenancePrivilegeRows(t, candidatePage...), nil
				}
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceState = orphanPrivilegeMaintenanceState{
		roundInitialized: true,
		roundHighWater:   12,
		roundStart:       10,
		accountCursor:    10,
	}

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.True(t, state.tenantSelected)
	require.Equal(t, maintenancePhysicalKey(candidatePage[len(candidatePage)-1]), state.tenantScan.Cursor)
	require.Equal(t, uint64(1), state.tenantCommittedPages)
	require.Equal(t, 1, lookupCount)

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	state = service.upgrade.orphanPrivilegeMaintenanceState
	require.False(t, state.tenantSelected)
	require.Zero(t, state.tenantCommittedPages)
	require.Equal(t, int32(11), state.accountCursor)
	require.Equal(t, 1, lookupCount, "an incomplete finite tenant scan must continue without reselecting its account")
}

func TestOrphanPrivilegeMaintenanceRoundFreezesHighWater(t *testing.T) {
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select account_id from mo_catalog.mo_account order by account_id desc limit 1":
				return buildMaintenanceAccountRows(12), nil
			case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account where"):
				require.Contains(t, sql, "account_id <= 12")
				require.NotContains(t, sql, "20")
				return buildMaintenanceAccountRows(12), nil
			case strings.Contains(sql, "from mo_catalog.mo_role_privs"):
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceState.restartSeed = 7

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.True(t, state.roundInitialized)
	require.Equal(t, int32(12), state.roundHighWater)
}

func TestOrphanPrivilegeMaintenanceLongRunningWarningInterval(t *testing.T) {
	for _, test := range []struct {
		pages uint64
		warn  bool
	}{
		{pages: 0},
		{pages: 1},
		{pages: orphanPrivilegeMaintenanceLongRunningPageInterval - 1},
		{pages: orphanPrivilegeMaintenanceLongRunningPageInterval, warn: true},
		{pages: orphanPrivilegeMaintenanceLongRunningPageInterval + 1},
		{pages: 2 * orphanPrivilegeMaintenanceLongRunningPageInterval, warn: true},
	} {
		require.Equal(t, test.warn, shouldWarnLongRunningOrphanPrivilegeMaintenance(test.pages), test.pages)
	}
}

func TestMaintainOrphanObjectPrivilegesEmitsLongRunningWarning(t *testing.T) {
	const tenantID = int32(10)
	candidates := make([]v4_0_6.OrphanPrivilegeKey, 1000)
	for i := range candidates {
		candidates[i] = v4_0_6.OrphanPrivilegeKey{
			RoleID: int32(i), ObjectType: "account", PrivilegeLevel: "*",
		}
	}
	highWater := v4_0_6.OrphanPrivilegeKey{
		RoleID: 2000, ObjectType: "account", PrivilegeLevel: "*",
	}

	for _, test := range []struct {
		committedPagesAfterPage uint64
		warn                    bool
	}{
		{committedPagesAfterPage: orphanPrivilegeMaintenanceLongRunningPageInterval - 1},
		{committedPagesAfterPage: orphanPrivilegeMaintenanceLongRunningPageInterval, warn: true},
		{committedPagesAfterPage: orphanPrivilegeMaintenanceLongRunningPageInterval + 1},
	} {
		t.Run(fmt.Sprintf("pages-%d", test.committedPagesAfterPage), func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			service := newServiceForTest(
				"",
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					if strings.HasPrefix(sql,
						"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) ") {
						return buildMaintenancePrivilegeRows(t, candidates...), nil
					}
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}),
				func(s *service) {},
			)
			service.logger = molog.GetServiceLogger(
				zap.New(core), metadata.ServiceType_CN, "warning-test").Named("upgrade-framework")
			service.upgrade.orphanPrivilegeMaintenanceState = orphanPrivilegeMaintenanceState{
				roundInitialized:     true,
				roundHighWater:       tenantID,
				roundStart:           tenantID,
				accountCursor:        tenantID,
				tenantSelected:       true,
				tenantID:             tenantID,
				tenantCommittedPages: test.committedPagesAfterPage - 1,
				tenantScan: v4_0_6.OrphanPrivilegeScan{
					Initialized: true,
					HighWater:   maintenancePhysicalKey(highWater),
				},
			}

			require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
			require.Equal(t, test.committedPagesAfterPage,
				service.upgrade.orphanPrivilegeMaintenanceState.tenantCommittedPages)
			entries := observed.All()
			if !test.warn {
				require.Empty(t, entries)
				return
			}
			require.Len(t, entries, 1)
			require.Equal(t, zap.WarnLevel, entries[0].Level)
			require.Equal(t, "orphan object privilege maintenance tenant remains active", entries[0].Message)
			fields := entries[0].ContextMap()
			require.EqualValues(t, tenantID, fields["tenant"])
			require.EqualValues(t, test.committedPagesAfterPage, fields["committed-pages"])
		})
	}
}

func TestOrphanPrivilegeMaintenanceRestartSeedAvoidsFixedZeroBias(t *testing.T) {
	const highWater = int32(127)
	starts := make(map[int32]struct{})
	for seed := uint64(1); seed <= 128; seed++ {
		starts[orphanPrivilegeMaintenanceRoundStart(seed, 0, highWater)] = struct{}{}
	}
	require.Greater(t, len(starts), 64)
	_, onlyZero := starts[0]
	require.False(t, onlyZero && len(starts) == 1)
	require.Zero(t, orphanPrivilegeMaintenanceRoundStart(1, 0, 0))
}

func TestMaintainOrphanObjectPrivilegesRestartCanReachOrphanAfterLivePrefix(t *testing.T) {
	const tenantID = int32(10)
	live := make([]v4_0_6.OrphanPrivilegeKey, 1000)
	liveIDs := make([]uint64, len(live))
	for i := range live {
		live[i] = v4_0_6.OrphanPrivilegeKey{
			RoleID:         1,
			ObjectType:     "database",
			ObjectID:       uint64(i + 1),
			PrivilegeID:    1,
			PrivilegeLevel: "d",
		}
		liveIDs[i] = live[i].ObjectID
	}
	orphan := v4_0_6.OrphanPrivilegeKey{
		RoleID:         2,
		ObjectType:     "database",
		ObjectID:       2000,
		PrivilegeID:    1,
		PrivilegeLevel: "d",
	}
	orphanPhysicalKey := maintenancePhysicalKey(orphan)
	orphanDeleted := false
	newRestartedService := func(start string) *service {
		s := newServiceForTest(
			"",
			&memLocker{},
			clock.NewHLCClock(func() int64 { return 0 }, 0),
			nil,
			executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case sql == "select account_id from mo_catalog.mo_account order by account_id desc limit 1":
					return buildMaintenanceAccountRows(tenantID), nil
				case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account where"):
					return buildMaintenanceAccountRows(tenantID), nil
				case strings.Contains(sql, "order by __mo_cpkey_col desc"):
					return buildMaintenancePrivilegeRows(t, orphan), nil
				case strings.HasPrefix(sql,
					"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "):
					if strings.Contains(sql, ">= unhex('"+orphanPhysicalKey+"')") {
						if orphanDeleted {
							return executor.Result{}, nil
						}
						return buildMaintenancePrivilegeRows(t, orphan), nil
					}
					return buildMaintenancePrivilegeRows(t, live...), nil
				case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
					return buildMaintenanceObjectIDRows(liveIDs...), nil
				case strings.HasPrefix(sql, "delete from mo_catalog.mo_role_privs"):
					require.Contains(t, sql, "(2,'database',2000,1,'d')")
					orphanDeleted = true
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			}),
			func(s *service) {},
		)
		s.upgrade.orphanPrivilegeMaintenanceStart = func() string { return start }
		return s
	}

	firstProcess := newRestartedService("")
	require.NoError(t, firstProcess.maintainOrphanObjectPrivileges(t.Context()))
	require.False(t, orphanDeleted)
	require.True(t, firstProcess.upgrade.orphanPrivilegeMaintenanceState.tenantSelected)

	// Drop every process-local field after one live page. A fresh process whose
	// random ring starts at the tail must reach the orphan immediately instead
	// of deterministically replaying the live prefix.
	secondProcess := newRestartedService(orphanPhysicalKey)
	require.NoError(t, secondProcess.maintainOrphanObjectPrivileges(t.Context()))
	require.True(t, orphanDeleted)
}

func TestMaintainOrphanObjectPrivilegesAdvancesAfterTenantError(t *testing.T) {
	fail := true
	lookupCount := 0
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
				lookupCount++
				return buildMaintenanceAccountRows(int32(lookupCount*2 + 8)), nil
			case strings.Contains(sql, "from mo_catalog.mo_role_privs"):
				if fail {
					fail = false
					return executor.Result{}, fmt.Errorf("injected candidate scan failure")
				}
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceState = orphanPrivilegeMaintenanceState{
		roundInitialized:     true,
		roundHighWater:       12,
		roundStart:           10,
		accountCursor:        10,
		tenantSelected:       true,
		tenantID:             10,
		tenantCommittedPages: orphanPrivilegeMaintenanceLongRunningPageInterval - 1,
	}

	require.ErrorContains(t, service.maintainOrphanObjectPrivileges(t.Context()), "injected candidate scan failure")
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.Equal(t, int32(11), state.accountCursor,
		"a broken tenant must not starve later accounts in the frozen round")
	require.False(t, state.tenantSelected)
	require.Zero(t, state.tenantCommittedPages)
	require.False(t, service.upgrade.orphanPrivilegeMaintenanceRunning.Load())

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Equal(t, 1, lookupCount)
}

func TestMaintainOrphanObjectPrivilegesHandlesCommittedPostWaitError(t *testing.T) {
	const tenantID = int32(10)
	postCommitWaitErr := errors.New("injected post-commit logtail wait failure")
	candidates := make([]v4_0_6.OrphanPrivilegeKey, 1000)
	for i := range candidates {
		candidates[i] = v4_0_6.OrphanPrivilegeKey{
			RoleID: 1, ObjectType: "database", ObjectID: uint64(i + 1),
			PrivilegeID: 1, PrivilegeLevel: "d",
		}
	}
	committedDelete := false
	pendingDelete := false
	deleteCalls := 0
	txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		switch {
		case sql == "select account_id from mo_catalog.mo_account order by account_id desc limit 1":
			return buildMaintenanceAccountRows(tenantID), nil
		case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account where"):
			return buildMaintenanceAccountRows(tenantID), nil
		case strings.Contains(sql, "order by __mo_cpkey_col desc"):
			if committedDelete {
				return executor.Result{}, nil
			}
			return buildMaintenancePrivilegeRows(t, candidates[len(candidates)-1]), nil
		case strings.HasPrefix(sql,
			"select role_id,obj_type,obj_id,privilege_id,privilege_level,hex(__mo_cpkey_col) "):
			require.False(t, committedDelete)
			return buildMaintenancePrivilegeRows(t, candidates...), nil
		case strings.HasPrefix(sql, "select dat_id from mo_catalog.mo_database"):
			return executor.Result{}, nil
		case strings.HasPrefix(sql, "delete from mo_catalog.mo_role_privs"):
			deleteCalls++
			pendingDelete = true
			return executor.Result{AffectedRows: uint64(len(candidates))}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	}, nil)
	sqlExec := &maintenancePostCommitErrorExecutor{
		txn:             txn,
		postCommitCall:  2,
		postCommitError: postCommitWaitErr,
		onCommit: func() {
			if pendingDelete {
				committedDelete = true
				pendingDelete = false
			}
		},
	}
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		sqlExec,
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceStart = func() string { return "" }

	err := service.maintainOrphanObjectPrivileges(t.Context())
	require.ErrorIs(t, err, postCommitWaitErr)
	require.True(t, committedDelete,
		"the DELETE commit precedes the visibility wait that returned the error")
	require.Equal(t, 1, deleteCalls)
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.False(t, state.tenantSelected)
	require.Zero(t, state.tenantScan,
		"an ExecTxn error must not publish the closure-local physical cursor")
	require.Equal(t, tenantID+1, state.accountCursor,
		"the terminal error deliberately advances account scheduling")

	// A later round can select the tenant again. The already committed exact
	// DELETE makes this rescan an idempotent empty completion.
	service.upgrade.orphanPrivilegeMaintenanceState.tenantSelected = true
	service.upgrade.orphanPrivilegeMaintenanceState.tenantID = tenantID
	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Equal(t, 1, deleteCalls)
}

func TestTenantUpgradePassRunsMaintenanceOnlyAfterFinalVersion(t *testing.T) {
	for _, test := range []struct {
		name       string
		final      bool
		wantLookup bool
	}{
		{name: "upgrade not complete"},
		{name: "final version complete", final: true, wantLookup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookup := false
			service := newServiceForTest(
				"",
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where state = 1"):
						return executor.Result{}, nil
					case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
						lookup = true
						return executor.Result{}, nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
					}
				}),
				func(s *service) {},
			)
			service.upgrade.finalVersionCompleted.Store(test.final)

			hasUpgradeTenants, err := service.newTenantUpgradePass(context.Background())()
			require.NoError(t, err)
			require.False(t, hasUpgradeTenants)
			require.Equal(t, test.wantLookup, lookup)
		})
	}
}

func TestAsyncUpgradeTenantTaskKeepsSingleMaintenanceOwner(t *testing.T) {
	maintenancePass := make(chan struct{}, 1)
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where state = 1"):
				return executor.Result{}, nil
			case strings.HasPrefix(sql, "select account_id from mo_catalog.mo_account"):
				select {
				case maintenancePass <- struct{}{}:
				default:
				}
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
		WithCheckUpgradeTenantDuration(time.Millisecond),
	)
	service.upgrade.finalVersionCompleted.Store(true)

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		service.asyncUpgradeTenantTask(firstCtx)
	}()
	select {
	case <-maintenancePass:
	case <-time.After(time.Second):
		t.Fatal("first maintenance worker did not run")
	}
	require.True(t, service.upgrade.orphanPrivilegeMaintenanceWorkerRunning.Load())

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		service.asyncUpgradeTenantTask(t.Context())
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("redundant maintenance worker did not exit")
	}

	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("maintenance owner did not stop after cancellation")
	}
	require.False(t, service.upgrade.orphanPrivilegeMaintenanceWorkerRunning.Load())
}

func TestMaintainOrphanObjectPrivilegesSkipsConcurrentLocalPass(t *testing.T) {
	service := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}),
		func(s *service) {},
	)
	service.upgrade.orphanPrivilegeMaintenanceRunning.Store(true)

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
}

type maintenancePostCommitErrorExecutor struct {
	txn             executor.TxnExecutor
	calls           int
	postCommitCall  int
	postCommitError error
	onCommit        func()
}

func (e *maintenancePostCommitErrorExecutor) Exec(
	context.Context,
	string,
	executor.Options,
) (executor.Result, error) {
	return executor.Result{}, errors.New("unexpected non-transactional exec")
}

func (e *maintenancePostCommitErrorExecutor) ExecTxn(
	_ context.Context,
	execFunc func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	e.calls++
	if err := execFunc(e.txn); err != nil {
		return err
	}
	if e.onCommit != nil {
		e.onCommit()
	}
	if e.calls == e.postCommitCall {
		return e.postCommitError
	}
	return nil
}

func buildMaintenanceObjectIDRows(ids ...uint64) executor.Result {
	result := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mpool.MustNewZero())
	result.NewBatchWithRowCount(len(ids))
	executor.AppendFixedRows(result, 0, ids)
	return result.GetResult()
}

func buildMaintenanceAccountRows(accountID int32) executor.Result {
	result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mpool.MustNewZero())
	result.NewBatchWithRowCount(1)
	executor.AppendFixedRows(result, 0, []int32{accountID})
	return result.GetResult()
}

func buildMaintenancePrivilegeRows(
	t *testing.T,
	keys ...v4_0_6.OrphanPrivilegeKey,
) executor.Result {
	t.Helper()
	result := executor.NewMemResult([]types.Type{
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
		types.T_uint64.ToType(),
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}, mpool.MustNewZero())
	result.NewBatchWithRowCount(len(keys))
	roleIDs := make([]int32, len(keys))
	objectTypes := make([]string, len(keys))
	objectIDs := make([]uint64, len(keys))
	privilegeIDs := make([]int32, len(keys))
	privilegeLevels := make([]string, len(keys))
	physicalKeys := make([]string, len(keys))
	for i, key := range keys {
		roleIDs[i] = key.RoleID
		objectTypes[i] = key.ObjectType
		objectIDs[i] = key.ObjectID
		privilegeIDs[i] = key.PrivilegeID
		privilegeLevels[i] = key.PrivilegeLevel
		physicalKeys[i] = maintenancePhysicalKey(key)
	}
	executor.AppendFixedRows(result, 0, roleIDs)
	require.NoError(t, executor.AppendStringRows(result, 1, objectTypes))
	executor.AppendFixedRows(result, 2, objectIDs)
	executor.AppendFixedRows(result, 3, privilegeIDs)
	require.NoError(t, executor.AppendStringRows(result, 4, privilegeLevels))
	require.NoError(t, executor.AppendStringRows(result, 5, physicalKeys))
	return result.GetResult()
}

func maintenancePhysicalKey(key v4_0_6.OrphanPrivilegeKey) string {
	var buffer [v4_0_6.OrphanPrivilegePhysicalKeyMaxSize]byte
	packer := types.NewPackerWithFixedBuffer(buffer[:])
	packer.EncodeInt32(key.RoleID)
	packer.EncodeStringType([]byte(key.ObjectType))
	packer.EncodeUint64(key.ObjectID)
	packer.EncodeInt32(key.PrivilegeID)
	packer.EncodeStringType([]byte(key.PrivilegeLevel))
	if packer.Err() != nil {
		panic(packer.Err())
	}
	return hex.EncodeToString(packer.GetBuf())
}
