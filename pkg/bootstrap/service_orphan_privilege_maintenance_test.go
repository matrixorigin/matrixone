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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestMaintainOrphanObjectPrivilegesCursor(t *testing.T) {
	lookup := 0
	selectedTenant := int32(0)
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
					require.Contains(t, sql, "account_id >= 0")
					selectedTenant = 10
					return buildMaintenanceAccountRows(10), nil
				case 2:
					require.Contains(t, sql, "account_id >= 11")
					selectedTenant = 12
					return buildMaintenanceAccountRows(12), nil
				case 3:
					require.Contains(t, sql, "account_id >= 13")
					return executor.Result{}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected lookup %d", lookup)
				}
			case strings.Contains(sql, "from `mo_catalog`.`mo_indexes`"):
				return buildMaintenanceStringRows("idx_mo_role_privs_obj_id"), nil
			case strings.Contains(sql, "mo_database d"):
				if selectedTenant == 12 && lookup == 2 {
					return executor.Result{AffectedRows: 1000}, nil
				}
				return executor.Result{}, nil
			case strings.Contains(sql, "mo_tables t"):
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Equal(t, int32(11), service.upgrade.orphanPrivilegeMaintenanceCursor.Load())

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Equal(t, int32(13), service.upgrade.orphanPrivilegeMaintenanceCursor.Load(),
		"a full page must advance so one tenant cannot starve later accounts")

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Zero(t, service.upgrade.orphanPrivilegeMaintenanceCursor.Load(),
		"the scan must wrap after the current account set is exhausted")
}

func TestOrphanPrivilegeMaintenanceCursorRestartsAndWraps(t *testing.T) {
	firstProcess := &service{}
	firstProcess.upgrade.orphanPrivilegeMaintenanceCursor.Store(42)

	restartedProcess := &service{}
	require.Zero(t, restartedProcess.upgrade.orphanPrivilegeMaintenanceCursor.Load(),
		"the optimization cursor must not become persistent correctness state")

	const maxInt32 = int32(^uint32(0) >> 1)
	restartedProcess.advanceOrphanPrivilegeMaintenanceCursor(maxInt32)
	require.Zero(t, restartedProcess.upgrade.orphanPrivilegeMaintenanceCursor.Load())
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
			case strings.Contains(sql, "from `mo_catalog`.`mo_indexes`"):
				if fail {
					fail = false
					return executor.Result{}, fmt.Errorf("injected index check failure")
				}
				return buildMaintenanceStringRows("idx_mo_role_privs_obj_id"), nil
			case strings.Contains(sql, "mo_database d"), strings.Contains(sql, "mo_tables t"):
				return executor.Result{}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {},
	)

	require.ErrorContains(t, service.maintainOrphanObjectPrivileges(t.Context()), "injected index check failure")
	require.Equal(t, int32(11), service.upgrade.orphanPrivilegeMaintenanceCursor.Load(),
		"a broken tenant must not starve all higher account IDs")
	require.False(t, service.upgrade.orphanPrivilegeMaintenanceRunning.Load())

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	require.Equal(t, int32(13), service.upgrade.orphanPrivilegeMaintenanceCursor.Load())
	require.Equal(t, 2, lookupCount)
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

func buildMaintenanceAccountRows(accountID int32) executor.Result {
	result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mpool.MustNewZero())
	result.NewBatchWithRowCount(1)
	executor.AppendFixedRows(result, 0, []int32{accountID})
	return result.GetResult()
}

func buildMaintenanceStringRows(value string) executor.Result {
	result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mpool.MustNewZero())
	result.NewBatchWithRowCount(1)
	executor.AppendStringRows(result, 0, []string{value})
	return result.GetResult()
}
