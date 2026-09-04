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

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
			case strings.Contains(sql, "order by role_id desc"):
				return buildMaintenancePrivilegeRows(t, highWater), nil
			case strings.HasPrefix(sql, "select role_id,obj_type,obj_id,privilege_id,privilege_level "):
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
	require.Equal(t, int32(999), state.tenantScan.Cursor.RoleID)
	require.Equal(t, 1, lookupCount)

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
	state = service.upgrade.orphanPrivilegeMaintenanceState
	require.False(t, state.tenantSelected)
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
		roundInitialized: true,
		roundHighWater:   12,
		roundStart:       10,
		accountCursor:    10,
	}

	require.ErrorContains(t, service.maintainOrphanObjectPrivileges(t.Context()), "injected candidate scan failure")
	state := service.upgrade.orphanPrivilegeMaintenanceState
	require.Equal(t, int32(11), state.accountCursor,
		"a broken tenant must not starve later accounts in the frozen round")
	require.False(t, state.tenantSelected)
	require.False(t, service.upgrade.orphanPrivilegeMaintenanceRunning.Load())

	require.NoError(t, service.maintainOrphanObjectPrivileges(t.Context()))
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
	}, mpool.MustNewZero())
	result.NewBatchWithRowCount(len(keys))
	roleIDs := make([]int32, len(keys))
	objectTypes := make([]string, len(keys))
	objectIDs := make([]uint64, len(keys))
	privilegeIDs := make([]int32, len(keys))
	privilegeLevels := make([]string, len(keys))
	for i, key := range keys {
		roleIDs[i] = key.RoleID
		objectTypes[i] = key.ObjectType
		objectIDs[i] = key.ObjectID
		privilegeIDs[i] = key.PrivilegeID
		privilegeLevels[i] = key.PrivilegeLevel
	}
	executor.AppendFixedRows(result, 0, roleIDs)
	require.NoError(t, executor.AppendStringRows(result, 1, objectTypes))
	executor.AppendFixedRows(result, 2, objectIDs)
	executor.AppendFixedRows(result, 3, privilegeIDs)
	require.NoError(t, executor.AppendStringRows(result, 4, privilegeLevels))
	return result.GetResult()
}
