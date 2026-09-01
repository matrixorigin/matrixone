// Copyright 2024 Matrix Origin
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

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_asyncUpgradeTenantTask(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Expected no panic")
				}
			}()

			var cnt atomic.Int32
			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				if cnt.Load() > 0 {
					return executor.Result{}, nil
				}
				cnt.Add(1)
				return executor.Result{}, moerr.NewInternalErrorNoCtx("return error")
			})

			b := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					h1 := newTestVersionHandler("1.2.0", "1.1.0", versions.Yes, versions.No, 10)
					h2 := newTestVersionHandler("2.0.0", "1.2.0", versions.Yes, versions.No, 2)
					s.handles = append(s.handles, h1)
					s.handles = append(s.handles, h2)
				},
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
			defer cancel()

			b.asyncUpgradeTenantTask(ctx)
		},
	)
}

func TestDrainUpgradeTenants(t *testing.T) {
	tests := []struct {
		name    string
		results []struct {
			hasTenants bool
			err        error
		}
		wantCalls int
	}{
		{
			name: "drains healthy work",
			results: []struct {
				hasTenants bool
				err        error
			}{
				{hasTenants: true},
				{hasTenants: true},
				{},
			},
			wantCalls: 3,
		},
		{
			name: "stops when no work remains",
			results: []struct {
				hasTenants bool
				err        error
			}{{}},
			wantCalls: 1,
		},
		{
			name: "stops on error",
			results: []struct {
				hasTenants bool
				err        error
			}{{err: moerr.NewInternalErrorNoCtx("upgrade failed")}},
			wantCalls: 1,
		},
		{
			name: "error wins over stale work flag",
			results: []struct {
				hasTenants bool
				err        error
			}{{hasTenants: true, err: moerr.NewInternalErrorNoCtx("upgrade failed")}},
			wantCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			drainUpgradeTenants(t.Context(), func() (bool, error) {
				if calls >= len(test.results) {
					t.Fatalf("unexpected call %d", calls+1)
				}
				result := test.results[calls]
				calls++
				return result.hasTenants, result.err
			})
			require.Equal(t, test.wantCalls, calls)
		})
	}
}

func TestDrainUpgradeTenantsStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	drainUpgradeTenants(ctx, func() (bool, error) {
		calls++
		cancel()
		return true, nil
	})

	require.Equal(t, 1, calls)
}

func Test_asyncUpgradeTenantTask_SkipsTenantAtTargetVersion(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var taskReady atomic.Bool
			var finalized atomic.Bool
			var tenantVersionUpdated atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 1, 0), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					if taskReady.Load() {
						return executor.Result{}, nil
					}
					return buildUpgradeTenantTaskRows([]uint64{200}, []int32{10}, []int32{10}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 10"):
					return buildUpgradeTenantAccountRows([]int32{10}, []string{"3.0.1"}), nil
				case strings.Contains(sql, "update mo_account set create_version = '3.0.1' where account_id = 10"):
					tenantVersionUpdated.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, "where id = 200"):
					taskReady.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 1, 0), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 1, ready_tenant = 0"):
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 1, ready_tenant = 1") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, taskReady.Load())
			require.True(t, finalized.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
			require.False(t, tenantVersionUpdated.Load())
		},
	)
}

type testIncrementalVersionHandle struct {
	*testVersionHandle
	stepCalls     atomic.Int32
	completeAfter int32
	onStep        func(call int32)
}

func (h *testIncrementalVersionHandle) HandleTenantUpgradeStep(
	context.Context,
	int32,
	executor.TxnExecutor,
) (bool, error) {
	call := h.stepCalls.Add(1)
	if h.onStep != nil {
		h.onStep(call)
	}
	return call >= h.completeAfter, nil
}

func TestShouldRunTenantUpgrade(t *testing.T) {
	for _, test := range []struct {
		name          string
		createVersion string
		fromVersion   string
		toVersion     string
		want          bool
	}{
		{name: "older tenant on version transition", createVersion: "3.0.0", fromVersion: "3.0.0", toVersion: "3.0.1", want: true},
		{name: "target tenant on version transition", createVersion: "3.0.1", fromVersion: "3.0.0", toVersion: "3.0.1"},
		{name: "target tenant on offset upgrade", createVersion: "4.0.6", fromVersion: "4.0.6", toVersion: "4.0.6", want: true},
		{name: "newer tenant on offset upgrade", createVersion: "4.0.7", fromVersion: "4.0.6", toVersion: "4.0.6"},
	} {
		t.Run(test.name, func(t *testing.T) {
			upgrade := versions.VersionUpgrade{
				FromVersion: test.fromVersion,
				ToVersion:   test.toVersion,
			}
			require.Equal(t, test.want, shouldRunTenantUpgrade(test.createVersion, upgrade))
		})
	}
}

func Test_asyncUpgradeTenantTask_RunsSameVersionOffsetUpgrade(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			const (
				tenantID            = int32(10)
				currentVersion      = "4.0.6"
				newVersionOffset    = uint32(5)
				upgradeID           = uint64(100)
				upgradeTenantTaskID = uint64(200)
			)

			var taskReady atomic.Bool
			var finalized atomic.Bool
			var tenantVersionUpdated atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					if finalized.Load() {
						return executor.Result{}, nil
					}
					return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
						currentVersion, currentVersion, newVersionOffset, 0,
						versions.Yes, versions.Yes, 1, 0), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					if taskReady.Load() {
						return executor.Result{}, nil
					}
					return buildUpgradeTenantTaskRows(
						[]uint64{upgradeTenantTaskID}, []int32{tenantID}, []int32{tenantID}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account"):
					return buildUpgradeTenantAccountRows([]int32{tenantID}, []string{currentVersion}), nil
				case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", currentVersion, tenantID):
					tenantVersionUpdated.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, fmt.Sprintf("where id = %d", upgradeTenantTaskID)):
					taskReady.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, fmt.Sprintf("where id = %d for update", upgradeID)):
					return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
						currentVersion, currentVersion, newVersionOffset, 0,
						versions.Yes, versions.Yes, 1, 0), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 1, ready_tenant = 1") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler(currentVersion, currentVersion, versions.Yes, versions.Yes, newVersionOffset)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, taskReady.Load())
			require.True(t, finalized.Load())
			require.True(t, tenantVersionUpdated.Load())
			require.Equal(t, uint64(1), h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_asyncUpgradeTenantTask_CommitsIncrementalPagesBeforeAdvancingTask(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			const (
				tenantID            = int32(10)
				currentVersion      = "4.0.6"
				newVersionOffset    = uint32(5)
				upgradeID           = uint64(100)
				upgradeTenantTaskID = uint64(200)
			)

			var taskReady atomic.Bool
			var finalized atomic.Bool
			var tenantVersionUpdated atomic.Bool
			var upgradeTransactions atomic.Int32
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					upgradeTransactions.Add(1)
					if finalized.Load() {
						return executor.Result{}, nil
					}
					return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
						currentVersion, currentVersion, newVersionOffset, 0,
						versions.Yes, versions.Yes, 1, 0), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					if taskReady.Load() {
						return executor.Result{}, nil
					}
					return buildUpgradeTenantTaskRows(
						[]uint64{upgradeTenantTaskID}, []int32{tenantID}, []int32{tenantID}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account"):
					return buildUpgradeTenantAccountRows([]int32{tenantID}, []string{currentVersion}), nil
				case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", currentVersion, tenantID):
					tenantVersionUpdated.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "update mo_upgrade_tenant set from_account_id = 11"):
					return executor.Result{}, nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, fmt.Sprintf("where id = %d", upgradeTenantTaskID)):
					taskReady.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, fmt.Sprintf("where id = %d for update", upgradeID)):
					return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
						currentVersion, currentVersion, newVersionOffset, 0,
						versions.Yes, versions.Yes, 1, 0), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 1, ready_tenant = 1") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := &testIncrementalVersionHandle{
				testVersionHandle: newTestVersionHandler(
					currentVersion, currentVersion, versions.Yes, versions.Yes, newVersionOffset),
				completeAfter: 2,
			}
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) { s.handles = append(s.handles, h) },
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()
			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.Equal(t, int32(2), h.stepCalls.Load())
			require.GreaterOrEqual(t, upgradeTransactions.Load(), int32(2))
			require.True(t, tenantVersionUpdated.Load())
			require.True(t, taskReady.Load())
			require.True(t, finalized.Load())
		})
}

func TestMaybeUpgradeTenantRejectsCallerOwnedTransactionBeforePages(t *testing.T) {
	const tenantID = int32(10)
	h := newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1)
	var sqls []string
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			sqls = append(sqls, sql)
			switch {
			case sql == "select create_version from mo_account where account_id = 10":
				return buildTenantVersionResult("1.0.0"), nil
			case strings.Contains(sql, "from mo_version"):
				return buildLatestVersionResult("2.0.0", 1, versions.StateReady), nil
			default:
				return executor.Result{}, fmt.Errorf("independent page must not start: %s", sql)
			}
		}, &testTxnOperator{}),
		func(s *service) {
			s.handles = append(s.handles, h)
			s.upgrade.finalVersionCompleted.Store(true)
		},
	)

	upgraded, err := s.MaybeUpgradeTenant(
		context.Background(),
		func() (int32, string, error) { return tenantID, "2.0.0", nil },
		&testTxnOperator{},
	)
	require.False(t, upgraded)
	require.ErrorContains(t, err, "without a caller-owned transaction")
	require.Len(t, sqls, 2)
	require.Zero(t, h.callHandleTenantUpgrade.Load())
}

func TestMaybeUpgradeTenantRoutesFromPersistedVersion(t *testing.T) {
	const tenantID = int32(10)
	h1 := newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1)
	h2 := newTestVersionHandler("3.0.0", "2.0.0", versions.Yes, versions.Yes, 1)
	persisted := "1.0.0"
	var lockedReads int
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 10":
				return buildTenantVersionResult(persisted), nil
			case sql == "select create_version from mo_account where account_id = 10 for update":
				lockedReads++
				return buildTenantVersionResult(persisted), nil
			case strings.Contains(sql, "from mo_version"):
				return buildLatestVersionResult("3.0.0", 1, versions.StateReady), nil
			case sql == "update mo_account set create_version = '2.0.0' where account_id = 10":
				persisted = "2.0.0"
				return executor.Result{AffectedRows: 1}, nil
			case sql == "update mo_account set create_version = '3.0.0' where account_id = 10":
				persisted = "3.0.0"
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {
			s.handles = append(s.handles, h1, h2)
			s.upgrade.finalVersionCompleted.Store(true)
		},
	)

	upgraded, err := s.MaybeUpgradeTenant(
		context.Background(),
		// The production CN wrapper supplies finalVersion here. Routing must still
		// start from persisted 1.0.0 and cannot skip the 2.0.0 handler.
		func() (int32, string, error) { return tenantID, "3.0.0", nil },
		nil,
	)
	require.NoError(t, err)
	require.True(t, upgraded)
	require.Equal(t, uint64(1), h1.callHandleTenantUpgrade.Load())
	require.Equal(t, uint64(1), h2.callHandleTenantUpgrade.Load())
	require.Equal(t, 3, lockedReads)
	require.Equal(t, "3.0.0", persisted)
}

func TestMaybeUpgradeTenantIgnoresStaleOlderCallbackForCurrentTenant(t *testing.T) {
	const tenantID = int32(10)
	h := newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1)
	var lockedReads int
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 10":
				return buildTenantVersionResult("2.0.0"), nil
			case strings.Contains(sql, "from mo_version"):
				return buildLatestVersionResult("2.0.0", 1, versions.StateReady), nil
			case strings.Contains(sql, "from mo_upgrade"):
				return executor.Result{}, nil
			case strings.Contains(sql, "for update"):
				lockedReads++
				return executor.Result{}, fmt.Errorf("unexpected page transaction")
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) {
			s.handles = append(s.handles, h)
			s.upgrade.finalVersionCompleted.Store(true)
		},
	)

	upgraded, err := s.MaybeUpgradeTenant(
		context.Background(),
		func() (int32, string, error) { return tenantID, "1.0.0", nil },
		nil,
	)
	require.NoError(t, err)
	require.False(t, upgraded)
	require.Zero(t, lockedReads)
	require.Zero(t, h.callHandleTenantUpgrade.Load())
}

func TestMaybeUpgradeTenantTreatsDeletedAccountAsComplete(t *testing.T) {
	const tenantID = int32(10)
	h := newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1)
	var sqls []string
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			sqls = append(sqls, sql)
			if sql == "select create_version from mo_account where account_id = 10" {
				return executor.Result{}, nil
			}
			return executor.Result{}, fmt.Errorf("unexpected sql after account deletion: %s", sql)
		}),
		func(s *service) { s.handles = append(s.handles, h) },
	)

	upgraded, err := s.MaybeUpgradeTenant(
		context.Background(),
		func() (int32, string, error) { return tenantID, "1.0.0", nil },
		nil,
	)
	require.NoError(t, err)
	require.False(t, upgraded)
	require.Equal(t, []string{"select create_version from mo_account where account_id = 10"}, sqls)
	require.Zero(t, h.callHandleTenantUpgrade.Load())
}

func TestUpgradeTenantDirectlyStopsWhenTenantDeletedBetweenPages(t *testing.T) {
	const tenantID = int32(10)
	deleted := false
	h := &testIncrementalVersionHandle{
		testVersionHandle: newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1),
		completeAfter:     10,
		onStep: func(call int32) {
			if call == 1 {
				deleted = true
			}
		},
	}
	var lockedReads int
	var versionUpdates int
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 10 for update":
				lockedReads++
				if deleted {
					return executor.Result{}, nil
				}
				return buildTenantVersionResult("1.0.0"), nil
			case strings.HasPrefix(sql, "update mo_account"):
				versionUpdates++
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) { s.handles = append(s.handles, h) },
	)

	exists, err := s.upgradeTenantDirectly(context.Background(), tenantID, false)
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, int32(1), h.stepCalls.Load())
	require.Equal(t, 2, lockedReads)
	require.Zero(t, versionUpdates)
}

func TestUpgradeTenantDirectlyRebuildsRouteAfterConcurrentAdvance(t *testing.T) {
	const tenantID = int32(10)
	persisted := "1.0.0"
	h1 := &testIncrementalVersionHandle{
		testVersionHandle: newTestVersionHandler("2.0.0", "1.0.0", versions.Yes, versions.Yes, 1),
		completeAfter:     10,
		onStep: func(call int32) {
			if call == 1 {
				persisted = "3.0.0"
			}
		},
	}
	h2 := newTestVersionHandler("3.0.0", "2.0.0", versions.Yes, versions.Yes, 1)
	var versionUpdates int
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			if sql == "select create_version from mo_account where account_id = 10 for update" {
				return buildTenantVersionResult(persisted), nil
			}
			if strings.HasPrefix(sql, "update mo_account") {
				versionUpdates++
				return executor.Result{AffectedRows: 1}, nil
			}
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}),
		func(s *service) { s.handles = append(s.handles, h1, h2) },
	)

	exists, err := s.upgradeTenantDirectly(context.Background(), tenantID, false)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int32(1), h1.stepCalls.Load())
	require.Zero(t, h2.callHandleTenantUpgrade.Load())
	require.Zero(t, versionUpdates, "concurrent progress must never be overwritten with an older version")
}

func TestUpgradeTenantDirectlyCommitsIncrementalPages(t *testing.T) {
	const (
		tenantID = int32(10)
		version  = "4.0.6"
	)
	var lockedReads atomic.Int32
	var versionUpdates atomic.Int32
	h := &testIncrementalVersionHandle{
		testVersionHandle: newTestVersionHandler(version, version, versions.Yes, versions.Yes, 5),
		completeAfter:     2,
	}
	s := newServiceForTest(
		"",
		&memLocker{},
		clock.NewHLCClock(func() int64 { return 0 }, 0),
		nil,
		executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 10 for update":
				lockedReads.Add(1)
				return buildTenantVersionResult(version), nil
			case sql == "update mo_account set create_version = '4.0.6' where account_id = 10":
				versionUpdates.Add(1)
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
			}
		}),
		func(s *service) { s.handles = append(s.handles, h) },
	)

	exists, err := s.upgradeTenantDirectly(context.Background(), tenantID, true)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int32(3), lockedReads.Load(),
		"two page transactions plus one locked completion check are required")
	require.Equal(t, int32(1), versionUpdates.Load(), "version is published only with the completed page")
	require.Equal(t, int32(2), h.stepCalls.Load())
}

func TestUpgradeTenantIncrementallyAdvancesRangeCursor(t *testing.T) {
	const (
		tenantID  = int32(10)
		upgradeID = uint64(100)
		taskID    = uint64(200)
		version   = "4.0.6"
	)
	var sqls []string
	txnExecutor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		switch {
		case sql == "update mo_account set create_version = '4.0.6' where account_id = 10":
			return executor.Result{AffectedRows: 1}, nil
		case strings.Contains(sql, "update mo_upgrade_tenant set from_account_id = 11"):
			return executor.Result{AffectedRows: 1}, nil
		case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "where id = 100 for update"):
			return buildUpgradeVersionResult(upgradeID, versions.StateUpgradingTenant,
				version, version, 5, 0, versions.Yes, versions.Yes, 2, 0), nil
		case strings.Contains(sql, "update mo_upgrade set total_tenant = 2, ready_tenant = 1"):
			return executor.Result{AffectedRows: 1}, nil
		default:
			return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
		}
	}, nil)
	h := &testIncrementalVersionHandle{
		testVersionHandle: newTestVersionHandler(version, version, versions.Yes, versions.Yes, 5),
		completeAfter:     1,
	}

	err := (&service{}).upgradeTenantIncrementally(
		context.Background(),
		versions.VersionUpgrade{
			ID:            upgradeID,
			FromVersion:   version,
			ToVersion:     version,
			TotalTenant:   2,
			ReadyTenant:   0,
			UpgradeTenant: versions.Yes,
		},
		taskID,
		tenantID,
		version,
		h,
		txnExecutor,
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), h.stepCalls.Load())
	require.Len(t, sqls, 4)
	require.Contains(t, sqls[1], "set from_account_id = 11")
	for _, sql := range sqls {
		require.NotContains(t, sql, "set ready = 1")
	}
}

func TestUpgradeTenantIncrementallyDoesNotDoubleCountStaleWorker(t *testing.T) {
	const (
		tenantID = int32(10)
		version  = "4.0.6"
	)
	var sqls []string
	txnExecutor := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		if strings.HasPrefix(sql, "update mo_account set create_version") {
			return executor.Result{AffectedRows: 1}, nil
		}
		if strings.HasPrefix(sql, "update mo_upgrade_tenant") {
			return executor.Result{}, nil
		}
		return executor.Result{}, fmt.Errorf("stale worker must not update ready count: %s", sql)
	}, nil)
	h := &testIncrementalVersionHandle{
		testVersionHandle: newTestVersionHandler(version, version, versions.Yes, versions.Yes, 5),
		completeAfter:     1,
	}

	err := (&service{}).upgradeTenantIncrementally(
		context.Background(),
		versions.VersionUpgrade{
			ID:            100,
			FromVersion:   version,
			ToVersion:     version,
			TotalTenant:   2,
			UpgradeTenant: versions.Yes,
		},
		200,
		tenantID,
		version,
		h,
		txnExecutor,
	)
	require.NoError(t, err)
	require.Len(t, sqls, 3)
	for _, sql := range sqls {
		require.NotContains(t, sql, "update mo_upgrade set")
	}
}

func Test_asyncUpgradeTenantTask_AutoCompletesDeletedTenantTasks(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var finalized atomic.Bool
			var deletedTasksReconciled atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return buildUpgradeTenantTaskRows([]uint64{200}, []int32{10}, []int32{12}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 12"):
					return buildUpgradeTenantAccountRows(nil, nil), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 13"):
					return buildUpgradeTenantTaskRows(nil, nil, nil), nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					if deletedTasksReconciled.Load() {
						return executor.Result{}, nil
					}
					return buildExistsResult(), nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1") &&
					strings.Contains(sql, "where upgrade_id = 100") &&
					strings.Contains(sql, "not exists"):
					deletedTasksReconciled.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, finalized.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_asyncUpgradeTenantTask_ReconcilesReadyCountWhenTasksAlreadyFinished(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var finalized atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3") &&
					strings.Contains(sql, "state = 2"):
					finalized.Store(true)
					cancel()
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.True(t, finalized.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_asyncUpgradeTenantTask_SkipsReconcileWhenConflictTasksRemain(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var reconciled atomic.Bool
			var upgraded atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return buildUpgradeTenantTaskRows([]uint64{200}, []int32{10}, []int32{12}), nil
				case strings.Contains(sql, "select account_id, create_version from mo_account where account_id >= 10 and account_id <= 12"):
					return executor.Result{}, moerr.NewLockConflictNoCtx()
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 13"):
					return executor.Result{}, nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					return buildExistsResult(), nil
				case strings.Contains(sql, "update mo_upgrade_tenant set ready = 1"):
					reconciled.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3"):
					upgraded.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.False(t, reconciled.Load())
			require.False(t, upgraded.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func Test_asyncUpgradeTenantTask_SkipsAlreadyReconciledUpgradeCounts(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			var upgraded atomic.Bool
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
			defer cancel()

			sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where state = 1") &&
					!strings.Contains(sql, "for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 2), nil
				case strings.Contains(sql, "from mo_upgrade_tenant where from_account_id >= 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "select 1 from mo_upgrade_tenant where upgrade_id = 100 and ready = 0"):
					return executor.Result{}, nil
				case strings.Contains(sql, "from mo_upgrade") &&
					strings.Contains(sql, "where id = 100 for update"):
					return buildUpgradeVersionResult(100, 1, "3.0.0", "3.0.1", 0, 1, 1, 1, 3, 3), nil
				case strings.Contains(sql, "update mo_upgrade set total_tenant = 3, ready_tenant = 3"):
					upgraded.Store(true)
					return executor.Result{AffectedRows: 1}, nil
				default:
					return executor.Result{}, fmt.Errorf("unexpected sql: %s", sql)
				}
			})

			h := newTestVersionHandler("3.0.1", "3.0.0", versions.Yes, versions.Yes, 0)
			s := newServiceForTest(
				sid,
				&memLocker{},
				clock.NewHLCClock(func() int64 { return 0 }, 0),
				nil,
				sqlExecutor,
				func(s *service) {
					s.handles = append(s.handles, h)
				},
				WithCheckUpgradeTenantDuration(time.Millisecond),
			)

			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{CN: sid}).AnyTimes()

			s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				return sqlExecutor.Exec(context.Background(), sql, executor.Options{})
			}, txnOperator)

			s.asyncUpgradeTenantTask(ctx)
			require.False(t, upgraded.Load())
			require.Zero(t, h.callHandleTenantUpgrade.Load())
		},
	)
}

func buildUpgradeVersionResult(
	id uint64,
	state int32,
	fromVersion, toVersion string,
	finalVersionOffset uint32,
	upgradeOrder, upgradeCluster, upgradeTenant, totalTenant, readyTenant int32,
) executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_uint64, 64, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_varchar, 50, 0),
			types.New(types.T_uint32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendFixedRows(memRes, 0, []uint64{id})
	executor.AppendStringRows(memRes, 1, []string{fromVersion})
	executor.AppendStringRows(memRes, 2, []string{toVersion})
	executor.AppendStringRows(memRes, 3, []string{toVersion})
	executor.AppendFixedRows(memRes, 4, []uint32{finalVersionOffset})
	executor.AppendFixedRows(memRes, 5, []int32{state})
	executor.AppendFixedRows(memRes, 6, []int32{upgradeOrder})
	executor.AppendFixedRows(memRes, 7, []int32{upgradeCluster})
	executor.AppendFixedRows(memRes, 8, []int32{upgradeTenant})
	executor.AppendFixedRows(memRes, 9, []int32{totalTenant})
	executor.AppendFixedRows(memRes, 10, []int32{readyTenant})
	return memRes.GetResult()
}

func buildUpgradeTenantTaskRows(taskIDs []uint64, fromIDs, toIDs []int32) executor.Result {
	if len(taskIDs) == 0 {
		return executor.Result{}
	}
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_uint64, 64, 0),
			types.New(types.T_int32, 32, 0),
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(len(taskIDs))
	executor.AppendFixedRows(memRes, 0, taskIDs)
	executor.AppendFixedRows(memRes, 1, fromIDs)
	executor.AppendFixedRows(memRes, 2, toIDs)
	return memRes.GetResult()
}

func buildUpgradeTenantAccountRows(tenantIDs []int32, createVersions []string) executor.Result {
	if len(tenantIDs) == 0 {
		return executor.Result{}
	}
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_int32, 32, 0),
			types.New(types.T_varchar, 50, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(len(tenantIDs))
	executor.AppendFixedRows(memRes, 0, tenantIDs)
	executor.AppendStringRows(memRes, 1, createVersions)
	return memRes.GetResult()
}

func buildExistsResult() executor.Result {
	memRes := executor.NewMemResult(
		[]types.Type{
			types.New(types.T_int32, 32, 0),
		},
		mpool.MustNewZero(),
	)
	memRes.NewBatchWithRowCount(1)
	executor.AppendFixedRows(memRes, 0, []int32{1})
	return memRes.GetResult()
}
