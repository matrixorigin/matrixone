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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Current-version fixtures follow the same registry as bootstrap and CNs;
// historical handler versions below remain explicit migration inputs.
func statisticsFinalUpgradeVersion() versions.Version {
	s := &service{}
	s.initUpgrade()
	return s.getFinalVersionHandle().Metadata()
}

func TestPublishedCatalogUpgradeRoutes(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		offset        uint32
		hops          [][2]string
	}{
		{name: "4.1", version: "4.0.1", offset: 1, hops: [][2]string{{"4.0.1", "4.0.2"}, {"4.0.2", "4.0.3"}, {"4.0.3", "4.0.4"}, {"4.0.4", "4.0.5"}, {"4.0.5", "4.0.6"}, {"4.0.6", "4.0.7"}}},
		{name: "4.2", version: "4.0.6", offset: 5, hops: [][2]string{{"4.0.6", "4.0.7"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				var upgrades []string
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case sql == versions.LatestVersionSQL:
						return statisticsLatestVersionResult(t, versions.Version{Version: tc.version, VersionOffset: tc.offset}, versions.StateReady), nil
					case strings.HasPrefix(sql, "select version from mo_version"):
						return newBootstrapStringResult(tc.version), nil
					case strings.HasPrefix(sql, "insert into mo_upgrade"):
						upgrades = append(upgrades, sql)
					}
					return executor.Result{}, nil
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				require.Equal(t, "4.0.7", s.GetFinalVersion())
				require.Equal(t, uint32(5), v4_0_6.Handler.Metadata().VersionOffset)
				require.NoError(t, s.doCheckUpgrade(t.Context()))
				require.Len(t, upgrades, len(tc.hops))
				for i, hop := range tc.hops {
					require.Contains(t, upgrades[i], fmt.Sprintf("values ('%s', '%s', '4.0.7',", hop[0], hop[1]))
				}
			})
		})
	}
}

func TestLateTenantUpgradeUsesPersistedVersionAndCachesOnlyCommittedResult(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		tenantVersion := "4.0.6"
		handler := newTestVersionHandler("4.0.7", "4.0.6", versions.No, versions.Yes, 1)
		final := handler.Metadata()
		final.State = versions.StateReady
		reads, updates := 0, 0
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select create_version from mo_account"):
				reads++
				return newBootstrapStringResult(tenantVersion), nil
			case strings.HasPrefix(sql, "select version, version_offset, state from mo_version"):
				return statisticsLatestVersionResult(t, final, versions.StateReady), nil
			case strings.HasPrefix(sql, "update mo_account set create_version"):
				updates++
				tenantVersion = "4.0.7"
				return executor.Result{AffectedRows: 1}, nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
			}
		}, &testTxnOperator{})
		newCN := func() *service {
			s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.handles = []VersionHandle{handler} })
			t.Cleanup(s.stopper.Stop)
			return s
		}
		s := newCN()
		fetch := func() (int32, string, error) { return 11, "4.0.7", nil }
		upgraded, err := s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, "4.0.7", tenantVersion)
		require.Equal(t, 1, updates)
		require.Equal(t, 2, reads)
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Equal(t, 2, reads)
		upgraded, err = newCN().MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Equal(t, 3, reads)
		require.Equal(t, uint64(1), handler.callHandleTenantUpgrade.Load())
	})
}

func TestMaybeUpgradeTenantDoesNotCacheUncommittedOrFailedChecks(t *testing.T) {
	final := statisticsFinalUpgradeVersion()
	for _, test := range []struct {
		name           string
		version        string
		clusterVersion string
		fail           bool
		failUpgrade    bool
		ownTxn         bool
	}{
		{name: "caller_owned_transaction", version: final.Version, ownTxn: true},
		{name: "catalog_read_failure", version: final.Version, fail: true},
		{name: "migration_failure", version: "4.0.6", failUpgrade: true},
		{name: "newer_catalog_version", version: "999.0.0"},
		{name: "different_cluster_version", version: "4.0.6", clusterVersion: "999.0.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				reads := 0
				txnOp := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
				txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "select create_version from mo_account"):
						reads++
						if test.fail {
							return executor.Result{}, moerr.NewInternalErrorNoCtx("catalog unavailable")
						}
						return newBootstrapStringResult(test.version), nil
					case strings.HasPrefix(sql, "select version, version_offset, state from mo_version"):
						latest := final
						if test.clusterVersion != "" {
							latest.Version = test.clusterVersion
						}
						return statisticsLatestVersionResult(t, latest, versions.StateReady), nil
					case strings.HasPrefix(sql, "SELECT tbl.rel_createsql") && test.failUpgrade:
						return executor.Result{}, moerr.NewInternalErrorNoCtx("view migration unavailable")
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
					}
				}, txnOp)
				b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0),
					nil, exec, func(s *service) { s.initUpgrade() })
				defer b.stopper.Stop()
				for range 2 {
					var txnOp client.TxnOperator
					if test.ownTxn {
						txnOp = &testTxnOperator{}
					}
					fetch := func() (int32, string, error) { return 11, final.Version, nil }
					upgraded, err := b.MaybeUpgradeTenant(t.Context(), fetch, txnOp)
					if test.ownTxn {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					require.False(t, upgraded)
					require.Empty(t, b.mu.tenants)
				}
				if test.failUpgrade {
					require.Equal(t, 6, reads, "each retry rechecks the version in its independently owned operation transaction")
				} else {
					require.Equal(t, 2, reads)
				}
			})
		})
	}
}

func TestMaybeUpgradeTenantRechecksVersionUnderLock(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := statisticsFinalUpgradeVersion()
		reads := 0
		exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 11":
				reads++
				return newBootstrapStringResult("4.0.6"), nil
			case sql == "select create_version from mo_account where account_id = 11 for update":
				reads++
				return newBootstrapStringResult(final.Version), nil // another worker finished
			case strings.HasPrefix(sql, "select version, version_offset, state from mo_version"):
				return statisticsLatestVersionResult(t, final, versions.StateReady), nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
			}
		})
		b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0),
			nil, exec, func(s *service) { s.initUpgrade() })
		defer b.stopper.Stop()
		upgraded, err := b.MaybeUpgradeTenant(t.Context(),
			func() (int32, string, error) { return 11, final.Version, nil }, nil)
		require.NoError(t, err)
		require.False(t, upgraded, "do not report or rerun an upgrade another worker completed")
		require.Equal(t, 2, reads)
		require.True(t, b.mu.tenants[11])
	})
}

func TestMaybeUpgradeTenantWaitHonorsCancellation(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := statisticsFinalUpgradeVersion()
		exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			switch {
			case sql == "select create_version from mo_account where account_id = 11":
				return newBootstrapStringResult("4.0.6"), nil
			case strings.HasPrefix(sql, "select version, version_offset, state from mo_version"):
				return statisticsLatestVersionResult(t, final, versions.StateCreated), nil
			case strings.Contains(sql, "from mo_upgrade"):
				return buildUpgradeVersionResult(100, versions.StateCreated, "4.0.6", final.Version,
					final.VersionOffset, 0, versions.No, versions.Yes, 1, 0), nil
			default:
				return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
			}
		})
		b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0),
			nil, exec, func(s *service) { s.initUpgrade() })
		defer b.stopper.Stop()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		upgraded, err := b.MaybeUpgradeTenant(ctx,
			func() (int32, string, error) { return 11, final.Version, nil }, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, upgraded)
		require.Empty(t, b.mu.tenants)
	})
}

func TestMaybeUpgradeTenantRejectsConcurrentAccountDeletion(t *testing.T) {
	for _, beforeLock := range []bool{false, true} {
		name := "after_authentication"
		if beforeLock {
			name = "before_locking_recheck"
		}
		t.Run(name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				final := statisticsFinalUpgradeVersion()
				var authenticated, dropped bool
				exec := &statisticsTransactionTracker{SQLExecutor: executor.NewMemExecutor(
					func(sql string) (executor.Result, error) {
						require.True(t, authenticated)
						switch sql {
						case "select create_version from mo_account where account_id = 11":
							if dropped {
								return executor.Result{}, nil
							}
							return newBootstrapStringResult("4.0.6"), nil
						case versions.LatestVersionSQL:
							dropped = true // DROP commits before the locking recheck
							return statisticsLatestVersionResult(t, final, versions.StateReady), nil
						case "select create_version from mo_account where account_id = 11 for update":
							require.True(t, dropped)
							return executor.Result{}, nil
						default:
							return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
						}
					})}
				b := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0),
					nil, exec, func(s *service) { s.initUpgrade() })
				defer b.stopper.Stop()
				fetch := func() (int32, string, error) {
					authenticated = true
					if !beforeLock {
						dropped = true // authenticated account disappears before compensation
					}
					return 11, final.Version, nil
				}
				for attempt := 1; attempt <= 2; attempt++ {
					require.NotPanics(t, func() {
						upgraded, err := b.MaybeUpgradeTenant(t.Context(), fetch, nil)
						require.False(t, upgraded)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotFound), "unexpected error: %v", err)
					})
					require.False(t, exec.active, "the failed compensation transaction must end")
					require.Equal(t, attempt, exec.failures)
					require.Empty(t, b.mu.tenants, "a deleted account must never be cached as checked")
				}
			})
		})
	}
}

type statisticsTransactionTracker struct {
	executor.SQLExecutor
	active   bool
	failures int
}

func (e *statisticsTransactionTracker) ExecTxn(
	ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options,
) (err error) {
	e.active = true
	defer func() {
		e.active = false
		if err != nil {
			e.failures++
		}
	}()
	return e.SQLExecutor.ExecTxn(ctx, fn, opts)
}

func statisticsLatestVersionResult(t *testing.T, version versions.Version, state int32) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_int32.ToType(),
	}, mp)
	res.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(res, 0, []string{version.Version}))
	require.NoError(t, executor.AppendFixedRows(res, 1, []uint32{version.VersionOffset}))
	require.NoError(t, executor.AppendFixedRows(res, 2, []int32{state}))
	return res.GetResult()
}
