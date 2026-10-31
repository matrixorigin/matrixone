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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRetiredCatalogClusterAdmission(t *testing.T) {
	for minor := 8; minor <= 17; minor++ {
		source := fmt.Sprintf("4.0.%d", minor)
		t.Run(source, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				queries := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					queries++
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case sql == versions.LatestVersionSQL:
						return recoveryVersionResult(t, versions.Version{Version: source, State: versions.StateReady}), nil
					default:
						t.Fatalf("unsupported source must fail before mutation: %s", sql)
						return executor.Result{}, nil
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				var terminal *terminalUpgradeError
				require.ErrorAs(t, s.doCheckUpgrade(t.Context()), &terminal)
				require.ErrorContains(t, terminal, "unsupported unpublished catalog source "+source)
				require.Equal(t, 2, queries)
				require.False(t, s.upgrade.finalVersionCompleted.Load())
			})
		})
	}
}

func TestRetiredCatalogTenantAdmission(t *testing.T) {
	for _, manual := range []bool{false, true} {
		for _, locked := range []bool{false, true} {
			t.Run(fmt.Sprintf("manual=%v/locked=%v", manual, locked), func(t *testing.T) {
				runtime.RunTest("", func(runtime.Runtime) {
					repair := &routineRepairTestHandle{VersionHandle: newTestVersionHandler("4.0.20", "4.0.7", versions.No, versions.Yes, 1)}
					reads := 0
					exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
						switch {
						case strings.HasPrefix(sql, "select create_version from mo_account"):
							reads++
							if locked && reads == 1 {
								return newBootstrapStringResult("4.0.7"), nil
							}
							return newBootstrapStringResult("4.0.17"), nil
						case sql == versions.LatestVersionSQL:
							return statisticsLatestVersionResult(t, repair.Metadata(), versions.StateReady), nil
						default:
							t.Fatalf("unsupported account must fail before mutation: %s", sql)
							return executor.Result{}, nil
						}
					}, &testTxnOperator{})
					s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
						s.handles = []VersionHandle{v4_0_7.Handler, repair}
						s.upgrade.finalVersionCompleted.Store(true)
					})
					t.Cleanup(s.stopper.Stop)
					var err error
					if manual {
						err = s.UpgradeOneTenant(t.Context(), 7)
					} else {
						_, err = s.MaybeUpgradeTenant(t.Context(), func() (int32, string, error) { return 7, "4.0.7", nil }, nil)
					}
					var terminal *terminalUpgradeError
					require.ErrorAs(t, err, &terminal)
					require.Equal(t, 0, repair.calls)
					require.Empty(t, s.mu.tenants)
					wantReads := 1
					if locked {
						wantReads++
					}
					require.Equal(t, wantReads, reads)
				})
			})
		}
	}
}

func TestRetiredCatalogPendingRouteAndScheduledTenant(t *testing.T) {
	for _, mode := range []string{"recheck", "recovery", "worker route", "worker account"} {
		t.Run(mode, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				repair := &routineRepairTestHandle{VersionHandle: newTestVersionHandler("4.0.20", "4.0.7", versions.No, versions.Yes, 1)}
				from := "4.0.17"
				if mode == "worker account" {
					from = "4.0.7"
				}
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case strings.HasPrefix(sql, "select state from mo_version"):
						return testCatalogUpgradeState(t, versions.StateCreated), nil
					case strings.Contains(sql, "from mo_upgrade_tenant"):
						return buildUpgradeTenantTaskRows([]uint64{20}, []int32{7}, []int32{8}), nil
					case strings.Contains(sql, "from mo_upgrade"):
						return ownRecoveryResult(t, buildUpgradeVersionResult(10, versions.StateUpgradingTenant, from, "4.0.20", 1, 0, versions.No, versions.Yes, 2, 0)), nil
					case strings.Contains(sql, "from mo_account"):
						// Even a later bad account must be checked before the first handler.
						return buildUpgradeTenantAccountRows([]int32{7, 8}, []string{"4.0.7", "4.0.17"}), nil
					default:
						t.Fatalf("unsupported route/account must fail before mutation: %s", sql)
						return executor.Result{}, nil
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
					s.handles = []VersionHandle{v4_0_7.Handler, repair}
				})
				t.Cleanup(s.stopper.Stop)
				var err error
				switch mode {
				case "recheck":
					err = s.checkUpgrade(t.Context(), true)
				case "recovery":
					err = exec.ExecTxn(t.Context(), func(tx executor.TxnExecutor) error { return s.checkUpgradeRecovery(repair.Metadata(), tx) }, executor.Options{})
				default:
					_, err = s.newTenantUpgradePass(t.Context())()
				}
				var terminal *terminalUpgradeError
				require.ErrorAs(t, err, &terminal)
				require.ErrorContains(t, err, "unsupported unpublished catalog source 4.0.17")
				require.Zero(t, repair.calls)
			})
		})
	}
}

func TestRetiredCatalogExplicitRegistration(t *testing.T) {
	s := &service{}
	s.initUpgrade()
	require.Error(t, s.checkRetiredCatalogSource("4.0.17"))
	s.handles = append(s.handles, newTestVersionHandler("4.0.17", "4.0.7", versions.No, versions.Yes, 1))
	require.NoError(t, s.checkRetiredCatalogSource("4.0.17"))
	for _, source := range []string{"4.0.1", "4.0.6", "4.0.7", "4.0.20"} {
		require.NoError(t, s.checkRetiredCatalogSource(source))
	}
}
