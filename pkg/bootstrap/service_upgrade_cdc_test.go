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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRejectLegacyCDCTasks(t *testing.T) {
	for _, count := range []int{0, 2, 33} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
				require.Contains(t, sql, "from mo_catalog.mo_cdc_task")
				require.NotContains(t, sql, "source_uri")
				res := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
				res.NewBatchWithRowCount(count)
				for col := 0; col < 4; col++ {
					values := make([]string, count)
					for i := range values {
						values[i] = fmt.Sprintf("column%d-row%d", col, i)
					}
					executor.AppendStringRows(res, col, values)
				}
				return res.GetResult(), nil
			}, &testTxnOperator{})
			err := exec.ExecTxn(context.Background(), rejectLegacyCDCTasks, executor.Options{})
			if count == 0 {
				require.NoError(t, err)
				return
			}
			var terminal *terminalUpgradeError
			require.ErrorAs(t, err, &terminal)
			require.ErrorContains(t, err, "account=column0-row0")
			require.ErrorContains(t, err, "task_id=column1-row1")
			require.Equal(t, count > 32, strings.Contains(err.Error(), "more tasks exist"))
		})
	}
	t.Run("query failure remains retryable", func(t *testing.T) {
		want := errors.New("catalog read failed")
		exec := executor.NewMemExecutor2(func(string) (executor.Result, error) { return executor.Result{}, want }, &testTxnOperator{})
		err := exec.ExecTxn(context.Background(), rejectLegacyCDCTasks, executor.Options{})
		var terminal *terminalUpgradeError
		require.False(t, errors.As(err, &terminal))
		require.ErrorIs(t, err, want)
	})
}

func TestLegacyCDCAdmissionBeforeUpgradeMutation(t *testing.T) {
	final := v4_0_7.Handler.Metadata()
	for _, current := range []versions.Version{
		{Version: "4.0.6", VersionOffset: 5, State: versions.StateReady},
		{Version: "4.0.7", VersionOffset: final.VersionOffset, State: versions.StateCreated},
	} {
		t.Run(fmt.Sprintf("%s-state%d", current.Version, current.State), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case sql == versions.LatestVersionSQL:
						return recoveryVersionResult(t, current), nil
					case strings.Contains(sql, "from mo_upgrade"):
						return ownRecoveryResult(t, buildUpgradeVersionResult(10, versions.StateCreated, "4.0.6", "4.0.7", final.VersionOffset, 0, versions.Yes, versions.Yes, 0, 0)), nil
					case strings.Contains(sql, "from mo_catalog.mo_cdc_task"):
						mp := mpool.MustNewZeroNoFixed()
						t.Cleanup(func() { mpool.DeleteMPool(mp) })
						res := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
						res.NewBatchWithRowCount(1)
						for i, value := range []string{"10001", "task-uuid", "old-task", "paused"} {
							executor.AppendStringRows(res, i, []string{value})
						}
						return res.GetResult(), nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL before CDC admission: %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				err := s.doCheckUpgrade(t.Context())
				var terminal *terminalUpgradeError
				require.ErrorAs(t, err, &terminal)
				require.ErrorContains(t, err, "account=10001 task_id=task-uuid")
				require.ErrorContains(t, err, "state=\"paused\"")
			})
		})
	}
}

// TestLegacyCDCAdmissionEmptyCatalogAdmitsSourceOnward pins that a source
// below the frozen stage with no legacy CDC tasks admits past the gate and
// proceeds to create the consolidated route - the successor keeps upstream's
// admission/passage contract at the 4.0.7 final.
func TestLegacyCDCAdmissionEmptyCatalogAdmitsSourceOnward(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		source := v4_0_6.Handler.Metadata()
		var routes []string
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
				return newBootstrapStringResult("mo_catalog"), nil
			case sql == versions.LatestVersionSQL:
				return recoveryVersionResult(t, versions.Version{Version: source.Version, VersionOffset: source.VersionOffset, State: versions.StateReady}), nil
			case strings.Contains(sql, "from mo_catalog.mo_cdc_task"):
				return executor.Result{}, nil
			case strings.Contains(sql, "select version from mo_version"):
				return newBootstrapStringResult(source.Version), nil
			case strings.HasPrefix(sql, "insert into mo_upgrade"):
				routes = append(routes, sql)
				return executor.Result{}, nil
			default:
				return executor.Result{}, nil
			}
		}, &testTxnOperator{})
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
		t.Cleanup(s.stopper.Stop)
		require.NoError(t, s.doCheckUpgrade(t.Context()))
		require.Len(t, routes, 1, "a released 4.0.6 source crosses only the consolidated 4.0.7 step")
		require.Contains(t, fmt.Sprint(routes), fmt.Sprintf("'%s', '%s', '4.0.7',", source.Version, v4_0_7.Handler.Metadata().Version))
	})
}

// TestLegacyCDCReadErrorStaysRetryableAtAdmission pins the successor's
// create-admission contract for a transient catalog read: the wrapped error
// stays retryable, terminates nothing, and no mutation runs.
func TestLegacyCDCReadErrorStaysRetryableAtAdmission(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		source := v4_0_6.Handler.Metadata()
		want := errors.New("catalog read failed")
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
				return newBootstrapStringResult("mo_catalog"), nil
			case sql == versions.LatestVersionSQL:
				return recoveryVersionResult(t, versions.Version{Version: source.Version, VersionOffset: source.VersionOffset, State: versions.StateReady}), nil
			case strings.Contains(sql, "from mo_catalog.mo_cdc_task"):
				return executor.Result{}, want
			default:
				return executor.Result{}, fmt.Errorf("no mutation may follow a failed read: %s", sql)
			}
		}, &testTxnOperator{})
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
		t.Cleanup(s.stopper.Stop)
		err := s.doCheckUpgrade(t.Context())
		require.ErrorIs(t, err, want)
		var terminal *terminalUpgradeError
		require.False(t, errors.As(err, &terminal))
		require.False(t, s.upgrade.finalVersionCompleted.Load())
	})
}

// TestLegacyCDCAdmissionRecoveryEnforcedBeforeResumeVerdict pins the
// recovery-path admission the successor registration newly reaches: a
// retained route to the frozen 4.0.7 stage admits only after the legacy CDC
// catalog is clean, and the paused-task rejection stays terminal before the
// resume verdict.
func TestLegacyCDCAdmissionRecoveryEnforcedBeforeResumeVerdict(t *testing.T) {
	frozen := v4_0_7.Handler.Metadata()
	target := versions.Version{Version: frozen.Version, VersionOffset: frozen.VersionOffset, State: versions.StateCreated}
	from := v4_0_6.Handler.Metadata().Version
	for _, paused := range []bool{true, false} {
		t.Run(fmt.Sprintf("paused=%v", paused), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					switch {
					case strings.Contains(sql, "from mo_upgrade"):
						return ownRecoveryResult(t, buildUpgradeVersionResult(10, versions.StateUpgradingTenant, from, frozen.Version, frozen.VersionOffset, 0, versions.Yes, versions.Yes, 2, 1)), nil
					case strings.Contains(sql, "from mo_catalog.mo_cdc_task"):
						if paused {
							mp := mpool.MustNewZeroNoFixed()
							t.Cleanup(func() { mpool.DeleteMPool(mp) })
							res := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
							res.NewBatchWithRowCount(1)
							for i, value := range []string{"10001", "task-uuid", "old-task", "paused"} {
								executor.AppendStringRows(res, i, []string{value})
							}
							return res.GetResult(), nil
						}
						return executor.Result{}, nil
					default:
						return executor.Result{}, fmt.Errorf("validation must read only the retained route and the CDC catalog: %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, nil, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				err := s.checkUpgradeRecovery(target, txn)
				if paused {
					var terminal *terminalUpgradeError
					require.ErrorAs(t, err, &terminal)
					require.ErrorContains(t, err, "account=10001 task_id=task-uuid")
					require.ErrorContains(t, err, "state=\"paused\"")
				} else {
					require.NoError(t, err, "a clean CDC catalog admits the retained route's resume verdict")
				}
			})
		})
	}
}
