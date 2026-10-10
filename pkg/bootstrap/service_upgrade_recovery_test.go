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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestDoCheckUpgradeRetainsInterruptedTarget(t *testing.T) {
	for _, state := range []int32{versions.StateCreated, versions.StateUpgradingTenant, versions.StateReady} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				old := newTestVersionHandler("4.0.11", "4.0.10", versions.Yes, versions.Yes, 2)
				final := newTestVersionHandler("4.0.12", "4.0.11", versions.No, versions.Yes, 1)
				target := old.Metadata()
				target.State = versions.StateCreated
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					switch {
					case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
						return newBootstrapStringResult("mo_catalog"), nil
					case sql == "select version, version_offset, state from mo_version order by create_at desc limit 1":
						return recoveryVersionResult(t, target), nil
					case strings.Contains(sql, "from mo_upgrade"):
						total, ready := int32(0), int32(0)
						if state == versions.StateUpgradingTenant {
							total, ready = 2, 1
						} else if state == versions.StateReady {
							total, ready = 2, 2
						}
						return ownRecoveryResult(t, buildUpgradeVersionResult(10, state, "4.0.10", "4.0.11", 2, 0, versions.Yes, versions.Yes, total, ready)), nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL (must not create a new route): %s", sql)
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec,
					func(s *service) { s.handles = []VersionHandle{old, final} })
				t.Cleanup(s.stopper.Stop)
				require.NoError(t, s.doCheckUpgrade(t.Context()))
				require.False(t, s.upgrade.finalVersionCompleted.Load())
				require.Zero(t, old.callHandleClusterUpgrade.Load())
				require.Zero(t, final.callHandleClusterUpgrade.Load())
			})
		})
	}
}

func TestManualTenantUpgradeWaitsForRecoveredRoute(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		s, _ := newRecoveryTestService(t, versions.StateReady)
		s.exec = executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch sql {
			case "select create_version from mo_account where account_id = 7":
				return newBootstrapStringResult("4.0.10"), nil
			case "select version, version_offset, state from mo_version order by create_at desc limit 1":
				return recoveryVersionResult(t, versions.Version{Version: "4.0.11", VersionOffset: 2}), nil
			default:
				return executor.Result{}, fmt.Errorf("must not upgrade a tenant past the retained route: %s", sql)
			}
		}, &testTxnOperator{})
		require.ErrorContains(t, s.UpgradeOneTenant(t.Context(), 7), "cluster latest version 4.0.11")
		require.Empty(t, s.mu.tenants)
		for _, handle := range s.handles {
			require.Zero(t, handle.(*testVersionHandle).callHandleTenantUpgrade.Load())
		}
	})
}

func TestUpgradeRecoveryRejectsUnsupportedCatalog(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		s, _ := newRecoveryTestService(t, versions.StateReady)
		for _, target := range []versions.Version{
			{Version: "4.0.10", VersionOffset: 1}, // Missing retained handler.
			{Version: "4.0.11", VersionOffset: 1}, // Changed handler offset.
			{Version: "4.0.12", VersionOffset: 0}, // Same-version offset recovery.
			{Version: "4.0.13", VersionOffset: 1}, // Never run a future target.
		} {
			require.False(t, s.canResumeUpgrade(target), "%+v", target)
		}
		for _, tc := range []struct {
			name   string
			from   string
			state  int32
			order  int32
			tenant int32
		}{
			{name: "invalid state", from: "4.0.10", state: 99},
			{name: "missing order", from: "4.0.10", order: 1},
			{name: "reverse step", from: "4.0.12"},
			{name: "unreachable handler", from: "4.0.9"},
			{name: "changed handler flags", from: "4.0.10", tenant: versions.Yes},
		} {
			t.Run(tc.name, func(t *testing.T) {
				txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
					return ownRecoveryResult(t, buildUpgradeVersionResult(10, tc.state, tc.from, "4.0.11", 2, tc.order, versions.Yes, tc.tenant, 0, 0)), nil
				}, &testTxnOperator{})
				require.ErrorContains(t, s.checkUpgradeRecovery(s.handles[0].Metadata(), txn), "cannot resume")
			})
		}
		txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }, &testTxnOperator{})
		require.ErrorContains(t, s.checkUpgradeRecovery(s.handles[0].Metadata(), txn), "incomplete upgrade route")
	})
}

func TestUpgradePassRecoversBeforeAdvancing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state int32
	}{
		{"cluster not started", versions.StateCreated},
		{"tenant pending", versions.StateUpgradingTenant},
		{"steps committed before version ready", versions.StateReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				s, db := newRecoveryTestService(t, tc.state)
				pass := s.newUpgradePass(t.Context())
				if tc.state == versions.StateUpgradingTenant {
					completed, err := pass()
					require.NoError(t, err)
					require.False(t, completed)
					require.Zero(t, db.state.routes)
					require.Equal(t, versions.StateCreated, db.state.oldState)
					// A tenant worker commits the last retained task. No new route
					// may have been created while that task was outstanding.
					db.state.stepState = versions.StateReady
				}
				completed, err := pass()
				require.NoError(t, err)
				require.False(t, completed, "old-target completion is not binary-target completion")
				require.False(t, s.upgrade.finalVersionCompleted.Load())
				require.Equal(t, versions.StateReady, db.state.oldState)
				require.Equal(t, 1, db.state.routes)
				require.Equal(t, 1, db.state.steps)
				completed, err = pass()
				require.NoError(t, err)
				require.True(t, completed)
				require.Equal(t, versions.StateReady, db.state.finalState)
				// Re-entry after completion does not recreate steps or run handlers.
				completed, err = pass()
				require.NoError(t, err)
				require.True(t, completed)
				require.Equal(t, 1, db.state.steps)
			})
		})
	}
}

func TestUpgradeRecoveryRetainsMultiStepProgress(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				prefix := newTestVersionHandler("4.0.10", "4.0.9", versions.Yes, versions.Yes, 1)
				old := &recoveryPrepareHandle{testVersionHandle: newTestVersionHandler("4.0.11", "4.0.10", versions.Yes, versions.No, 2)}
				final := newTestVersionHandler("4.0.12", "4.0.11", versions.No, versions.No, 1)
				steps := []versions.VersionUpgrade{
					{ID: 10, FromVersion: "4.0.9", ToVersion: "4.0.10", FinalVersion: "4.0.11", FinalVersionOffset: 2,
						UpgradeOrder: 0, State: versions.StateReady, UpgradeCluster: versions.Yes, UpgradeTenant: versions.Yes, TotalTenant: 2, ReadyTenant: 2},
					{ID: 11, FromVersion: "4.0.10", ToVersion: "4.0.11", FinalVersion: "4.0.11", FinalVersionOffset: 2,
						UpgradeOrder: 1, State: versions.StateCreated, UpgradeCluster: versions.Yes},
				}
				if pending {
					steps[0].State, steps[0].ReadyTenant = versions.StateUpgradingTenant, 1
				}
				var writes []string
				txn := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
					sql = strings.Join(strings.Fields(sql), " ")
					switch {
					case strings.HasPrefix(sql, "select state from mo_version"):
						res := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mpool.MustNewZero())
						res.NewBatchWithRowCount(1)
						require.NoError(t, executor.AppendFixedRows(res, 0, []int32{versions.StateCreated}))
						return ownRecoveryResult(t, res.GetResult()), nil
					case strings.Contains(sql, "from mo_upgrade"):
						return recoveryRouteResult(t, steps), nil
					case strings.HasPrefix(sql, "update mo_upgrade"), strings.HasPrefix(sql, "update mo_version"):
						writes = append(writes, sql)
						return executor.Result{}, nil
					default:
						return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
					}
				}, &testTxnOperator{})
				s, _ := newRecoveryTestService(t, versions.StateReady)
				s.handles = []VersionHandle{prefix, old, final}
				require.NoError(t, s.checkUpgradeRecovery(old.Metadata(), txn))
				completed, err := s.performUpgrade(t.Context(), old.Metadata(), txn)
				require.NoError(t, err)
				require.Equal(t, !pending, completed)
				require.Zero(t, prefix.callHandleClusterUpgrade.Load(), "do not replay a committed prefix")
				require.Zero(t, prefix.callHandleTenantUpgrade.Load())
				if pending {
					require.Empty(t, writes, "do not advance past an incomplete tenant prefix")
					require.Empty(t, old.preparedFinal)
				} else {
					require.Equal(t, uint64(1), old.callHandleClusterUpgrade.Load())
					require.Equal(t, []bool{true}, old.preparedFinal, "Prepare final is relative to the retained route")
					require.Len(t, writes, 2)
					require.Contains(t, writes[0], "final_version = '4.0.11' and final_version_offset = 2 and upgrade_order = 1")
				}
				steps[1].FromVersion = "4.0.9"
				require.ErrorContains(t, s.checkUpgradeRecovery(old.Metadata(), txn), "invalid upgrade step")
			})
		})
	}
}

type recoveryPrepareHandle struct {
	*testVersionHandle
	preparedFinal []bool
}

func (h *recoveryPrepareHandle) Prepare(_ context.Context, _ executor.TxnExecutor, final bool) error {
	h.preparedFinal = append(h.preparedFinal, final)
	return nil
}

func TestUpgradeRecoveryTransactionFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		txn  int
	}{
		{name: "state read", sql: "select state"},
		{name: "target read", sql: "select version, version_offset"},
		{name: "step read", sql: "select id,"},
		{name: "old ready write", sql: "update mo_version"},
		{name: "old ready commit", txn: 1},
		{name: "new version write", sql: "insert into mo_version"},
		{name: "new steps write", sql: "insert into mo_upgrade"},
		{name: "new route commit", txn: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				s, db := newRecoveryTestService(t, versions.StateReady)
				db.failSQL, db.failCommit = tc.sql, tc.txn
				pass := s.newUpgradePass(t.Context())
				completed, err := pass()
				require.ErrorIs(t, err, errRecoveryTest)
				require.False(t, completed)
				require.False(t, s.upgrade.finalVersionCompleted.Load())
				require.Zero(t, db.state.routes)
				require.Zero(t, db.state.steps)
				if tc.txn == 1 {
					require.Equal(t, versions.StateCreated, db.state.oldState)
				}
				// A fresh pass re-reads durable state, including the boundary where
				// the old target committed but inserting our route rolled back.
				completed, err = pass()
				require.NoError(t, err)
				require.False(t, completed)
				require.Equal(t, 1, db.state.routes)
				require.Equal(t, 1, db.state.steps)
				completed, err = pass()
				require.NoError(t, err)
				require.True(t, completed)
			})
		})
	}
}

func TestUpgradeRecoveryRecheckAfterCompetingCN(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				s, db := newRecoveryTestService(t, versions.StateReady)
				db.afterCommit = func(n int) {
					if n == 1 {
						// Another CN creates our route after the retained target
						// commits, before we can acquire the table lock to recheck.
						db.state.routes, db.state.steps = 1, 1
						if ready {
							db.state.finalState = versions.StateReady
						}
					}
				}
				completed, err := s.newUpgradePass(t.Context())()
				require.NoError(t, err)
				require.False(t, completed)
				require.Equal(t, ready, s.upgrade.finalVersionCompleted.Load())
				require.Equal(t, 1, db.state.routes)
				require.Equal(t, 1, db.state.steps)
			})
		})
	}
}

func TestUpgradeCompletionPublishedAfterCommit(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		s, db := newRecoveryTestService(t, versions.StateReady)
		db.state.routes, db.state.steps, db.state.finalState = 1, 1, versions.StateReady
		db.failCommit = 1
		require.ErrorIs(t, s.checkUpgrade(t.Context(), true), errRecoveryTest)
		require.False(t, s.upgrade.finalVersionCompleted.Load())
		require.NoError(t, s.checkUpgrade(t.Context(), true))
		require.True(t, s.upgrade.finalVersionCompleted.Load())
	})
}

func TestUpgradeRecoveryAsyncLifecycle(t *testing.T) {
	for _, cancelPending := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelPending), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				runtime.RunTest("", func(runtime.Runtime) {
					s, db := newRecoveryTestService(t, versions.StateUpgradingTenant)
					s.upgrade.checkUpgradeDuration = time.Second
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					done := make(chan struct{})
					go func() { defer close(done); s.asyncUpgradeTask(ctx) }()
					synctest.Wait()
					time.Sleep(time.Second) // Virtual time advances the production timer.
					synctest.Wait()
					db.mu.Lock()
					routes := db.state.routes
					db.mu.Unlock()
					require.Zero(t, routes)
					if !cancelPending {
						// Wait observes the previous pass; the mutex also publishes
						// this simulated tenant commit to the next pass.
						db.mu.Lock()
						db.state.stepState = versions.StateReady
						db.mu.Unlock()
						time.Sleep(2 * time.Second)
						synctest.Wait()
						require.True(t, s.upgrade.finalVersionCompleted.Load())
						db.mu.Lock()
						steps := db.state.steps
						db.mu.Unlock()
						require.Equal(t, 1, steps)
					} else {
						cancel()
					}
					<-done
					require.Equal(t, !cancelPending, s.upgrade.finalVersionCompleted.Load())
				})
			})
		})
	}
}

var errRecoveryTest = errors.New("injected upgrade failure")

type recoveryTestState struct {
	oldState, stepState, finalState int32
	routes, steps                   int
}

// This fixture models transaction commit/rollback rather than treating observed
// UPDATE statements as durable progress. Integration tests supply the real locks.
type recoveryTestExecutor struct {
	executor.SQLExecutor
	mu          sync.Mutex
	t           *testing.T
	state       recoveryTestState
	txns        int
	oldTenant   int32
	inTxn       bool
	failCommit  int
	failSQL     string
	afterCommit func(int)
}

func (e *recoveryTestExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	require.False(e.t, e.inTxn, "route recheck must not nest a transaction holding step locks")
	e.inTxn = true
	defer func() { e.inTxn = false }()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.txns++
	before := e.state
	err := fn(executor.NewMemTxnExecutor(e.exec, &testTxnOperator{}))
	if err == nil && e.txns == e.failCommit {
		err = errRecoveryTest
	}
	if err != nil {
		e.state = before
		return err
	}
	if e.afterCommit != nil {
		e.afterCommit(e.txns)
	}
	return nil
}

func (e *recoveryTestExecutor) exec(sql string) (executor.Result, error) {
	sql = strings.Join(strings.Fields(sql), " ")
	if e.failSQL != "" && strings.HasPrefix(sql, e.failSQL) {
		e.failSQL = ""
		return executor.Result{}, errRecoveryTest
	}
	switch {
	case strings.HasPrefix(sql, "SELECT reldatabase, relname, account_id FROM mo_catalog.mo_tables"):
		return newBootstrapStringResult("mo_catalog"), nil
	case sql == "select version, version_offset, state from mo_version order by create_at desc limit 1":
		v := versions.Version{Version: "4.0.11", VersionOffset: 2, State: e.state.oldState}
		if e.state.routes > 0 {
			v = versions.Version{Version: "4.0.12", VersionOffset: 1, State: e.state.finalState}
		}
		return recoveryVersionResult(e.t, v), nil
	case strings.HasPrefix(sql, "select state from mo_version"):
		state := e.state.oldState
		if strings.Contains(sql, "'4.0.12'") {
			if e.state.routes == 0 {
				return executor.Result{}, nil
			}
			state = e.state.finalState
		}
		mp := mpool.MustNewZeroNoFixed()
		e.t.Cleanup(func() { mpool.DeleteMPool(mp) })
		res := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
		res.NewBatchWithRowCount(1)
		require.NoError(e.t, executor.AppendFixedRows(res, 0, []int32{state}))
		return res.GetResult(), nil
	case strings.Contains(sql, "from mo_upgrade"):
		if strings.Contains(sql, "'4.0.12'") {
			return ownRecoveryResult(e.t, buildUpgradeVersionResult(11, versions.StateCreated, "4.0.11", "4.0.12", 1, 0, versions.No, versions.No, 0, 0)), nil
		}
		total, ready := int32(0), int32(0)
		if e.oldTenant == versions.Yes {
			total, ready = 2, 1
			if e.state.stepState == versions.StateReady {
				ready = total
			}
		}
		return ownRecoveryResult(e.t, buildUpgradeVersionResult(10, e.state.stepState, "4.0.10", "4.0.11", 2, 0, versions.Yes, e.oldTenant, total, ready)), nil
	case strings.HasPrefix(sql, "update mo_upgrade set state = 2"):
		if strings.Contains(sql, "'4.0.11'") {
			e.state.stepState = versions.StateReady
		}
	case strings.HasPrefix(sql, "update mo_version set state = 2"):
		if strings.Contains(sql, "'4.0.11'") {
			e.state.oldState = versions.StateReady
		} else {
			e.state.finalState = versions.StateReady
		}
	case strings.HasPrefix(sql, "insert into mo_version"):
		require.Equal(e.t, versions.StateReady, e.state.oldState)
		e.state.routes++
	case strings.HasPrefix(sql, "select version from mo_version where state = 2"):
		return newBootstrapStringResult("4.0.11"), nil
	case strings.HasPrefix(sql, "insert into mo_upgrade"):
		e.state.steps++
	default:
		return executor.Result{}, fmt.Errorf("unexpected SQL: %s", sql)
	}
	return executor.Result{}, nil
}

func newRecoveryTestService(t *testing.T, state int32) (*service, *recoveryTestExecutor) {
	t.Helper()
	db := &recoveryTestExecutor{t: t, state: recoveryTestState{stepState: state}}
	if state == versions.StateUpgradingTenant {
		db.oldTenant = versions.Yes
	}
	s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, db,
		func(s *service) {
			s.handles = []VersionHandle{
				newTestVersionHandler("4.0.11", "4.0.10", versions.Yes, db.oldTenant, 2),
				newTestVersionHandler("4.0.12", "4.0.11", versions.No, versions.No, 1),
			}
		})
	t.Cleanup(s.stopper.Stop)
	return s, db
}

// Unlike the older single-step fixture, every row here belongs to the same
// retained final target, even when its ToVersion is only an intermediate hop.
func recoveryRouteResult(t *testing.T, steps []versions.VersionUpgrade) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_uint32.ToType(), types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType(),
		types.T_int32.ToType(), types.T_int32.ToType(), types.T_int32.ToType(),
	}, mp)
	for _, u := range steps {
		res.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(res, 0, []uint64{u.ID}))
		require.NoError(t, executor.AppendStringRows(res, 1, []string{u.FromVersion}))
		require.NoError(t, executor.AppendStringRows(res, 2, []string{u.ToVersion}))
		require.NoError(t, executor.AppendStringRows(res, 3, []string{u.FinalVersion}))
		require.NoError(t, executor.AppendFixedRows(res, 4, []uint32{u.FinalVersionOffset}))
		for i, value := range []int32{u.State, u.UpgradeOrder, u.UpgradeCluster, u.UpgradeTenant, u.TotalTenant, u.ReadyTenant} {
			require.NoError(t, executor.AppendFixedRows(res, i+5, []int32{value}))
		}
	}
	return res.GetResult()
}

// The SQL reader owns Result.Close; this fixture also owns the result's pool.
func ownRecoveryResult(t *testing.T, result executor.Result) executor.Result {
	t.Cleanup(func() { mpool.DeleteMPool(result.Mp) })
	return result
}

func recoveryVersionResult(t *testing.T, v versions.Version) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_int32.ToType(),
	}, mp)
	res.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(res, 0, []string{v.Version}))
	require.NoError(t, executor.AppendFixedRows(res, 1, []uint32{v.VersionOffset}))
	require.NoError(t, executor.AppendFixedRows(res, 2, []int32{v.State}))
	return res.GetResult()
}
