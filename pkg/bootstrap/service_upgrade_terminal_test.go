// Copyright 2026 Matrix Origin
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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	molog "github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestTerminalUpgradeFailureRechecksCommittedProgress(t *testing.T) {
	for _, tc := range []struct {
		name          string
		state         int32
		readErr       error
		wantPublished bool
		wantReady     bool
	}{
		{name: "pending", state: versions.StateCreated, wantPublished: true},
		{name: "another worker completed", state: versions.StateReady, wantPublished: true, wantReady: true},
		{name: "recheck failed", readErr: errors.New("recheck unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					require.Contains(t, sql, "for update")
					if tc.readErr != nil {
						return executor.Result{}, tc.readErr
					}
					mp := mpool.MustNewZeroNoFixed()
					t.Cleanup(func() { mpool.DeleteMPool(mp) })
					result := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(result, 0, []int32{tc.state}))
					return result.GetResult(), nil
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
					s.handles = append(s.handles, newTestVersionHandler("4.0.7", "4.0.6", versions.Yes, versions.Yes, 1))
				})
				t.Cleanup(s.stopper.Stop)
				cause := errors.New("administrator input needed")
				require.Equal(t, tc.wantPublished, s.recordTerminalUpgradeFailure(t.Context(), &terminalUpgradeError{cause}))
				ready, err := s.UpgradeStatus()
				require.Equal(t, tc.wantReady, ready)
				if tc.wantPublished && !tc.wantReady {
					require.ErrorIs(t, err, cause)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}

func TestRetryRunStopsOnTerminalUpgradeFailure(t *testing.T) {
	cause := errors.New("operator action required")
	calls := 0
	err := retryRun(t.Context(), runtime.DefaultRuntime().Logger(), "terminal", func(context.Context) error {
		calls++
		return &terminalUpgradeError{cause}
	})
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, calls)
}

func TestCatalogMemoryPressureIsContextualAndRetryable(t *testing.T) {
	cause := moerr.NewMPoolCapacityNoCtxf("pool cap exhausted")
	err := catalogOperationError(cause, "tenant 7", "foreign-key table db.t", 64<<20, true)
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "tenant 7 foreign-key table db.t")
	require.ErrorContains(t, err, "will retry")
	var terminal *terminalUpgradeError
	require.False(t, errors.As(err, &terminal), "shared pool pressure may clear or another CN may finish")
}

type capacityRetryTestExecutor struct {
	executor.SQLExecutor
	attempts chan struct{}
	released chan struct{}
}

func (e *capacityRetryTestExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	select {
	case <-e.released:
		return e.SQLExecutor.ExecTxn(ctx, fn, opts)
	default:
	}
	select {
	case e.attempts <- struct{}{}:
	default:
	}
	return catalogOperationError(moerr.NewMPoolCapacityNoCtxf("pool cap exhausted"), "cluster", "catalog entry", 0, false)
}

func TestUpgradeRetriesSharedMemoryPressure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		otherReady bool
	}{
		{name: "pressure clears and another CN has completed", otherReady: true},
		{name: "persistent pressure can be canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				mp := mpool.MustNewZeroNoFixed()
				defer mpool.DeleteMPool(mp)
				readyExecutor := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					res := executor.NewMemResult([]types.Type{types.T_int32.ToType()}, mp)
					res.NewBatchWithRowCount(1)
					if err := executor.AppendFixedRows(res, 0, []int32{versions.StateReady}); err != nil {
						return executor.Result{}, err
					}
					return res.GetResult(), nil
				}, &testTxnOperator{})
				exec := &capacityRetryTestExecutor{SQLExecutor: readyExecutor, attempts: make(chan struct{}, 8), released: make(chan struct{})}
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
					s.handles = append(s.handles, newTestVersionHandler("4.0.7", "4.0.6", versions.Yes, versions.Yes, 1))
				}, WithCheckUpgradeDuration(time.Millisecond))
				defer s.stopper.Stop()
				core, observed := observer.New(zap.ErrorLevel)
				s.logger = molog.GetServiceLogger(zap.New(core), metadata.ServiceType_CN, "capacity-test")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan struct{})
				go func() { defer close(done); s.asyncUpgradeTask(ctx) }()
				for range 2 {
					select {
					case <-exec.attempts:
					case <-time.After(5 * time.Second):
						t.Fatal("upgrade did not retry capacity pressure")
					}
				}
				ready, err := s.UpgradeStatus()
				require.NoError(t, err)
				require.False(t, ready)
				if tc.otherReady {
					// The committed ready state is observed after pressure clears;
					// this worker must not publish a stale terminal failure.
					close(exec.released)
				} else {
					cancel()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("upgrade did not stop")
				}
				ready, err = s.UpgradeStatus()
				require.NoError(t, err)
				require.Equal(t, tc.otherReady, ready)
				require.NotEmpty(t, observed.FilterMessage("upgrade attempt failed; will retry").All())
				require.Contains(t, observed.All()[0].ContextMap()["error"], "provision more memory")
			})
		})
	}
}
