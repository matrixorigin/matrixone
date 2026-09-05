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

package issues

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// These tests exercise the same transaction owner as the two-CN regression,
// without a cluster, SQL data, sleeps, or timeout-triggered scheduling.
func TestCDCRaceTxn(t *testing.T) {
	injectedErr := errors.New("injected transaction failure")
	for _, tc := range []struct {
		name          string
		startupError  bool
		statementFail int
		commitError   bool
		wantCalls     int
	}{
		{name: "success", wantCalls: 2},
		{name: "startup failure without callback", startupError: true},
		{name: "first statement failure", statementFail: 1, wantCalls: 1},
		{name: "partial initialization failure", statementFail: 2, wantCalls: 2},
		{name: "commit failure after readiness", commitError: true, wantCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			sqlExec := &cdcRaceSQLExecutor{execTxn: func(_ context.Context, callback func(executor.TxnExecutor) error, _ executor.Options) error {
				if tc.startupError {
					return injectedErr
				}
				err := callback(&cdcRaceStatementExecutor{exec: func(string, executor.StatementOption) (executor.Result, error) {
					calls++
					if calls == tc.statementFail {
						return executor.Result{}, injectedErr
					}
					return executor.Result{}, nil
				}})
				if err != nil {
					return err
				}
				if tc.commitError {
					return injectedErr
				}
				return nil
			}}
			release := make(chan struct{})
			run := startCDCRaceTxn(t, ctx, sqlExec, release, "first", "second")
			readyErr := run.waitReady(ctx)
			if tc.startupError || tc.statementFail != 0 {
				require.ErrorIs(t, readyErr, injectedErr)
				select {
				case <-run.ready:
					t.Fatal("failed transaction published readiness")
				default:
				}
			} else {
				require.NoError(t, readyErr)
				select {
				case <-run.done:
					t.Fatal("holder completed before release")
				default:
				}
				close(release)
			}
			wantErr := error(nil)
			if tc.startupError || tc.statementFail != 0 || tc.commitError {
				wantErr = injectedErr
			}
			require.ErrorIs(t, run.wait(ctx), wantErr)
			// Joining is repeatable: cleanup must not compete for a consumed result.
			require.ErrorIs(t, run.wait(ctx), wantErr)
			require.Equal(t, tc.wantCalls, calls)
		})
	}

	for _, beforeCallback := range []bool{true, false} {
		name := "cancel while holding"
		if beforeCallback {
			name = "cancel before callback"
		}
		t.Run(name, func(t *testing.T) {
			joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer joinCancel()
			ctx, cancel := context.WithCancel(joinCtx)
			defer cancel()
			entered := make(chan struct{})
			sqlExec := &cdcRaceSQLExecutor{execTxn: func(ctx context.Context, callback func(executor.TxnExecutor) error, _ executor.Options) error {
				close(entered)
				if beforeCallback {
					<-ctx.Done()
					return ctx.Err()
				}
				return callback(nil)
			}}
			run := startCDCRaceTxn(t, ctx, sqlExec, make(chan struct{}))
			select {
			case <-entered:
			case <-joinCtx.Done():
				t.Fatal("executor did not start")
			}
			if !beforeCallback {
				require.NoError(t, run.waitReady(ctx))
			}
			cancel()
			if beforeCallback {
				require.ErrorIs(t, run.waitReady(ctx), context.Canceled)
			}
			require.ErrorIs(t, run.wait(joinCtx), context.Canceled)
		})
	}

	t.Run("unheld contender completes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		run := startCDCRaceTxn(t, ctx, &cdcRaceSQLExecutor{execTxn: func(_ context.Context, callback func(executor.TxnExecutor) error, _ executor.Options) error {
			return callback(nil)
		}}, nil)
		require.NoError(t, run.wait(ctx))
		require.NoError(t, run.waitReady(ctx))
	})

	t.Run("wait deadlines do not depend on executor progress", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		run := startCDCRaceTxn(t, ctx, &cdcRaceSQLExecutor{execTxn: func(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
			<-ctx.Done()
			return ctx.Err()
		}}, nil)
		// An already-expired deadline is deterministic; no scheduler sleep is needed.
		waitCtx, waitCancel := context.WithDeadline(ctx, time.Time{})
		defer waitCancel()
		require.ErrorIs(t, run.waitReady(waitCtx), context.DeadlineExceeded)
		require.ErrorIs(t, run.wait(waitCtx), context.DeadlineExceeded)
	})

	t.Run("cleanup joins before fixture teardown", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var holder, contender *cdcRaceTxn
		t.Run("abandon active transactions", func(t *testing.T) {
			// This is registered first just like catalog cleanup in the real case.
			t.Cleanup(func() {
				for _, run := range []*cdcRaceTxn{holder, contender} {
					if run == nil { // A prior assertion may have prevented admission.
						continue
					}
					select {
					case <-run.done:
						require.ErrorIs(t, run.err, context.Canceled)
					default:
						t.Fatal("fixture teardown overtook transaction cleanup")
					}
				}
			})
			holder = startCDCRaceTxn(t, ctx, &cdcRaceSQLExecutor{execTxn: func(_ context.Context, callback func(executor.TxnExecutor) error, _ executor.Options) error {
				return callback(nil)
			}}, make(chan struct{}))
			require.NoError(t, holder.waitReady(ctx))
			contender = startCDCRaceTxn(t, ctx, &cdcRaceSQLExecutor{execTxn: func(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
				<-ctx.Done()
				return ctx.Err()
			}}, nil)
			// Do not release, cancel, or join: registered cleanup owns all three.
		})
	})
}

type cdcRaceSQLExecutor struct {
	executor.SQLExecutor
	execTxn func(context.Context, func(executor.TxnExecutor) error, executor.Options) error
}

func (e *cdcRaceSQLExecutor) ExecTxn(ctx context.Context, callback func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.execTxn(ctx, callback, opts)
}

type cdcRaceStatementExecutor struct {
	executor.TxnExecutor
	exec func(string, executor.StatementOption) (executor.Result, error)
}

func (e *cdcRaceStatementExecutor) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	return e.exec(sql, opts)
}
