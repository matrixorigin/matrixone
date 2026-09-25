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

package trace

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type blockedTraceExecutor struct {
	executor.SQLExecutor
	entered chan context.Context
	release chan struct{}
}

func (e *blockedTraceExecutor) ExecTxn(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
	select {
	case e.entered <- ctx:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		return context.Canceled
	}
}

func TestTraceCloseCancelsBlockedWatchFetch(t *testing.T) {
	exec := &blockedTraceExecutor{entered: make(chan context.Context, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(exec.release) })
	s := &service{stopper: stopper.NewStopper(t.Name()), executor: exec, logger: runtime.DefaultRuntime().Logger()}
	s.atomic.flushEnabled.Store(true)
	s.entryC, s.txnC, s.loadC = make(chan event), make(chan event), make(chan loadAction)
	s.txnActionC, s.statementC = make(chan event), make(chan event)
	ticks := make(chan time.Time, 1)
	require.NoError(t, s.stopper.RunTask(func(ctx context.Context) { s.watchWithTicks(ctx, ticks) }))
	ticks <- time.Now()
	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("watch fetch did not start")
	}
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("trace close did not cancel watch fetch")
	}
}

func TestTraceWatchFilterRefreshObservesCancellation(t *testing.T) {
	for name, refresh := range map[string]func(*service, context.Context) error{
		"table":     (*service).refreshTableFilters,
		"txn":       (*service).refreshTxnFilters,
		"statement": (*service).refreshStatementFilters,
	} {
		t.Run(name, func(t *testing.T) {
			exec := &blockedTraceExecutor{entered: make(chan context.Context, 1), release: make(chan struct{})}
			t.Cleanup(func() { close(exec.release) })
			s := &service{executor: exec, clock: clock.NewHLCClock(func() int64 { return 0 }, 0)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- refresh(s, ctx) }()
			select {
			case <-exec.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("filter refresh did not start")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("filter refresh ignored watch cancellation")
			}
		})
	}
}

func TestTraceCloseCancelsBlockedLoadAndRetryWait(t *testing.T) {
	for _, blockedIO := range []bool{true, false} {
		name := "retry wait"
		if blockedIO {
			name = "blocked I/O"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			s := &service{stopper: stopper.NewStopper(t.Name()), logger: runtime.DefaultRuntime().Logger()}
			s.entryC, s.txnC, s.loadC = make(chan event), make(chan event), make(chan loadAction, 1)
			s.options.writeFunc = func(ctx context.Context, _ loadAction) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				if blockedIO {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-release:
						return context.Canceled
					}
				}
				return context.DeadlineExceeded
			}
			require.NoError(t, s.stopper.RunTask(s.handleLoad))
			s.loadC <- loadAction{}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("trace load did not start")
			}
			done := make(chan struct{})
			go func() { s.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("trace close did not cancel load")
			}
		})
	}
}

func TestTraceCloseCancelsBlockedTxnErrorWrite(t *testing.T) {
	exec := &blockedTraceExecutor{entered: make(chan context.Context, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(exec.release) })
	s := &service{
		stopper:  stopper.NewStopper(t.Name()),
		executor: exec,
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		logger:   runtime.DefaultRuntime().Logger(),
	}
	s.entryC, s.txnC, s.loadC = make(chan event), make(chan event), make(chan loadAction)
	s.txnErrorC = make(chan string, 1)
	require.NoError(t, s.stopper.RunTask(s.handleTxnError))
	s.txnErrorC <- "insert into trace_txn_error values (1)"
	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("trace txn-error write did not start")
	}
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("trace close did not cancel txn-error write")
	}
}
