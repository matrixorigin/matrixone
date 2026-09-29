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
	"encoding/csv"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

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

type traceFlushPhaseContext struct {
	context.Context
	doneCalls atomic.Int32
	sendAt    int32
	atSend    chan struct{}
	reentered chan struct{}
}

func (c *traceFlushPhaseContext) Done() <-chan struct{} {
	switch c.doneCalls.Add(1) {
	case c.sendAt:
		close(c.atSend)
	case c.sendAt + 1:
		close(c.reentered)
	}
	return c.Context.Done()
}

func TestTraceCanceledFlushExitsBeforeReadingBacklog(t *testing.T) {
	for _, trigger := range []string{"event", "ticker"} {
		t.Run(trigger, func(t *testing.T) {
			core, _ := observer.New(zapcore.FatalLevel)
			logger := runtime.DefaultRuntime().Logger().WithOptions(
				zap.WrapCore(func(zapcore.Core) zapcore.Core { return core }),
				zap.WithFatalHook(zapcore.WriteThenPanic),
			)
			parent, cancel := context.WithCancel(context.Background())
			ctx := &traceFlushPhaseContext{
				Context:   parent,
				sendAt:    2,
				atSend:    make(chan struct{}),
				reentered: make(chan struct{}),
			}
			s := &service{dir: t.TempDir(), loadC: make(chan loadAction), logger: logger}
			s.atomic.flushEnabled.Store(true)
			s.options.flushBytes = 1
			if trigger == "ticker" {
				ctx.sendAt = 4 // empty tick, event, then flush tick
				s.options.flushBytes = 1 << 30
			}
			events := make(chan event, 1)
			var ticks chan time.Time
			if trigger == "ticker" {
				ticks = make(chan time.Time)
			}
			done := make(chan any, 1)
			exited := make(chan struct{})
			go func() {
				defer close(exited)
				defer func() { done <- recover() }()
				s.handleEventWithTicks(ctx, 8, EventTxnTable, events, ticks)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-exited:
				case <-time.After(2 * time.Second):
					t.Error("trace event handler did not exit")
				}
			})

			if trigger == "ticker" {
				// An empty tick must not close or enqueue the active file.
				ticks <- time.Now()
				converted := make(chan struct{})
				events <- event{csv: traceBarrierEvent{before: func() { close(converted) }}}
				select {
				case <-converted:
				case <-time.After(2 * time.Second):
					t.Fatal("trace event was not converted")
				}
				ticks <- time.Now()
			} else {
				events <- event{csv: txnEvent{eventType: txnCreateEvent}}
			}
			select {
			case <-ctx.atSend:
			case <-time.After(2 * time.Second):
				t.Fatal("flush did not reach the blocked load send")
			}
			events <- event{csv: txnEvent{eventType: txnCreateEvent}}
			cancel()
			select {
			case panicValue := <-done:
				require.Nil(t, panicValue, "canceled flush used a closed CSV writer")
			case <-time.After(2 * time.Second):
				t.Fatal("canceled flush did not exit")
			}
			select {
			case <-ctx.reentered:
				t.Fatal("event handler reentered its select after canceling the flush")
			default:
			}

			entries, err := os.ReadDir(s.dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "cancellation must not open a new CSV file")
			file, err := os.Open(filepath.Join(s.dir, entries[0].Name()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			rows, err := csv.NewReader(file).ReadAll()
			require.NoError(t, err)
			require.Len(t, rows, 1, "the queued event must not be written after cancellation")
			require.Equal(t, txnCreateEvent, rows[0][3])
		})
	}
}
