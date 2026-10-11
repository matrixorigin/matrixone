// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/stretchr/testify/require"
)

type borrowedAllocatorStore struct {
	IncrValueStore
	run func(context.Context) error
}

func (s *borrowedAllocatorStore) Allocate(ctx context.Context, _ uint64, _ string, _ int, _ client.TxnOperator) (uint64, uint64, timestamp.Timestamp, error) {
	return 1, 3, timestamp.Timestamp{}, s.run(ctx)
}
func (s *borrowedAllocatorStore) UpdateMinValue(ctx context.Context, _ uint64, _ string, _ uint64, _ client.TxnOperator) error {
	return s.run(ctx)
}
func (s *borrowedAllocatorStore) ForceSetOffset(ctx context.Context, _ uint64, _ int, _ string, _ uint64, _ client.TxnOperator) error {
	return s.run(ctx)
}

type allocatorRollbackWorkspace struct {
	client.Workspace
	rollbacks atomic.Int64
}

func (w *allocatorRollbackWorkspace) Rollback(context.Context) error { w.rollbacks.Add(1); return nil }
func (w *allocatorRollbackWorkspace) Readonly() bool                 { return false }

func waitAllocatorBorrowPhase(t *testing.T, phase <-chan struct{}) {
	t.Helper()
	select {
	case <-phase:
	case <-time.After(10 * time.Second):
		t.Fatal("allocator ownership phase did not complete")
	}
}
func allocatorWaiter(op client.TxnOperator) interface {
	CancelAndWaitRunningSQL(context.Context, uint64) error
} {
	return op.(interface {
		CancelAndWaitRunningSQL(context.Context, uint64) error
	})
}

// Manual test queues have no worker to return a borrow on assertion failure.
func releaseQueuedAllocatorBorrow(a *allocator) {
	select {
	case queued := <-a.c:
		if queued.release != nil {
			queued.release()
		}
	default:
	}
}

func TestAllocatorBorrowedTxnSurvivesCallerCancellation(t *testing.T) {
	for _, method := range []string{"async", "allocate", "update", "force"} {
		t.Run(method, func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx, cancel := context.WithCancel(defines.AttachAccountId(t.Context(), catalog.System_Account))
				defer cancel()
				op, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer op.Rollback(context.Background())
				ws := &allocatorRollbackWorkspace{}
				op.AddWorkspace(ws)
				closed := make(chan struct{})
				op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error { close(closed); return nil }})
				entered, release := make(chan struct{}), make(chan struct{})
				callbackEntered, callbackRelease := make(chan struct{}), make(chan struct{})
				store := &borrowedAllocatorStore{IncrValueStore: NewMemStore(), run: func(ctx context.Context) error { close(entered); <-release; return context.Cause(ctx) }}
				a := newValueAllocator("", store).(*allocator)
				defer a.close()
				// These releases precede the allocator's drain even on assertion failure.
				releaseStore := sync.OnceFunc(func() { close(release) })
				releaseCallback := sync.OnceFunc(func() { close(callbackRelease) })
				defer releaseCallback()
				defer releaseStore()
				result := make(chan error, 1)
				go func() {
					switch method {
					case "async":
						result <- a.asyncAllocate(ctx, 42, "id", 2, op, func(uint64, uint64, timestamp.Timestamp, error) { close(callbackEntered); <-callbackRelease })
					case "allocate":
						_, _, _, err := a.allocate(ctx, 42, "id", 2, op)
						result <- err
					case "update":
						result <- a.updateMinValue(ctx, 42, "id", 2, op)
					case "force":
						result <- a.forceSetOffset(ctx, 42, 0, "id", 2, op)
					}
				}()
				waitAllocatorBorrowPhase(t, entered)
				cancel()
				select {
				case err = <-result:
					if method == "async" {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, context.Canceled)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("caller did not stop waiting")
				}
				canceled, cancelWait := context.WithCancel(context.Background())
				cancelWait()
				require.ErrorIs(t, allocatorWaiter(op).CancelAndWaitRunningSQL(canceled, 0), context.Canceled)
				require.ErrorIs(t, op.Rollback(canceled), context.Canceled)
				require.Zero(t, ws.rollbacks.Load(), "rollback must not clear a store-held workspace")
				_, err = client.TryEnterRunSqlWithTokenAndSQL(op, func() {}, "new SQL")
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed), "terminal owner must seal new borrows")
				releaseStore()
				// The async callback is another old-generation consumer, not just a signal.
				if method == "async" {
					waitAllocatorBorrowPhase(t, callbackEntered)
					require.ErrorIs(t, allocatorWaiter(op).CancelAndWaitRunningSQL(canceled, 0), context.Canceled)
					require.Zero(t, ws.rollbacks.Load(), "rollback must also wait for the callback")
				}
				releaseCallback()
				waitAllocatorBorrowPhase(t, closed)
				require.Equal(t, int64(1), ws.rollbacks.Load())
			})
		})
	}
}

func TestAllocatorQueuedBorrowIsReleasedWithoutStoreEntry(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		defer op.Rollback(context.Background())
		ws := &allocatorRollbackWorkspace{}
		op.AddWorkspace(ws)
		var calls atomic.Int64
		a := &allocator{c: make(chan action, 1), done: make(chan struct{}), store: &borrowedAllocatorStore{run: func(context.Context) error { calls.Add(1); return nil }}}
		defer releaseQueuedAllocatorBorrow(a)
		var callbackErr error
		callbacks := 0
		require.NoError(t, a.asyncAllocate(ctx, 42, "id", 2, op, func(_ uint64, _ uint64, _ timestamp.Timestamp, err error) { callbacks++; callbackErr = err }))
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, op.Rollback(canceled), context.Canceled)
		require.Zero(t, ws.rollbacks.Load())
		a.doAction(<-a.c)
		require.Zero(t, calls.Load(), "canceled queued work must not enter the old workspace")
		require.Equal(t, 1, callbacks)
		require.ErrorIs(t, callbackErr, context.Canceled)
		require.Eventually(t, func() bool { return ws.rollbacks.Load() == 1 }, 10*time.Second, time.Millisecond)
		require.NoError(t, allocatorWaiter(op).CancelAndWaitRunningSQL(canceled, 0))
	})
}

type observedAllocatorBorrow struct {
	client.TxnOperator
	admitted   chan struct{}
	beforeExit func()
}

func (op *observedAllocatorBorrow) TryEnterRunSqlWithTokenAndSQL(cancel context.CancelFunc, sql string) (uint64, error) {
	token, err := client.TryEnterRunSqlWithTokenAndSQL(op.TxnOperator, cancel, sql)
	if err == nil && op.admitted != nil {
		close(op.admitted)
	}
	return token, err
}
func (op *observedAllocatorBorrow) ExitRunSqlWithToken(token uint64) {
	if op.beforeExit != nil {
		op.beforeExit()
	}
	op.TxnOperator.ExitRunSqlWithToken(token)
}

func TestAllocatorRejectedEnqueueReleasesBorrowOutsideQueueLock(t *testing.T) {
	for _, reason := range []string{"closed", "sealed", "full"} {
		t.Run(reason, func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
				raw, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer raw.Rollback(context.Background())
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				a := &allocator{c: make(chan action, 1), done: make(chan struct{})}
				var released, unlocked atomic.Bool
				op := &observedAllocatorBorrow{TxnOperator: raw, admitted: make(chan struct{}), beforeExit: func() {
					released.Store(true)
					if a.mu.TryLock() {
						unlocked.Store(true)
						a.mu.Unlock()
					}
				}}
				var callbacks atomic.Int64
				if reason == "closed" {
					a.closed = true
				}
				if reason == "sealed" {
					require.NoError(t, raw.Rollback(context.Background()))
				}
				if reason == "full" {
					a.c <- action{}
				}
				result := make(chan error, 1)
				go func() {
					result <- a.asyncAllocate(ctx, 42, "id", 2, op, func(uint64, uint64, timestamp.Timestamp, error) { callbacks.Add(1) })
				}()
				if reason == "full" {
					waitAllocatorBorrowPhase(t, op.admitted)
					cancel()
				}
				select {
				case err = <-result:
				case <-time.After(10 * time.Second):
					t.Fatal("rejected enqueue hung")
				}
				switch reason {
				case "closed":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
				case "sealed":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
				case "full":
					require.ErrorIs(t, err, context.Canceled)
					require.True(t, released.Load())
					require.True(t, unlocked.Load())
				}
				require.Zero(t, callbacks.Load(), "unpublished work has no callback owner")
				canceled, stop := context.WithCancel(context.Background())
				stop()
				require.NoError(t, allocatorWaiter(raw).CancelAndWaitRunningSQL(canceled, 0), "failed enqueue must not leak a token")
			})
		})
	}
}

func TestAllocatorBorrowReleasedAfterActionUnwinds(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
				op, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer op.Rollback(context.Background())
				var unwound atomic.Bool
				store := &borrowedAllocatorStore{run: func(context.Context) error {
					defer unwound.Store(true)
					if mode == "panic" {
						panic("store failure")
					}
					goruntime.Goexit()
					return nil
				}}
				a := &allocator{c: make(chan action, 1), done: make(chan struct{}), store: store}
				defer releaseQueuedAllocatorBorrow(a)
				require.NoError(t, a.asyncAllocate(ctx, 42, "id", 2, op, func(uint64, uint64, timestamp.Timestamp, error) { t.Error("unwound store must not invoke callback") }))
				act := <-a.c
				done := make(chan struct{})
				go func() { defer close(done); defer func() { _ = recover() }(); a.doAction(act) }()
				waitAllocatorBorrowPhase(t, done)
				require.True(t, unwound.Load())
				require.ErrorIs(t, context.Cause(act.ctx), context.Canceled)
				canceled, stop := context.WithCancel(context.Background())
				stop()
				require.NoError(t, allocatorWaiter(op).CancelAndWaitRunningSQL(canceled, 0))
			})
		})
	}
}

func TestAllocatorBorrowDrainsSuccessAndStoreError(t *testing.T) {
	for _, storeErr := range []error{nil, errors.New("allocator store failed")} {
		name := "success"
		if storeErr != nil {
			name = "store-error"
		}
		t.Run(name, func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
				op, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer op.Rollback(context.Background())
				a := newValueAllocator("", &borrowedAllocatorStore{IncrValueStore: NewMemStore(), run: func(context.Context) error { return storeErr }}).(*allocator)
				defer a.close()
				result := make(chan error, 1)
				require.NoError(t, a.asyncAllocate(ctx, 42, "id", 2, op, func(from, to uint64, _ timestamp.Timestamp, err error) {
					if from != 1 || to != 3 {
						err = errors.New("allocator changed the reserved range")
					}
					result <- err
				}))
				a.close()
				require.ErrorIs(t, <-result, storeErr)
				canceled, stop := context.WithCancel(context.Background())
				stop()
				require.NoError(t, allocatorWaiter(op).CancelAndWaitRunningSQL(canceled, 0))
			})
		})
	}
}

func TestAllocatorDetachedColumnPrefetchBorrowsTransaction(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		defer op.Rollback(context.Background())
		ws := &allocatorRollbackWorkspace{}
		op.AddWorkspace(ws)
		closed := make(chan struct{})
		op.AppendEventCallback(client.ClosedEvent, client.TxnEventCallback{Func: func(context.Context, client.TxnOperator, client.TxnEvent, any) error { close(closed); return nil }})
		entered, release := make(chan struct{}), make(chan struct{})
		releaseStore := sync.OnceFunc(func() { close(release) })
		var calls atomic.Int64
		store := &borrowedAllocatorStore{IncrValueStore: NewMemStore(), run: func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				return nil
			}
			close(entered)
			<-release
			return context.Cause(ctx)
		}}
		a := newValueAllocator("", store).(*allocator)
		defer a.close()
		defer releaseStore()
		cfg := Config{CountPerAllocate: 2}
		cfg.adjust()
		col, err := newColumnCache(ctx, "", 42, AutoColumn{ColName: "id", Step: 1}, cfg, false, a, op)
		require.NoError(t, err)
		detached := defines.AttachAccountId(context.Background(), catalog.System_Account)
		col.preAllocate(detached, 42, 3, op)
		waitAllocatorBorrowPhase(t, entered)
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, op.Rollback(canceled), context.Canceled)
		require.Zero(t, ws.rollbacks.Load())
		require.NoError(t, context.Cause(detached), "terminal cancellation must target the borrow, not its parent")
		releaseStore()
		a.close()
		waitAllocatorBorrowPhase(t, closed)
		require.Equal(t, int64(1), ws.rollbacks.Load())
		col.Lock()
		defer col.Unlock()
		require.False(t, col.allocating, "real cache callback must retire the allocation")
	})
}

// This external legacy contract deliberately treats zero as an opaque token.
// Embedding TxnOperator exposes no richer admission method.
type zeroTokenAllocatorBorrow struct {
	client.TxnOperator
	enters, exits int
	token         uint64
}

func (op *zeroTokenAllocatorBorrow) EnterRunSqlWithTokenAndSQL(context.CancelFunc, string) uint64 {
	op.enters++
	return 0
}
func (op *zeroTokenAllocatorBorrow) ExitRunSqlWithToken(token uint64) { op.exits++; op.token = token }
func TestAllocatorLegacyZeroTokenStillExecutesAndReleases(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		raw, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		defer raw.Rollback(context.Background())
		op := &zeroTokenAllocatorBorrow{TxnOperator: raw}
		a := newValueAllocator("", &borrowedAllocatorStore{IncrValueStore: NewMemStore(), run: func(context.Context) error { return nil }}).(*allocator)
		defer a.close()
		result := make(chan error, 1)
		require.NoError(t, a.asyncAllocate(ctx, 42, "id", 2, op, func(uint64, uint64, timestamp.Timestamp, error) { result <- nil }))
		a.close()
		require.NoError(t, <-result)
		require.Equal(t, 1, op.enters)
		require.Equal(t, 1, op.exits)
		require.Zero(t, op.token)
	})
}
