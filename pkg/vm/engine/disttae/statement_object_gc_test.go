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

package disttae

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

func awaitStatementGC(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("statement object cleanup made no progress")
	}
}

func waitStatementGCState(t *testing.T, e *Engine, ready func(*statementObjectGC) bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		e.statementGC.Lock()
		ok, changed := ready(&e.statementGC), e.statementGC.changed
		e.statementGC.Unlock()
		if ok {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("statement object cleanup state did not advance")
		}
	}
}

func newStatementGCTestEngine(t *testing.T, count int) *Engine {
	t.Helper()
	e := &Engine{}
	quota := int64(count) * statementObjectPinQuota
	e.config.memThrottler = rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(quota))
	_, ok := e.AcquireQuota(quota)
	require.True(t, ok)
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = e.closeStatementObjects(ctx)
		e.statementGC.Lock()
		done := e.statementGC.done
		e.statementGC.Unlock()
		if done != nil {
			awaitStatementGC(t, done)
		}
	})
	return e
}

func TestStatementObjectGCProgressAndViewShutdown(t *testing.T) {
	e := newStatementGCTestEngine(t, 3)
	entered, allow := make(chan struct{}, 3), make(chan struct{}, 3)
	cleanup := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	e.fs = &statementGCFileService{delete: func(ctx context.Context, names ...string) error {
		require.Equal(t, int32(1), active.Add(1), "one engine worker owns Delete")
		defer active.Add(-1)
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-allow:
		case <-cleanup:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		return nil
	}}
	t.Cleanup(func() { close(cleanup); require.NoError(t, e.CloseStatementObjects()) })
	for range 3 {
		require.NoError(t, e.registerStatementView())
	}
	first := &statementObjectPin{name: "first"}
	e.releaseStatementView(first, first)
	awaitStatementGC(t, entered)
	second := &statementObjectPin{name: "while-active"}
	e.releaseStatementView(second, second)
	allow <- struct{}{}
	awaitStatementGC(t, entered)
	allow <- struct{}{}
	waitStatementGCState(t, e, func(g *statementObjectGC) bool { return !g.inflight && g.head == nil })
	// Enqueue after the empty observation as well as during Delete. Both must
	// make progress without a later query or an opportunistic manual drain.
	closed := make(chan error, 1)
	go func() { closed <- e.CloseStatementObjects() }()
	waitStatementGCState(t, e, func(g *statementObjectGC) bool { return g.closing })
	require.True(t, moerr.IsMoErrCode(e.registerStatementView(), moerr.ErrTxnNeedRetry))
	shortCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.closeStatementObjects(shortCtx), context.Canceled, "a second caller does not own shutdown")
	select {
	case err := <-closed:
		t.Fatalf("shutdown lost its registered view: %v", err)
	default:
	}
	last := &statementObjectPin{name: "after-idle-during-shutdown"}
	e.releaseStatementView(last, last)
	awaitStatementGC(t, entered)
	select {
	case err := <-closed:
		t.Fatalf("shutdown returned before Delete completed: %v", err)
	default:
	}
	allow <- struct{}{}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not join worker")
	}
	awaitStatementGC(t, e.statementGC.done)
	require.Equal(t, int32(3), calls.Load())
	require.Equal(t, int64(3)*statementObjectPinQuota, e.config.memThrottler.Available())
	require.NoError(t, e.CloseStatementObjects())
	for range 100 {
		require.NoError(t, e.closeStatementObjects(shortCtx), "a canceled late caller cannot overwrite completed success")
	}
}

func TestStatementObjectGCPartiallyDeletedChunkRemainsOwned(t *testing.T) {
	// The minimum beyond one chunk proves quota is returned independently for
	// each success, while an ambiguous later deletion keeps its whole charge.
	const count = GCBatchOfFileCount + 1
	e := newStatementGCTestEngine(t, count)
	files := make(map[string]bool, count)
	var head, tail *statementObjectPin
	for i := range count {
		name := fmt.Sprint(i)
		files[name] = true
		pin := &statementObjectPin{name: name}
		if tail == nil {
			head = pin
		} else {
			tail.next = pin
		}
		tail = pin
	}
	deleteErr := moerr.NewInternalErrorNoCtx("partial object delete")
	failed, retry, allow := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(allow) })
	var calls int
	var mu sync.Mutex
	e.fs = &statementGCFileService{delete: func(ctx context.Context, names ...string) error {
		require.NoError(t, context.Cause(ctx))
		_, bounded := ctx.Deadline()
		require.True(t, bounded)
		mu.Lock()
		calls++
		call := calls
		for _, name := range names {
			delete(files, name)
		}
		mu.Unlock()
		if call == 2 {
			close(failed)
			return deleteErr // Gone remotely, but still owned until acknowledged.
		}
		if call == 3 {
			close(retry)
			select {
			case <-allow:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		return nil
	}}
	t.Cleanup(func() { finish(); require.NoError(t, e.CloseStatementObjects()) })
	require.NoError(t, e.registerStatementView())
	e.releaseStatementView(head, tail)
	awaitStatementGC(t, failed)
	awaitStatementGC(t, retry)
	require.Equal(t, int64(count-1)*statementObjectPinQuota, e.config.memThrottler.Available())
	mu.Lock()
	require.Empty(t, files)
	mu.Unlock()
	finish()
	require.NoError(t, e.CloseStatementObjects())
	mu.Lock()
	require.Equal(t, 3, calls)
	mu.Unlock()
	require.Equal(t, int64(count)*statementObjectPinQuota, e.config.memThrottler.Available())

}

func TestStatementObjectGCFailureIsolatesNamesAndTerminalShutdown(t *testing.T) {
	e := newStatementGCTestEngine(t, 3)
	deleteErr := moerr.NewInternalErrorNoCtx("permanently unavailable object")
	goodDeleted := make(chan struct{})
	e.fs = &statementGCFileService{delete: func(ctx context.Context, names ...string) error {
		for _, name := range names {
			if name == "bad" {
				return deleteErr
			}
		}
		close(goodDeleted)
		return nil
	}}
	for range 2 {
		require.NoError(t, e.registerStatementView())
	}
	bad, good := &statementObjectPin{name: "bad"}, &statementObjectPin{name: "good"}
	bad.next = good
	e.releaseStatementView(bad, good)
	awaitStatementGC(t, goodDeleted) // Failed batch members retry individually.
	waitStatementGCState(t, e, func(g *statementObjectGC) bool { return g.lastErr != nil && !g.inflight })
	require.Equal(t, statementObjectPinQuota, e.config.memThrottler.Available())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- e.closeStatementObjects(ctx) }()
	waitStatementGCState(t, e, func(g *statementObjectGC) bool { return g.closing })
	cancel()
	var err error
	select {
	case err = <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("failed shutdown did not return")
	}
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, deleteErr)
	awaitStatementGC(t, e.statementGC.done)
	late := &statementObjectPin{name: "late-view"}
	e.releaseStatementView(late, late)
	require.ErrorIs(t, e.CloseStatementObjects(), deleteErr)
	require.Equal(t, statementObjectPinQuota, e.config.memThrottler.Available(), "failed and late obligations stay charged")
	e.statementGC.Lock()
	require.NotNil(t, e.statementGC.head)
	require.Same(t, late, e.statementGC.tail)
	e.statementGC.Unlock()
}

func TestStatementObjectGCShutdownCancelsIOAndRetainsLateHandoff(t *testing.T) {
	for _, lateBeforeExit := range []bool{false, true} {
		t.Run(fmt.Sprint(lateBeforeExit), func(t *testing.T) {
			e := newStatementGCTestEngine(t, 2)
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			t.Cleanup(finish)
			e.fs = &statementGCFileService{delete: func(ctx context.Context, names ...string) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release // Models a backend that does not immediately join.
				return context.Cause(ctx)
			}}
			for range 2 {
				require.NoError(t, e.registerStatementView())
			}
			pin := &statementObjectPin{name: "inflight"}
			e.releaseStatementView(pin, pin)
			awaitStatementGC(t, entered)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, e.closeStatementObjects(ctx), context.Canceled)
			awaitStatementGC(t, canceled)
			late := &statementObjectPin{name: "late"}
			if lateBeforeExit {
				e.releaseStatementView(late, late)
			}
			finish()
			awaitStatementGC(t, e.statementGC.done)
			if !lateBeforeExit {
				e.releaseStatementView(late, late)
			}
			require.ErrorIs(t, e.CloseStatementObjects(), context.Canceled)
			require.Zero(t, e.config.memThrottler.Available())
			e.statementGC.Lock()
			require.False(t, e.statementGC.inflight)
			require.Zero(t, e.statementGC.views)
			require.NotNil(t, e.statementGC.head.next)
			e.statementGC.Unlock()
		})
	}
	// An engine with no captured views never needs a worker.
	e := &Engine{}
	require.NoError(t, e.CloseStatementObjects())
	require.NoError(t, e.CloseStatementObjects())
	require.Nil(t, e.statementGC.done)
}
