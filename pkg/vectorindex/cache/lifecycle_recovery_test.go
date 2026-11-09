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

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pipelinepb "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type recoveryAlgo struct {
	MockSearch
	loads, destroys  atomic.Int32
	retries          int32
	panicLoad        bool
	started, release chan struct{}
}

var supersededCause = moerr.NewInvalidStateNoCtx("generation superseded")
var errSupersededForTest = NewRetryableLoadError(supersededCause)

func (a *recoveryAlgo) Load(*sqlexec.SqlProcess) error {
	n := a.loads.Add(1)
	if a.started != nil {
		close(a.started)
		<-a.release
	}
	if a.panicLoad {
		panic("injected load panic")
	}
	if n <= a.retries {
		return errSupersededForTest
	}
	return nil
}
func (a *recoveryAlgo) Destroy() { a.destroys.Add(1) }

func TestCacheRetryBudgetAndCanceledAdmission(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			defer c.Destroy()
			a := &recoveryAlgo{retries: 100}
			err := <-startCacheSearch(c, nil, a, into)
			require.Same(t, supersededCause, err)
			msg := &pipelinepb.Message{Err: pipelinepb.EncodedMessageError(context.Background(), err)}
			decoded, ok := msg.TryToGetMoErr()
			require.True(t, ok)
			require.True(t, moerr.IsMoErrCode(decoded, moerr.ErrInvalidState))
			require.EqualValues(t, 2, a.loads.Load())
			require.EqualValues(t, 2, a.destroys.Load())
			_, ok = c.IndexMap.Load("key")
			require.False(t, ok)
			replacement := &recoveryAlgo{}
			require.NoError(t, <-startCacheSearch(c, nil, replacement, into))
			c.Remove("key")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
			never := &recoveryAlgo{}
			require.ErrorIs(t, <-startCacheSearch(c, proc, never, into), context.Canceled)
			require.Zero(t, never.loads.Load())
		})
	}
}

func TestCacheLoadPanicAndWaiterCancellation(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			defer c.Destroy()
			a := &recoveryAlgo{panicLoad: true, started: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(a.release) }) }
			defer release()
			owner := startCacheSearch(c, nil, a, into)
			<-a.started
			v, ok := c.IndexMap.Load("key")
			require.True(t, ok)
			entry := v.(*VectorIndexSearch)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
			canceled := startCacheSearch(c, proc, &recoveryAlgo{}, into)
			waiting := startCacheSearch(c, nil, &recoveryAlgo{}, into)
			require.Eventually(t, func() bool { return entry.loadWaiters.Load() == 2 }, time.Second, time.Millisecond)
			cancel()
			select {
			case err := <-canceled:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("canceled follower remained blocked")
			}
			release()
			require.ErrorContains(t, <-owner, "injected load panic")
			require.ErrorContains(t, <-waiting, "injected load panic")
			require.EqualValues(t, 1, a.destroys.Load())
			_, ok = c.IndexMap.Load("key")
			require.False(t, ok)
			require.NoError(t, <-startCacheSearch(c, nil, &recoveryAlgo{}, into))
		})
	}
}

func blockingShutdownHook(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	lifecycleHooks.Lock()
	previous := lifecycleHooks.hooks
	lifecycleHooks.hooks = []func(bool){func(shutdown bool) {
		if shutdown {
			close(entered)
			<-release
		}
	}}
	lifecycleHooks.Unlock()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); lifecycleHooks.Lock(); lifecycleHooks.hooks = previous; lifecycleHooks.Unlock() })
	return entered, unblock
}

func TestCacheConcurrentAndRepeatedDestroy(t *testing.T) {
	entered, release := blockingShutdownHook(t)
	c := NewVectorIndexCache()
	c.Once()
	const n = 16
	done := make(chan struct{}, n)
	go func() { c.Destroy(); done <- struct{}{} }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not reach hook")
	}
	for i := 1; i < n; i++ {
		go func() { c.Destroy(); done <- struct{}{} }()
	}
	select {
	case <-done:
		t.Fatal("Destroy returned before shutdown finished")
	default:
	}
	release()
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Destroy blocked")
		}
	}
	c.Destroy()
	c.Once()
	require.True(t, c.exited.Load())
}

func TestCacheCancelWhileLoadPublishes(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			entry := NewVectorIndexSearch(&recoveryAlgo{})
			entry.ttlMu.Lock()
			var once sync.Once
			release := func() { once.Do(entry.ttlMu.Unlock) }
			defer release()
			loaded := make(chan error, 1)
			go func() { loaded <- entry.Load(nil) }()
			require.Eventually(t, func() bool { return entry.Status.Load() == STATUS_LOADED }, time.Second, time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
			result := make(chan error, 1)
			go func() {
				if into {
					result <- entry.SearchInto(proc, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
				} else {
					_, _, err := entry.Search(proc, nil, nil, vectorindex.RuntimeConfig{})
					result <- err
				}
			}()
			require.Eventually(t, func() bool { return entry.loadWaiters.Load() == 1 }, time.Second, time.Millisecond)
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("waiter blocked on the loader's lock")
			}
			release()
			require.NoError(t, <-loaded)
			entry.Destroy()
		})
	}
}

func TestFollowerRetryBudgetDoesNotWaitForFinalTeardown(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			old := NewVectorIndexSearch(&MockSearch{})
			c.IndexMap.Store("key", old)
			a := &teardownRetryAlgo{loadStarted: make(chan struct{}), releaseLoad: make(chan struct{}), destroyStarted: make(chan struct{}), releaseDestroy: make(chan struct{})}
			var loadOnce, destroyOnce sync.Once
			releaseLoad := func() { loadOnce.Do(func() { close(a.releaseLoad) }) }
			releaseDestroy := func() { destroyOnce.Do(func() { close(a.releaseDestroy) }) }
			defer func() { releaseLoad(); releaseDestroy(); old.Destroy(); c.Destroy() }()
			result := startCacheSearch(c, nil, a, into)
			require.Eventually(t, func() bool { return old.loadWaiters.Load() == 1 }, time.Second, time.Millisecond)
			old.evicting.Store(true)
			require.True(t, c.IndexMap.CompareAndDelete("key", old))
			old.markRemoved()
			select {
			case <-a.loadStarted:
			case <-time.After(time.Second):
				t.Fatal("retry did not load")
			}
			removed := make(chan struct{})
			go func() { c.Remove("key"); close(removed) }()
			require.Eventually(t, func() bool { _, ok := c.IndexMap.Load("key"); return !ok }, time.Second, time.Millisecond)
			releaseLoad()
			select {
			case <-a.destroyStarted:
			case <-time.After(time.Second):
				t.Fatal("destroy did not start")
			}
			select {
			case err := <-result:
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
			case <-time.After(time.Second):
				t.Fatal("exhausted request waited for foreign teardown")
			}
			releaseDestroy()
			select {
			case <-removed:
			case <-time.After(time.Second):
				t.Fatal("destroy did not finish")
			}
			require.EqualValues(t, 1, a.loads.Load())
		})
	}
}

type panicDestroyAlgo struct{ recoveryAlgo }

func (*panicDestroyAlgo) Destroy() { panic("injected destroy panic") }

func TestCacheDestroyPanicPreservesLoadError(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			defer c.Destroy()
			a := &panicDestroyAlgo{recoveryAlgo: recoveryAlgo{panicLoad: true}}
			err := <-startCacheSearch(c, nil, a, into)
			require.ErrorContains(t, err, "injected load panic")
			require.NotContains(t, err.Error(), "injected destroy panic")
			_, ok := c.IndexMap.Load("key")
			require.False(t, ok)
		})
	}
}

func TestCacheStartAndSignalRaceWithDestroy(t *testing.T) {
	t.Run("shutdown before start", func(t *testing.T) {
		c := NewVectorIndexCache()
		c.Destroy()
		c.Once()
		require.False(t, c.started.Load())
	})
	t.Run("start before shutdown", func(t *testing.T) {
		c := NewVectorIndexCache()
		c.Once()
		c.Destroy()
		require.True(t, c.started.Load())
		require.True(t, c.exited.Load())
	})
	t.Run("signal and explicit shutdown", func(t *testing.T) {
		entered, release := blockingShutdownHook(t)
		c := NewVectorIndexCache()
		c.Once()
		c.sigc <- syscall.SIGTERM
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("signal shutdown did not reach hook")
		}
		done := make(chan struct{})
		go func() { c.Destroy(); close(done) }()
		select {
		case <-done:
			t.Fatal("explicit shutdown returned before signal cleanup")
		default:
		}
		release()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("signal/shutdown blocked")
		}
		require.True(t, c.exited.Load())
	})
}

func TestCacheShutdownRejectsNewEntries(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			a := &teardownRetryAlgo{loadStarted: make(chan struct{}), releaseLoad: make(chan struct{}), destroyStarted: make(chan struct{}), releaseDestroy: make(chan struct{})}
			var loadOnce, destroyOnce sync.Once
			releaseLoad := func() { loadOnce.Do(func() { close(a.releaseLoad) }) }
			releaseDestroy := func() { destroyOnce.Do(func() { close(a.releaseDestroy) }) }
			defer func() { releaseLoad(); releaseDestroy(); c.Destroy() }()
			owner := startCacheSearch(c, nil, a, into)
			<-a.loadStarted
			shutdown := make(chan struct{})
			go func() { c.Destroy(); close(shutdown) }()
			require.Eventually(t, c.exited.Load, time.Second, time.Millisecond)
			newAlgo := &recoveryAlgo{}
			err := <-startCacheSearch(c, nil, newAlgo, into)
			require.ErrorIs(t, err, errCacheClosed)
			require.Zero(t, newAlgo.loads.Load())
			releaseLoad()
			select {
			case <-a.destroyStarted:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not claim entry")
			}
			releaseDestroy()
			select {
			case <-shutdown:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not finish")
			}
			require.Error(t, <-owner)
			require.ErrorIs(t, <-startCacheSearch(c, nil, newAlgo, into), errCacheClosed)
			_, ok := c.IndexMap.Load("key")
			require.False(t, ok)
		})
	}
	var zero VectorIndexCache
	require.NotPanics(t, zero.Destroy)
}
