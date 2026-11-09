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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pipelinepb "github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type invalidationFailureAlgo struct {
	MockSearch
	cause                  error
	started, release       chan struct{}
	invalidations          atomic.Int32
	destroys               atomic.Int32
	panicDuringDestruction bool
}

func (a *invalidationFailureAlgo) OnCacheInvalidated(string) {
	a.invalidations.Add(1)
	if a.started != nil {
		close(a.started)
		<-a.release
	}
	if a.cause != nil {
		panic(a.cause)
	}
}

func (a *invalidationFailureAlgo) Destroy() {
	a.destroys.Add(1)
	if a.panicDuringDestruction {
		panic("destroy panic")
	}
}

func searchForInvalidationTest(c *VectorIndexCache, proc *sqlexec.SqlProcess, key string, algo VectorIndexSearchIf, into bool) error {
	if into {
		return c.SearchInto(proc, key, algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
	}
	_, _, err := c.Search(proc, key, algo, nil, vectorindex.RuntimeConfig{})
	return err
}

func waitForCacheShutdown(t *testing.T, c *VectorIndexCache) bool {
	t.Helper()
	done := make(chan struct{})
	go func() { c.Destroy(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		t.Error("cache shutdown did not finish")
		return false
	}
}

func requireInvalidationFailure(t *testing.T, err error) {
	t.Helper()
	require.ErrorContains(t, err, "invalidation panic")
	require.IsType(t, (*moerr.Error)(nil), err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState), "%v", err)
}

func TestInvalidationPanicClosesCache(t *testing.T) {
	for _, trigger := range []string{"remove", "shutdown"} {
		for _, into := range []bool{false, true} {
			t.Run(trigger+map[bool]string{false: "/Search", true: "/SearchInto"}[into], func(t *testing.T) {
				c := NewVectorIndexCache()
				bad := &invalidationFailureAlgo{
					cause: moerr.NewInvalidStateNoCtx("invalidation panic"), panicDuringDestruction: trigger == "remove",
				}
				other := &recoveryAlgo{}
				require.NoError(t, searchForInvalidationTest(c, nil, "bad", bad, into))
				require.NoError(t, searchForInvalidationTest(c, nil, "other", other, into))

				done := make(chan struct{})
				go func() {
					if trigger == "remove" {
						c.RemoveWithReason("bad", "ttl_expired")
					} else {
						c.Destroy()
					}
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("invalidation or shutdown did not finish")
				}
				require.True(t, waitForCacheShutdown(t, c))
				fresh := &recoveryAlgo{}
				err := searchForInvalidationTest(c, nil, "new", fresh, into)
				requireInvalidationFailure(t, err)
				msg := &pipelinepb.Message{Err: pipelinepb.EncodedMessageError(context.Background(), err)}
				decoded, ok := msg.TryToGetMoErr()
				require.True(t, ok)
				require.True(t, moerr.IsMoErrCode(decoded, moerr.ErrInvalidState))
				require.Zero(t, fresh.loads.Load())
				require.EqualValues(t, 1, bad.invalidations.Load())
				require.EqualValues(t, 1, bad.destroys.Load())
				require.EqualValues(t, 1, other.destroys.Load())
				_, ok = c.IndexMap.Load("bad")
				require.False(t, ok)
				_, ok = c.IndexMap.Load("other")
				require.False(t, ok)
			})
		}
	}
}

func TestInvalidationPanicUnblocksLoadingWaiter(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			bad := &invalidationFailureAlgo{cause: moerr.NewInvalidStateNoCtx("invalidation panic")}
			require.NoError(t, searchForInvalidationTest(c, nil, "bad", bad, into))
			loading := &recoveryAlgo{started: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(loading.release) }) }
			t.Cleanup(func() { release(); waitForCacheShutdown(t, c) })
			owner := startCacheSearch(c, nil, loading, into)
			select {
			case <-loading.started:
			case <-time.After(5 * time.Second):
				t.Fatal("load did not start")
			}
			value, ok := c.IndexMap.Load("key")
			require.True(t, ok)
			entry := value.(*VectorIndexSearch)
			follower := startCacheSearch(c, nil, &recoveryAlgo{}, into)
			require.Eventually(t, func() bool { return entry.loadWaiters.Load() == 1 }, time.Second, time.Millisecond)

			c.RemoveWithReason("bad", "ttl_expired")
			select {
			case err := <-follower:
				requireInvalidationFailure(t, err)
			case <-time.After(time.Second):
				t.Fatal("follower did not observe cache shutdown")
			}
			release()
			select {
			case err := <-owner:
				requireInvalidationFailure(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("loading owner did not finish")
			}
			require.True(t, waitForCacheShutdown(t, c))
			require.EqualValues(t, 1, loading.loads.Load())
			require.EqualValues(t, 1, loading.destroys.Load())
		})
	}
}

type observedDoneContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestShutdownUnblocksRemovedWaiterAndJoinsEviction(t *testing.T) {
	c := NewVectorIndexCache()
	blocked := &invalidationFailureAlgo{started: make(chan struct{}), release: make(chan struct{})}
	bad := &invalidationFailureAlgo{cause: moerr.NewInvalidStateNoCtx("invalidation panic")}
	require.NoError(t, searchForInvalidationTest(c, nil, "blocked", blocked, false))
	require.NoError(t, searchForInvalidationTest(c, nil, "bad", bad, false))

	hookObserved := make(chan int32, 1)
	lifecycleHooks.Lock()
	previous := lifecycleHooks.hooks
	lifecycleHooks.hooks = []func(bool){func(shutdown bool) {
		if shutdown {
			hookObserved <- blocked.destroys.Load()
		}
	}}
	lifecycleHooks.Unlock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	removed := make(chan struct{})
	go func() { c.RemoveWithReason("blocked", "ttl_expired"); close(removed) }()
	t.Cleanup(func() {
		release()
		select {
		case <-removed:
		case <-time.After(5 * time.Second):
			t.Error("blocked eviction did not finish")
		}
		waitForCacheShutdown(t, c)
		lifecycleHooks.Lock()
		lifecycleHooks.hooks = previous
		lifecycleHooks.Unlock()
	})
	select {
	case <-blocked.started:
	case <-time.After(5 * time.Second):
		t.Fatal("invalidation did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &observedDoneContext{Context: ctx, entered: make(chan struct{})}
	proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: observed}}
	waiter := make(chan error, 1)
	go func() { waiter <- searchForInvalidationTest(c, proc, "blocked", &recoveryAlgo{}, false) }()
	select {
	case <-observed.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("search did not wait for removal")
	}
	badRemoved := make(chan struct{})
	go func() { c.RemoveWithReason("bad", "generation_changed"); close(badRemoved) }()
	select {
	case <-badRemoved:
	case <-time.After(5 * time.Second):
		t.Fatal("failed invalidation did not finish")
	}
	select {
	case err := <-waiter:
		requireInvalidationFailure(t, err)
	case <-time.After(time.Second):
		t.Fatal("removed waiter did not observe cache shutdown")
	}
	release()
	select {
	case <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked eviction did not finish")
	}
	require.True(t, waitForCacheShutdown(t, c))
	select {
	case destroyed := <-hookObserved:
		require.EqualValues(t, 1, destroyed, "shutdown hooks must run after the eviction finishes")
	case <-time.After(time.Second):
		t.Fatal("shutdown hook did not run")
	}
}

func TestDetachedDestroyAttemptsCleanupAfterInvalidationPanic(t *testing.T) {
	a := &invalidationFailureAlgo{cause: moerr.NewInvalidStateNoCtx("invalidation panic"), panicDuringDestruction: true}
	entry := newVectorIndexSearch(a)
	entry.DestroyWithReason("ttl_expired")
	require.EqualValues(t, 1, a.invalidations.Load())
	require.EqualValues(t, 1, a.destroys.Load())
	result := entry.destroyResult.Load()
	require.NotNil(t, result)
	requireInvalidationFailure(t, result.err)
}

func TestShutdownWaitsForHousekeepingHook(t *testing.T) {
	c := NewVectorIndexCache()
	started, release := make(chan struct{}), make(chan struct{})
	var finished atomic.Bool
	observed := make(chan bool, 1)
	lifecycleHooks.Lock()
	previous := lifecycleHooks.hooks
	lifecycleHooks.hooks = []func(bool){func(shutdown bool) {
		if shutdown {
			observed <- finished.Load()
			return
		}
		close(started)
		<-release
		finished.Store(true)
	}}
	lifecycleHooks.Unlock()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		waitForCacheShutdown(t, c)
		lifecycleHooks.Lock()
		lifecycleHooks.hooks = previous
		lifecycleHooks.Unlock()
	})
	go c.HouseKeeping()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("housekeeping hook did not start")
	}
	go c.Destroy()
	require.Eventually(t, c.exited.Load, time.Second, time.Millisecond)
	unblock()
	require.True(t, waitForCacheShutdown(t, c))
	select {
	case afterHousekeeping := <-observed:
		require.True(t, afterHousekeeping)
	case <-time.After(time.Second):
		t.Fatal("shutdown hook did not run")
	}
}

type blockingDestroyAlgo struct {
	MockSearch
	started, release chan struct{}
}

func (a *blockingDestroyAlgo) Destroy() {
	close(a.started)
	<-a.release
}

func TestSearchAdmissionDoesNotWaitForDestroy(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "destroying", true: "writer queued"}[queued], func(t *testing.T) {
			a := &blockingDestroyAlgo{started: make(chan struct{}), release: make(chan struct{})}
			entry := newVectorIndexSearch(a)
			require.NoError(t, entry.Load(nil))
			require.NoError(t, entry.awaitReady(context.Background()))
			var releaseOnce, unlockOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(a.release) }) }
			unlock := func() {
				unlockOnce.Do(func() {
					if queued {
						entry.Mutex.RUnlock()
					}
				})
			}
			if queued {
				entry.Mutex.RLock()
			}
			done := make(chan struct{})
			go func() { entry.Destroy(); close(done) }()
			t.Cleanup(func() {
				unlock()
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("entry destruction did not finish")
				}
			})
			if queued {
				require.Eventually(t, entry.evicting.Load, time.Second, time.Millisecond)
			} else {
				select {
				case <-a.started:
				case <-time.After(5 * time.Second):
					t.Fatal("entry destruction did not start")
				}
			}
			result := make(chan error, 1)
			go func() {
				err := entry.lockForSearch()
				if err == nil {
					entry.Mutex.RUnlock()
				}
				result <- err
			}()
			select {
			case err := <-result:
				require.ErrorIs(t, err, errIndexDestroyed)
			case <-time.After(time.Second):
				t.Fatal("search admission waited for the destroy writer")
			}
			unlock()
			release()
		})
	}
}

func TestEntryLoadRunsOnce(t *testing.T) {
	a := &recoveryAlgo{}
	entry := newVectorIndexSearch(a)
	require.NoError(t, entry.Load(nil))
	require.ErrorContains(t, entry.Load(nil), "already initialized")
	require.EqualValues(t, 1, a.loads.Load())
	entry.Destroy()
}

func TestZeroValueCacheSearchAndShutdown(t *testing.T) {
	var c VectorIndexCache
	require.NoError(t, searchForInvalidationTest(&c, nil, "key", &recoveryAlgo{}, false))
	require.True(t, waitForCacheShutdown(t, &c))
	require.ErrorIs(t, searchForInvalidationTest(&c, nil, "key", &recoveryAlgo{}, false), errCacheClosed)
}

func TestCanceledLoadDoesNotStartSearch(t *testing.T) {
	for _, into := range []bool{false, true} {
		t.Run(map[bool]string{false: "Search", true: "SearchInto"}[into], func(t *testing.T) {
			c := NewVectorIndexCache()
			loading := &recoveryAlgo{started: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(loading.release) }) }
			t.Cleanup(func() { release(); waitForCacheShutdown(t, c) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			proc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: ctx}}
			owner := startCacheSearch(c, proc, loading, into)
			select {
			case <-loading.started:
			case <-time.After(5 * time.Second):
				t.Fatal("load did not start")
			}
			cancel()
			release()
			select {
			case err := <-owner:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("canceled load owner did not finish")
			}
			fresh := &recoveryAlgo{}
			require.NoError(t, <-startCacheSearch(c, nil, fresh, into))
			require.EqualValues(t, 1, loading.loads.Load())
			require.Zero(t, fresh.loads.Load(), "the successful load remains available to other queries")
		})
	}
}
