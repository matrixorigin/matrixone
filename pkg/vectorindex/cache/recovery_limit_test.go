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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func waitCacheSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestInvalidationRecoveryLimitAndShutdownDrain(t *testing.T) {
	c := NewVectorIndexCache()
	var releases []func()
	releaseLoad := func() {}
	var overflow *recoverableInvalidationAlgo
	hookObserved := make(chan int32, 1)
	lifecycleHooks.Lock()
	previousHooks := lifecycleHooks.hooks
	lifecycleHooks.hooks = []func(bool){func(shutdown bool) {
		if shutdown && overflow != nil {
			hookObserved <- overflow.destroys.Load()
		}
	}}
	lifecycleHooks.Unlock()
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
		releaseLoad()
		waitForCacheShutdown(t, c)
		lifecycleHooks.Lock()
		lifecycleHooks.hooks = previousHooks
		lifecycleHooks.Unlock()
	})

	type recovery struct {
		algo    *recoverableInvalidationAlgo
		entry   *VectorIndexSearch
		release func()
	}
	startRecovery := func(key string) recovery {
		t.Helper()
		algo := &recoverableInvalidationAlgo{
			invalidationFailureAlgo: invalidationFailureAlgo{cause: moerr.NewInvalidStateNoCtx("invalidation panic")},
			repairStarted:           make(chan struct{}),
			releaseRepair:           make(chan struct{}),
		}
		release := sync.OnceFunc(func() { close(algo.releaseRepair) })
		releases = append(releases, release)
		require.NoError(t, searchForInvalidationTest(c, nil, key, algo, false))
		value, ok := c.IndexMap.Load(key)
		require.True(t, ok)
		entry := value.(*VectorIndexSearch)
		c.RemoveWithReason(key, evictionGeneration)
		waitCacheSignal(t, algo.repairStarted, "recovery to start")
		return recovery{algo: algo, entry: entry, release: release}
	}

	var recoveries []recovery
	for i := 0; i < maxConcurrentInvalidationRecoveries; i++ {
		key := fmt.Sprintf("recover-%d", i)
		r := startRecovery(key)
		if i == 0 {
			// A second eviction of the same claimed entry must not start another recovery.
			c.RemoveWithReason(key, evictionGeneration)
			require.EqualValues(t, 1, r.algo.repairs.Load())
		}
		recoveries = append(recoveries, r)
	}
	require.NoError(t, searchForInvalidationTest(c, nil, "other", &recoveryAlgo{}, false))

	// A completed recovery releases its slot for another key.
	recoveries[0].release()
	waitCacheSignal(t, recoveries[0].entry.destroyed, "first recovery destruction")
	require.Eventually(t, func() bool {
		c.lifecycleMu.Lock()
		defer c.lifecycleMu.Unlock()
		return c.recoveries == maxConcurrentInvalidationRecoveries-1
	}, 5*time.Second, time.Millisecond)
	recoveries = append(recoveries, startRecovery("replacement"))
	require.False(t, c.exited.Load())

	overflow = &recoverableInvalidationAlgo{
		invalidationFailureAlgo: invalidationFailureAlgo{cause: moerr.NewInvalidStateNoCtx("overflow invalidation panic")},
		loadStarted:             make(chan struct{}),
		releaseLoad:             make(chan struct{}),
	}
	releaseLoad = sync.OnceFunc(func() { close(overflow.releaseLoad) })
	owner := make(chan error, 1)
	go func() { owner <- searchForInvalidationTest(c, nil, "overflow", overflow, false) }()
	waitCacheSignal(t, overflow.loadStarted, "overflow load")
	value, ok := c.IndexMap.Load("overflow")
	require.True(t, ok)
	entry := value.(*VectorIndexSearch)
	follower := make(chan error, 1)
	go func() { follower <- searchForInvalidationTest(c, nil, "overflow", &recoveryAlgo{}, true) }()
	require.Eventually(t, func() bool { return entry.loadWaiters.Load() == 1 }, 5*time.Second, time.Millisecond)

	removed := make(chan struct{})
	go func() { c.RemoveWithReason("overflow", evictionGeneration); close(removed) }()
	waitCacheSignal(t, removed, "overflow eviction to hand off to shutdown")
	waitCacheSignal(t, c.done, "cache shutdown")
	failure := entry.invalidationResult.Load()
	require.NotNil(t, failure)
	require.ErrorContains(t, failure.err, "overflow invalidation panic")
	requireOverflowFailure := func(err error) {
		t.Helper()
		requireInvalidationFailure(t, err)
		require.Same(t, failure.err, err)
	}
	select {
	case err := <-follower:
		requireOverflowFailure(err)
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not receive the invalidation error")
	}
	requireOverflowFailure(searchForInvalidationTest(c, nil, "other", &recoveryAlgo{}, false))
	require.Zero(t, overflow.repairs.Load(), "overflow must not start another recovery")
	_, ok = c.IndexMap.Load("overflow")
	require.True(t, ok, "shutdown must retain the claim while registered recoveries run")

	shutdown := make(chan struct{})
	go func() { c.Destroy(); close(shutdown) }()
	for _, release := range releases {
		release()
	}
	require.Eventually(t, func() bool {
		_, ok := c.IndexMap.Load("overflow")
		return !ok
	}, 5*time.Second, time.Millisecond, "shutdown did not take the reserved entry")
	select {
	case <-shutdown:
		t.Fatal("shutdown returned while the overflow load was still active")
	default:
	}
	releaseLoad()
	select {
	case err := <-owner:
		requireOverflowFailure(err)
	case <-time.After(5 * time.Second):
		t.Fatal("overflow load owner did not return")
	}
	waitCacheSignal(t, shutdown, "shutdown to finish")
	require.EqualValues(t, 1, overflow.destroys.Load())
	select {
	case destroyed := <-hookObserved:
		require.EqualValues(t, 1, destroyed, "shutdown hooks must run after the reserved entry is destroyed")
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hook did not run")
	}
	for _, r := range recoveries {
		require.EqualValues(t, 1, r.algo.destroys.Load())
	}
}

func TestInvalidationRecoveryHandoffAfterShutdown(t *testing.T) {
	c := NewVectorIndexCache()
	bad := &recoverableInvalidationAlgo{
		invalidationFailureAlgo: invalidationFailureAlgo{
			cause:   moerr.NewInvalidStateNoCtx("invalidation panic"),
			started: make(chan struct{}),
			release: make(chan struct{}),
		},
		loadStarted: make(chan struct{}),
		releaseLoad: make(chan struct{}),
	}
	releaseCallback := sync.OnceFunc(func() { close(bad.release) })
	releaseLoad := sync.OnceFunc(func() { close(bad.releaseLoad) })
	t.Cleanup(func() {
		releaseCallback()
		releaseLoad()
		waitForCacheShutdown(t, c)
	})
	owner := make(chan error, 1)
	go func() { owner <- searchForInvalidationTest(c, nil, "bad", bad, false) }()
	waitCacheSignal(t, bad.loadStarted, "load")
	removed := make(chan struct{})
	go func() { c.RemoveWithReason("bad", evictionGeneration); close(removed) }()
	waitCacheSignal(t, bad.started, "invalidation callback")
	shutdown := make(chan struct{})
	go func() { c.Destroy(); close(shutdown) }()
	waitCacheSignal(t, c.done, "cache shutdown")
	releaseCallback()
	waitCacheSignal(t, removed, "failed invalidation handoff")
	require.Zero(t, bad.repairs.Load(), "shutdown must reject a late recovery")
	select {
	case <-shutdown:
		t.Fatal("shutdown returned while the old load was still active")
	default:
	}
	releaseLoad()
	select {
	case err := <-owner:
		requireInvalidationFailure(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("load owner did not receive the invalidation error")
	}
	waitCacheSignal(t, shutdown, "shutdown to finish")
	require.EqualValues(t, 1, bad.destroys.Load())
}
