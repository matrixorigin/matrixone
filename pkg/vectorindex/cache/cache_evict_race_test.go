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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingDestroySearch is a cache entry whose teardown blocks until released, so a test can hold an
// eviction "in flight" (entry already removed from the map, Destroy not yet finished) and observe
// what a concurrent evictor does in that window.
type blockingDestroySearch struct {
	MockSearch
	started chan struct{} // closed when Destroy begins
	release chan struct{} // Destroy returns only after this is closed
}

func (m *blockingDestroySearch) Destroy() {
	close(m.started)
	<-m.release
}

// TestEvictKeyWaitsForInFlightEviction proves EvictKey does not report a premature completion when
// it loses the claim to a concurrent evictor (#28985). Evictor A wins the key and parks inside
// Destroy (entry already removed from the map, native handle still alive). Evictor B, finding the
// entry gone, must WAIT for A's teardown to finish before returning 0 -- not return immediately
// while the old resource is still live.
func TestEvictKeyWaitsForInFlightEviction(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(func() { c.Destroy() })

	const key = "victim"
	mock := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	c.IndexMap.Store(key, newVectorIndexSearch(mock))

	// A wins the eviction and blocks inside Destroy.
	aReturned := make(chan int64, 1)
	go func() { aReturned <- c.EvictKey(context.Background(), key) }()
	<-mock.started // A has removed the entry and is now inside Destroy (in-flight).

	// B loses the claim (entry gone) and must block until A's teardown completes.
	bReturned := make(chan int64, 1)
	go func() { bReturned <- c.EvictKey(context.Background(), key) }()

	// B must NOT report completion while A's Destroy is still running.
	select {
	case <-bReturned:
		t.Fatal("EvictKey returned before the in-flight eviction finished (premature success)")
	case <-aReturned:
		t.Fatal("A's Destroy returned though it was still blocked")
	case <-time.After(200 * time.Millisecond):
		// still blocked, as required.
	}

	// Let A's teardown finish; now both calls settle.
	close(mock.release)
	require.Equal(t, int64(1), <-aReturned, "A performed the eviction")
	select {
	case got := <-bReturned:
		require.Equal(t, int64(0), got, "B removed nothing, but only returned after the teardown settled")
	case <-time.After(5 * time.Second):
		t.Fatal("EvictKey blocked forever after the in-flight eviction completed")
	}

	// The key is fully gone and no in-flight eviction remains registered.
	require.Zero(t, c.CountKey(key))
	require.False(t, c.hasInFlightEviction(key), "the in-flight eviction registry entry must be cleared")
}

// hasInFlightEviction reports whether any in-flight eviction is still registered for the cache key.
// evictInFlight is keyed by the per-eviction done channel (value = cache key), so this ranges rather
// than a single Load.
func (c *VectorIndexCache) hasInFlightEviction(key string) bool {
	found := false
	c.evictInFlight.Range(func(_, v any) bool {
		if ks, ok := v.(string); ok && ks == key {
			found = true
			return false
		}
		return true
	})
	return found
}

// TestEvictKeyWaitsForOlderGenerationUnderSameKey proves the #28985 P2 fix: when two generations of
// the SAME cache key are tearing down at once, the completion signal of the older generation must not
// be overwritten by the newer one. Generation A parks in Destroy (removed from the map, still alive);
// a reload B lands under the same key, is evicted, and parks too; B finishes first. A later EvictKey
// must still WAIT for A -- a key->chan registry lost A's channel when B stored over it, letting
// EvictKey return 0 while A's resources were alive.
func TestEvictKeyWaitsForOlderGenerationUnderSameKey(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(func() { c.Destroy() })

	const key = "victim"

	// Generation A: evicted first, parks inside Destroy (removed from map, done_A in-flight).
	genA := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	c.IndexMap.Store(key, newVectorIndexSearch(genA))
	aReturned := make(chan int64, 1)
	go func() { aReturned <- c.EvictKey(context.Background(), key) }()
	<-genA.started

	// Generation B: a reload lands under the SAME key, is evicted, and parks too (done_B in-flight
	// alongside done_A -- the state a key->chan registry cannot represent).
	genB := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	c.IndexMap.Store(key, newVectorIndexSearch(genB))
	bReturned := make(chan int64, 1)
	go func() { bReturned <- c.EvictKey(context.Background(), key) }()
	<-genB.started

	// B's own generation finishes tearing down first. This must NOT clear A's in-flight signal, and --
	// critically -- B's EvictCall removed the current generation (returns 1) yet must STILL wait for the
	// prior generation A before returning: the success path shares the same completion barrier as the
	// failure path (the P2 fix). Returning 1 here while A is alive would be a premature "gone".
	close(genB.release)

	// A third EvictKey finds no map entry, but A is still tearing down: it too MUST wait, not report 0.
	cReturned := make(chan int64, 1)
	go func() { cReturned <- c.EvictKey(context.Background(), key) }()

	// Neither the success-path call (B, removed 1) nor the no-occupant call (C, removed 0) may return
	// while generation A is still tearing down under the same key.
	select {
	case <-bReturned:
		t.Fatal("EvictKey returned after removing generation B while prior generation A was still tearing down (success path skipped the completion barrier, #28985)")
	case <-cReturned:
		t.Fatal("EvictKey returned before generation A's teardown finished (older-generation signal overwritten, #28985)")
	case <-aReturned:
		t.Fatal("A's Destroy returned though it was still blocked")
	case <-time.After(200 * time.Millisecond):
		// correctly still blocked on A
	}

	// Release A; A's own EvictKey, B's success-path call, and the waiting third call all settle.
	close(genA.release)
	require.Equal(t, int64(1), <-aReturned, "A performed its eviction")
	require.Equal(t, int64(1), <-bReturned, "B removed generation B, returning only after A settled")
	select {
	case got := <-cReturned:
		require.Equal(t, int64(0), got, "third call removed nothing, returning only after A settled")
	case <-time.After(5 * time.Second):
		t.Fatal("EvictKey blocked forever after generation A completed")
	}

	require.Zero(t, c.CountKey(key))
	require.False(t, c.hasInFlightEviction(key), "no in-flight eviction may remain for the key")
}

// TestEvictKeyWaitsForBlockedFailedLoadCleanup proves the failed-load teardown path honors the
// same completion barrier as ordinary eviction (#28985 P2). discardFailedLoad removes the exact
// failed entry and blocks inside destroyFailedLoad's Destroy; a concurrent EvictKey that finds no
// occupant and no map entry must still WAIT for that in-flight teardown -- before the fix it drained
// an empty evictInFlight and returned a premature "gone" while the native handle was still alive.
func TestEvictKeyWaitsForBlockedFailedLoadCleanup(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(func() { c.Destroy() })

	const key = "victim"
	mock := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	entry := newVectorIndexSearch(mock)
	c.IndexMap.Store(key, entry)

	// A failed load discards this exact entry: it removes it from the map and parks inside
	// destroyFailedLoad's Destroy (native handle alive), registered in evictInFlight.
	discardDone := make(chan struct{})
	go func() { defer close(discardDone); c.discardFailedLoad(key, entry) }()
	<-mock.started // discardFailedLoad removed the entry and is now inside Destroy.

	// EvictKey finds no occupant and no map entry, but the failed-load teardown is in flight; it
	// must block on it, not report a premature completion.
	evictDone := make(chan int64, 1)
	go func() { evictDone <- c.EvictKey(context.Background(), key) }()
	select {
	case <-evictDone:
		t.Fatal("EvictKey returned before the blocked failed-load cleanup finished (barrier bypassed, #28985)")
	case <-discardDone:
		t.Fatal("discardFailedLoad returned though its Destroy was still blocked")
	case <-time.After(200 * time.Millisecond):
		// correctly still blocked on the in-flight failed-load teardown
	}

	// Let the failed-load teardown finish; EvictKey now settles, having removed nothing.
	close(mock.release)
	select {
	case got := <-evictDone:
		require.Equal(t, int64(0), got, "EvictKey removed nothing, returning only after the teardown settled")
	case <-time.After(5 * time.Second):
		t.Fatal("EvictKey blocked forever after the failed-load cleanup completed")
	}
	<-discardDone

	require.Zero(t, c.CountKey(key))
	require.False(t, c.hasInFlightEviction(key), "the in-flight failed-load registration must be cleared")
}

// TestEvictKeyFollowerCancelReturnsPromptly proves the Rev-2 cancellation contract (#28985): a
// FOLLOWER EvictKey parked behind another teardown honors its caller's context. Evictor A wins the
// key and parks inside Destroy (entry removed, teardown in flight). Follower B finds the entry gone
// and waits on A's in-flight completion channel; cancelling B's context must let B return PROMPTLY
// without waiting for A, and WITHOUT disturbing A -- A's cleanup completes on its own afterward.
// Before the fix EvictKey drained the pending channels with an unconditional receive, so B could not
// honor its deadline and the server handler stayed blocked until A finished.
func TestEvictKeyFollowerCancelReturnsPromptly(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(func() { c.Destroy() })

	const key = "victim"
	mock := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	c.IndexMap.Store(key, newVectorIndexSearch(mock))

	// A wins the eviction and blocks inside Destroy (entry removed, done_A registered in-flight).
	aReturned := make(chan int64, 1)
	go func() { aReturned <- c.EvictKey(context.Background(), key) }()
	<-mock.started

	// B is a follower: the map entry is gone, so it waits on A's in-flight channel. Its context is
	// cancellable.
	ctx, cancel := context.WithCancel(context.Background())
	bReturned := make(chan int64, 1)
	go func() { bReturned <- c.EvictKey(ctx, key) }()

	// B is blocked behind A (A is still parked in Destroy).
	select {
	case <-bReturned:
		t.Fatal("follower returned before its context was cancelled -- it was not actually waiting")
	case <-time.After(200 * time.Millisecond):
		// correctly blocked on A's in-flight teardown
	}

	// Cancel B; it must return promptly even though A is still tearing down.
	cancel()
	select {
	case got := <-bReturned:
		require.Equal(t, int64(0), got, "cancelled follower removed nothing")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled follower did not return -- the follower wait is not cancellable")
	}

	// B's cancellation must NOT have interrupted the cleanup owner: A is still parked, not returned.
	select {
	case <-aReturned:
		t.Fatal("owner's teardown returned though it was still blocked -- cancellation interrupted the cleanup owner")
	case <-time.After(100 * time.Millisecond):
		// A correctly still owns and is completing its own teardown
	}

	// Release A; its teardown completes and the key is fully gone.
	close(mock.release)
	require.Equal(t, int64(1), <-aReturned, "owner performed the eviction")
	require.Zero(t, c.CountKey(key))
	require.False(t, c.hasInFlightEviction(key), "the in-flight eviction registry entry must be cleared")
}
