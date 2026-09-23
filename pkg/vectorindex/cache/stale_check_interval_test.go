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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A positive override shortens the base ticker; a non-positive value keeps the default. The
// every-Nth-tick sweep multiplier (stalenessCheckEveryNTicks) is unchanged either way -- the
// override never switches the sweep to run every tick.
func TestStaleCheckIntervalOverride(t *testing.T) {
	c := NewVectorIndexCache()

	// Default: no override.
	require.Equal(t, c.TickerInterval, c.staleTickerInterval())

	// Override to a short interval: the base ticker follows it.
	c.SetStaleCheckInterval(2 * time.Second)
	require.Equal(t, 2*time.Second, c.staleTickerInterval())

	// Restore the default with 0.
	c.SetStaleCheckInterval(0)
	require.Equal(t, c.TickerInterval, c.staleTickerInterval())

	// A negative value is normalized to the default, never a negative ticker.
	c.SetStaleCheckInterval(-5 * time.Second)
	require.Zero(t, c.staleCheckIntervalNs.Load())
	require.Equal(t, c.TickerInterval, c.staleTickerInterval())

	// A positive value below the floor is clamped up to MinStaleCheckInterval, never a busy loop.
	c.SetStaleCheckInterval(time.Millisecond)
	require.Equal(t, MinStaleCheckInterval, c.staleTickerInterval())
	c.SetStaleCheckInterval(MinStaleCheckInterval) // the floor itself is kept as-is
	require.Equal(t, MinStaleCheckInterval, c.staleTickerInterval())
}

// SetStaleCheckInterval before serve() only stores the value (no started ticker to reset); the
// stored value is what serve() would arm the ticker from.
func TestStaleCheckIntervalBeforeServeIsSafe(t *testing.T) {
	c := NewVectorIndexCache()
	require.False(t, c.started.Load())
	c.SetStaleCheckInterval(3 * time.Second) // must not panic on a nil ticker
	require.Equal(t, 3*time.Second, c.staleTickerInterval())
}

// With a running ticker, SetStaleCheckInterval re-arms it; after Destroy (exited) it only stores
// the value and does not touch the stopped ticker.
func TestSetStaleCheckIntervalResetsRunningTicker(t *testing.T) {
	c := NewVectorIndexCache()
	c.ticker = time.NewTicker(time.Hour)
	t.Cleanup(c.ticker.Stop)
	c.started.Store(true) // simulate a started serve()

	c.SetStaleCheckInterval(2 * time.Second) // takes the Reset branch
	require.Equal(t, 2*time.Second, c.staleTickerInterval())

	c.exited.Store(true) // after Destroy: value still stored, ticker not reset
	c.SetStaleCheckInterval(3 * time.Second)
	require.Equal(t, 3*time.Second, c.staleTickerInterval())
}

// The startup race: an override landing while serve() is arming the ticker must not be lost.
// serveMu serializes serve()'s ticker-create+started-publish with SetStaleCheckInterval's
// check+reset, so after both run the override governs (either serve() read it when creating the
// ticker, or SetStaleCheckInterval Reset the live ticker). Run under -race to prove no data race
// or deadlock on serveMu/ticker/started across the two goroutines.
// TestSetStaleCheckIntervalDuringServeIsNotLost proves an override reaches the LIVE sweep ticker
// (effectiveTickerNs), not merely the stored value, across the serve()/SetStaleCheckInterval race.
// The original bug stored the new interval yet left the running ticker on the old cadence, so the
// effectiveTickerNs assertion is what catches it. Both orderings are forced deterministically
// (controlled synchronization) rather than only hoped for over a concurrent loop, and every case
// tears the cache down unconditionally so a failed require never leaks a serve goroutine.
func TestSetStaleCheckIntervalDuringServeIsNotLost(t *testing.T) {
	const want = int64(2 * time.Second)

	// Ordering A: the override lands BEFORE startup (started not yet published, so its own Reset is
	// skipped) -- serve() must build the live ticker from the stored override.
	t.Run("override before serve", func(t *testing.T) {
		c := NewVectorIndexCache()
		t.Cleanup(c.Destroy)
		c.SetStaleCheckInterval(2 * time.Second)
		c.serve()
		require.Equal(t, want, c.effectiveTickerNs.Load(),
			"serve() must create the live ticker from an override that landed before startup")
	})

	// Ordering B: the override lands AFTER startup -- SetStaleCheckInterval must Reset the already
	// live ticker (the path the original bug skipped).
	t.Run("override after serve", func(t *testing.T) {
		c := NewVectorIndexCache()
		t.Cleanup(c.Destroy)
		c.serve()
		c.SetStaleCheckInterval(2 * time.Second)
		require.Equal(t, want, c.effectiveTickerNs.Load(),
			"SetStaleCheckInterval must Reset the already-running ticker")
	})

	// The actual overlap, many times. serveMu serializes the two, so whichever wins, the override
	// reaches the live ticker. Read effectiveTickerNs and Destroy BEFORE asserting, so a failure
	// still reclaims the serve goroutine.
	t.Run("concurrent", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			c := NewVectorIndexCache()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); c.serve() }()
			go func() { defer wg.Done(); c.SetStaleCheckInterval(2 * time.Second) }()
			wg.Wait()
			got := c.effectiveTickerNs.Load()
			c.Destroy()
			require.Equal(t, want, got, "the override must reach the live ticker regardless of interleaving")
		}
	})

	// Forced interleave: reproduce the exact original bug ordering deterministically rather than
	// relying on the scheduler -- serve() reads the (default) interval, THEN the override lands
	// (setter-before-started, so its own Reset is skipped), THEN serve() would publish a ticker built
	// from the OLD interval. serveStartBarrier fires inside serve() in exactly that window. The fix
	// must still land the override on the live ticker via the post-unlock Reset (serialized by
	// serveMu). The barrier is reset unconditionally and the setter goroutine is joined.
	t.Run("forced interleave: override between interval read and ticker publish", func(t *testing.T) {
		c := NewVectorIndexCache()
		t.Cleanup(c.Destroy)
		t.Cleanup(func() { serveStartBarrier = nil })

		setterDone := make(chan struct{})
		serveStartBarrier = func() {
			serveStartBarrier = nil // one-shot: only the first serve() startup
			go func() {
				defer close(setterDone)
				c.SetStaleCheckInterval(2 * time.Second) // Store lands, then blocks on serveMu (serve holds it)
			}()
			// Return only after the override is stored -- i.e. it landed BEFORE serve publishes
			// started. serve then builds the ticker from the OLD interval it already read.
			for c.staleCheckIntervalNs.Load() != int64(2*time.Second) {
				runtime.Gosched()
			}
		}
		c.serve()    // reads old -> barrier -> publishes the OLD ticker -> unlocks serveMu
		<-setterDone // the override's Reset ran after unlock (join)
		require.Equal(t, want, c.effectiveTickerNs.Load(),
			"an override that landed before started-publication must still reach the live ticker")
	})
}

// The rigorous, placement-free proof of the mechanism the configurable interval accelerates: the
// periodic sweep evicts a stale entry (so the next load is fresh) and leaves a fresh one alone.
// A multi-CN BVT cannot prove this deterministically (query placement is non-deterministic), so it
// is proven here in one process with a controlled StaleChecker. TestStaleCheckIntervalOverride
// proves the *cadence* is configurable; this proves *what the cadence drives*.
func TestStaleSweepEvictsStaleEntryAndKeepsFresh(t *testing.T) {
	c := NewVectorIndexCache()

	// A loaded entry whose generation has advanced (IsStale -> true).
	stale := newVectorIndexSearch(&countingSearch{stale: true})
	stale.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("stale", stale)

	// A loaded entry that is current (IsStale -> false).
	fresh := newVectorIndexSearch(&countingSearch{stale: false})
	fresh.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("fresh", fresh)

	// checkStale marks only the stale one.
	c.checkStale()
	require.True(t, stale.stale.Load(), "checkStale must mark a stale entry")
	require.False(t, fresh.stale.Load(), "checkStale must not mark a current entry")

	// HouseKeeping force-evicts the marked entry (regardless of warm/busy) and keeps the fresh one;
	// the next Search on the evicted key would reload a current generation.
	c.HouseKeeping()
	_, staleStillCached := c.IndexMap.Load("stale")
	_, freshStillCached := c.IndexMap.Load("fresh")
	require.False(t, staleStillCached, "the stale entry must be evicted by the sweep")
	require.True(t, freshStillCached, "the current entry must survive the sweep")
}

// TestNonConsumerCNStaleEntryClearedOnlyBySweep pins the #28985 cross-CN invariant the COPY ALTER
// BVT cannot prove deterministically (consumer/query placement is non-deterministic in a multi-CN
// cluster, and a single-process launch collapses every CN onto one shared cache). Two independent
// VectorIndexCache instances model two CNs: RemoveIdle is pure-local to the CDC-consumer CN, so a
// stale warm entry on a NON-consumer CN survives the consumer's flush and is removed ONLY by that
// CN's periodic IsStale sweep -- which is what drives the cluster-summed cached count to zero.
// TestPeriodicSweepTickerEvictsStaleEntry proves the AUTOMATIC ticker dispatch, not just that
// checkStale works when called by hand: with #29227 an ordinary CDC flush no longer evicts the warm
// cache (RemoveIdle is gone), so on EVERY CN a warm entry a CDC append made stale is removed solely
// by that CN's own periodic sweep ticker. A real serve() ticker (no manual checkStale/HouseKeeping
// call) must evict the stale entry on its own and leave a current one resident.
//
// (True separate-CN placement needs multiple OS processes -- a single-process launch shares one
// global cache -- so the per-CN aspect is modeled as one cache's own automatic ticker; the ticker
// DISPATCH and stale/fresh selectivity are what this proves deterministically.)
func TestPeriodicSweepTickerEvictsStaleEntry(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(c.Destroy)

	// Drive the base sweep ticker at a millisecond cadence so the every-Nth-tick automatic sweep
	// fires in tens of ms, not the ~10m default. Set the field directly to bypass the 1s operator
	// floor (MinStaleCheckInterval) -- this is the internal cadence knob the ticker reads.
	c.staleCheckIntervalNs.Store(int64(2 * time.Millisecond))
	c.serve() // starts the housekeeping + every-Nth-tick sweep goroutine

	stale := newVectorIndexSearch(&countingSearch{stale: true})
	stale.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("stale", stale)
	fresh := newVectorIndexSearch(&countingSearch{stale: false})
	fresh.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("fresh", fresh)

	// The ticker-driven sweep (checkStale marks -> HouseKeeping evicts) must remove the stale entry
	// with NO manual call, and must not touch the current one.
	require.Eventually(t, func() bool { return c.CountKey("stale") == 0 }, 5*time.Second, 5*time.Millisecond,
		"the periodic sweep ticker must evict a stale entry on its own")
	require.Equal(t, int64(1), c.CountKey("fresh"),
		"the automatic sweep must not evict a current entry")
}

// CountKey reports per-key cache occupancy (0 when absent/evicted), the signal the
// GetVectorIndexCacheInfo mo_ctl sums across CNs to observe eviction deterministically.
func TestCountKey(t *testing.T) {
	c := NewVectorIndexCache()
	require.False(t, c.started.Load(), "CountKey must work before serve() is lazily started")
	require.Equal(t, int64(0), c.CountKey("idx"))

	e := newVectorIndexSearch(&countingSearch{})
	e.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("idx", e)
	require.Equal(t, int64(1), c.CountKey("idx"))
	require.Equal(t, int64(1), c.CountKey(""), "empty key counts all entries")
	require.Equal(t, int64(0), c.CountKey("other"))

	// The ivf family keys by "<index-table>:<version>": the EXACT key matches; the bare index-table
	// name does NOT (the exact-key contract, so what GetVectorIndexCacheInfo reports is what
	// EvictVectorIndexCache would drop).
	ivf := newVectorIndexSearch(&countingSearch{})
	ivf.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("tbl:7", ivf)
	require.Equal(t, int64(1), c.CountKey("tbl:7"), "ivf exact key matches")
	require.Equal(t, int64(0), c.CountKey("tbl"), "bare table name must not match an ivf :version key")
	require.Equal(t, int64(2), c.CountKey(""), "empty key counts all entries")

	c.IndexMap.Delete("idx") // eviction -> back to 0
	require.Equal(t, int64(0), c.CountKey("idx"))
}

// EvictKey drops a key's entries and reports how many; empty key is a refused no-op.
func TestEvictKey(t *testing.T) {
	c := NewVectorIndexCache()
	require.False(t, c.started.Load(), "EvictKey must work before serve() is lazily started")
	require.Equal(t, int64(0), c.EvictKey("idx"), "evicting an absent key removes nothing")

	e := newVectorIndexSearch(&countingSearch{})
	e.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("idx", e)
	require.Equal(t, int64(1), c.EvictKey("idx"), "evicts the cached entry and reports 1")
	require.Equal(t, int64(0), c.CountKey("idx"), "entry is gone after evict")

	// Empty key must not flush everything.
	other := newVectorIndexSearch(&countingSearch{})
	other.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("keep", other)
	require.Equal(t, int64(0), c.EvictKey(""), "empty key is a no-op")
	require.Equal(t, int64(1), c.CountKey("keep"), "empty-key evict must not drop other entries")

	// ivf family: only the EXACT "<index-table>:<version>" key evicts; the bare table name is a
	// no-op that leaves the entry resident.
	ivf := newVectorIndexSearch(&countingSearch{})
	ivf.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("tbl:7", ivf)
	require.Equal(t, int64(0), c.EvictKey("tbl"), "bare table name must not evict an ivf :version key")
	require.Equal(t, int64(1), c.CountKey("tbl:7"), "the ivf entry survives a bare-name evict")
	require.Equal(t, int64(1), c.EvictKey("tbl:7"), "exact ivf key evicts")
	require.Equal(t, int64(0), c.CountKey("tbl:7"), "entry is gone after exact evict")
}

// EvictKey must report only the removal THIS call performed. Before the fix it returned a pre-read
// occupancy count, so N concurrent callers could all read 1 and all report evicted=1 while only one
// owned the removal (and a losing caller reported success though it removed nothing). Exactly one
// caller wins the claim and reports 1; the rest report 0, and the entry is gone afterward.
func TestEvictKeyConcurrentReportsSingleOwner(t *testing.T) {
	c := NewVectorIndexCache()
	e := newVectorIndexSearch(&countingSearch{})
	e.Status.Store(STATUS_LOADED)
	c.IndexMap.Store("idx", e)

	const n = 16
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	var total atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			total.Add(c.EvictKey("idx"))
		}()
	}
	start.Done()
	wg.Wait()

	require.Equal(t, int64(1), total.Load(), "exactly one concurrent caller may report the eviction")
	require.Equal(t, int64(0), c.CountKey("idx"), "the entry is gone after concurrent evict")
}

// Keys lists the exact cache keys, sorted -- the signal GetVectorIndexCacheKeys aggregates across
// CNs so an operator can discover the exact key (e.g. an ivf "<table>:<version>") to evict.
func TestKeys(t *testing.T) {
	c := NewVectorIndexCache()
	require.Empty(t, c.Keys(), "an empty cache lists no keys")

	for _, k := range []string{"ft2_tbl", "ivf_tbl:9", "ivf_tbl:7"} {
		e := newVectorIndexSearch(&countingSearch{})
		e.Status.Store(STATUS_LOADED)
		c.IndexMap.Store(k, e)
	}
	require.Equal(t, []string{"ft2_tbl", "ivf_tbl:7", "ivf_tbl:9"}, c.Keys(), "keys are returned sorted")
}
