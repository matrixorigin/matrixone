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
	"sync"
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
func TestSetStaleCheckIntervalDuringServeIsNotLost(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewVectorIndexCache()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.serve() }()
		go func() { defer wg.Done(); c.SetStaleCheckInterval(2 * time.Second) }()
		wg.Wait()
		require.Equal(t, 2*time.Second, c.staleTickerInterval())
		c.Destroy()
	}
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
