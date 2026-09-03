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

// Bound on resident named-snapshot generations (#27927), and the staleness exemption that
// makes the bound worth having.
//
// Each distinct snapshot TS is its own cache key, so without a bound one client can admit an
// unbounded number of FULL index instances by querying N snapshots of one large index inside
// a single TTL window. The bound REFUSES rather than evicts -- see MaxHistoricalIndexes for
// why eviction livelocks -- so the properties to pin are: the (N+1)th load is refused, the
// refusal does not disturb anything already resident, same-TS queries still share one load,
// and current-generation entries are never bounded or displaced.

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

const boundIdxTable = "__mo_index_secondary_bound_test"

// countingSearch records how many times it was actually LOADED, which is what distinguishes
// "shared one entry" from "loaded twice".
type countingSearch struct {
	loads atomic.Int64
	stale bool
}

func (m *countingSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{2.0}, nil
}
func (m *countingSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (m *countingSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}
func (m *countingSearch) Load(*sqlexec.SqlProcess) error { m.loads.Add(1); return nil }
func (m *countingSearch) Destroy()                       {}

// IsStale makes this a StaleChecker; the sweep must consult it for a current-generation entry
// and skip it for a snapshot one.
func (m *countingSearch) IsStale() (bool, error) { return m.stale, nil }

func snapshotTS(physical int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: physical, LogicalTime: 1}
}

// newBoundCache returns a cache with the limit set for the test and every entry cleaned up
// afterwards, so the process-wide Cache singleton is never touched.
func newBoundCache(t *testing.T, limit int) *VectorIndexCache {
	t.Helper()
	c := NewVectorIndexCache()
	orig := MaxHistoricalIndexes
	MaxHistoricalIndexes = limit
	t.Cleanup(func() {
		MaxHistoricalIndexes = orig
		c.IndexMap.Range(func(key, _ any) bool {
			c.IndexMap.Delete(key)
			return true
		})
	})
	return c
}

func searchAt(c *VectorIndexCache, key string, algo VectorIndexSearchIf) error {
	_, _, err := c.Search(nil, key, algo, nil, vectorindex.RuntimeConfig{})
	return err
}

// The (N+1)th distinct snapshot generation is refused, and the refusal leaves every resident
// entry -- including the current-generation one -- untouched. Before the bound, the (N+1)th
// simply loaded another full index instance.
func TestSnapshotBoundRefusesBeyondLimit(t *testing.T) {
	c := newBoundCache(t, 2)

	// The current generation is never bounded: it is the shared warm entry every ordinary
	// query depends on.
	require.NoError(t, searchAt(c, boundIdxTable, &countingSearch{}))

	first := SnapshotKey(boundIdxTable, snapshotTS(100))
	second := SnapshotKey(boundIdxTable, snapshotTS(200))
	require.NoError(t, searchAt(c, first, &countingSearch{}))
	require.NoError(t, searchAt(c, second, &countingSearch{}))

	third := &countingSearch{}
	err := searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(300)), third)
	require.Error(t, err, "the third snapshot generation must be refused at limit 2")
	require.Contains(t, err.Error(), "too many named-snapshot index generations")
	require.EqualValues(t, 0, third.loads.Load(), "a refused load must not pay for the index")

	// Refusal is not eviction: nothing resident was disturbed.
	for _, k := range []string{boundIdxTable, first, second} {
		_, ok := c.IndexMap.Load(k)
		require.True(t, ok, "refusing a new generation must not evict %q", k)
	}
	// And the refused key left no half-built entry behind.
	_, ok := c.IndexMap.Load(SnapshotKey(boundIdxTable, snapshotTS(300)))
	require.False(t, ok, "a refused load must not leave its key in the map")
}

// A current-generation load is never refused, however many snapshots are resident: bounding
// it would break every ordinary query on the index.
func TestSnapshotBoundNeverRefusesCurrentGeneration(t *testing.T) {
	c := newBoundCache(t, 1)
	require.NoError(t, searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(100)), &countingSearch{}))

	// At limit for snapshots, yet the bare key still admits.
	require.Error(t, searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(200)), &countingSearch{}))
	require.NoError(t, searchAt(c, boundIdxTable, &countingSearch{}))
	require.NoError(t, searchAt(c, "__mo_index_secondary_other_table", &countingSearch{}))
}

// Repeated queries on the SAME snapshot share one entry and one load, so they consume one
// unit of the budget -- the bound counts generations, not queries.
func TestSnapshotBoundSameTSSharesOneLoad(t *testing.T) {
	c := newBoundCache(t, 1)
	key := SnapshotKey(boundIdxTable, snapshotTS(100))

	shared := &countingSearch{}
	for i := 0; i < 5; i++ {
		require.NoError(t, searchAt(c, key, shared))
	}
	require.EqualValues(t, 1, shared.loads.Load(), "same-TS queries must share one load")

	// Concurrently too: LoadOrStore single-flights, so the budget is still 1.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = searchAt(c, key, shared) }()
	}
	wg.Wait()
	require.EqualValues(t, 1, shared.loads.Load())
	require.NoError(t, searchAt(c, key, shared), "a resident generation is never refused")
}

// A refusal is transient: once a resident generation ages out, the previously refused load
// succeeds. This is what makes "refuse" an acceptable answer instead of a dead end.
func TestSnapshotBoundRefusalIsTransient(t *testing.T) {
	c := newBoundCache(t, 1)
	first := SnapshotKey(boundIdxTable, snapshotTS(100))
	require.NoError(t, searchAt(c, first, &countingSearch{}))

	second := SnapshotKey(boundIdxTable, snapshotTS(200))
	require.Error(t, searchAt(c, second, &countingSearch{}))

	// Age the incumbent out exactly as HouseKeeping's TTL sweep would.
	value, ok := c.IndexMap.Load(first)
	require.True(t, ok)
	value.(*VectorIndexSearch).ExpireAt.Store(1)
	c.HouseKeeping()
	_, ok = c.IndexMap.Load(first)
	require.False(t, ok, "the expired generation must be reclaimed")

	require.NoError(t, searchAt(c, second, &countingSearch{}), "the refused load must now succeed")
}

// A named-snapshot generation is IMMUTABLE, so the cross-CN freshness sweep must skip it. It
// reports stale whenever the index has moved on from the snapshot -- and reports stale on a
// query error, which is what a DROPped index produces -- so without the exemption a snapshot
// entry is evicted on every sweep and no bound can keep one resident.
func TestStaleSweepSkipsSnapshotGenerations(t *testing.T) {
	c := newBoundCache(t, 4)

	current := &countingSearch{stale: true}
	historical := &countingSearch{stale: true}
	require.NoError(t, searchAt(c, boundIdxTable, current))
	histKey := SnapshotKey(boundIdxTable, snapshotTS(100))
	require.NoError(t, searchAt(c, histKey, historical))

	c.checkStale()

	curEntry, ok := c.IndexMap.Load(boundIdxTable)
	require.True(t, ok)
	require.True(t, curEntry.(*VectorIndexSearch).stale.Load(),
		"a stale CURRENT generation must still be marked -- the exemption must not disable the sweep")

	histEntry, ok := c.IndexMap.Load(histKey)
	require.True(t, ok)
	require.False(t, histEntry.(*VectorIndexSearch).stale.Load(),
		"a snapshot generation is immutable and must never be marked stale")

	// And it therefore survives the sweep that reclaims the current one.
	c.HouseKeeping()
	_, ok = c.IndexMap.Load(histKey)
	require.True(t, ok, "the snapshot generation must survive HouseKeeping")
}

// The key helpers are the single source of truth shared by the search TVFs (which build keys)
// and the cache (which classifies them); a drift between them would silently unbound the
// cache or bound the current generation.
func TestSnapshotKeyRoundTrip(t *testing.T) {
	key := SnapshotKey(boundIdxTable, timestamp.Timestamp{PhysicalTime: 17, LogicalTime: 3})
	require.Equal(t, boundIdxTable+"@17-3", key)
	require.True(t, IsSnapshotKey(key))
	require.False(t, IsSnapshotKey(boundIdxTable), "a bare index table name is the current generation")
	require.NotEqual(t, key, SnapshotKey(boundIdxTable, timestamp.Timestamp{PhysicalTime: 17, LogicalTime: 4}),
		"the logical clock must be part of the identity")
}
