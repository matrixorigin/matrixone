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

// Tests for the per-index bound on resident named-snapshot generations and the staleness
// exemption for snapshot entries (#27927).

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

const boundIdxTable = "__mo_index_secondary_bound_test"

// countingSearch counts Load calls.
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

// IsStale implements StaleChecker.
func (m *countingSearch) IsStale() (bool, error) { return m.stale, nil }

func snapshotTS(physical int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: physical, LogicalTime: 1}
}

// newBoundCache returns a private cache with MaxHistoricalIndexes set to limit, restored and
// emptied on cleanup.
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

// The (N+1)th generation is refused and no resident entry is removed.
func TestSnapshotBoundRefusesBeyondLimit(t *testing.T) {
	c := newBoundCache(t, 2)

	require.NoError(t, searchAt(c, boundIdxTable, &countingSearch{}))

	first := SnapshotKey(boundIdxTable, snapshotTS(100))
	second := SnapshotKey(boundIdxTable, snapshotTS(200))
	require.NoError(t, searchAt(c, first, &countingSearch{}))
	require.NoError(t, searchAt(c, second, &countingSearch{}))

	third := &countingSearch{}
	err := searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(300)), third)
	require.Error(t, err, "the third snapshot generation must be refused at limit 2")
	require.Contains(t, err.Error(), "named-snapshot generations cached")
	require.EqualValues(t, 0, third.loads.Load(), "a refused load must not pay for the index")

	for _, k := range []string{boundIdxTable, first, second} {
		_, ok := c.IndexMap.Load(k)
		require.True(t, ok, "refusing a new generation must not evict %q", k)
	}
	_, ok := c.IndexMap.Load(SnapshotKey(boundIdxTable, snapshotTS(300)))
	require.False(t, ok, "a refused load must not leave its key in the map")
}

// Current-generation loads are never refused.
func TestSnapshotBoundNeverRefusesCurrentGeneration(t *testing.T) {
	c := newBoundCache(t, 1)
	require.NoError(t, searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(100)), &countingSearch{}))

	// At limit for snapshots, yet the bare key still admits.
	require.Error(t, searchAt(c, SnapshotKey(boundIdxTable, snapshotTS(200)), &countingSearch{}))
	require.NoError(t, searchAt(c, boundIdxTable, &countingSearch{}))
	require.NoError(t, searchAt(c, "__mo_index_secondary_other_table", &countingSearch{}))
}

// The budget applies per index table: saturating one index does not affect another.
func TestSnapshotBoundIsPerIndexTable(t *testing.T) {
	c := newBoundCache(t, 2)
	const tblA = "__mo_index_secondary_tbl_a"
	const tblB = "__mo_index_secondary_tbl_b"

	// Saturate index A.
	require.NoError(t, searchAt(c, SnapshotKey(tblA, snapshotTS(100)), &countingSearch{}))
	require.NoError(t, searchAt(c, SnapshotKey(tblA, snapshotTS(200)), &countingSearch{}))
	require.Error(t, searchAt(c, SnapshotKey(tblA, snapshotTS(300)), &countingSearch{}),
		"index A is at its own limit")

	// Index B has its own budget.
	for _, ts := range []int64{100, 200} {
		b := &countingSearch{}
		require.NoError(t, searchAt(c, SnapshotKey(tblB, snapshotTS(ts)), b),
			"another index's snapshots must not consume this index's budget")
		require.EqualValues(t, 1, b.loads.Load())
	}
	require.Error(t, searchAt(c, SnapshotKey(tblB, snapshotTS(300)), &countingSearch{}),
		"index B has its own limit, applied independently")

	for _, tbl := range []string{tblA, tblB} {
		require.NoError(t, searchAt(c, tbl, &countingSearch{}))
	}
}

// extendForSearch renews ExpireAt on every search, so a hot incumbent is never reclaimed and
// the refusal stands while it stays in use.
func TestSnapshotBoundRefusalPersistsWhileIncumbentsAreHot(t *testing.T) {
	c := newBoundCache(t, 1)
	const tbl = "__mo_index_secondary_hot"
	incumbent := &countingSearch{}
	require.NoError(t, searchAt(c, SnapshotKey(tbl, snapshotTS(100)), incumbent))

	for i := 0; i < 3; i++ {
		require.NoError(t, searchAt(c, SnapshotKey(tbl, snapshotTS(100)), incumbent))
		c.HouseKeeping()
		require.Error(t, searchAt(c, SnapshotKey(tbl, snapshotTS(200)), &countingSearch{}),
			"a hot incumbent is never reclaimed, so the refusal stands")
	}
	require.EqualValues(t, 1, incumbent.loads.Load(), "the incumbent is never evicted or reloaded")
}

// Queries on the same snapshot share one entry and one Load.
func TestSnapshotBoundSameTSSharesOneLoad(t *testing.T) {
	c := newBoundCache(t, 1)
	key := SnapshotKey(boundIdxTable, snapshotTS(100))

	shared := &countingSearch{}
	for i := 0; i < 5; i++ {
		require.NoError(t, searchAt(c, key, shared))
	}
	require.EqualValues(t, 1, shared.loads.Load(), "same-TS queries must share one load")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = searchAt(c, key, shared) }()
	}
	wg.Wait()
	require.EqualValues(t, 1, shared.loads.Load())
	require.NoError(t, searchAt(c, key, shared), "a resident generation is never refused")
}

// Once a resident generation expires, a previously refused load succeeds.
func TestSnapshotBoundRefusalIsTransient(t *testing.T) {
	c := newBoundCache(t, 1)
	first := SnapshotKey(boundIdxTable, snapshotTS(100))
	require.NoError(t, searchAt(c, first, &countingSearch{}))

	second := SnapshotKey(boundIdxTable, snapshotTS(200))
	require.Error(t, searchAt(c, second, &countingSearch{}))

	// Expire the incumbent.
	value, ok := c.IndexMap.Load(first)
	require.True(t, ok)
	value.(*VectorIndexSearch).ExpireAt.Store(1)
	c.HouseKeeping()
	_, ok = c.IndexMap.Load(first)
	require.False(t, ok, "the expired generation must be reclaimed")

	require.NoError(t, searchAt(c, second, &countingSearch{}), "the refused load must now succeed")
}

// checkStale skips snapshot entries and still marks current-generation ones.
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

	c.HouseKeeping()
	_, ok = c.IndexMap.Load(histKey)
	require.True(t, ok, "the snapshot generation must survive HouseKeeping")
}

// SnapshotKey format and IsSnapshotKey classification.
func TestSnapshotKeyRoundTrip(t *testing.T) {
	key := SnapshotKey(boundIdxTable, timestamp.Timestamp{PhysicalTime: 17, LogicalTime: 3})
	require.Equal(t, boundIdxTable+"@17-3", key)
	require.True(t, IsSnapshotKey(key))
	require.False(t, IsSnapshotKey(boundIdxTable), "a bare index table name is the current generation")
	require.NotEqual(t, key, SnapshotKey(boundIdxTable, timestamp.Timestamp{PhysicalTime: 17, LogicalTime: 4}),
		"the logical clock must be part of the identity")
}

// The effective bound comes from the session variable max_snapshot_index_cache.
func TestSnapshotCacheLimitFromSessionVariable(t *testing.T) {
	c := newBoundCache(t, 1)
	const tbl = "__mo_index_secondary_sessvar"

	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	var resolved atomic.Int64
	resolved.Store(3)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "max_snapshot_index_cache" {
			return resolved.Load(), nil
		}
		return nil, nil
	})
	sp := &sqlexec.SqlProcess{Proc: proc}

	for _, ts := range []int64{100, 200, 300} {
		_, _, err := c.Search(sp, SnapshotKey(tbl, snapshotTS(ts)), &countingSearch{}, nil, vectorindex.RuntimeConfig{})
		require.NoError(t, err, "the session limit of 3 must admit three generations")
	}
	_, _, err := c.Search(sp, SnapshotKey(tbl, snapshotTS(400)), &countingSearch{}, nil, vectorindex.RuntimeConfig{})
	require.Error(t, err, "the fourth must be refused at a session limit of 3")
	require.Contains(t, err.Error(), "max_snapshot_index_cache")
	require.Contains(t, err.Error(), "3 of 3", "the message must report the EFFECTIVE limit, not the default")
}

// snapshotCacheLimit falls back to MaxHistoricalIndexes when the variable is unreadable.
func TestSnapshotCacheLimitFallsBackToDefault(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)

	require.Equal(t, MaxHistoricalIndexes, snapshotCacheLimit(nil), "no sqlproc")
	require.Equal(t, MaxHistoricalIndexes, snapshotCacheLimit(&sqlexec.SqlProcess{}), "no proc or sqlctx")

	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return nil, moerr.NewInternalErrorNoCtx("boom")
	})
	require.Equal(t, MaxHistoricalIndexes, snapshotCacheLimit(&sqlexec.SqlProcess{Proc: proc}), "resolver error")

	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "not an int", nil })
	require.Equal(t, MaxHistoricalIndexes, snapshotCacheLimit(&sqlexec.SqlProcess{Proc: proc}), "wrong type")

	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return int64(0), nil })
	require.Equal(t, MaxHistoricalIndexes, snapshotCacheLimit(&sqlexec.SqlProcess{Proc: proc}), "non-positive")
}

// The package default matches the declared session variable default.
func TestSnapshotCacheLimitDefaultMatchesDeclaredVariable(t *testing.T) {
	require.Equal(t, 4, MaxHistoricalIndexes,
		"max_snapshot_index_cache is declared with Default int64(4) in pkg/frontend/variables.go")
	require.True(t, strings.HasPrefix(SnapshotKey("t", snapshotTS(1)), "t@"))
}
