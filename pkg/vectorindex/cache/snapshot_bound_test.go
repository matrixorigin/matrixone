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

// Tests for named-snapshot cache keys: shared loads per timestamp, and the staleness
// exemption that keeps an immutable snapshot generation from being swept (#27927).

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

// countingSearch counts Load calls and reports a fixed per-arena size.
type countingSearch struct {
	loads  atomic.Int64
	stale  bool
	host   int64
	device int64
}

func (m *countingSearch) Preload(*sqlexec.SqlProcess) error { return nil }
func (m *countingSearch) GetIndexSize() (int64, int64)      { return m.host, m.device }

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

// newBoundCache returns a private cache, emptied on cleanup.
func newBoundCache(t *testing.T) *VectorIndexCache {
	t.Helper()
	c := NewVectorIndexCache()
	t.Cleanup(func() {
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

// Queries on the same snapshot share one entry and one Load.
func TestSnapshotBoundSameTSSharesOneLoad(t *testing.T) {
	c := newBoundCache(t)
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

// checkStale skips snapshot entries and still marks current-generation ones.
func TestStaleSweepSkipsSnapshotGenerations(t *testing.T) {
	c := newBoundCache(t)

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

// residentSnapshots counts snapshot keys in the cache.
func residentSnapshots(c *VectorIndexCache) int {
	n := 0
	c.IndexMap.Range(func(key, _ any) bool {
		if k, ok := key.(string); ok && IsSnapshotKey(k) {
			n++
		}
		return true
	})
	return n
}
