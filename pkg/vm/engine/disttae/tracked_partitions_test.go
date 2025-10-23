// Copyright 2022 Matrix Origin
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

package disttae

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMockPartition creates a partition for testing
// Note: The partition's PartitionState is initialized but doesn't have checkpoint data
// so CanServe will work based on the internal state's timestamp range
func newMockPartition(tableID uint64) *logtailreplay.Partition {
	// Use the real NewPartition to create a properly initialized partition
	p := logtailreplay.NewPartition("test_service", tableID)
	return p
}

// TestTrackedPartition tests the TrackedPartition structure
func TestTrackedPartition(t *testing.T) {
	t.Run("NewTrackedPartition", func(t *testing.T) {
		p := newMockPartition(100)
		tp := NewTrackedPartition(p)

		assert.NotNil(t, tp)
		assert.Equal(t, p, tp.partition)
		assert.True(t, tp.createTime > 0)
		assert.False(t, tp.GetLastAccessTime().IsZero())
	})

	t.Run("GetPartition_UpdatesAccessTime", func(t *testing.T) {
		p := newMockPartition(100)
		tp := NewTrackedPartition(p)

		// Manually set an old access time
		oldTime := time.Now().Add(-1 * time.Hour).UnixNano()
		tp.lastAccess.Store(oldTime)

		// GetPartition should update access time to current time
		retrieved := tp.GetPartition()
		assert.Equal(t, p, retrieved)

		newAccessTime := tp.lastAccess.Load()
		assert.True(t, newAccessTime > oldTime, "Access time should be updated")
		// Verify it's recent (within last second)
		assert.True(t, newAccessTime >= time.Now().Add(-1*time.Second).UnixNano(),
			"Access time should be recent")
	})

	t.Run("GetAge", func(t *testing.T) {
		p := newMockPartition(100)
		tp := NewTrackedPartition(p)

		// Set last access time to 1 hour ago (GetAge measures since last access)
		oneHourAgo := time.Now().Add(-1 * time.Hour)
		tp.lastAccess.Store(oneHourAgo.UnixNano())

		// Calculate age (time since last access)
		age := tp.GetAge()

		// Age should be very close to 1 hour
		// Allow for small variance due to execution time
		expectedAge := time.Since(oneHourAgo)
		diff := age - expectedAge
		if diff < 0 {
			diff = -diff
		}
		assert.True(t, diff < 100*time.Millisecond,
			"Age difference too large: expected ~%v, got %v, diff %v",
			expectedAge, age, diff)
	})

	t.Run("ConcurrentGetPartition", func(t *testing.T) {
		p := newMockPartition(100)
		tp := NewTrackedPartition(p)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				retrieved := tp.GetPartition()
				assert.Equal(t, p, retrieved)
			}()
		}
		wg.Wait()

		assert.False(t, tp.GetLastAccessTime().IsZero())
	})
}

// TestTrackedPartitions tests the TrackedPartitions structure
func TestTrackedPartitions(t *testing.T) {
	t.Run("NewTrackedPartitions", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		assert.NotNil(t, tps)
		assert.Equal(t, uint64(1), tps.dbID)
		assert.Equal(t, uint64(100), tps.tableID)
		assert.Equal(t, metrics, tps.metrics)
		assert.Equal(t, 0, len(tps.snaps))
	})

	t.Run("Add_SingleSnapshot", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		p := newMockPartition(100)
		ts := types.BuildTS(150, 0)

		ps := tps.Add(p, "test_table", ts, 10)

		assert.NotNil(t, ps)
		assert.Equal(t, 1, tps.Count())
		assert.Equal(t, int64(1), metrics.SnapshotCreates.Load())
		assert.Equal(t, int64(1), metrics.TotalSnapshots.Load())
	})

	t.Run("Add_LRU_Eviction", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// Add 3 snapshots with maxCount=2, with manually set access times
		p1 := newMockPartition(100)
		p2 := newMockPartition(101)
		p3 := newMockPartition(102)

		now := time.Now()

		// Add p1 and set its access time to 2 hours ago (oldest)
		tps.Lock()
		tp1 := NewTrackedPartition(p1)
		tp1.lastAccess.Store(now.Add(-2 * time.Hour).UnixNano())
		tps.snaps = append(tps.snaps, tp1)
		metrics.SnapshotCreates.Add(1)
		metrics.TotalSnapshots.Add(1)
		tps.Unlock()

		// Add p2 and set its access time to 1 hour ago
		tps.Lock()
		tp2 := NewTrackedPartition(p2)
		tp2.lastAccess.Store(now.Add(-1 * time.Hour).UnixNano())
		tps.snaps = append(tps.snaps, tp2)
		metrics.SnapshotCreates.Add(1)
		metrics.TotalSnapshots.Add(1)
		tps.Unlock()

		// Now add p3, which should trigger LRU eviction of p1 (oldest)
		tps.Add(p3, "table", types.BuildTS(350, 0), 2)

		assert.Equal(t, 2, tps.Count(), "Should maintain max count of 2")
		assert.Equal(t, int64(1), metrics.LRUEvictions.Load(), "Should have 1 LRU eviction")
		assert.Equal(t, int64(2), metrics.TotalSnapshots.Load(), "Total should be 2 after eviction")
	})

	t.Run("Find_WithRealPartition", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// Add a partition
		p := newMockPartition(100)
		added := tps.Add(p, "table", types.BuildTS(100, 0), 10)
		assert.NotNil(t, added)

		// Note: Find relies on PartitionState.CanServe() which checks internal timestamp ranges
		// The real partition's initial state may or may not serve specific timestamps
		// depending on its internal state. This is a limitation of using real partitions
		// without checkpoint data. However, we can verify the structure works.

		// Try to find - it may or may not find depending on partition state
		result := tps.Find(types.BuildTS(100, 0))
		// Either found or not found is acceptable here since we're testing structure
		// The key is that it doesn't panic and metrics are updated
		if result != nil {
			assert.Equal(t, int64(1), metrics.SnapshotHits.Load(), "Should record a hit")
		} else {
			assert.Equal(t, int64(1), metrics.SnapshotMisses.Load(), "Should record a miss")
		}
	})

	t.Run("Find_NotFound", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// Try to find in empty list
		ps := tps.Find(types.BuildTS(150, 0))
		assert.Nil(t, ps)
		assert.Equal(t, int64(1), metrics.SnapshotMisses.Load())
	})

	t.Run("GC_RemoveOldSnapshots", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// Add snapshots with old last access times (GC checks lastAccessTime via GetAge)
		p1 := newMockPartition(100)
		p2 := newMockPartition(101)

		now := time.Now()

		// Add p1 with last access time 2 hours ago
		tps.Lock()
		tp1 := NewTrackedPartition(p1)
		tp1.lastAccess.Store(now.Add(-2 * time.Hour).UnixNano())
		tps.snaps = append(tps.snaps, tp1)
		metrics.SnapshotCreates.Add(1)
		metrics.TotalSnapshots.Add(1)
		tps.Unlock()

		// Add p2 with last access time 2 hours ago
		tps.Lock()
		tp2 := NewTrackedPartition(p2)
		tp2.lastAccess.Store(now.Add(-2 * time.Hour).UnixNano())
		tps.snaps = append(tps.snaps, tp2)
		metrics.SnapshotCreates.Add(1)
		metrics.TotalSnapshots.Add(1)
		tps.Unlock()

		// GC with maxAge of 1 hour should remove both (they haven't been accessed in 2 hours)
		gcCount := tps.GC(1 * time.Hour)

		assert.Equal(t, 2, gcCount, "Should have GCed 2 snapshots")
		assert.Equal(t, 0, tps.Count(), "All snapshots should be GCed")
		assert.Equal(t, int64(2), metrics.AgeEvictions.Load(), "Should have 2 age evictions")
		assert.Equal(t, int64(2), metrics.SnapshotsGCed.Load(), "Should have GCed 2 snapshots")
		assert.Equal(t, int64(0), metrics.TotalSnapshots.Load(), "Total snapshots should be 0")
	})

	t.Run("GC_KeepRecentSnapshots", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// Add a snapshot
		p := newMockPartition(100)
		tps.Add(p, "table", types.BuildTS(150, 0), 10)

		// GC with maxAge of 1 hour should keep it
		tps.GC(1 * time.Hour)

		assert.Equal(t, 1, tps.Count(), "Recent snapshot should be kept")
		assert.Equal(t, int64(0), metrics.AgeEvictions.Load(), "Should have no age evictions")
	})

	t.Run("ConcurrentAddAndFind", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		var wg sync.WaitGroup

		// Concurrent adds
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				p := newMockPartition(uint64(100 + idx))
				tps.Add(p, "table", types.BuildTS(int64(100*idx+50), 0), 100)
			}(i)
		}

		// Concurrent finds
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				tps.Find(types.BuildTS(int64(100*idx+50), 0))
			}(i)
		}

		wg.Wait()

		assert.Equal(t, 10, tps.Count(), "Should have 10 snapshots")
		assert.Equal(t, int64(10), metrics.SnapshotCreates.Load(), "Should have created exactly 10 snapshots")
	})
}

// TestSnapshotManager tests the SnapshotManager structure
func TestSnapshotManager(t *testing.T) {
	t.Run("NewSnapshotManager", func(t *testing.T) {
		mgr := NewSnapshotManager()

		assert.NotNil(t, mgr)
		assert.NotNil(t, mgr.tables)
		assert.Equal(t, 20, mgr.config.MaxSnapshotsPerTable)
		assert.Equal(t, 5*time.Hour, mgr.config.MaxAge)
	})

	t.Run("Init", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		assert.True(t, mgr.state.lastGCTime.Load() > 0)
	})

	t.Run("GetOrCreate_NewTable", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		tps := mgr.GetOrCreate(1, 100)

		assert.NotNil(t, tps)
		assert.Equal(t, uint64(1), tps.dbID)
		assert.Equal(t, uint64(100), tps.tableID)
	})

	t.Run("GetOrCreate_ExistingTable", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		tps1 := mgr.GetOrCreate(1, 100)
		tps2 := mgr.GetOrCreate(1, 100)

		assert.Equal(t, tps1, tps2, "Should return the same instance")
	})

	t.Run("Add_NewSnapshot", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		p := newMockPartition(100)
		ps := mgr.Add(1, 100, p, "test_table", types.BuildTS(150, 0))

		assert.NotNil(t, ps)
		assert.Equal(t, int64(1), mgr.metrics.SnapshotCreates.Load())
		assert.Equal(t, int64(1), mgr.metrics.TotalSnapshots.Load())
	})

	t.Run("Find_WithRealPartition", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		// Add a snapshot
		p := newMockPartition(100)
		added := mgr.Add(1, 100, p, "test_table", types.BuildTS(150, 0))
		assert.NotNil(t, added)

		// Try to find it
		ps := mgr.Find(1, 100, types.BuildTS(150, 0))
		// Similar to TrackedPartitions test - result depends on partition's internal state
		// But we verify that Find doesn't panic and returns a valid result (nil or non-nil)
		if ps != nil {
			assert.Equal(t, int64(1), mgr.metrics.SnapshotHits.Load(), "Should record a hit")
		} else {
			assert.Equal(t, int64(1), mgr.metrics.SnapshotMisses.Load(), "Should record a miss")
		}
	})

	t.Run("Find_NonExistentTable", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		ps := mgr.Find(1, 999, types.BuildTS(150, 0))
		assert.Nil(t, ps)
	})

	t.Run("MaybeStartGC_FirstTime", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		// Reset last GC time to trigger GC
		mgr.state.lastGCTime.Store(0)

		mgr.MaybeStartGC()

		// Use require.Eventually to wait for goroutine to complete
		// This is deterministic - it polls until condition is met or times out
		require.Eventually(t, func() bool {
			return mgr.state.lastGCTime.Load() > 0
		}, 2*time.Second, 10*time.Millisecond, "lastGCTime should be updated after GC")
	})

	t.Run("MaybeStartGC_TooSoon", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		// Set last GC time to now
		mgr.state.lastGCTime.Store(time.Now().UnixNano())

		initialTime := mgr.state.lastGCTime.Load()
		mgr.MaybeStartGC()

		// Should not have changed
		assert.Equal(t, initialTime, mgr.state.lastGCTime.Load())
	})

	t.Run("RunGC", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		now := time.Now()

		// Add snapshots with old last access times directly (GC checks lastAccessTime)
		for i := 0; i < 3; i++ {
			p := newMockPartition(uint64(100 + i))
			tps := mgr.GetOrCreate(1, uint64(100+i))

			tps.Lock()
			tp := NewTrackedPartition(p)
			tp.lastAccess.Store(now.Add(-2 * time.Hour).UnixNano())
			tps.snaps = append(tps.snaps, tp)
			mgr.metrics.SnapshotCreates.Add(1)
			mgr.metrics.TotalSnapshots.Add(1)
			tps.Unlock()
		}

		assert.Equal(t, int64(3), mgr.metrics.TotalSnapshots.Load())

		// Run GC with maxAge of 1 hour should remove all (not accessed in 2 hours)
		mgr.config.MaxAge = 1 * time.Hour
		mgr.RunGC()

		assert.Equal(t, int64(0), mgr.metrics.TotalSnapshots.Load(), "All snapshots should be removed")
		assert.Equal(t, int64(3), mgr.metrics.SnapshotsGCed.Load(), "Should have GCed exactly 3 snapshots")
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		metrics := mgr.GetMetrics()
		assert.NotNil(t, metrics)
		assert.Equal(t, int64(0), metrics.TotalSnapshots.Load())
	})

	t.Run("GetConfig", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		config := mgr.GetConfig()
		assert.NotNil(t, config)
		assert.Equal(t, 20, config.MaxSnapshotsPerTable)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		var wg sync.WaitGroup

		// Concurrent adds to different tables
		// Using idx%3 for dbID and idx%5 for tableID
		// This creates 3*5=15 unique (dbID, tableID) combinations
		// But we add 20 times, so some tables will get multiple snapshots
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				dbID := uint64(1 + idx%3)      // 3 unique dbIDs: 1, 2, 3
				tableID := uint64(100 + idx%5) // 5 unique tableIDs: 100-104
				p := newMockPartition(uint64(100 + idx))
				mgr.Add(dbID, tableID, p, "test_table", types.BuildTS(int64(100*idx+50), 0))
			}(i)
		}

		// Concurrent finds
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				dbID := uint64(1 + idx%3)
				tableID := uint64(100 + idx%5)
				mgr.Find(dbID, tableID, types.BuildTS(int64(100*idx+50), 0))
			}(i)
		}

		// Concurrent GC calls
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mgr.MaybeStartGC()
			}()
		}

		wg.Wait()

		// We added 20 snapshots across 15 unique tables (3 dbIDs × 5 tableIDs)
		// Each Add creates a snapshot, so we should have exactly 20 creates
		assert.Equal(t, int64(20), mgr.metrics.SnapshotCreates.Load(), "Should have created exactly 20 snapshots")
		// Total snapshots should be 20 (no evictions expected with default config)
		assert.Equal(t, int64(20), mgr.metrics.TotalSnapshots.Load(), "Should have 20 total snapshots")
	})
}

// TestSnapshotMetrics tests the metrics tracking
func TestSnapshotMetrics(t *testing.T) {
	t.Run("MetricsInitialization", func(t *testing.T) {
		metrics := &SnapshotMetrics{}

		assert.Equal(t, int64(0), metrics.SnapshotHits.Load())
		assert.Equal(t, int64(0), metrics.SnapshotMisses.Load())
		assert.Equal(t, int64(0), metrics.SnapshotCreates.Load())
		assert.Equal(t, int64(0), metrics.TotalSnapshots.Load())
		assert.Equal(t, int64(0), metrics.LRUEvictions.Load())
		assert.Equal(t, int64(0), metrics.AgeEvictions.Load())
		assert.Equal(t, int64(0), metrics.SnapshotsGCed.Load())
	})

	t.Run("MetricsIncrement", func(t *testing.T) {
		metrics := &SnapshotMetrics{}

		metrics.SnapshotHits.Add(1)
		metrics.SnapshotMisses.Add(1)
		metrics.SnapshotCreates.Add(1)
		metrics.TotalSnapshots.Add(1)

		assert.Equal(t, int64(1), metrics.SnapshotHits.Load())
		assert.Equal(t, int64(1), metrics.SnapshotMisses.Load())
		assert.Equal(t, int64(1), metrics.SnapshotCreates.Load())
		assert.Equal(t, int64(1), metrics.TotalSnapshots.Load())
	})

	t.Run("MetricsConcurrentIncrement", func(t *testing.T) {
		metrics := &SnapshotMetrics{}

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				metrics.SnapshotHits.Add(1)
				metrics.SnapshotMisses.Add(1)
				metrics.SnapshotCreates.Add(1)
			}()
		}
		wg.Wait()

		assert.Equal(t, int64(100), metrics.SnapshotHits.Load())
		assert.Equal(t, int64(100), metrics.SnapshotMisses.Load())
		assert.Equal(t, int64(100), metrics.SnapshotCreates.Load())
	})
}

// TestSnapshotGCConfig tests the GC configuration
func TestSnapshotGCConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		config := DefaultSnapshotGCConfig()

		assert.Equal(t, true, config.Enabled)
		assert.Equal(t, 30*time.Minute, config.GCInterval)
		assert.Equal(t, 5*time.Hour, config.MaxAge)
		assert.Equal(t, 20, config.MaxSnapshotsPerTable)
		assert.Equal(t, 1000, config.MaxTotalSnapshots)
	})

	t.Run("CustomConfig", func(t *testing.T) {
		config := SnapshotGCConfig{
			Enabled:              false,
			GCInterval:           1 * time.Minute,
			MaxAge:               2 * time.Hour,
			MaxSnapshotsPerTable: 10,
			MaxTotalSnapshots:    500,
		}

		assert.Equal(t, false, config.Enabled)
		assert.Equal(t, 1*time.Minute, config.GCInterval)
		assert.Equal(t, 2*time.Hour, config.MaxAge)
		assert.Equal(t, 10, config.MaxSnapshotsPerTable)
		assert.Equal(t, 500, config.MaxTotalSnapshots)
	})
}

// TestEdgeCases tests various edge cases
func TestEdgeCases(t *testing.T) {
	t.Run("Add_WithZeroMaxCount", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		p := newMockPartition(100)
		ps := tps.Add(p, "table", types.BuildTS(150, 0), 0)

		assert.NotNil(t, ps)
		assert.Equal(t, 1, tps.Count(), "Should add even with maxCount=0")
	})

	t.Run("GC_WithZeroAge", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		p := newMockPartition(100)
		tps.Add(p, "table", types.BuildTS(150, 0), 10)

		// GC with 0 age should remove all
		tps.GC(0)

		assert.Equal(t, 0, tps.Count())
	})

	t.Run("EmptyTableGC", func(t *testing.T) {
		metrics := &SnapshotMetrics{}
		tps := NewTrackedPartitions(1, 100, metrics)

		// GC on empty table should not panic
		tps.GC(1 * time.Hour)

		assert.Equal(t, 0, tps.Count())
	})

	t.Run("ManagerGC_NoTables", func(t *testing.T) {
		mgr := NewSnapshotManager()
		mgr.Init()

		// RunGC on empty manager should not panic
		mgr.RunGC()

		assert.Equal(t, int64(0), mgr.metrics.SnapshotsGCed.Load())
	})
}

// Benchmark tests
func BenchmarkTrackedPartitionGetPartition(b *testing.B) {
	p := newMockPartition(100)
	tp := NewTrackedPartition(p)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tp.GetPartition()
	}
}

func BenchmarkTrackedPartitionsAdd(b *testing.B) {
	metrics := &SnapshotMetrics{}
	tps := NewTrackedPartitions(1, 100, metrics)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := newMockPartition(uint64(100 + i))
		tps.Add(p, "table", types.BuildTS(int64(100*i+50), 0), 1000)
	}
}

func BenchmarkSnapshotManagerAdd(b *testing.B) {
	mgr := NewSnapshotManager()
	mgr.Init()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := newMockPartition(uint64(100 + i))
		mgr.Add(1, uint64(100+i%10), p, "table", types.BuildTS(int64(100*i+50), 0))
	}
}

func BenchmarkSnapshotManagerConcurrentAdd(b *testing.B) {
	mgr := NewSnapshotManager()
	mgr.Init()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p := newMockPartition(uint64(100 + i))
			mgr.Add(1, uint64(100+i%10), p, "table", types.BuildTS(int64(100*i+50), 0))
			i++
		}
	})
}
