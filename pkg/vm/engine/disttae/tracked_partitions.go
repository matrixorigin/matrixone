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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

// TrackedPartition wraps a partition with access tracking information
type TrackedPartition struct {
	partition  *logtailreplay.Partition
	lastAccess atomic.Int64 // Unix timestamp in nanoseconds
	createTime int64        // Unix timestamp in nanoseconds
}

// TrackedPartitions manages a collection of tracked partitions for a table
type TrackedPartitions struct {
	sync.Mutex
	snaps   []*TrackedPartition
	dbID    uint64
	tableID uint64
	metrics *SnapshotMetrics // pointer to engine's global metrics
}

// SnapshotManager manages all tracked partitions across all tables
type SnapshotManager struct {
	sync.Mutex
	tables  map[[2]uint64]*TrackedPartitions
	config  SnapshotGCConfig
	state   SnapshotGCState
	metrics SnapshotMetrics
}

// NewTrackedPartition creates a new TrackedPartition
func NewTrackedPartition(partition *logtailreplay.Partition) *TrackedPartition {
	now := time.Now().UnixNano()
	tp := &TrackedPartition{
		partition:  partition,
		createTime: now,
	}
	tp.lastAccess.Store(now)
	return tp
}

// GetPartition returns the underlying partition and updates access time
func (tp *TrackedPartition) GetPartition() *logtailreplay.Partition {
	tp.lastAccess.Store(time.Now().UnixNano())
	return tp.partition
}

// GetLastAccessTime returns the last access time
func (tp *TrackedPartition) GetLastAccessTime() time.Time {
	return time.Unix(0, tp.lastAccess.Load())
}

// GetCreateTime returns the creation time
func (tp *TrackedPartition) GetCreateTime() time.Time {
	return time.Unix(0, tp.createTime)
}

// GetAge returns the age since last access
func (tp *TrackedPartition) GetAge() time.Duration {
	return time.Since(tp.GetLastAccessTime())
}

// NewSnapshotManager creates a new SnapshotManager
func NewSnapshotManager() *SnapshotManager {
	return &SnapshotManager{
		tables: make(map[[2]uint64]*TrackedPartitions),
		config: DefaultSnapshotGCConfig(),
	}
}

// Init initializes the snapshot manager with configuration
func (sm *SnapshotManager) Init() {
	sm.state.lastGCTime.Store(time.Now().UnixNano())
}

// NewTrackedPartitions creates a new TrackedPartitions manager
func NewTrackedPartitions(dbID, tableID uint64, metrics *SnapshotMetrics) *TrackedPartitions {
	return &TrackedPartitions{
		snaps:   make([]*TrackedPartition, 0),
		dbID:    dbID,
		tableID: tableID,
		metrics: metrics,
	}
}

// Find searches for a partition that can serve the given timestamp
// Returns the partition state if found, or nil if not found
// Automatically updates access time and metrics
func (tp *TrackedPartitions) Find(ts types.TS) *logtailreplay.PartitionState {
	tp.Lock()
	defer tp.Unlock()

	for _, trackedSnap := range tp.snaps {
		partition := trackedSnap.GetPartition() // GetPartition auto-updates access time
		if partition.Snapshot().CanServe(ts) {
			// Update metrics
			if tp.metrics != nil {
				tp.metrics.SnapshotHits.Add(1)
			}

			logutil.Debug(
				"Snapshot-Cache-Hit",
				zap.Uint64("db-id", tp.dbID),
				zap.Uint64("table-id", tp.tableID),
				zap.String("ts", ts.ToString()),
				zap.Time("last-access", trackedSnap.GetLastAccessTime()),
			)

			return partition.Snapshot()
		}
	}

	// Cache miss
	if tp.metrics != nil {
		tp.metrics.SnapshotMisses.Add(1)
	}

	return nil
}

// Add adds a new tracked partition, enforcing the maximum count limit with LRU eviction
// Returns the partition state of the added partition
func (tp *TrackedPartitions) Add(
	partition *logtailreplay.Partition,
	tableName string,
	ts types.TS,
	maxCount int,
) *logtailreplay.PartitionState {
	tp.Lock()
	defer tp.Unlock()

	// Wrap partition with tracking information
	trackedSnap := NewTrackedPartition(partition)

	// Check if we need to evict old snapshots (LRU by count)
	// Only evict if maxCount > 0 and we have reached the limit
	if maxCount > 0 && len(tp.snaps) >= maxCount {
		// Sort by last access time (oldest first)
		sort.Slice(tp.snaps, func(i, j int) bool {
			return tp.snaps[i].GetLastAccessTime().Before(
				tp.snaps[j].GetLastAccessTime())
		})

		// Evict oldest snapshots to make room for new one
		evictCount := len(tp.snaps) - maxCount + 1
		for i := 0; i < evictCount; i++ {
			evicted := tp.snaps[i]
			logutil.Info(
				"Snapshot-LRU-Evict",
				zap.Uint64("db-id", tp.dbID),
				zap.Uint64("table-id", tp.tableID),
				zap.String("table-name", tableName),
				zap.Duration("age", evicted.GetAge()),
				zap.Time("last-access", evicted.GetLastAccessTime()),
			)
		}
		tp.snaps = tp.snaps[evictCount:]

		if tp.metrics != nil {
			tp.metrics.LRUEvictions.Add(int64(evictCount))
			tp.metrics.TotalSnapshots.Add(-int64(evictCount)) // Decrease total
		}
	}

	// Add new snapshot
	tp.snaps = append(tp.snaps, trackedSnap)

	// Update metrics (SnapshotMisses already updated in Find method)
	if tp.metrics != nil {
		tp.metrics.SnapshotCreates.Add(1)
		tp.metrics.TotalSnapshots.Add(1)
	}

	logutil.Info(
		"Snapshot-Created",
		zap.Uint64("db-id", tp.dbID),
		zap.Uint64("table-id", tp.tableID),
		zap.String("table-name", tableName),
		zap.String("ts", ts.ToString()),
		zap.Int("total-snaps-for-table", len(tp.snaps)),
	)

	return partition.Snapshot()
}

// GC removes partitions that haven't been accessed within maxAge
// Returns the number of partitions removed
func (tp *TrackedPartitions) GC(maxAge time.Duration) int {
	tp.Lock()
	defer tp.Unlock()

	activeSnaps := make([]*TrackedPartition, 0, len(tp.snaps))
	gcedCount := 0

	for _, trackedSnap := range tp.snaps {
		age := trackedSnap.GetAge()

		if age > maxAge {
			gcedCount++
			logutil.Info(
				"Snapshot-GC-Remove",
				zap.Uint64("db-id", tp.dbID),
				zap.Uint64("table-id", tp.tableID),
				zap.Duration("age", age),
				zap.Time("last-access", trackedSnap.GetLastAccessTime()),
				zap.Time("create-time", trackedSnap.GetCreateTime()),
			)
		} else {
			activeSnaps = append(activeSnaps, trackedSnap)
		}
	}

	tp.snaps = activeSnaps

	if tp.metrics != nil && gcedCount > 0 {
		tp.metrics.AgeEvictions.Add(int64(gcedCount))
		tp.metrics.SnapshotsGCed.Add(int64(gcedCount))
		tp.metrics.TotalSnapshots.Add(-int64(gcedCount))
	}

	return gcedCount
}

// Count returns the current number of tracked partitions
func (tp *TrackedPartitions) Count() int {
	tp.Lock()
	defer tp.Unlock()
	return len(tp.snaps)
}

// ========== SnapshotManager Methods ==========

// GetOrCreate gets or creates a TrackedPartitions for the given table
func (sm *SnapshotManager) GetOrCreate(dbID, tableID uint64) *TrackedPartitions {
	sm.Lock()
	defer sm.Unlock()

	key := [2]uint64{dbID, tableID}
	tblSnaps, ok := sm.tables[key]
	if !ok {
		tblSnaps = NewTrackedPartitions(dbID, tableID, &sm.metrics)
		sm.tables[key] = tblSnaps
	}
	return tblSnaps
}

// Find searches for a snapshot that can serve the given timestamp for a table
func (sm *SnapshotManager) Find(dbID, tableID uint64, ts types.TS) *logtailreplay.PartitionState {
	tblSnaps := sm.GetOrCreate(dbID, tableID)
	return tblSnaps.Find(ts)
}

// Add adds a new partition to the manager for a table
func (sm *SnapshotManager) Add(
	dbID, tableID uint64,
	partition *logtailreplay.Partition,
	tableName string,
	ts types.TS,
) *logtailreplay.PartitionState {
	tblSnaps := sm.GetOrCreate(dbID, tableID)
	return tblSnaps.Add(partition, tableName, ts, sm.config.MaxSnapshotsPerTable)
}

// MaybeStartGC checks if GC should run and starts it if needed
func (sm *SnapshotManager) MaybeStartGC() {
	if !sm.config.Enabled {
		return
	}

	// Check if enough time has passed since last GC
	lastGCTime := time.Unix(0, sm.state.lastGCTime.Load())
	if time.Since(lastGCTime) < sm.config.GCInterval {
		return
	}

	// Check if GC is already running
	if sm.state.gcRunning.Load() {
		return
	}

	// Update last GC time and mark as running
	sm.state.lastGCTime.Store(time.Now().UnixNano())
	sm.state.gcRunning.Store(true)

	// Run GC asynchronously
	go func() {
		defer sm.state.gcRunning.Store(false)
		sm.RunGC()
	}()
}

// RunGC performs garbage collection on all tracked partitions
func (sm *SnapshotManager) RunGC() {
	startTime := time.Now()
	maxAge := sm.config.MaxAge

	totalTables := 0
	totalSnapsBefore := 0
	totalSnapsAfter := 0
	totalGCed := 0

	logutil.Info("Snapshot-GC-Start",
		zap.Duration("max-age", maxAge),
		zap.Duration("gc-interval", sm.config.GCInterval),
		zap.Int("max-snaps-per-table", sm.config.MaxSnapshotsPerTable),
	)

	// Get snapshot of all table keys to avoid holding global lock
	sm.Lock()
	tables := make([][2]uint64, 0, len(sm.tables))
	for key := range sm.tables {
		tables = append(tables, key)
	}
	sm.Unlock()

	// Process each table
	for _, tableKey := range tables {
		sm.Lock()
		tblSnaps, exists := sm.tables[tableKey]
		sm.Unlock()

		if !exists {
			continue
		}

		beforeCount := tblSnaps.Count()
		totalSnapsBefore += beforeCount
		totalTables++

		// GC expired snapshots
		gcedCount := tblSnaps.GC(maxAge)
		totalGCed += gcedCount

		totalSnapsAfter += tblSnaps.Count()
	}

	duration := time.Since(startTime)

	// Update metrics
	sm.metrics.GCRuns.Add(1)
	sm.metrics.LastGCDuration.Store(duration.Nanoseconds())

	logutil.Info(
		"Snapshot-GC-Complete",
		zap.Int("total-tables", totalTables),
		zap.Int("snaps-before", totalSnapsBefore),
		zap.Int("snaps-after", totalSnapsAfter),
		zap.Int("gced-snaps", totalGCed),
		zap.Duration("duration", duration),
		zap.Int64("total-gc-runs", sm.metrics.GCRuns.Load()),
	)
}

// GetMetrics returns a pointer to current metrics
func (sm *SnapshotManager) GetMetrics() *SnapshotMetrics {
	return &sm.metrics
}

// GetConfig returns a copy of the configuration
func (sm *SnapshotManager) GetConfig() SnapshotGCConfig {
	return sm.config
}

// SnapshotGCConfig holds configuration for snapshot GC
type SnapshotGCConfig struct {
	Enabled              bool          // Whether snapshot GC is enabled
	GCInterval           time.Duration // Interval between GC runs
	MaxAge               time.Duration // Max age for inactive snapshots
	MaxSnapshotsPerTable int           // Max snapshots per table
	MaxTotalSnapshots    int           // Max total snapshots across all tables
}

// DefaultSnapshotGCConfig returns the default configuration
func DefaultSnapshotGCConfig() SnapshotGCConfig {
	return SnapshotGCConfig{
		Enabled:              true,
		GCInterval:           30 * time.Minute,
		MaxAge:               1 * time.Hour,
		MaxSnapshotsPerTable: 20,
		MaxTotalSnapshots:    1000,
	}
}

// SnapshotGCState holds the runtime state for snapshot GC
type SnapshotGCState struct {
	lastGCTime atomic.Int64 // Unix timestamp in nanoseconds
	gcRunning  atomic.Bool  // Whether GC is currently running
}

// SnapshotMetrics holds metrics for snapshot management
type SnapshotMetrics struct {
	TotalSnapshots  atomic.Int64 // Current total snapshots
	TotalTables     atomic.Int64 // Current tables with snapshots
	GCRuns          atomic.Int64 // Total GC runs
	SnapshotsGCed   atomic.Int64 // Total snapshots GCed
	LastGCDuration  atomic.Int64 // Last GC duration in nanoseconds
	SnapshotHits    atomic.Int64 // Snapshot cache hits (reuse)
	SnapshotMisses  atomic.Int64 // Snapshot cache misses (create)
	SnapshotCreates atomic.Int64 // Total snapshots created
	LRUEvictions    atomic.Int64 // Evictions due to count limit
	AgeEvictions    atomic.Int64 // Evictions due to age limit
}
