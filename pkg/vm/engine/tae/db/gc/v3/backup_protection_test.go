// Copyright 2024 Matrix Origin
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

package gc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

// createMockCheckpointEntry creates a mock checkpoint entry for testing
func createMockCheckpointEntry(start, end int64) *checkpoint.CheckpointEntry {
	entry := checkpoint.NewCheckpointEntry(
		"test-sid",
		types.BuildTS(start, 0),
		types.BuildTS(end, 0),
		checkpoint.ET_Incremental,
		checkpoint.WithStateEntryOption(checkpoint.ST_Finished),
	)
	return entry
}

// createTestCheckpointCleaner creates a minimal checkpointCleaner for testing
func createTestCheckpointCleaner(t *testing.T) *checkpointCleaner {
	ctx := context.Background()
	dir := InitTestEnv(ModuleName, t.Name())
	t.Cleanup(func() {
		RemoveDefaultTestPath(ModuleName, t.Name())
	})

	fs, err := fileservice.NewFileService(
		ctx,
		fileservice.Config{
			Name:    "test-fs",
			Backend: "DISK",
			DataDir: dir,
			Cache:   fileservice.DisabledCacheConfig,
		},
		nil,
	)
	require.NoError(t, err)

	cleaner := &checkpointCleaner{
		ctx: ctx,
		sid: "test-sid",
		fs:  fs,
		mp:  mpool.MustNewZeroNoFixed(),
	}
	cleaner.deleter = NewDeleter(fs)
	cleaner.checker.extras = make(map[string]func(item any) bool)
	cleaner.mutation.metaFiles = make(map[string]ioutil.TSRangeFile)
	cleaner.mutation.snapshotMeta = logtail.NewSnapshotMeta()
	cleaner.backupProtection.isActive = false

	return cleaner
}

// TestBackupProtectionBasicOperations tests basic backup protection operations
func TestBackupProtectionBasicOperations(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Test SetBackupProtection
	protectedTS := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS)

	ts, lastUpdate, isActive := cleaner.GetBackupProtection()
	require.True(t, isActive)
	require.True(t, ts.Equal(&protectedTS))
	require.True(t, time.Since(lastUpdate) < time.Second)

	// Test UpdateBackupProtection
	newProtectedTS := types.BuildTS(200, 0)
	time.Sleep(10 * time.Millisecond) // Ensure different update time
	cleaner.UpdateBackupProtection(newProtectedTS)

	ts, lastUpdate2, isActive := cleaner.GetBackupProtection()
	require.True(t, isActive)
	require.True(t, ts.Equal(&newProtectedTS))
	require.True(t, lastUpdate2.After(lastUpdate))

	// Test RemoveBackupProtection
	cleaner.RemoveBackupProtection()

	ts, _, isActive = cleaner.GetBackupProtection()
	require.False(t, isActive)
	require.True(t, ts.IsEmpty())
}

// TestBackupProtectionCheckpointFiltering tests checkpoint filtering with backup protection
func TestBackupProtectionCheckpointFiltering(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Create test checkpoints
	ckp1 := createMockCheckpointEntry(0, 50)    // Should be protected
	ckp2 := createMockCheckpointEntry(50, 100)  // Should be protected (end == protectedTS)
	ckp3 := createMockCheckpointEntry(100, 150) // Should NOT be protected (end > protectedTS)
	ckp4 := createMockCheckpointEntry(150, 200) // Should NOT be protected

	protectedTS := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS)

	// Start GC to create snapshot
	cleaner.StartMutationTask("gc-process")
	defer cleaner.StopMutationTask()

	// Update snapshot (simulating what Process does)
	cleaner.backupProtection.Lock()
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// Test checkBackupProtection
	// ckp1: end(50) <= protectedTS(100) -> should be protected (return false)
	require.False(t, cleaner.checkBackupProtection(ckp1))

	// ckp2: end(100) <= protectedTS(100) -> should be protected (return false)
	require.False(t, cleaner.checkBackupProtection(ckp2))

	// ckp3: end(150) > protectedTS(100) -> should NOT be protected (return true)
	require.True(t, cleaner.checkBackupProtection(ckp3))

	// ckp4: end(200) > protectedTS(100) -> should NOT be protected (return true)
	require.True(t, cleaner.checkBackupProtection(ckp4))
}

// TestBackupProtectionFilterCheckpoints tests filterCheckpoints with backup protection
func TestBackupProtectionFilterCheckpoints(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Create test checkpoints
	checkpoints := []*checkpoint.CheckpointEntry{
		createMockCheckpointEntry(0, 50),    // Should be protected
		createMockCheckpointEntry(50, 100),  // Should be protected
		createMockCheckpointEntry(100, 150), // Should NOT be protected
		createMockCheckpointEntry(150, 200), // Should NOT be protected
	}

	protectedTS := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS)

	// Start GC to create snapshot
	cleaner.StartMutationTask("gc-process")
	defer cleaner.StopMutationTask()

	// Update snapshot
	cleaner.backupProtection.Lock()
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// Test filterCheckpoints in mergeCheckpointFilesLocked context
	// This simulates the filtering logic in mergeCheckpointFilesLocked
	highWater := types.BuildTS(200, 0)
	filtered, err := cleaner.filterCheckpoints(&highWater, checkpoints)
	require.NoError(t, err)
	require.Equal(t, 4, len(filtered)) // All checkpoints pass highWater filter

	// Now filter by backup protection (simulating mergeCheckpointFilesLocked logic)
	protectedTS2, isActive := cleaner.getBackupProtectionSnapshot()
	require.True(t, isActive)

	finalFiltered := make([]*checkpoint.CheckpointEntry, 0, len(filtered))
	for _, ckp := range filtered {
		endTS := ckp.GetEnd()
		// Protect checkpoints whose end timestamp is <= protected timestamp
		if endTS.LE(&protectedTS2) {
			continue // Skip protected checkpoints
		}
		finalFiltered = append(finalFiltered, ckp)
	}

	// Only ckp3 and ckp4 should remain (end > protectedTS)
	require.Equal(t, 2, len(finalFiltered))
	end1 := finalFiltered[0].GetEnd()
	end2 := finalFiltered[1].GetEnd()
	ts1 := types.BuildTS(150, 0)
	ts2 := types.BuildTS(200, 0)
	require.True(t, end1.EQ(&ts1))
	require.True(t, end2.EQ(&ts2))
}

// TestBackupProtectionExpiration tests backup protection expiration
func TestBackupProtectionExpiration(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	protectedTS := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS)

	// Manually set lastUpdateTime to 21 minutes ago
	cleaner.backupProtection.Lock()
	cleaner.backupProtection.lastUpdateTime = time.Now().Add(-21 * time.Minute)
	cleaner.backupProtection.Unlock()

	// Start GC process (which should detect and remove expired protection)
	cleaner.StartMutationTask("gc-process")
	defer cleaner.StopMutationTask()

	// Simulate Process logic: check expiration and remove if expired
	cleaner.backupProtection.Lock()
	if cleaner.backupProtection.isActive && time.Since(cleaner.backupProtection.lastUpdateTime) > 20*time.Minute {
		cleaner.backupProtection.isActive = false
		cleaner.backupProtection.protectedTS = types.TS{}
	}
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// Verify protection was removed
	ts, _, isActive := cleaner.GetBackupProtection()
	require.False(t, isActive)
	require.True(t, ts.IsEmpty())

	// Verify snapshot also reflects inactive state
	protectedTS2, isActive2 := cleaner.getBackupProtectionSnapshot()
	require.False(t, isActive2)
	require.True(t, protectedTS2.IsEmpty())
}

// TestBackupProtectionGCConsistency tests GC consistency with backup protection
func TestBackupProtectionGCConsistency(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Set initial protection
	protectedTS1 := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS1)

	// Start GC process
	cleaner.StartMutationTask("gc-process")

	// Create snapshot at GC start
	cleaner.backupProtection.Lock()
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// Verify snapshot captured initial protection
	snapshotTS, snapshotActive := cleaner.getBackupProtectionSnapshot()
	require.True(t, snapshotActive)
	require.True(t, snapshotTS.Equal(&protectedTS1))

	// Now update protection while GC is running (should not affect GC)
	protectedTS2 := types.BuildTS(200, 0)
	cleaner.UpdateBackupProtection(protectedTS2)

	// Verify current protection was updated
	currentTS, _, currentActive := cleaner.GetBackupProtection()
	require.True(t, currentActive)
	require.True(t, currentTS.Equal(&protectedTS2))

	// But snapshot should still have old value (GC consistency)
	snapshotTS2, snapshotActive2 := cleaner.getBackupProtectionSnapshot()
	require.True(t, snapshotActive2)
	require.True(t, snapshotTS2.Equal(&protectedTS1)) // Still old value

	// Test checkpoint filtering uses snapshot
	ckp := createMockCheckpointEntry(50, 100)
	// With snapshot TS=100, ckp end=100 should be protected
	require.False(t, cleaner.checkBackupProtection(ckp))

	cleaner.StopMutationTask()

	// After GC stops, GetBackupProtection should return current value
	ts, _, isActive := cleaner.GetBackupProtection()
	require.True(t, isActive)
	require.True(t, ts.Equal(&protectedTS2))
}

// TestBackupProtectionConcurrentAccess tests concurrent access to backup protection
func TestBackupProtectionConcurrentAccess(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent Set/Update operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ts := types.BuildTS(int64(100+idx*10), 0)
			if idx == 0 {
				cleaner.SetBackupProtection(ts)
			} else {
				cleaner.UpdateBackupProtection(ts)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state is consistent
	ts, _, isActive := cleaner.GetBackupProtection()
	require.True(t, isActive)
	require.True(t, !ts.IsEmpty())
}

// TestBackupProtectionUpdateWithoutSet tests UpdateBackupProtection when not active
func TestBackupProtectionUpdateWithoutSet(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Try to update without setting first
	protectedTS := types.BuildTS(100, 0)
	cleaner.UpdateBackupProtection(protectedTS)

	// Should not be active
	ts, _, isActive := cleaner.GetBackupProtection()
	require.False(t, isActive)
	require.True(t, ts.IsEmpty())
}

// TestBackupProtectionCheckpointEdgeCases tests edge cases for checkpoint protection
func TestBackupProtectionCheckpointEdgeCases(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	protectedTS := types.BuildTS(100, 0)
	cleaner.SetBackupProtection(protectedTS)

	cleaner.StartMutationTask("gc-process")
	defer cleaner.StopMutationTask()

	cleaner.backupProtection.Lock()
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// Test with empty checkpoint (should not panic)
	emptyCkp := &checkpoint.CheckpointEntry{}
	// Empty checkpoint should be allowed (return true means can GC)
	require.True(t, cleaner.checkBackupProtection(emptyCkp))

	// Test with non-checkpoint item (should allow GC)
	require.True(t, cleaner.checkBackupProtection("not-a-checkpoint"))

	// Test with checkpoint exactly at protected TS
	exactCkp := createMockCheckpointEntry(50, 100)            // end == protectedTS
	require.False(t, cleaner.checkBackupProtection(exactCkp)) // Should be protected

	// Test with checkpoint just before protected TS
	beforeCkp := createMockCheckpointEntry(50, 99)             // end < protectedTS
	require.False(t, cleaner.checkBackupProtection(beforeCkp)) // Should be protected

	// Test with checkpoint just after protected TS
	afterCkp := createMockCheckpointEntry(100, 101)          // end > protectedTS
	require.True(t, cleaner.checkBackupProtection(afterCkp)) // Should NOT be protected
}

// TestBackupProtectionNoProtectionActive tests behavior when no protection is active
func TestBackupProtectionNoProtectionActive(t *testing.T) {
	cleaner := createTestCheckpointCleaner(t)

	// Don't set protection
	cleaner.StartMutationTask("gc-process")
	defer cleaner.StopMutationTask()

	// Update snapshot (no protection)
	cleaner.backupProtection.Lock()
	cleaner.mutation.backupProtectionSnapshot.protectedTS = cleaner.backupProtection.protectedTS
	cleaner.mutation.backupProtectionSnapshot.isActive = cleaner.backupProtection.isActive
	cleaner.backupProtection.Unlock()

	// All checkpoints should be allowed to GC
	ckp1 := createMockCheckpointEntry(0, 50)
	ckp2 := createMockCheckpointEntry(50, 100)
	ckp3 := createMockCheckpointEntry(100, 150)

	require.True(t, cleaner.checkBackupProtection(ckp1))
	require.True(t, cleaner.checkBackupProtection(ckp2))
	require.True(t, cleaner.checkBackupProtection(ckp3))
}
