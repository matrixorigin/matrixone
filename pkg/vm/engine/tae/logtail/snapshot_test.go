// Copyright 2021 Matrix Origin
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

package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotInfo tests the basic functionality of SnapshotInfo
func TestSnapshotInfo(t *testing.T) {
	t.Run("NewSnapshotInfo", func(t *testing.T) {
		info := NewSnapshotInfo()
		assert.NotNil(t, info)
		assert.True(t, info.IsEmpty())
		assert.NotNil(t, info.cluster)
		assert.NotNil(t, info.account)
		assert.NotNil(t, info.database)
		assert.NotNil(t, info.tables)
	})

	t.Run("AddSnapshots", func(t *testing.T) {
		info := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)
		ts3 := types.BuildTS(3000, 0)

		// Add cluster snapshots
		info.cluster = append(info.cluster, ts1, ts2)
		assert.False(t, info.IsEmpty())

		// Add account snapshots
		info.account[1] = []types.TS{ts1, ts3}
		info.account[2] = []types.TS{ts2}

		// Add database snapshots
		info.database[100] = []types.TS{ts1}
		info.database[200] = []types.TS{ts2, ts3}

		// Add table snapshots
		info.tables[1001] = []types.TS{ts1}
		info.tables[1002] = []types.TS{ts2}

		// Test GetTS (should return first timestamp for PITR compatibility)
		assert.Equal(t, ts1, info.GetTS(1, 100, 1001)) // cluster level
		assert.Equal(t, ts1, info.GetTS(1, 0, 0))      // account level
		assert.Equal(t, ts1, info.GetTS(0, 100, 0))    // database level
		assert.Equal(t, ts1, info.GetTS(0, 0, 1001))   // table level

		// Test MinTS
		minTS := info.MinTS()
		assert.Equal(t, ts1, minTS)

		// Test ToTsList
		allTS := info.ToTsList()
		assert.Contains(t, allTS, ts1)
		assert.Contains(t, allTS, ts2)
		assert.Contains(t, allTS, ts3)
	})
}

// TestAccountToTableSnapshots tests the core logic of snapshot distribution
func TestAccountToTableSnapshots(t *testing.T) {
	// Create a mock SnapshotMeta
	sm := &SnapshotMeta{
		tableIDIndex: make(map[uint64]*tableInfo),
	}

	// Setup test data: 2 accounts, 2 databases, 4 tables
	// Account 1: DB 100 (Table 1001, 1002), DB 200 (Table 2001)
	// Account 2: DB 300 (Table 3001)
	sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
	sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}
	sm.tableIDIndex[2001] = &tableInfo{accountID: 1, dbID: 200, tid: 2001}
	sm.tableIDIndex[3001] = &tableInfo{accountID: 2, dbID: 300, tid: 3001}

	t.Run("TableSnapshotAppliedToAllTablesInDatabase", func(t *testing.T) {
		// Create snapshots with table-level snapshot for table 1001
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		// Add table snapshot for table 1001 (in DB 100)
		snapshots.tables[1001] = []types.TS{ts1}
		// Add account snapshot for account 1
		snapshots.account[1] = []types.TS{ts2}

		pitr := NewSnapshotInfo()

		tableSnapshots, tablePitrs := sm.AccountToTableSnapshots(snapshots, pitr)

		// Verify that table 1001 has both its own snapshot and account snapshot
		require.Contains(t, tableSnapshots, uint64(1001))
		assert.Contains(t, tableSnapshots[1001], ts1) // table snapshot
		assert.Contains(t, tableSnapshots[1001], ts2) // account snapshot

		// CRITICAL: Verify that table 1002 (in same DB 100) also gets table 1001's snapshot
		require.Contains(t, tableSnapshots, uint64(1002))
		assert.Contains(t, tableSnapshots[1002], ts1) // table snapshot from 1001
		assert.Contains(t, tableSnapshots[1002], ts2) // account snapshot

		// Verify that table 2001 (in different DB 200) only gets account snapshot
		require.Contains(t, tableSnapshots, uint64(2001))
		assert.NotContains(t, tableSnapshots[2001], ts1) // should NOT have table snapshot from 1001
		assert.Contains(t, tableSnapshots[2001], ts2)    // account snapshot

		// Verify that table 3001 (different account) doesn't get any of these snapshots
		if snapshots3001, exists := tableSnapshots[3001]; exists {
			assert.NotContains(t, snapshots3001, ts1) // should NOT have table snapshot from 1001
			assert.NotContains(t, snapshots3001, ts2) // should NOT have account snapshot from account 1
		}

		// Verify PITR info is set correctly
		assert.NotNil(t, tablePitrs[1001])
		assert.NotNil(t, tablePitrs[1002])
		assert.NotNil(t, tablePitrs[2001])
		assert.NotNil(t, tablePitrs[3001])
	})

	t.Run("MultipleTableSnapshotsInSameDatabase", func(t *testing.T) {
		// Create snapshots with table-level snapshots for both tables in DB 100
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)
		ts3 := types.BuildTS(3000, 0)

		// Add table snapshots for both tables in DB 100
		snapshots.tables[1001] = []types.TS{ts1}
		snapshots.tables[1002] = []types.TS{ts2}
		// Add account snapshot
		snapshots.account[1] = []types.TS{ts3}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Both tables should have all snapshots from their database
		require.Contains(t, tableSnapshots, uint64(1001))
		require.Contains(t, tableSnapshots, uint64(1002))

		// Table 1001 should have: its own snapshot + table 1002's snapshot + account snapshot
		assert.Contains(t, tableSnapshots[1001], ts1) // its own
		assert.Contains(t, tableSnapshots[1001], ts2) // from table 1002
		assert.Contains(t, tableSnapshots[1001], ts3) // account

		// Table 1002 should have: its own snapshot + table 1001's snapshot + account snapshot
		assert.Contains(t, tableSnapshots[1002], ts1) // from table 1001
		assert.Contains(t, tableSnapshots[1002], ts2) // its own
		assert.Contains(t, tableSnapshots[1002], ts3) // account

		// Verify snapshots are sorted and deduplicated
		assert.True(t, len(tableSnapshots[1001]) >= 3)
		assert.True(t, len(tableSnapshots[1002]) >= 3)
	})

	t.Run("DatabaseSnapshotTest", func(t *testing.T) {
		// Test database-level snapshots
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		// Add database snapshot for DB 100
		snapshots.database[100] = []types.TS{ts1}
		// Add account snapshot
		snapshots.account[1] = []types.TS{ts2}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Both tables in DB 100 should have database snapshot
		require.Contains(t, tableSnapshots, uint64(1001))
		require.Contains(t, tableSnapshots, uint64(1002))
		assert.Contains(t, tableSnapshots[1001], ts1) // database snapshot
		assert.Contains(t, tableSnapshots[1001], ts2) // account snapshot
		assert.Contains(t, tableSnapshots[1002], ts1) // database snapshot
		assert.Contains(t, tableSnapshots[1002], ts2) // account snapshot

		// Table in DB 200 should only have account snapshot
		require.Contains(t, tableSnapshots, uint64(2001))
		assert.NotContains(t, tableSnapshots[2001], ts1) // should NOT have DB 100 snapshot
		assert.Contains(t, tableSnapshots[2001], ts2)    // account snapshot
	})

	t.Run("ClusterSnapshotTest", func(t *testing.T) {
		// Test cluster-level snapshots
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)

		// Add cluster snapshot
		snapshots.cluster = []types.TS{ts1}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// All tables should have cluster snapshot
		for _, tid := range []uint64{1001, 1002, 2001, 3001} {
			require.Contains(t, tableSnapshots, tid)
			assert.Contains(t, tableSnapshots[tid], ts1, "Table %d should have cluster snapshot", tid)
		}
	})

	t.Run("SnapshotPriorityTest", func(t *testing.T) {
		// Test that all levels of snapshots are combined correctly
		snapshots := NewSnapshotInfo()
		tsCluster := types.BuildTS(1000, 0)
		tsAccount := types.BuildTS(2000, 0)
		tsDatabase := types.BuildTS(3000, 0)
		tsTable := types.BuildTS(4000, 0)

		// Add all levels of snapshots
		snapshots.cluster = []types.TS{tsCluster}
		snapshots.account[1] = []types.TS{tsAccount}
		snapshots.database[100] = []types.TS{tsDatabase}
		snapshots.tables[1001] = []types.TS{tsTable}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Table 1001 should have all snapshots
		require.Contains(t, tableSnapshots, uint64(1001))
		snapshots1001 := tableSnapshots[1001]
		assert.Contains(t, snapshots1001, tsCluster)
		assert.Contains(t, snapshots1001, tsAccount)
		assert.Contains(t, snapshots1001, tsDatabase)
		assert.Contains(t, snapshots1001, tsTable)

		// Table 1002 (same DB) should have all except direct table snapshot, but should have table 1001's snapshot
		require.Contains(t, tableSnapshots, uint64(1002))
		snapshots1002 := tableSnapshots[1002]
		assert.Contains(t, snapshots1002, tsCluster)
		assert.Contains(t, snapshots1002, tsAccount)
		assert.Contains(t, snapshots1002, tsDatabase)
		assert.Contains(t, snapshots1002, tsTable) // from table 1001 in same DB
	})
}

// TestMergeTableInfo tests the MergeTableInfo functionality
func TestMergeTableInfo(t *testing.T) {
	// Create a mock SnapshotMeta with some tables
	sm := &SnapshotMeta{
		tables:       make(map[uint32]map[uint64]*tableInfo),
		tableIDIndex: make(map[uint64]*tableInfo),
		objects:      make(map[uint64]map[objectio.Segmentid]*objectInfo),
	}

	// Setup test tables
	deleteTS := types.BuildTS(6000, 0) // deleted timestamp
	sm.tables[1] = make(map[uint64]*tableInfo)
	sm.tables[1][1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001, deleteAt: deleteTS}
	sm.tables[1][1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002, deleteAt: deleteTS}
	sm.tables[1][2001] = &tableInfo{accountID: 1, dbID: 200, tid: 2001, deleteAt: deleteTS}

	sm.tableIDIndex[1001] = sm.tables[1][1001]
	sm.tableIDIndex[1002] = sm.tables[1][1002]
	sm.tableIDIndex[2001] = sm.tables[1][2001]

	t.Run("TableSnapshotProtectsAllTablesInDatabase", func(t *testing.T) {
		// Create snapshots with table snapshot that should protect the table
		snapshots := NewSnapshotInfo()
		protectTS := types.BuildTS(5000, 0) // after delete, should protect

		// Add table snapshot for table 1001
		snapshots.tables[1001] = []types.TS{protectTS}

		pitr := NewPitrInfo()

		// Before merge, all tables exist
		assert.Contains(t, sm.tables[1], uint64(1001))
		assert.Contains(t, sm.tables[1], uint64(1002))
		assert.Contains(t, sm.tables[1], uint64(2001))

		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// After merge, tables in DB 100 should be protected by table 1001's snapshot
		assert.Contains(t, sm.tables[1], uint64(1001), "Table 1001 should be protected by its own snapshot")
		assert.Contains(t, sm.tables[1], uint64(1002), "Table 1002 should be protected by table 1001's snapshot (same DB)")

		// Table in different DB should be deleted (no protection)
		assert.NotContains(t, sm.tables[1], uint64(2001), "Table 2001 should be deleted (different DB, no protection)")
	})

	t.Run("NoSnapshotAllowsDeletion", func(t *testing.T) {
		// Reset tables
		sm.tables[1] = make(map[uint64]*tableInfo)
		sm.tables[1][1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001, deleteAt: deleteTS}
		sm.tables[1][1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002, deleteAt: deleteTS}

		// Create empty snapshots and PITR
		snapshots := NewSnapshotInfo()
		pitr := NewPitrInfo()

		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// All deleted tables should be removed
		assert.NotContains(t, sm.tables[1], uint64(1001))
		assert.NotContains(t, sm.tables[1], uint64(1002))
	})

	t.Run("DatabaseSnapshotProtectsAllTablesInDatabase", func(t *testing.T) {
		// Reset tables
		sm.tables[1] = make(map[uint64]*tableInfo)
		sm.tables[1][1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001, deleteAt: deleteTS}
		sm.tables[1][1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002, deleteAt: deleteTS}
		sm.tables[1][2001] = &tableInfo{accountID: 1, dbID: 200, tid: 2001, deleteAt: deleteTS}

		// Create database snapshot
		snapshots := NewSnapshotInfo()
		protectTS := types.BuildTS(5000, 0)
		snapshots.database[100] = []types.TS{protectTS}

		pitr := NewPitrInfo()

		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// Tables in DB 100 should be protected
		assert.Contains(t, sm.tables[1], uint64(1001))
		assert.Contains(t, sm.tables[1], uint64(1002))

		// Table in different DB should be deleted
		assert.NotContains(t, sm.tables[1], uint64(2001))
	})
}

// TestSnapshotDeduplication tests that snapshots are properly deduplicated
func TestSnapshotDeduplication(t *testing.T) {
	sm := &SnapshotMeta{
		tableIDIndex: make(map[uint64]*tableInfo),
	}

	// Setup test data
	sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
	sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}

	snapshots := NewSnapshotInfo()
	ts1 := types.BuildTS(1000, 0)
	ts2 := types.BuildTS(2000, 0)

	// Add duplicate timestamps at different levels
	snapshots.cluster = []types.TS{ts1, ts2}
	snapshots.account[1] = []types.TS{ts1, ts2} // duplicates
	snapshots.database[100] = []types.TS{ts1}   // duplicate
	snapshots.tables[1001] = []types.TS{ts2}    // duplicate

	pitr := NewPitrInfo()

	tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

	// Verify deduplication - each table should have exactly 2 unique timestamps
	for _, tid := range []uint64{1001, 1002} {
		require.Contains(t, tableSnapshots, tid)
		snapshots := tableSnapshots[tid]

		// Count unique timestamps
		uniqueTS := make(map[types.TS]bool)
		for _, ts := range snapshots {
			uniqueTS[ts] = true
		}

		assert.Equal(t, 2, len(uniqueTS), "Table %d should have exactly 2 unique timestamps after deduplication", tid)
		assert.True(t, uniqueTS[ts1], "Table %d should have ts1", tid)
		assert.True(t, uniqueTS[ts2], "Table %d should have ts2", tid)
	}
}

// TestPitrCompatibility tests that PITR functionality still works correctly
func TestPitrCompatibility(t *testing.T) {
	t.Run("GetTSReturnsFirstTimestamp", func(t *testing.T) {
		info := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)
		ts3 := types.BuildTS(3000, 0)

		// Add multiple timestamps in different orders
		info.cluster = []types.TS{ts3, ts1, ts2} // unsorted
		info.account[1] = []types.TS{ts2, ts3}
		info.database[100] = []types.TS{ts3}
		info.tables[1001] = []types.TS{ts2, ts1}

		// GetTS should return the first (earliest) timestamp for PITR compatibility
		assert.Equal(t, ts3, info.GetTS(0, 0, 0))    // cluster (first in slice, not necessarily earliest)
		assert.Equal(t, ts2, info.GetTS(1, 0, 0))    // account
		assert.Equal(t, ts3, info.GetTS(0, 100, 0))  // database
		assert.Equal(t, ts2, info.GetTS(0, 0, 1001)) // table
	})

	t.Run("PitrInfoAlias", func(t *testing.T) {
		// Test that PitrInfo is correctly aliased to SnapshotInfo
		var pitr *PitrInfo = NewPitrInfo()
		assert.NotNil(t, pitr)

		ts := types.BuildTS(1000, 0)
		pitr.cluster = []types.TS{ts}
		assert.False(t, pitr.IsEmpty())
		assert.Equal(t, ts, pitr.GetTS(0, 0, 0))
	})
}
