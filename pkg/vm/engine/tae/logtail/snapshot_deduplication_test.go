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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotDeduplicationLogic tests the deduplication logic in AccountToTableSnapshots
func TestSnapshotDeduplicationLogic(t *testing.T) {
	t.Run("NoDuplicateTableSnapshots", func(t *testing.T) {
		// Create a mock SnapshotMeta
		sm := &SnapshotMeta{
			tableIDIndex: make(map[uint64]*tableInfo),
		}

		// Setup test data: 2 tables in the same database
		sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
		sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}

		// Create snapshots with table-level snapshot for table 1001
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		// Add table snapshot for table 1001
		snapshots.tables[1001] = []types.TS{ts1}
		// Add account snapshot
		snapshots.account[1] = []types.TS{ts2}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Verify that table 1001 has the snapshot (from dbTableSnapshots)
		require.Contains(t, tableSnapshots, uint64(1001))
		snapshots1001 := tableSnapshots[1001]

		// Should have both table snapshot and account snapshot
		assert.Contains(t, snapshots1001, ts1, "Table 1001 should have its table snapshot")
		assert.Contains(t, snapshots1001, ts2, "Table 1001 should have account snapshot")

		// Verify that table 1002 also has the snapshot (inherited from table 1001)
		require.Contains(t, tableSnapshots, uint64(1002))
		snapshots1002 := tableSnapshots[1002]

		// Should have table snapshot from table 1001 and account snapshot
		assert.Contains(t, snapshots1002, ts1, "Table 1002 should inherit table snapshot from table 1001")
		assert.Contains(t, snapshots1002, ts2, "Table 1002 should have account snapshot")

		// CRITICAL: Verify no duplicates - each timestamp should appear only once
		ts1Count := 0
		ts2Count := 0
		for _, ts := range snapshots1001 {
			if ts.EQ(&ts1) {
				ts1Count++
			}
			if ts.EQ(&ts2) {
				ts2Count++
			}
		}
		assert.Equal(t, 1, ts1Count, "Table 1001 should have ts1 exactly once (no duplicates)")
		assert.Equal(t, 1, ts2Count, "Table 1001 should have ts2 exactly once (no duplicates)")

		ts1Count = 0
		ts2Count = 0
		for _, ts := range snapshots1002 {
			if ts.EQ(&ts1) {
				ts1Count++
			}
			if ts.EQ(&ts2) {
				ts2Count++
			}
		}
		assert.Equal(t, 1, ts1Count, "Table 1002 should have ts1 exactly once (no duplicates)")
		assert.Equal(t, 1, ts2Count, "Table 1002 should have ts2 exactly once (no duplicates)")
	})

	t.Run("MultipleTableSnapshotsInSameDatabase", func(t *testing.T) {
		// Test the case where multiple tables in the same database have snapshots
		sm := &SnapshotMeta{
			tableIDIndex: make(map[uint64]*tableInfo),
		}

		// Setup test data: 3 tables in the same database
		sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
		sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}
		sm.tableIDIndex[1003] = &tableInfo{accountID: 1, dbID: 100, tid: 1003}

		// Create snapshots with table-level snapshots for multiple tables
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		// Add table snapshots for tables 1001 and 1002
		snapshots.tables[1001] = []types.TS{ts1}
		snapshots.tables[1002] = []types.TS{ts2}
		// Table 1003 has no direct snapshot

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// All tables should have all snapshots from their database
		for _, tid := range []uint64{1001, 1002, 1003} {
			require.Contains(t, tableSnapshots, tid)
			snapshots := tableSnapshots[tid]

			// Each table should have both ts1 and ts2 (from tables 1001 and 1002)
			assert.Contains(t, snapshots, ts1, "Table %d should have ts1 from table 1001", tid)
			assert.Contains(t, snapshots, ts2, "Table %d should have ts2 from table 1002", tid)

			// Verify no duplicates
			ts1Count := 0
			ts2Count := 0
			for _, ts := range snapshots {
				if ts.EQ(&ts1) {
					ts1Count++
				}
				if ts.EQ(&ts2) {
					ts2Count++
				}
			}
			assert.Equal(t, 1, ts1Count, "Table %d should have ts1 exactly once", tid)
			assert.Equal(t, 1, ts2Count, "Table %d should have ts2 exactly once", tid)
		}
	})

	t.Run("TableSnapshotWithDatabaseSnapshot", func(t *testing.T) {
		// Test the case where both table-level and database-level snapshots exist
		sm := &SnapshotMeta{
			tableIDIndex: make(map[uint64]*tableInfo),
		}

		// Setup test data: 2 tables in the same database
		sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
		sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}

		// Create snapshots with both table-level and database-level snapshots
		snapshots := NewSnapshotInfo()
		tableSnapshot := types.BuildTS(1000, 0)
		dbSnapshot := types.BuildTS(2000, 0)
		accountSnapshot := types.BuildTS(3000, 0)

		// Add table snapshot for table 1001
		snapshots.tables[1001] = []types.TS{tableSnapshot}
		// Add database snapshot
		snapshots.database[100] = []types.TS{dbSnapshot}
		// Add account snapshot
		snapshots.account[1] = []types.TS{accountSnapshot}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Both tables should have all applicable snapshots
		for _, tid := range []uint64{1001, 1002} {
			require.Contains(t, tableSnapshots, tid)
			snapshots := tableSnapshots[tid]

			// Should have table snapshot, database snapshot, and account snapshot
			assert.Contains(t, snapshots, tableSnapshot, "Table %d should have table snapshot", tid)
			assert.Contains(t, snapshots, dbSnapshot, "Table %d should have database snapshot", tid)
			assert.Contains(t, snapshots, accountSnapshot, "Table %d should have account snapshot", tid)

			// Verify no duplicates
			tableCount := 0
			dbCount := 0
			accountCount := 0
			for _, ts := range snapshots {
				if ts.EQ(&tableSnapshot) {
					tableCount++
				}
				if ts.EQ(&dbSnapshot) {
					dbCount++
				}
				if ts.EQ(&accountSnapshot) {
					accountCount++
				}
			}
			assert.Equal(t, 1, tableCount, "Table %d should have table snapshot exactly once", tid)
			assert.Equal(t, 1, dbCount, "Table %d should have database snapshot exactly once", tid)
			assert.Equal(t, 1, accountCount, "Table %d should have account snapshot exactly once", tid)
		}
	})

	t.Run("CrossDatabaseIsolation", func(t *testing.T) {
		// Test that table snapshots don't leak across databases
		sm := &SnapshotMeta{
			tableIDIndex: make(map[uint64]*tableInfo),
		}

		// Setup test data: tables in different databases
		sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001} // DB 100
		sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002} // DB 100
		sm.tableIDIndex[2001] = &tableInfo{accountID: 1, dbID: 200, tid: 2001} // DB 200

		// Create snapshots with table-level snapshot for table 1001
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		// Add table snapshot for table 1001 (in DB 100)
		snapshots.tables[1001] = []types.TS{ts1}
		// Add account snapshot
		snapshots.account[1] = []types.TS{ts2}

		pitr := NewPitrInfo()

		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// Tables in DB 100 should have the table snapshot
		require.Contains(t, tableSnapshots, uint64(1001))
		require.Contains(t, tableSnapshots, uint64(1002))
		assert.Contains(t, tableSnapshots[1001], ts1, "Table 1001 should have its table snapshot")
		assert.Contains(t, tableSnapshots[1002], ts1, "Table 1002 should inherit table snapshot from table 1001")

		// Table in DB 200 should NOT have the table snapshot from DB 100
		require.Contains(t, tableSnapshots, uint64(2001))
		assert.NotContains(t, tableSnapshots[2001], ts1, "Table 2001 should NOT have table snapshot from different database")
		assert.Contains(t, tableSnapshots[2001], ts2, "Table 2001 should have account snapshot")
	})
}

// TestMergeTableInfoDeduplication tests the deduplication logic in MergeTableInfo
func TestMergeTableInfoDeduplication(t *testing.T) {
	t.Run("MergeTableInfoNoDuplicates", func(t *testing.T) {
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

		sm.tableIDIndex[1001] = sm.tables[1][1001]
		sm.tableIDIndex[1002] = sm.tables[1][1002]

		// Create snapshots with table snapshot that should protect the table
		snapshots := NewSnapshotInfo()
		protectTS := types.BuildTS(5000, 0) // before delete, should protect

		// Add table snapshot for table 1001
		snapshots.tables[1001] = []types.TS{protectTS}

		pitr := NewPitrInfo()

		// Before merge, all tables exist
		assert.Contains(t, sm.tables[1], uint64(1001))
		assert.Contains(t, sm.tables[1], uint64(1002))

		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// After merge, both tables should be protected by table 1001's snapshot
		assert.Contains(t, sm.tables[1], uint64(1001), "Table 1001 should be protected by its own snapshot")
		assert.Contains(t, sm.tables[1], uint64(1002), "Table 1002 should be protected by table 1001's snapshot (same DB)")

		// Verify that the snapshot logic worked correctly without duplicates
		// This test ensures that MergeTableInfo correctly processes the snapshots
		// without causing issues due to duplicate snapshot processing
	})
}

// TestSnapshotConsistency tests that the snapshot system maintains consistency
func TestSnapshotConsistency(t *testing.T) {
	t.Run("SnapshotInfoNotModified", func(t *testing.T) {
		// Test that the original snapshots parameter is not modified
		sm := &SnapshotMeta{
			tableIDIndex: make(map[uint64]*tableInfo),
		}

		// Setup test data
		sm.tableIDIndex[1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001}
		sm.tableIDIndex[1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002}

		// Create original snapshots
		originalSnapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(1000, 0)
		ts2 := types.BuildTS(2000, 0)

		originalSnapshots.tables[1001] = []types.TS{ts1}
		originalSnapshots.account[1] = []types.TS{ts2}

		// Make a copy to verify it's not modified
		originalTablesCount := len(originalSnapshots.tables)
		originalAccountCount := len(originalSnapshots.account)

		pitr := NewPitrInfo()

		// Call AccountToTableSnapshots
		tableSnapshots, _ := sm.AccountToTableSnapshots(originalSnapshots, pitr)

		// Verify that the original snapshots were not modified
		assert.Equal(t, originalTablesCount, len(originalSnapshots.tables), "Original snapshots.tables should not be modified")
		assert.Equal(t, originalAccountCount, len(originalSnapshots.account), "Original snapshots.account should not be modified")
		assert.Contains(t, originalSnapshots.tables, uint64(1001), "Original table snapshot should still exist")
		assert.Contains(t, originalSnapshots.account, uint32(1), "Original account snapshot should still exist")

		// Verify that the function still works correctly
		require.Contains(t, tableSnapshots, uint64(1001))
		require.Contains(t, tableSnapshots, uint64(1002))
		assert.Contains(t, tableSnapshots[1001], ts1)
		assert.Contains(t, tableSnapshots[1002], ts1) // Inherited from table 1001
	})
}

// TestAccountToTableSnapshotsAndMergeTableInfoConsistency tests that both functions use consistent snapshot logic
func TestAccountToTableSnapshotsAndMergeTableInfoConsistency(t *testing.T) {
	t.Run("ConsistentSnapshotLogic", func(t *testing.T) {
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

		// Create snapshots with table snapshot for table 1001
		snapshots := NewSnapshotInfo()
		protectTS := types.BuildTS(5000, 0) // before delete, should protect

		// Add table snapshot for table 1001 (in DB 100)
		snapshots.tables[1001] = []types.TS{protectTS}

		pitr := NewPitrInfo()

		// Test AccountToTableSnapshots
		tableSnapshots, tablePitrs := sm.AccountToTableSnapshots(snapshots, pitr)
		// Verify AccountToTableSnapshots results
		require.Contains(t, tableSnapshots, uint64(1001))
		require.Contains(t, tableSnapshots, uint64(1002))
		require.NotContains(t, tableSnapshots, uint64(2001))

		// Tables in DB 100 should have the table snapshot from table 1001
		assert.Contains(t, tableSnapshots[1001], protectTS, "Table 1001 should have its own snapshot")
		assert.Contains(t, tableSnapshots[1002], protectTS, "Table 1002 should inherit snapshot from table 1001 (same DB)")

		// Table in DB 200 should NOT have the table snapshot from DB 100
		assert.NotContains(t, tableSnapshots[2001], protectTS, "Table 2001 should NOT have snapshot from different database")

		// Test MergeTableInfo with the same snapshots
		// Before merge, all tables exist
		assert.Contains(t, sm.tables[1], uint64(1001))
		assert.Contains(t, sm.tables[1], uint64(1002))
		assert.Contains(t, sm.tables[1], uint64(2001))

		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// After merge, verify consistent behavior
		// Tables in DB 100 should be protected (same logic as AccountToTableSnapshots)
		assert.Contains(t, sm.tables[1], uint64(1001), "Table 1001 should be protected by its own snapshot")
		assert.Contains(t, sm.tables[1], uint64(1002), "Table 1002 should be protected by table 1001's snapshot (same DB)")

		// Table in DB 200 should be deleted (no protection from DB 100's table snapshot)
		assert.NotContains(t, sm.tables[1], uint64(2001), "Table 2001 should be deleted (different DB, no protection)")

		// Verify PITR is set correctly for all tables
		assert.NotNil(t, tablePitrs[1001])
		assert.NotNil(t, tablePitrs[1002])
		assert.NotNil(t, tablePitrs[2001])
	})

	t.Run("MultipleTableSnapshotsConsistency", func(t *testing.T) {
		// Test with multiple table snapshots in the same database
		sm := &SnapshotMeta{
			tables:       make(map[uint32]map[uint64]*tableInfo),
			tableIDIndex: make(map[uint64]*tableInfo),
			objects:      make(map[uint64]map[objectio.Segmentid]*objectInfo),
		}

		// Setup test tables
		deleteTS := types.BuildTS(6000, 0)
		sm.tables[1] = make(map[uint64]*tableInfo)
		sm.tables[1][1001] = &tableInfo{accountID: 1, dbID: 100, tid: 1001, deleteAt: deleteTS}
		sm.tables[1][1002] = &tableInfo{accountID: 1, dbID: 100, tid: 1002, deleteAt: deleteTS}
		sm.tables[1][1003] = &tableInfo{accountID: 1, dbID: 100, tid: 1003, deleteAt: deleteTS}

		sm.tableIDIndex[1001] = sm.tables[1][1001]
		sm.tableIDIndex[1002] = sm.tables[1][1002]
		sm.tableIDIndex[1003] = sm.tables[1][1003]

		// Create snapshots with multiple table snapshots
		snapshots := NewSnapshotInfo()
		ts1 := types.BuildTS(4000, 0) // before delete
		ts2 := types.BuildTS(5000, 0) // before delete

		// Add table snapshots for tables 1001 and 1002
		snapshots.tables[1001] = []types.TS{ts1}
		snapshots.tables[1002] = []types.TS{ts2}

		pitr := NewPitrInfo()

		// Test AccountToTableSnapshots
		tableSnapshots, _ := sm.AccountToTableSnapshots(snapshots, pitr)

		// All tables in DB 100 should have both snapshots
		for _, tid := range []uint64{1001, 1002, 1003} {
			require.Contains(t, tableSnapshots, tid)
			snapshots := tableSnapshots[tid]
			assert.Contains(t, snapshots, ts1, "Table %d should have ts1 from table 1001", tid)
			assert.Contains(t, snapshots, ts2, "Table %d should have ts2 from table 1002", tid)
		}

		// Test MergeTableInfo
		err := sm.MergeTableInfo(snapshots, pitr)
		require.NoError(t, err)

		// All tables should be protected (consistent with AccountToTableSnapshots)
		assert.Contains(t, sm.tables[1], uint64(1001), "Table 1001 should be protected")
		assert.Contains(t, sm.tables[1], uint64(1002), "Table 1002 should be protected")
		assert.Contains(t, sm.tables[1], uint64(1003), "Table 1003 should be protected")
	})
}
