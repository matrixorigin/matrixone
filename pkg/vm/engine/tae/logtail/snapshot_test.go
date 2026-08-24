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
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

	t.Run("EmptyPITRPlaceholder", func(t *testing.T) {
		pitr := NewPitrInfo()
		assert.True(t, pitr.IsEmpty())
		pitr.account[1] = []types.TS{{}}
		assert.True(t, pitr.IsEmpty())
		pitr.account[1] = []types.TS{types.BuildTS(1, 0)}
		assert.False(t, pitr.IsEmpty())
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

func TestCopyObjectsLockedDeepCopiesEntries(t *testing.T) {
	segmentID := *objectio.NewSegmentid()
	originalDeleteAt := types.BuildTS(10, 1)
	objects := map[uint64]map[objectio.Segmentid]*objectInfo{
		42: {
			segmentID: {deleteAt: originalDeleteAt},
		},
	}

	copied := copyObjectsLocked(objects)
	require.NotSame(t, objects[42][segmentID], copied[42][segmentID])
	objects[42][segmentID].deleteAt = types.BuildTS(20, 2)
	require.Equal(t, originalDeleteAt, copied[42][segmentID].deleteAt)
}

func TestParseSnapshotTS(t *testing.T) {
	ts, err := parseSnapshotTS("10-2")
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(10, 2), ts)

	for _, value := range []string{"", "x-1", "1-x", "1-2-3", "-1-0"} {
		t.Run(value, func(t *testing.T) {
			_, err := parseSnapshotTS(value)
			require.Error(t, err)
		})
	}
}

func TestRetainMinimumISCPWatermarkKeepsEmptyLowerBound(t *testing.T) {
	tables := make(map[uint64]types.TS)
	retainMinimumISCPWatermark(tables, 42, types.TS{})
	retainMinimumISCPWatermark(tables, 42, types.BuildTS(20, 0))
	require.Equal(t, types.TS{}, tables[42])

	retainMinimumISCPWatermark(tables, 7, types.BuildTS(20, 0))
	retainMinimumISCPWatermark(tables, 7, types.BuildTS(10, 0))
	retainMinimumISCPWatermark(tables, 7, types.BuildTS(30, 0))
	require.Equal(t, types.BuildTS(10, 0), tables[7])
}

func TestCheckedSnapshotAccountIDRejectsTruncation(t *testing.T) {
	id, err := checkedSnapshotAccountID(context.Background(), "snapshot", math.MaxUint32)
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), id)

	_, err = checkedSnapshotAccountID(
		context.Background(), "snapshot", uint64(math.MaxUint32)+1,
	)
	require.ErrorContains(t, err, "exceeds uint32")
}

func TestGetSnapshotsByLevelRejectsAccountIDTruncation(t *testing.T) {
	info := NewSnapshotInfo()
	info.account[0] = []types.TS{types.BuildTS(1, 0)}
	require.Nil(t, info.GetSnapshotsByLevel(
		PitrLevelAccount, uint64(math.MaxUint32)+1,
	))
}

func TestCollectCheckpointObjectMutationsConvertsReaderPanicToError(t *testing.T) {
	data, tombstones, err := collectCheckpointObjectMutations(
		context.Background(), &CKPReader{withTableID: true},
	)
	require.ErrorContains(t, err, "checkpoint iteration failed")
	require.Nil(t, data)
	require.Nil(t, tombstones)
}

func TestCollectTableInfoUpdatePlanRejectsReversedRange(t *testing.T) {
	plan, err := collectTableInfoUpdatePlan(
		context.Background(),
		testutil.NewSharedFS(),
		nil,
		nil,
		types.BuildTS(2, 0),
		types.BuildTS(1, 0),
	)
	require.Nil(t, plan)
	require.ErrorContains(t, err, "is reversed")
}

func TestApplyTableInfoUpdatePlanRejectsInvalidStateAtomically(t *testing.T) {
	sm := NewSnapshotMeta()
	sm.tables[7] = map[uint64]*tableInfo{8: nil}
	pending := types.BuildTS(9, 0)
	err := sm.applyTableInfoUpdatePlan(&tableInfoUpdatePlan{
		pendingAObjectDeletes: map[types.TS]struct{}{pending: {}},
	})
	require.ErrorContains(t, err, "has nil metadata")
	require.NotContains(t, sm.aobjDelTsMap, pending)
}

func TestForEachObjectBlockLocation(t *testing.T) {
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 3))

	var blockIDs []uint16
	require.NoError(t, forEachObjectBlockLocation(
		context.Background(), *stats, func(location objectio.Location) error {
			blockIDs = append(blockIDs, location.ID())
			return nil
		},
	))
	require.Equal(t, []uint16{0, 1, 2}, blockIDs)

	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 0))
	err := forEachObjectBlockLocation(
		context.Background(), *stats, func(objectio.Location) error { return nil },
	)
	require.ErrorContains(t, err, "has no blocks")

	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1<<16+1))
	err = forEachObjectBlockLocation(
		context.Background(), *stats, func(objectio.Location) error { return nil },
	)
	require.ErrorContains(t, err, "unsupported block count")
}

func TestSnapshotMetaRoundTripKeepsSpecialTombstonesSeparate(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	source := NewSnapshotMeta()
	source.pitr.tid = 101
	source.iscp.tid = 202

	pitrStats := objectio.NewObjectStats()
	pitrName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	require.NoError(t, objectio.SetObjectStatsObjectName(pitrStats, pitrName))
	iscpStats := objectio.NewObjectStats()
	iscpName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	require.NoError(t, objectio.SetObjectStatsObjectName(iscpStats, iscpName))
	pitrDeleteAt := types.BuildTS(20, 1)
	iscpDeleteAt := types.BuildTS(30, 2)
	source.pitr.tombstones[pitrName.SegmentId()] = &objectInfo{
		stats: *pitrStats, createAt: types.BuildTS(10, 1), deleteAt: pitrDeleteAt,
	}
	source.iscp.tombstones[iscpName.SegmentId()] = &objectInfo{
		stats: *iscpStats, createAt: types.BuildTS(11, 2), deleteAt: iscpDeleteAt,
	}

	const fileName = "snapshot-meta-special-tombstones"
	size, err := source.SaveMeta(fileName, fs)
	require.NoError(t, err)
	require.NotZero(t, size, "tombstone-only metadata must not be skipped")

	restored := NewSnapshotMeta()
	restored.pitr.tid = source.pitr.tid
	restored.iscp.tid = source.iscp.tid
	staleName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	restored.objects[999] = map[objectio.Segmentid]*objectInfo{
		staleName.SegmentId(): {stats: *pitrStats},
	}
	restored.pitr.tombstones[staleName.SegmentId()] = &objectInfo{stats: *pitrStats}
	require.NoError(t, restored.ReadMeta(ctx, fileName, fs))
	require.Len(t, restored.pitr.tombstones, 1)
	require.Len(t, restored.iscp.tombstones, 1)
	require.Equal(t, pitrDeleteAt, restored.pitr.tombstones[pitrName.SegmentId()].deleteAt)
	require.Equal(t, iscpDeleteAt, restored.iscp.tombstones[iscpName.SegmentId()].deleteAt)
	require.NotContains(t, restored.pitr.tombstones, iscpName.SegmentId())
	require.NotContains(t, restored.iscp.tombstones, pitrName.SegmentId())
	require.NotContains(t, restored.objects, uint64(999))
	require.NotContains(t, restored.pitr.tombstones, staleName.SegmentId())
}

func TestSnapshotTableInfoRoundTripKeepsDeleteTimestampsWithoutTables(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	source := NewSnapshotMeta()
	deleteAt := types.BuildTS(42, 7)
	source.aobjDelTsMap[deleteAt] = struct{}{}

	const fileName = "snapshot-table-info-delete-timestamps"
	size, err := source.SaveTableInfo(fileName, fs)
	require.NoError(t, err)
	require.NotZero(t, size, "delete-timestamp-only metadata must not be skipped")

	restored := NewSnapshotMeta()
	staleDeleteAt := types.BuildTS(99, 0)
	restored.aobjDelTsMap[staleDeleteAt] = struct{}{}
	restored.tables[7] = map[uint64]*tableInfo{8: {tid: 8, accountID: 7}}
	restored.tableIDIndex[8] = restored.tables[7][8]
	restored.pitr.tid = 1001
	restored.iscp.tid = 1002
	require.NoError(t, restored.ReadTableInfo(ctx, fileName, fs))
	require.Contains(t, restored.aobjDelTsMap, deleteAt)
	require.NotContains(t, restored.aobjDelTsMap, staleDeleteAt)
	require.Empty(t, restored.tables)
	require.Empty(t, restored.tableIDIndex)
	require.Zero(t, restored.pitr.tid)
	require.Zero(t, restored.iscp.tid)
}

func TestSnapshotMetaGetSnapshotSkipsPersistedAborts(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	input := batch.NewWithSize(len(snapshotSchemaTypes) + 2)
	for i, typ := range snapshotSchemaTypes {
		input.Vecs[i] = vector.NewVec(typ)
	}
	input.Vecs[len(snapshotSchemaTypes)] = vector.NewVec(objectio.TSType)
	input.Vecs[len(snapshotSchemaTypes)+1] = vector.NewVec(types.T_bool.ToType())
	for row, ts := range []int64{100, 200, 300} {
		require.NoError(t, vector.AppendFixed(input.Vecs[ColSnapshotId], uint64(row+1), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[ColSName], []byte("snapshot"), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[ColTS], ts, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[ColLevel], types.Enum(SnapshotTypeCluster), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[ColAccountName], nil, false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[ColDatabaseName], nil, false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[ColTableName], nil, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[ColObjId], uint64(0), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[len(snapshotSchemaTypes)], types.BuildTS(1, 0), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[len(snapshotSchemaTypes)+1], row == 0, false, mp))
	}
	input.SetRowCount(3)
	seqnums := make([]uint16, 0, len(snapshotSchemaTypes)+2)
	for i := range snapshotSchemaTypes {
		seqnums = append(seqnums, uint16(i))
	}
	seqnums = append(seqnums, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT)
	writer := ioutil.ConstructWriter(0, seqnums, -1, false, false, fs)
	writer.SetAppendable()
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	input.Clean(mp)

	// A committed tombstone can legitimately carry an HLC timestamp ahead of
	// the local wall clock. Snapshot reads must still apply it; only the exact
	// UncommitTS sentinel is excluded.
	tombstoneInput := batch.NewWithSize(len(objectio.TombstoneSeqnums_DN_Created))
	tombstoneInput.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneInput.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	tombstoneInput.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	targetBlock := stats.ConstructBlockInfo(0).BlockID
	require.NoError(t, vector.AppendFixed(
		tombstoneInput.Vecs[0], types.NewRowid(&targetBlock, 1), false, mp,
	))
	require.NoError(t, vector.AppendFixed(tombstoneInput.Vecs[1], uint64(2), false, mp))
	require.NoError(t, vector.AppendFixed(
		tombstoneInput.Vecs[2],
		types.BuildTS(time.Now().Add(time.Hour).UnixNano(), 0),
		false,
		mp,
	))
	tombstoneInput.SetRowCount(1)
	tombstoneName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	tombstoneWriter, err := ioutil.NewBlockWriterNew(
		fs,
		tombstoneName,
		0,
		objectio.TombstoneSeqnums_DN_Created,
		true,
	)
	require.NoError(t, err)
	tombstoneWriter.SetAppendable()
	_, err = tombstoneWriter.WriteBatch(tombstoneInput)
	require.NoError(t, err)
	_, _, err = tombstoneWriter.Sync(ctx)
	require.NoError(t, err)
	tombstoneStats := tombstoneWriter.GetObjectStats(objectio.WithAppendable())
	tombstoneInput.Clean(mp)

	sm := NewSnapshotMeta()
	const tableID = uint64(1)
	sm.objects[tableID] = map[objectio.Segmentid]*objectInfo{
		stats.ObjectName().SegmentId(): {stats: stats},
	}
	sm.tombstones[tableID] = map[objectio.Segmentid]*objectInfo{
		tombstoneStats.ObjectName().SegmentId(): {stats: tombstoneStats},
	}
	snapshots, err := sm.GetSnapshot(ctx, "test", fs, mp)
	require.NoError(t, err)
	require.Equal(t, []types.TS{types.BuildTS(300, 0)}, snapshots.cluster)
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
