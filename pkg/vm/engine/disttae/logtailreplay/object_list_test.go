// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTailCheckFn tests the tailCheckFn function which filters objects based on time range
func TestTailCheckFn(t *testing.T) {
	// Test case 1: CreateTime within range [start, end]
	t.Run("CreateTimeWithinRange", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(15, 0),
			DeleteTime: types.TS{},
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 2: CreateTime equals start
	t.Run("CreateTimeEqualsStart", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 0),
			DeleteTime: types.TS{},
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 3: CreateTime equals end
	t.Run("CreateTimeEqualsEnd", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(20, 0),
			DeleteTime: types.TS{},
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 4: CreateTime before start, no DeleteTime
	t.Run("CreateTimeBeforeStartNoDelete", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(5, 0),
			DeleteTime: types.TS{},
		}
		assert.False(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 5: CreateTime after end, no DeleteTime
	t.Run("CreateTimeAfterEndNoDelete", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(25, 0),
			DeleteTime: types.TS{},
		}
		assert.False(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 6: CreateTime before start, DeleteTime within range
	t.Run("CreateTimeBeforeStartDeleteTimeWithinRange", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(5, 0),
			DeleteTime: types.BuildTS(15, 0),
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 7: CreateTime before start, DeleteTime equals start
	t.Run("CreateTimeBeforeStartDeleteTimeEqualsStart", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(5, 0),
			DeleteTime: types.BuildTS(10, 0),
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 8: CreateTime before start, DeleteTime equals end
	t.Run("CreateTimeBeforeStartDeleteTimeEqualsEnd", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(5, 0),
			DeleteTime: types.BuildTS(20, 0),
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 9: CreateTime before start, DeleteTime before start
	t.Run("CreateTimeBeforeStartDeleteTimeBeforeStart", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(3, 0),
			DeleteTime: types.BuildTS(5, 0),
		}
		assert.False(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 10: CreateTime before start, DeleteTime after end
	t.Run("CreateTimeBeforeStartDeleteTimeAfterEnd", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(5, 0),
			DeleteTime: types.BuildTS(25, 0),
		}
		assert.False(t, tailCheckFn(objEntry, start, end))
	})

	// Test case 11: Both CreateTime and DeleteTime within range
	t.Run("BothTimesWithinRange", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(12, 0),
			DeleteTime: types.BuildTS(18, 0),
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})
}

// TestSnapshotCheckFn tests the snapshotCheckFn function
func TestSnapshotCheckFn(t *testing.T) {
	// Test case 1: CreateTime before snapshot, no DeleteTime
	t.Run("CreateTimeBeforeSnapshotNoDelete", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 0),
			DeleteTime: types.TS{},
		}
		assert.True(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	// Test case 2: CreateTime equals snapshot, no DeleteTime
	// snapshotCheckFn uses GT (greater than), so CreateTime == snapshotTS means object is visible
	t.Run("CreateTimeEqualsSnapshotNoDelete", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(20, 0),
			DeleteTime: types.TS{},
		}
		// CreateTime is NOT > snapshotTS, so object is visible (returns true)
		assert.True(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	// Test case 3: CreateTime after snapshot
	t.Run("CreateTimeAfterSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(25, 0),
			DeleteTime: types.TS{},
		}
		assert.False(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	// Test case 4: CreateTime before snapshot, DeleteTime before snapshot
	t.Run("CreateTimeBeforeSnapshotDeleteTimeBeforeSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 0),
			DeleteTime: types.BuildTS(15, 0),
		}
		assert.False(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	// Test case 5: CreateTime before snapshot, DeleteTime equals snapshot
	t.Run("CreateTimeBeforeSnapshotDeleteTimeEqualsSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 0),
			DeleteTime: types.BuildTS(20, 0),
		}
		assert.False(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	// Test case 6: CreateTime before snapshot, DeleteTime after snapshot
	t.Run("CreateTimeBeforeSnapshotDeleteTimeAfterSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 0)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 0),
			DeleteTime: types.BuildTS(25, 0),
		}
		assert.True(t, snapshotCheckFn(objEntry, snapshotTS))
	})
}

// TestCreateObjectListBatch tests the CreateObjectListBatch function
func TestCreateObjectListBatch(t *testing.T) {
	bat := CreateObjectListBatch()

	assert.NotNil(t, bat)
	assert.Equal(t, len(ObjectListAttrs), len(bat.Vecs))
	assert.Equal(t, len(ObjectListTypes), len(bat.Vecs))

	// Verify attributes
	for i, attr := range ObjectListAttrs {
		assert.Equal(t, attr, bat.Attrs[i])
	}

	// Verify vector types
	assert.Equal(t, types.T_varchar.ToType().Oid, bat.Vecs[ObjectListAttr_DbName_Idx].GetType().Oid)
	assert.Equal(t, types.T_varchar.ToType().Oid, bat.Vecs[ObjectListAttr_TableName_Idx].GetType().Oid)
	assert.Equal(t, types.T_char.ToType().Oid, bat.Vecs[ObjectListAttr_Stats_Idx].GetType().Oid)
	assert.Equal(t, types.T_TS.ToType().Oid, bat.Vecs[ObjectListAttr_CreateAt_Idx].GetType().Oid)
	assert.Equal(t, types.T_TS.ToType().Oid, bat.Vecs[ObjectListAttr_DeleteAt_Idx].GetType().Oid)
	assert.Equal(t, types.T_bool.ToType().Oid, bat.Vecs[ObjectListAttr_IsTombstone_Idx].GetType().Oid)
}

// TestObjectListAttrsAndTypes tests the ObjectListAttrs and ObjectListTypes constants
func TestObjectListAttrsAndTypes(t *testing.T) {
	// Verify attribute names
	assert.Equal(t, "dbname", ObjectListAttr_DbName)
	assert.Equal(t, "tablename", ObjectListAttr_TableName)
	assert.Equal(t, "stats", ObjectListAttr_Stats)
	assert.Equal(t, "create_at", ObjectListAttr_CreateAt)
	assert.Equal(t, "delete_at", ObjectListAttr_DeleteAt)
	assert.Equal(t, "is_tombstone", ObjectListAttr_IsTombstone)

	// Verify indices
	assert.Equal(t, 0, ObjectListAttr_DbName_Idx)
	assert.Equal(t, 1, ObjectListAttr_TableName_Idx)
	assert.Equal(t, 2, ObjectListAttr_Stats_Idx)
	assert.Equal(t, 3, ObjectListAttr_CreateAt_Idx)
	assert.Equal(t, 4, ObjectListAttr_DeleteAt_Idx)
	assert.Equal(t, 5, ObjectListAttr_IsTombstone_Idx)

	// Verify attrs length
	assert.Equal(t, 6, len(ObjectListAttrs))
	assert.Equal(t, 6, len(ObjectListTypes))
}

// TestGetObjectListFromCKPEmptyEntries tests GetObjectListFromCKP with empty checkpoint entries
func TestGetObjectListFromCKPEmptyEntries(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	var bat *batch.Batch
	checkpointEntries := []*checkpoint.CheckpointEntry{}

	err := GetObjectListFromCKP(
		ctx,
		1,  // tid
		"", // sid
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		checkpointEntries,
		&bat,
		mp,
		fs,
	)

	require.NoError(t, err)
	// When batch is nil and entries are empty, batch should be created
	require.NotNil(t, bat)
	assert.Equal(t, 0, bat.RowCount())
}

// TestGetObjectListFromCKPWithExistingBatch tests GetObjectListFromCKP with existing batch
func TestGetObjectListFromCKPWithExistingBatch(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	// Pre-create a batch
	bat := CreateObjectListBatch()
	checkpointEntries := []*checkpoint.CheckpointEntry{}

	err := GetObjectListFromCKP(
		ctx,
		1,  // tid
		"", // sid
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		checkpointEntries,
		&bat,
		mp,
		fs,
	)

	require.NoError(t, err)
	require.NotNil(t, bat)
	assert.Equal(t, 0, bat.RowCount())
}

// TestGetObjectListFromCKPBatchCreation tests that GetObjectListFromCKP creates batch when nil
func TestGetObjectListFromCKPBatchCreation(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	var bat *batch.Batch = nil
	checkpointEntries := []*checkpoint.CheckpointEntry{}

	err := GetObjectListFromCKP(
		ctx,
		1,  // tid
		"", // sid
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		checkpointEntries,
		&bat,
		mp,
		fs,
	)

	require.NoError(t, err)
	require.NotNil(t, bat)

	// Verify batch structure
	assert.Equal(t, len(ObjectListAttrs), len(bat.Vecs))
	for i, attr := range ObjectListAttrs {
		assert.Equal(t, attr, bat.Attrs[i])
	}
}

// TestCollectObjectListEmptyState tests CollectObjectList with empty partition state
func TestCollectObjectListEmptyState(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)
	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 0, bat.RowCount())
}

// TestCollectObjectListWithDataObjects tests CollectObjectList with data objects
func TestCollectObjectListWithDataObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add data objects
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)

	// Object 1: CreateTime within range
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	// Object 2: CreateTime before range, DeleteTime within range
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(5, 0),
		DeleteTime:  types.BuildTS(50, 0),
	})

	// Object 3: Outside range (should be filtered)
	objID3 := objectio.NewObjectid()
	stats3 := objectio.NewObjectStatsWithObjectID(&objID3, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats3,
		CreateTime:  types.BuildTS(5, 0),
		DeleteTime:  types.BuildTS(8, 0),
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 2, bat.RowCount())

	// Verify all rows have correct dbname and tablename
	dbnames := bat.Vecs[ObjectListAttr_DbName_Idx]
	tablenames := bat.Vecs[ObjectListAttr_TableName_Idx]
	isTombstones := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[ObjectListAttr_IsTombstone_Idx])

	for i := 0; i < bat.RowCount(); i++ {
		assert.Equal(t, []byte("testdb"), dbnames.GetBytesAt(i))
		assert.Equal(t, []byte("testtable"), tablenames.GetBytesAt(i))
		assert.False(t, isTombstones[i]) // Data objects, not tombstones
	}
}

// TestCollectObjectListWithTombstoneObjects tests CollectObjectList with tombstone objects
func TestCollectObjectListWithTombstoneObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add tombstone objects
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, true, false)
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify tombstone flag
	isTombstones := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[ObjectListAttr_IsTombstone_Idx])
	assert.True(t, isTombstones[0])
}

// TestCollectObjectListWithMixedObjects tests CollectObjectList with both data and tombstone objects
func TestCollectObjectListWithMixedObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add data object
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	// Add tombstone object
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, true, false)
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(60, 0),
		DeleteTime:  types.TS{},
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 2, bat.RowCount())

	// Verify both data and tombstone objects are present
	isTombstones := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[ObjectListAttr_IsTombstone_Idx])
	hasData := false
	hasTombstone := false
	for i := 0; i < bat.RowCount(); i++ {
		if isTombstones[i] {
			hasTombstone = true
		} else {
			hasData = true
		}
	}
	assert.True(t, hasData)
	assert.True(t, hasTombstone)
}

// TestCollectObjectListDeletedObjects tests CollectObjectList with objects that have DeleteTime
func TestCollectObjectListDeletedObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add data object with DeleteTime within range
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.BuildTS(80, 0),
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify DeleteTime is recorded
	deleteTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_DeleteAt_Idx])
	assert.Equal(t, types.BuildTS(80, 0), deleteTimes[0])
}

// TestCollectObjectListDeletedButNotInRange tests objects deleted but DeleteTime outside range
func TestCollectObjectListDeletedButDeleteTimeAfterEnd(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add data object with CreateTime in range but DeleteTime after range
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.BuildTS(150, 0), // After end
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify DeleteTime is empty (since it's after end, treated as not deleted within range)
	deleteTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_DeleteAt_Idx])
	assert.True(t, deleteTimes[0].IsEmpty())
}

// TestCollectSnapshotObjectListEmptyState tests CollectSnapshotObjectList with empty partition state
func TestCollectSnapshotObjectListEmptyState(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)
	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 0, bat.RowCount())
}

// TestCollectSnapshotObjectListWithDataObjects tests CollectSnapshotObjectList with data objects
func TestCollectSnapshotObjectListWithDataObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Object 1: Created before snapshot, not deleted
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	// Object 2: Created before snapshot, deleted after snapshot
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(60, 0),
		DeleteTime:  types.BuildTS(150, 0),
	})

	// Object 3: Created after snapshot (should be filtered)
	objID3 := objectio.NewObjectid()
	stats3 := objectio.NewObjectStatsWithObjectID(&objID3, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats3,
		CreateTime:  types.BuildTS(150, 0),
		DeleteTime:  types.TS{},
	})

	// Object 4: Created before snapshot, deleted before snapshot (should be filtered)
	objID4 := objectio.NewObjectid()
	stats4 := objectio.NewObjectStatsWithObjectID(&objID4, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats4,
		CreateTime:  types.BuildTS(30, 0),
		DeleteTime:  types.BuildTS(50, 0),
	})

	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 2, bat.RowCount())
}

// TestCollectSnapshotObjectListWithTombstoneObjects tests CollectSnapshotObjectList with tombstone objects
func TestCollectSnapshotObjectListWithTombstoneObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add tombstone object created before snapshot
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, true, false)
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify tombstone flag
	isTombstones := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[ObjectListAttr_IsTombstone_Idx])
	assert.True(t, isTombstones[0])
}

// TestCollectSnapshotObjectListDeletedButAfterSnapshot tests objects with DeleteTime after snapshot
func TestCollectSnapshotObjectListDeletedButAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Object with DeleteTime after snapshot - should appear but with empty DeleteTime
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.BuildTS(150, 0),
	})

	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// DeleteTime should be empty since it's after snapshot
	deleteTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_DeleteAt_Idx])
	assert.True(t, deleteTimes[0].IsEmpty())
}

// TestCollectObjectListMultipleObjects tests CollectObjectList with many objects
func TestCollectObjectListMultipleObjects(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add 10 data objects within range
	for i := 0; i < 10; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(20+i*5), 0),
			DeleteTime:  types.TS{},
		})
	}

	// Add 5 tombstone objects within range
	for i := 0; i < 5; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, true, false)
		state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(25+i*5), 0),
			DeleteTime:  types.TS{},
		})
	}

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 15, bat.RowCount())

	// Count data and tombstone objects
	isTombstones := vector.MustFixedColNoTypeCheck[bool](bat.Vecs[ObjectListAttr_IsTombstone_Idx])
	dataCount := 0
	tombstoneCount := 0
	for i := 0; i < bat.RowCount(); i++ {
		if isTombstones[i] {
			tombstoneCount++
		} else {
			dataCount++
		}
	}
	assert.Equal(t, 10, dataCount)
	assert.Equal(t, 5, tombstoneCount)
}

// TestObjectStatsInBatch tests that ObjectStats is correctly stored in batch
func TestObjectStatsInBatch(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add data object with specific stats
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats, 1000))

	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify ObjectStats is stored correctly
	statsVec := bat.Vecs[ObjectListAttr_Stats_Idx]
	storedStats := objectio.ObjectStats(statsVec.GetBytesAt(0))
	assert.Equal(t, uint32(100), storedStats.Rows())
	assert.Equal(t, uint32(1000), storedStats.Size())
}

// TestGetObjectListFromCKPWithEmptyLocation tests GetObjectListFromCKP with checkpoint entry having empty location
// Note: This test verifies that the function panics when given an empty location,
// which is expected behavior since empty locations should never be passed to this function
func TestGetObjectListFromCKPWithEmptyLocation(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	// Create checkpoint entry with empty location
	entry := checkpoint.NewCheckpointEntry(
		"test-sid",
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		checkpoint.ET_Incremental,
	)

	var bat *batch.Batch
	checkpointEntries := []*checkpoint.CheckpointEntry{entry}

	// This will panic because location is empty and ReadMeta attempts to read from it
	// We use recover to verify the panic behavior
	defer func() {
		if r := recover(); r != nil {
			// Expected: panic when location is empty
			t.Logf("Expected panic occurred: %v", r)
		}
	}()

	_ = GetObjectListFromCKP(
		ctx,
		1,  // tid
		"", // sid
		types.BuildTS(0, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		checkpointEntries,
		&bat,
		mp,
		fs,
	)

	// If we reach here, the function didn't panic which is unexpected
	t.Fatal("Expected panic for empty location, but function completed normally")
}

// TestCollectObjectListNilBatch tests CollectObjectList does not panic with nil batch
func TestCollectObjectListNilBatch(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)
	bat := CreateObjectListBatch()

	// Add an object
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(50, 0),
		DeleteTime:  types.TS{},
	})

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.NotNil(t, bat)
}

// TestCollectObjectListVerifyTimestamps tests that timestamps are correctly recorded
func TestCollectObjectListVerifyTimestamps(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	createTime := types.BuildTS(50, 5)
	deleteTime := types.BuildTS(80, 10)

	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  createTime,
		DeleteTime:  deleteTime,
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify timestamps
	createTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_CreateAt_Idx])
	deleteTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_DeleteAt_Idx])

	assert.Equal(t, createTime, createTimes[0])
	assert.Equal(t, deleteTime, deleteTimes[0])
}

// TestTailCheckFnWithLogicalTime tests tailCheckFn with logical timestamps
func TestTailCheckFnWithLogicalTime(t *testing.T) {
	// Test with logical component of timestamp
	t.Run("CreateTimeWithLogicalComponentWithinRange", func(t *testing.T) {
		start := types.BuildTS(10, 5)
		end := types.BuildTS(10, 15)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 10),
			DeleteTime: types.TS{},
		}
		assert.True(t, tailCheckFn(objEntry, start, end))
	})

	t.Run("CreateTimeWithLogicalComponentOutsideRange", func(t *testing.T) {
		start := types.BuildTS(10, 5)
		end := types.BuildTS(10, 15)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(10, 3),
			DeleteTime: types.TS{},
		}
		assert.False(t, tailCheckFn(objEntry, start, end))
	})
}

// TestSnapshotCheckFnWithLogicalTime tests snapshotCheckFn with logical timestamps
func TestSnapshotCheckFnWithLogicalTime(t *testing.T) {
	t.Run("CreateTimeWithLogicalComponentBeforeSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 10)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(20, 5),
			DeleteTime: types.TS{},
		}
		assert.True(t, snapshotCheckFn(objEntry, snapshotTS))
	})

	t.Run("CreateTimeWithLogicalComponentAfterSnapshot", func(t *testing.T) {
		snapshotTS := types.BuildTS(20, 10)

		objEntry := objectio.ObjectEntry{
			CreateTime: types.BuildTS(20, 15),
			DeleteTime: types.TS{},
		}
		assert.False(t, snapshotCheckFn(objEntry, snapshotTS))
	})
}

// TestCollectObjectListLargeDataset tests CollectObjectList with large number of objects
func TestCollectObjectListLargeDataset(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	numObjects := 100
	for i := 0; i < numObjects; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(10+i), 0),
			DeleteTime:  types.TS{},
		})
	}

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(0, 0),
		types.BuildTS(1000, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, numObjects, bat.RowCount())
}

// TestCollectSnapshotObjectListLargeDataset tests CollectSnapshotObjectList with large number of objects
func TestCollectSnapshotObjectListLargeDataset(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	numObjects := 100
	for i := 0; i < numObjects; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(10+i), 0),
			DeleteTime:  types.TS{},
		})
	}

	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(1000, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, numObjects, bat.RowCount())
}

// TestCollectObjectListAllFiltered tests when all objects are filtered out
func TestCollectObjectListAllFiltered(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add objects all outside the time range
	for i := 0; i < 5; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(5+i), 0),
			DeleteTime:  types.TS{},
		})
	}

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		types.BuildTS(200, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 0, bat.RowCount())
}

// TestCollectSnapshotObjectListAllFiltered tests when all objects are filtered out
func TestCollectSnapshotObjectListAllFiltered(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Add objects all created after snapshot
	for i := 0; i < 5; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(200+i), 0),
			DeleteTime:  types.TS{},
		})
	}

	bat := CreateObjectListBatch()

	_, err := CollectSnapshotObjectList(
		ctx,
		state,
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 0, bat.RowCount())
}

// TestCollectObjectListObjectDeletedExactlyAtEnd tests object deleted exactly at end time
func TestCollectObjectListObjectDeletedExactlyAtEnd(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	end := types.BuildTS(100, 0)

	// Object created before start, deleted exactly at end
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(5, 0),
		DeleteTime:  end,
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		end,
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 1, bat.RowCount())

	// Verify DeleteTime is recorded
	deleteTimes := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[ObjectListAttr_DeleteAt_Idx])
	assert.Equal(t, end, deleteTimes[0])
}

// TestCollectObjectListTimeBoundaries tests time boundary conditions
func TestCollectObjectListTimeBoundaries(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	state := NewPartitionState("test", false, 42, false)

	// Object at exact start time
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(10, 0), // Exact start
		DeleteTime:  types.TS{},
	})

	// Object at exact end time
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(100, 0), // Exact end
		DeleteTime:  types.TS{},
	})

	// Object just before start (should be filtered)
	objID3 := objectio.NewObjectid()
	stats3 := objectio.NewObjectStatsWithObjectID(&objID3, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats3,
		CreateTime:  types.BuildTS(9, 0),
		DeleteTime:  types.TS{},
	})

	// Object just after end (should be filtered)
	objID4 := objectio.NewObjectid()
	stats4 := objectio.NewObjectStatsWithObjectID(&objID4, false, false, false)
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats4,
		CreateTime:  types.BuildTS(101, 0),
		DeleteTime:  types.TS{},
	})

	bat := CreateObjectListBatch()

	_, err := CollectObjectList(
		ctx,
		state,
		types.BuildTS(10, 0),
		types.BuildTS(100, 0),
		"testdb",
		"testtable",
		&bat,
		mp,
	)

	require.NoError(t, err)
	assert.Equal(t, 2, bat.RowCount())
}
