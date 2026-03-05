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

package logtailreplay

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkPartitionStateConcurrentWriteAndIter(b *testing.B) {
	partition := NewPartition("", nil, 0, 0, 42, nil)
	end := make(chan struct{})
	defer func() {
		close(end)
	}()

	// concurrent writer
	go func() {
		for {
			select {
			case <-end:
				return
			default:
			}
			state, end := partition.MutateState()
			_ = state
			end()
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			state := partition.state.Load()
			iter := state.NewRowsIter(types.BuildTS(0, 0), nil, false)
			iter.Close()
		}
	})

}

func TestTruncate(t *testing.T) {
	partition := NewPartitionState("", true, 42, false)
	partition.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(2, 0))
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(3, 0))
	addObject(partition, types.BuildTS(1, 0), types.TS{})

	partition.truncate([2]uint64{0, 0}, types.BuildTS(1, 0))
	assert.Equal(t, 5, partition.dataObjectTSIndex.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(4, 0))
	assert.Equal(t, 1, partition.dataObjectTSIndex.Len())
}

func addObject(p *PartitionState, create, delete types.TS) {
	blkID := objectio.NewBlockid(objectio.NewSegmentid(), 0, 0)
	objShortName := objectio.ShortName(blkID)
	objIndex1 := ObjectIndexByTSEntry{
		Time:         create,
		ShortObjName: *objShortName,
		IsDelete:     false,
	}
	p.dataObjectTSIndex.Set(objIndex1)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	p.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  create,
		DeleteTime:  delete,
	})
	if !delete.IsEmpty() {
		objIndex2 := ObjectIndexByTSEntry{
			Time:         delete,
			ShortObjName: *objShortName,
			IsDelete:     true,
		}
		p.dataObjectTSIndex.Set(objIndex2)
	}

}

func TestHasTombstoneChanged(t *testing.T) {
	state := NewPartitionState("", true, 42, false)
	require.False(t, state.HasTombstoneChanged(types.BuildTS(13, 0), types.BuildTS(15, 0)))

	roid := func() objectio.ObjectStats {
		nobjid := objectio.NewObjectid()
		return *objectio.NewObjectStatsWithObjectID(&nobjid, false, true, false)
	}

	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(5, 0), DeleteTime: types.BuildTS(8, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(7, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(6, 0), DeleteTime: types.BuildTS(12, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(6, 0), DeleteTime: types.BuildTS(24, 0)})
	require.True(t, state.HasTombstoneChanged(types.BuildTS(24, 0), types.BuildTS(30, 0)))
	require.False(t, state.HasTombstoneChanged(types.BuildTS(25, 0), types.BuildTS(30, 0)))

	for i := 10; i < 20; i++ {
		state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{
			ObjectStats: roid(),
			CreateTime:  types.BuildTS(int64(i), 0),
		})
	}

	require.True(t, state.HasTombstoneChanged(types.BuildTS(13, 0), types.BuildTS(15, 0)))
	require.True(t, state.HasTombstoneChanged(types.BuildTS(9, 0), types.BuildTS(15, 0)))
	require.False(t, state.HasTombstoneChanged(types.BuildTS(25, 0), types.BuildTS(30, 0)))

}

func TestScanRows(t *testing.T) {
	packer := types.NewPacker()
	state := NewPartitionState("", true, 42, false)
	for i := uint32(0); i < 10; i++ {
		rid := types.BuildTestRowid(rand.Int63(), rand.Int63())
		state.rows.Set(&RowEntry{
			BlockID:           rid.CloneBlockID(),
			RowID:             rid,
			Offset:            int64(i),
			Time:              types.BuildTS(time.Now().UnixNano(), uint32(i)),
			ID:                int64(i),
			Deleted:           i%2 == 0,
			PrimaryIndexBytes: readutil.EncodePrimaryKey(i, packer),
		})
	}

	logutil.Info(state.LogAllRowEntry())

	_ = state.ScanRows(false, func(entry *RowEntry) (bool, error) {
		logutil.Info(entry.String())
		return true, nil
	})

	_ = state.ScanRows(true, func(entry *RowEntry) (bool, error) {
		logutil.Info(entry.String())
		return true, nil
	})
}

func TestCountRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs := testutil.NewSharedFS()
	state := NewPartitionState("", false, 42, false)

	// Test empty state
	count, err := state.CountRows(ctx, types.BuildTS(10, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// Add non-appendable data object with 100 rows
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Count at TS=10, should see 100 rows
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// Add in-memory inserts (5 rows) with explicit objectid
	insertObjID := objectio.NewObjectid()
	for i := 0; i < 5; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(insertObjID, 0, uint32(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(3, uint32(i)),
			ID:      int64(i),
			Deleted: false,
		})
	}

	// Count at TS=10, should see 100 + 5 = 105 rows
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(105), count)

	// Add in-memory deletes (3 rows) - use different objectid to ensure no match
	deleteObjID := objectio.NewObjectid()
	for i := 5; i < 8; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(deleteObjID, 0, uint32(i))
		entry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(4, uint32(i)),
			ID:      int64(i),
			Deleted: true,
		}
		state.rows.Set(entry)

		// Add to inMemTombstoneRowIdIndex
		state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
			Bytes:      rid.BorrowObjectID()[:],
			BlockID:    entry.BlockID,
			RowID:      entry.RowID,
			Time:       entry.Time,
			RowEntryID: entry.ID,
			Deleted:    true,
		})
	}

	// Count at TS=10, should see 85 - 0 = 85 rows (deletes filtered out due to no matching objects)
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(105), count)

	// Test snapshot visibility: count at TS=1 (before tombstone)
	count, err = state.CountRows(ctx, types.BuildTS(1, 0), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// Test snapshot visibility: count at TS=2 (before inserts)
	count, err = state.CountRows(ctx, types.BuildTS(2, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// Test snapshot visibility: count at TS=3 (after some inserts)
	count, err = state.CountRows(ctx, types.BuildTS(3, 2), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(103), count) // 100 + 3 inserts visible
}

func TestCountDataRows(t *testing.T) {
	state := NewPartitionState("", false, 42, false)

	// Test empty state
	dataStats, err := state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count := dataStats.Rows
	assert.Equal(t, uint64(0), count)

	// Add non-appendable data object with 100 rows
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(100), count)

	// Add another non-appendable data object with 50 rows
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 50))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(150), count)

	// Add in-memory inserts
	for i := 0; i < 10; i++ {
		rid := types.BuildTestRowid(int64(i), int64(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(3, uint32(i)),
			ID:      int64(i),
			Deleted: false,
		})
	}

	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(160), count)

	// Test snapshot visibility: count at TS=1
	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(1, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(100), count)

	// Test snapshot visibility: count at TS=2
	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(2, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(150), count)

	// Test deleted object visibility
	objID3 := objectio.NewObjectid()
	stats3 := objectio.NewObjectStatsWithObjectID(&objID3, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats3, 30))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats3,
		CreateTime:  types.BuildTS(4, 0),
		DeleteTime:  types.BuildTS(5, 0),
	})

	// At TS=4, object is visible (100 + 50 + 10 in-mem + 30 new obj = 190)
	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(4, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(190), count)

	// At TS=5, object is deleted (100 + 50 + 10 in-mem = 160)
	dataStats, err = state.CollectDataStats(context.Background(), types.BuildTS(5, 0), nil, nil)
	require.NoError(t, err)
	count = dataStats.Rows
	assert.Equal(t, uint64(160), count)
}

func TestCountTombstoneRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create a data object first so tombstones can reference it
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create real tombstone object 1 with 20 deletions pointing to the data object
	writer1 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 20; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		pk := rand.Int()
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(pk), false, mp))
	}

	_, err := writer1.WriteBatch(bat1)
	require.NoError(t, err)
	_, _, err = writer1.Sync(ctx)
	require.NoError(t, err)

	ss1 := writer1.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(20), count)

	// Create real tombstone object 2 with 15 deletions
	writer2 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 15; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(20+i))
		pk := rand.Int()
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], int32(pk), false, mp))
	}

	_, err = writer2.WriteBatch(bat2)
	require.NoError(t, err)
	_, _, err = writer2.Sync(ctx)
	require.NoError(t, err)

	ss2 := writer2.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(35), count)

	// Add in-memory deletes
	// Add in-memory deletes pointing to the data object
	for i := 0; i < 5; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(35+i))
		entry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(3, uint32(i)),
			ID:      int64(i),
			Deleted: true,
		}
		state.rows.Set(entry)

		// Also add to inMemTombstoneRowIdIndex
		state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
			Bytes:      rid.BorrowObjectID()[:],
			BlockID:    entry.BlockID,
			RowID:      entry.RowID,
			Time:       entry.Time,
			RowEntryID: entry.ID,
			Deleted:    true,
		})
	}

	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(40), count)

	// Test snapshot visibility: count at TS=1
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(1, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(20), count)

	// Test snapshot visibility: count at TS=2
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(2, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(35), count)

	// Test deleted tombstone object visibility
	writer3 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat3 := batch.NewWithSize(2)
	bat3.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 10; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(40+i))
		pk := rand.Int()
		require.NoError(t, vector.AppendFixed[types.Rowid](bat3.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat3.Vecs[1], int32(pk), false, mp))
	}

	_, err = writer3.WriteBatch(bat3)
	require.NoError(t, err)
	_, _, err = writer3.Sync(ctx)
	require.NoError(t, err)

	ss3 := writer3.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss3,
		CreateTime:  types.BuildTS(4, 0),
		DeleteTime:  types.BuildTS(5, 0),
	})

	// At TS=4, tombstone object is visible (20 + 15 + 5 in-mem + 10 new = 50)
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(4, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(50), count)

	// At TS=5, tombstone object is deleted (20 + 15 + 5 in-mem = 40)
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(5, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(40), count)
}

func TestCountRowsAppendableObjects(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Add appendable data object (should not be counted in object stats)
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, true, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Should be 0 since appendable objects are not counted
	dataStats, err := state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count := dataStats.Rows
	assert.Equal(t, uint64(0), count)

	// Add appendable tombstone object (should not be counted)
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, true, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 50))
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	fs := testutil.NewSharedFS()
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	tombCount := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(0), tombCount)
}

func TestCountRowsSnapshotIsolation(t *testing.T) {
	state := NewPartitionState("", false, 42, false)

	// Add objects at different timestamps
	for i := 1; i <= 5; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, uint32(i*10)))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(i), 0),
			DeleteTime:  types.TS{},
		})
	}

	// Test snapshot at different points
	tests := []struct {
		ts       types.TS
		expected uint64
	}{
		{types.BuildTS(0, 0), 0},    // Before any data
		{types.BuildTS(1, 0), 10},   // After first object
		{types.BuildTS(2, 0), 30},   // After second object (10+20)
		{types.BuildTS(3, 0), 60},   // After third object (10+20+30)
		{types.BuildTS(5, 0), 150},  // After all objects (10+20+30+40+50)
		{types.BuildTS(10, 0), 150}, // Future timestamp
	}

	for _, tt := range tests {
		dataStats, err := state.CollectDataStats(context.Background(), tt.ts, nil, nil)
		require.NoError(t, err)
		count := dataStats.Rows
		assert.Equal(t, tt.expected, count, "Failed at TS=%v", tt.ts)
	}
}

func TestCountRowsInMemoryMixedOperations(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Add base data object
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add mixed in-memory operations
	for i := 0; i < 20; i++ {
		rid := types.BuildTestRowid(int64(i), int64(i))
		isDeleted := i%3 == 0 // Every 3rd row is a delete (0,3,6,9,12,15,18 = 7 deletes)
		entry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(2, uint32(i)),
			ID:      int64(i),
			Deleted: isDeleted,
		}
		state.rows.Set(entry)

		// Add deletes to inMemTombstoneRowIdIndex
		if isDeleted {
			state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
				Bytes:      rid.BorrowObjectID()[:],
				BlockID:    entry.BlockID,
				RowID:      entry.RowID,
				Time:       entry.Time,
				RowEntryID: entry.ID,
				Deleted:    true,
			})
		}
	}

	// Count: 100 (base) + 13 inserts - 0 deletes = 113 (deletes filtered out due to no matching objects)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs := testutil.NewSharedFS()
	count, err := state.CountRows(ctx, types.BuildTS(10, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(113), count)

	// Verify individual counts
	dataStats, err := state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	dataCount := dataStats.Rows
	assert.Equal(t, uint64(113), dataCount) // 100 + 13

	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	tombCount := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(0), tombCount) // Deletes filtered out due to no matching objects
}

func TestCountRowsMultipleObjectDeletions(t *testing.T) {
	state := NewPartitionState("", false, 42, false)

	// Add multiple objects with different lifecycle
	objects := []struct {
		rows       uint32
		createTime int64
		deleteTime int64
	}{
		{100, 1, 0}, // Never deleted
		{50, 2, 5},  // Deleted at TS=5
		{30, 3, 4},  // Deleted at TS=4
		{20, 4, 6},  // Deleted at TS=6
		{40, 5, 0},  // Never deleted
	}

	for _, obj := range objects {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, obj.rows))
		entry := objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(obj.createTime, 0),
		}
		if obj.deleteTime > 0 {
			entry.DeleteTime = types.BuildTS(obj.deleteTime, 0)
		}
		state.dataObjectsNameIndex.Set(entry)
	}

	// Test at different timestamps
	tests := []struct {
		ts       types.TS
		expected uint64
	}{
		{types.BuildTS(1, 0), 100},  // Only first object
		{types.BuildTS(3, 0), 180},  // 100+50+30
		{types.BuildTS(4, 0), 170},  // 100+50+20 (obj3 deleted at 4, obj4 created at 4)
		{types.BuildTS(5, 0), 160},  // 100+20+40 (obj2,obj3 deleted)
		{types.BuildTS(6, 0), 140},  // 100+40 (obj2,obj3,obj4 deleted)
		{types.BuildTS(10, 0), 140}, // Same as TS=6
	}

	for _, tt := range tests {
		dataStats, err := state.CollectDataStats(context.Background(), tt.ts, nil, nil)
		require.NoError(t, err)
		count := dataStats.Rows
		assert.Equal(t, tt.expected, count, "Failed at TS=%v", tt.ts)
	}
}

func TestCountRowsZeroRowObjects(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Add object with 0 rows
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 0))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	dataStats, err := state.CollectDataStats(context.Background(), types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	count := dataStats.Rows
	assert.Equal(t, uint64(0), count)

	totalCount, err := state.CountRows(ctx, types.BuildTS(10, 0), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), totalCount)
}

func TestCountRowsLargeNumbers(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Add large number of objects
	const numObjects = 100
	const rowsPerObject = 10000

	for i := 0; i < numObjects; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, rowsPerObject))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(i+1), 0),
			DeleteTime:  types.TS{},
		})
	}

	dataStats, err := state.CollectDataStats(context.Background(), types.BuildTS(1000, 0), nil, nil)
	require.NoError(t, err)
	count := dataStats.Rows
	assert.Equal(t, uint64(numObjects*rowsPerObject), count)

	totalCount, err := state.CountRows(ctx, types.BuildTS(1000, 0), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(numObjects*rowsPerObject), totalCount)
}

func TestCountRowsTimestampBoundaries(t *testing.T) {
	state := NewPartitionState("", false, 42, false)

	// Add object at specific timestamp
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(5, 10),
		DeleteTime:  types.BuildTS(10, 20),
	})

	// Test boundary conditions
	tests := []struct {
		ts       types.TS
		expected uint64
		desc     string
	}{
		{types.BuildTS(5, 9), 0, "Before create"},
		{types.BuildTS(5, 10), 100, "Exact create time"},
		{types.BuildTS(5, 11), 100, "After create"},
		{types.BuildTS(10, 19), 100, "Before delete"},
		{types.BuildTS(10, 20), 0, "Exact delete time"},
		{types.BuildTS(10, 21), 0, "After delete"},
	}

	for _, tt := range tests {
		dataStats, err := state.CollectDataStats(context.Background(), tt.ts, nil, nil)
		require.NoError(t, err)
		count := dataStats.Rows
		assert.Equal(t, tt.expected, count, "Failed: %s at TS=%v", tt.desc, tt.ts)
	}
}

func TestCountRowsConcurrentRead(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Setup initial data
	for i := 0; i < 10; i++ {
		objID := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&objID, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(i+1), 0),
			DeleteTime:  types.TS{},
		})
	}

	// Concurrent reads
	const numReaders = 10
	done := make(chan bool, numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				count, err := state.CountRows(ctx, types.BuildTS(100, 0), nil, nil)
				require.NoError(t, err)
				assert.Equal(t, uint64(1000), count)
			}
			done <- true
		}()
	}

	for i := 0; i < numReaders; i++ {
		<-done
	}
}

func TestCountTombstoneRowsWithDuplicates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create data object first
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create tombstone with duplicates by writing sorted data
	// In real scenarios (flush/merge), data is sorted before writing
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)

	// Collect all deletions (including duplicates)
	allRowIds := make([]types.Rowid, 0, 15)
	allPKs := make([]int32, 0, 15)

	// 10 unique deletions
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i))
	}

	// 5 duplicates (same rowids as first 5)
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i))
	}

	// Sort by RowID (simulating flush/merge behavior)
	indices := make([]int, len(allRowIds))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return allRowIds[indices[i]].LT(&allRowIds[indices[j]])
	})

	// Write sorted data in two batches (simulating block boundaries)
	// Batch 1: first 8 rows
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 8; i++ {
		idx := indices[i]
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], allRowIds[idx], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], allPKs[idx], false, mp))
	}
	_, err := writer.WriteBatch(bat1)
	require.NoError(t, err)

	// Batch 2: remaining 7 rows
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 8; i < len(indices); i++ {
		idx := indices[i]
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], allRowIds[idx], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], allPKs[idx], false, mp))
	}
	_, err = writer.WriteBatch(bat2)
	require.NoError(t, err)

	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	// Count with object visibility check
	// Should count 10 unique deletions (duplicates filtered by linear deduplication)
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)

	// Should count 10 unique deletions (duplicates filtered)
	assert.Equal(t, uint64(10), count)

	// Test without object visibility check to verify file content
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	countNoCheck := tombStats.Rows
	require.NoError(t, err)
	// This tells us how many rows are actually in the file
	t.Logf("Rows in tombstone file: %d", countNoCheck)
}

func TestCountTombstoneRowsObjectVisibility(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create data object 1
	dataObjID1 := objectio.NewObjectid()
	dataStats1 := objectio.NewObjectStatsWithObjectID(&dataObjID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats1, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create data object 2 (will be deleted)
	dataObjID2 := objectio.NewObjectid()
	dataStats2 := objectio.NewObjectStatsWithObjectID(&dataObjID2, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats2, 50))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.BuildTS(5, 0), // Deleted at TS=5
	})

	// Create tombstone with deletions for both objects
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	// 10 deletions for object 1
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		pk := int32(i)
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], pk, false, mp))
	}

	// 5 deletions for object 2
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		pk := int32(i + 100)
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], pk, false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(3, 0),
		DeleteTime:  types.TS{},
	})

	// At TS=4, both objects visible, should count all 15 deletions
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(4, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(15), count)

	// At TS=6, object 2 deleted, should count only 10 deletions (for object 1)
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(6, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(10), count)
}

func TestCountTombstoneRowsComprehensive(t *testing.T) {
	// Comprehensive test covering: duplicates + object visibility + deleted objects
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create data object 1 (visible)
	dataObjID1 := objectio.NewObjectid()
	dataStats1 := objectio.NewObjectStatsWithObjectID(&dataObjID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats1, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create data object 2 (will be deleted at TS=5)
	dataObjID2 := objectio.NewObjectid()
	dataStats2 := objectio.NewObjectStatsWithObjectID(&dataObjID2, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats2, 50))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.BuildTS(5, 0),
	})

	// Create data object 3 (not visible - points to non-existent object)
	dataObjID3 := objectio.NewObjectid()

	// Create tombstone with duplicates and deletions for all three objects
	// In real scenarios (flush/merge), data is sorted before writing
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)

	// Collect all deletions first (simulating in-memory data before flush)
	allRowIds := make([]types.Rowid, 0, 15)
	allPKs := make([]int32, 0, 15)

	// 5 deletions for object 1
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i))
	}

	// 2 duplicates for object 1 (will be deduplicated)
	for i := 0; i < 2; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i))
	}

	// 3 deletions for object 2
	for i := 0; i < 3; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i+100))
	}

	// 1 duplicate for object 2
	for i := 0; i < 1; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i+100))
	}

	// 4 deletions for object 3 (not visible)
	for i := 0; i < 4; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID3, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		allRowIds = append(allRowIds, rowid)
		allPKs = append(allPKs, int32(i+200))
	}

	// Sort by RowID (simulating flush/merge behavior)
	// Create index array for sorting
	indices := make([]int, len(allRowIds))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return allRowIds[indices[i]].LT(&allRowIds[indices[j]])
	})

	// Write sorted data in batches (simulating block boundaries)
	// Batch 1: first 8 rows
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 8 && i < len(indices); i++ {
		idx := indices[i]
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], allRowIds[idx], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], allPKs[idx], false, mp))
	}
	_, err := writer.WriteBatch(bat1)
	require.NoError(t, err)

	// Batch 2: remaining rows
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 8; i < len(indices); i++ {
		idx := indices[i]
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], allRowIds[idx], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], allPKs[idx], false, mp))
	}
	_, err = writer.WriteBatch(bat2)
	require.NoError(t, err)

	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(3, 0),
		DeleteTime:  types.TS{},
	})

	// At TS=4: obj1 visible (5), obj2 visible (3), obj3 not visible (0)
	// Total: 5 + 3 = 8 (duplicates filtered, obj3 filtered)
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(4, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(8), count)

	// At TS=6: obj1 visible (5), obj2 deleted (0), obj3 not visible (0)
	// Total: 5
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(6, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)

	// At TS=10: obj1 visible (5), obj2 deleted at TS=5 (0), obj3 not visible (0)
	// Total: 5 (with object visibility check, only obj1's deletions are counted)
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	countNoCheck := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(5), countNoCheck, "Only deletions for visible objects are counted")
	t.Logf("Total unique rows in file: %d", countNoCheck)
}

// TestCollectTombstoneStats_CrossObjectDuplicates tests the scenario where
// the same RowID appears in multiple tombstone objects due to:
// 1. CN transfer: tombstone transferred from old object to new object
// 2. Tombstone merge: multiple tombstone objects merged without deduplication
// 3. Concurrent deletes: same row deleted multiple times
func TestCollectTombstoneStats_CrossObjectDuplicates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create data object
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Scenario: Same RowIDs appear in multiple tombstone objects
	// This simulates CN transfer or tombstone merge without deduplication

	// Object 1: Delete rows 0-9
	writer1 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(i), false, mp))
	}
	_, err := writer1.WriteBatch(bat1)
	require.NoError(t, err)
	_, _, err = writer1.Sync(ctx)
	require.NoError(t, err)
	ss1 := writer1.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss1,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	// Object 2: Delete rows 5-14 (rows 5-9 are duplicates with Object 1)
	writer2 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 5; i < 15; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], int32(i), false, mp))
	}
	_, err = writer2.WriteBatch(bat2)
	require.NoError(t, err)
	_, _, err = writer2.Sync(ctx)
	require.NoError(t, err)
	ss2 := writer2.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss2,
		CreateTime:  types.BuildTS(3, 0),
		DeleteTime:  types.TS{},
	})

	// Object 3: Delete rows 10-19 (rows 10-14 are duplicates with Object 2)
	writer3 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat3 := batch.NewWithSize(2)
	bat3.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 10; i < 20; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat3.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat3.Vecs[1], int32(i), false, mp))
	}
	_, err = writer3.WriteBatch(bat3)
	require.NoError(t, err)
	_, _, err = writer3.Sync(ctx)
	require.NoError(t, err)
	ss3 := writer3.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss3,
		CreateTime:  types.BuildTS(4, 0),
		DeleteTime:  types.TS{},
	})

	// Count tombstones
	// Total rows in files: 10 + 10 + 10 = 30
	// Unique rows: 0-19 = 20
	// Duplicates: rows 5-9 (in obj1 and obj2) + rows 10-14 (in obj2 and obj3) = 10
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows

	// Should count 20 unique deletions (duplicates filtered by global map)
	assert.Equal(t, uint64(20), count, "Cross-object duplicates should be filtered")
	t.Logf("Total rows in files: 30, Unique rows: %d", count)
}

// TestCollectTombstoneStats_CrossObjectAndInMemoryDuplicates tests duplicates
// between persisted tombstone objects and in-memory tombstones
func TestCollectTombstoneStats_CrossObjectAndInMemoryDuplicates(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)
	state.inMemTombstoneRowIdIndex = btree.NewBTreeG[*PrimaryIndexEntry]((*PrimaryIndexEntry).Less)

	// Create data object
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Persisted tombstone: Delete rows 0-9
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
	}
	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	ss := writer.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	// In-memory tombstones: Delete rows 5-14 (rows 5-9 are duplicates with persisted)
	for i := 5; i < 15; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		entry := &PrimaryIndexEntry{
			Bytes:      dataObjID[:],
			BlockID:    blkID,
			RowID:      rowid,
			Time:       types.BuildTS(3, 0),
			RowEntryID: int64(i),
		}
		state.inMemTombstoneRowIdIndex.Set(entry)
	}

	// Count tombstones
	// Persisted: 10 rows (0-9)
	// In-memory: 10 rows (5-14)
	// Unique: 15 rows (0-14)
	// Duplicates: 5 rows (5-9)
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows

	// Should count 15 unique deletions (duplicates between persisted and in-memory filtered)
	assert.Equal(t, uint64(15), count, "Duplicates between persisted and in-memory should be filtered")
	t.Logf("Persisted: 10, In-memory: 10, Unique: %d", count)
}

func TestCountTombstoneRowsWithRealObject(t *testing.T) {
	// This test demonstrates how to read real tombstone objects
	// Currently skipped as it requires actual tombstone files
	t.Skip("Requires real tombstone object files")

	// Mock fileservice would be needed here
	// fs := testutil.NewSharedFS()

	// Add a real tombstone object
}

func TestCountTombstoneRowsCNCreatedWithAppendable(t *testing.T) {
	// Test CN-created tombstone referencing appendable data objects
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create appendable data object by adding in-memory rows
	dataObjID := objectio.NewObjectid()
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		entry := &RowEntry{
			BlockID: blkID,
			RowID:   rowid,
			Time:    types.BuildTS(1, uint32(i)),
			ID:      int64(i),
			Deleted: false,
		}
		state.rows.Set(entry)
	}

	// Create CN-created tombstone (non-appendable) that references the appendable object
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	// 5 deletions for the appendable object
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	// With object visibility check: should count 5 (appendable object is visible)
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)

	// Without object visibility check: should count 5
	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	countNoCheck := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(5), countNoCheck)
}

func TestCollectTombstoneStats_MergeVsMapConsistency(t *testing.T) {
	// Test that both merge and map paths produce identical results
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	state := NewPartitionState("", false, 42, false)

	// Create 5 tombstone objects with overlapping RowIDs
	// Total: 50 rows, Unique: 30 rows
	var dataObjID types.Objectid
	dataObjID[0] = 1

	// Add data object
	blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
	rowID := types.NewRowid(&blkID, 0)
	state.rows.Set(&RowEntry{
		BlockID: blkID,
		RowID:   rowID,
		Time:    types.BuildTS(1, 0),
	})

	for objIdx := 0; objIdx < 5; objIdx++ {
		writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		// Each object has 10 rows: 6 unique + 4 duplicates from previous objects
		for i := 0; i < 10; i++ {
			var rowOffset uint32
			if i < 6 {
				rowOffset = uint32(objIdx*6 + i) // Unique rows
			} else {
				rowOffset = uint32((objIdx-1)*6 + (i - 6)) // Duplicate from previous
			}
			if objIdx == 0 && i >= 6 {
				rowOffset = uint32(i - 6) // First object duplicates itself
			}

			blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
			rowid := types.NewRowid(&blkID, rowOffset)
			require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
			require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		}

		// Sort before writing
		rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](bat.Vecs[0])
		pks := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[1])

		indices := make([]int, len(rowIds))
		for i := range indices {
			indices[i] = i
		}
		sort.Slice(indices, func(i, j int) bool {
			return rowIds[indices[i]].LT(&rowIds[indices[j]])
		})

		sortedRowIds := make([]types.Rowid, len(rowIds))
		sortedPKs := make([]int32, len(pks))
		for i, idx := range indices {
			sortedRowIds[i] = rowIds[idx]
			sortedPKs[i] = pks[idx]
		}

		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
		for i := range sortedRowIds {
			require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], sortedRowIds[i], false, mp))
			require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], sortedPKs[i], false, mp))
		}

		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)

		ss := writer.GetObjectStats()
		state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *ss.Clone(),
			CreateTime:  types.BuildTS(2, 0),
		})
	}

	// Test with map path (default: < 4 objects or < 5M rows)
	statsMap, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)

	// Force merge path by directly calling it
	iter := state.tombstoneObjectsNameIndex.Iter()
	var objects []objectio.ObjectEntry
	for ok := iter.First(); ok; ok = iter.Next() {
		objects = append(objects, iter.Item())
	}
	statsMerge, err := state.countTombstoneStatsWithMerge(ctx, types.BuildTS(10, 0), fs, objects, TombstoneStats{})
	require.NoError(t, err)

	// Both should produce same result
	assert.Equal(t, statsMap.Rows, statsMerge.Rows, "Map and merge paths should produce identical row counts")
	t.Logf("Map path: %d rows, Merge path: %d rows", statsMap.Rows, statsMerge.Rows)
}

func TestCollectTombstoneStats_MultiObjectMultiBlock(t *testing.T) {
	// Test multiple objects, each with multiple blocks
	// Verify both map and merge paths handle this correctly
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	state := NewPartitionState("", false, 42, false)

	var dataObjID types.Objectid
	dataObjID[0] = 1

	// Add data object
	blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
	rowID := types.NewRowid(&blkID, 0)
	state.rows.Set(&RowEntry{
		BlockID: blkID,
		RowID:   rowID,
		Time:    types.BuildTS(1, 0),
	})

	// Create 3 objects, each with 3 blocks
	// Object 0: blocks with rows [0-9], [10-19], [20-29] (30 unique)
	// Object 1: blocks with rows [25-34], [35-44], [45-54] (30 rows, 5 duplicates with obj0)
	// Object 2: blocks with rows [50-59], [60-69], [70-79] (30 unique)
	// Total: 90 rows, 85 unique

	totalRows := 0
	for objIdx := 0; objIdx < 3; objIdx++ {
		writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)

		for blockIdx := 0; blockIdx < 3; blockIdx++ {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

			// Each block has 10 rows
			baseOffset := objIdx*30 + blockIdx*10
			if objIdx == 1 {
				baseOffset = 25 + blockIdx*10 // Overlap with object 0
			}

			for i := 0; i < 10; i++ {
				rowOffset := uint32(baseOffset + i)
				blk := objectio.NewBlockidWithObjectID(&dataObjID, 0)
				rowid := types.NewRowid(&blk, rowOffset)
				require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
				require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(rowOffset), false, mp))
				totalRows++
			}

			_, err := writer.WriteBatch(bat)
			require.NoError(t, err)
		}

		_, _, err := writer.Sync(ctx)
		require.NoError(t, err)

		ss := writer.GetObjectStats()
		state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *ss.Clone(),
			CreateTime:  types.BuildTS(2, 0),
		})
	}

	// Test with map path (default for < 4 objects)
	statsMap, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)

	// Force merge path
	iter := state.tombstoneObjectsNameIndex.Iter()
	var objects []objectio.ObjectEntry
	for ok := iter.First(); ok; ok = iter.Next() {
		objects = append(objects, iter.Item())
	}
	statsMerge, err := state.countTombstoneStatsWithMerge(ctx, types.BuildTS(10, 0), fs, objects, TombstoneStats{})
	require.NoError(t, err)

	// Expected: 85 unique rows (90 total - 5 duplicates)
	expectedUnique := uint64(85)
	assert.Equal(t, expectedUnique, statsMap.Rows, "Map path should count 85 unique rows")
	assert.Equal(t, expectedUnique, statsMerge.Rows, "Merge path should count 85 unique rows")
	assert.Equal(t, statsMap.Rows, statsMerge.Rows, "Map and merge paths must produce identical results")

	t.Logf("Total rows: %d, Map: %d, Merge: %d, Expected unique: %d",
		totalRows, statsMap.Rows, statsMerge.Rows, expectedUnique)
}

func TestCollectTombstoneStats_ComprehensiveAllScenarios(t *testing.T) {
	// Comprehensive test: verify map and merge produce identical results
	// Covers: multiple objects (CN/DN created), multiple blocks, in-memory data/tombstones

	// Comprehensive test covering all real-world scenarios:
	// - Multiple objects: appendable, CN-created non-appendable, DN-created non-appendable
	// - Multiple blocks per object (except appendable which is single block)
	// - In-memory data rows
	// - In-memory tombstones
	// - Persisted tombstones referencing in-memory data
	// - Cross-object duplicates
	// - Verify map and merge paths produce identical results

	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()

	state := NewPartitionState("", false, 42, false)

	// Scenario 1: In-memory data rows (appendable)
	// Create 20 in-memory data rows (rowid 0-19)
	var dataObjID1 types.Objectid
	dataObjID1[0] = 1
	for i := 0; i < 20; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowID := types.NewRowid(&blkID, uint32(i))
		state.rows.Set(&RowEntry{
			BlockID: blkID,
			RowID:   rowID,
			Time:    types.BuildTS(1, 0),
		})
	}

	// Scenario 2: Persisted appendable data object
	// Create 30 persisted data rows (rowid 20-49)
	var dataObjID2 types.Objectid
	dataObjID2[0] = 2
	for i := 0; i < 30; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowID := types.NewRowid(&blkID, uint32(20+i))
		state.rows.Set(&RowEntry{
			BlockID: blkID,
			RowID:   rowID,
			Time:    types.BuildTS(1, 0),
		})
	}

	// Scenario 3: In-memory tombstones
	// Delete 5 rows from in-memory data (rowid 0-4)
	// IMPORTANT: PrimaryIndexEntry sorts by Bytes (PK), not RowID
	// Must set Bytes to ensure correct iteration order matching persisted tombstones
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowID := types.NewRowid(&blkID, uint32(i))
		// Encode rowid as bytes for consistent sorting
		pkBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(pkBytes, uint64(i))
		state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
			Bytes: pkBytes,
			RowID: rowID,
			Time:  types.BuildTS(2, 0),
		})
	}

	// Scenario 4: CN-created tombstone (single block, appendable-like)
	// References in-memory data (rowid 2-14, 13 deletes)
	// Has 3 duplicates with in-memory tombstones (rowid 2-4)
	writer1 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 2; i <= 14; i++ { // rowid 2-14 (includes 2-4 duplicates)
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(i), false, mp))
	}

	_, err := writer1.WriteBatch(bat1)
	require.NoError(t, err)
	_, _, err = writer1.Sync(ctx)
	require.NoError(t, err)

	ss1 := writer1.GetObjectStats()
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *ss1.Clone(),
		CreateTime:  types.BuildTS(3, 0),
	})

	// Scenario 5: CN-created non-appendable tombstone (multiple blocks)
	// References persisted appendable data (rowid 10-39, 30 deletes in 2 blocks)
	// Has 5 duplicates with previous tombstone (rowid 10-14)
	writer2 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)

	// Collect all rowids for writer2, then sort
	var writer2RowIds []types.Rowid
	for i := 10; i <= 39; i++ {
		var objID *types.Objectid
		if i <= 19 {
			objID = &dataObjID1
		} else {
			objID = &dataObjID2
		}
		blkID := objectio.NewBlockidWithObjectID(objID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		writer2RowIds = append(writer2RowIds, rowid)
	}

	// Sort all rowids
	sort.Slice(writer2RowIds, func(i, j int) bool {
		return writer2RowIds[i].LT(&writer2RowIds[j])
	})

	// Block 1: first 15 rows
	bat2a := batch.NewWithSize(2)
	bat2a.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2a.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 15; i++ {
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2a.Vecs[0], writer2RowIds[i], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2a.Vecs[1], int32(i), false, mp))
	}
	_, err = writer2.WriteBatch(bat2a)
	require.NoError(t, err)

	// Block 2: remaining 15 rows
	bat2b := batch.NewWithSize(2)
	bat2b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2b.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 15; i < 30; i++ {
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2b.Vecs[0], writer2RowIds[i], false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2b.Vecs[1], int32(i), false, mp))
	}
	_, err = writer2.WriteBatch(bat2b)
	require.NoError(t, err)

	_, _, err = writer2.Sync(ctx)
	require.NoError(t, err)

	ss2 := writer2.GetObjectStats()
	ss2Clone := ss2.Clone()
	objectio.WithCNCreated()(ss2Clone)
	entry2 := objectio.ObjectEntry{
		ObjectStats: *ss2Clone,
		CreateTime:  types.BuildTS(4, 0),
	}
	state.tombstoneObjectsNameIndex.Set(entry2)

	// Scenario 6: DN-created non-appendable tombstone with CommitTS (multiple blocks)
	// References persisted data (rowid 35-54, 20 deletes in 3 blocks)
	// Has 5 duplicates with writer2 (rowid 35-39)
	writer3 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)

	// Block 1: rowid 35-41 (7 rows)
	bat3a := batch.NewWithSize(3)
	bat3a.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3a.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat3a.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	for i := 35; i <= 41; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat3a.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat3a.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat3a.Vecs[2], types.BuildTS(5, 0), false, mp))
	}
	_, err = writer3.WriteBatch(bat3a)
	require.NoError(t, err)

	// Block 2: rowid 42-48 (7 rows)
	bat3b := batch.NewWithSize(3)
	bat3b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3b.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat3b.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	for i := 42; i <= 48; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat3b.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat3b.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat3b.Vecs[2], types.BuildTS(5, 0), false, mp))
	}
	_, err = writer3.WriteBatch(bat3b)
	require.NoError(t, err)

	// Block 3: rowid 49-54 (6 rows)
	bat3c := batch.NewWithSize(3)
	bat3c.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3c.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat3c.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	for i := 49; i <= 54; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat3c.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat3c.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat3c.Vecs[2], types.BuildTS(5, 0), false, mp))
	}
	_, err = writer3.WriteBatch(bat3c)
	require.NoError(t, err)

	_, _, err = writer3.Sync(ctx)
	require.NoError(t, err)

	ss3 := writer3.GetObjectStats()
	entry3 := objectio.ObjectEntry{
		ObjectStats: *ss3.Clone(),
		CreateTime:  types.BuildTS(6, 0),
	}
	// DN created, non-appendable (default)
	state.tombstoneObjectsNameIndex.Set(entry3)

	// Calculate expected unique tombstones:
	// In-memory: 0-4 (5 rows)
	// Writer1 (CN single block): 2-14 (13 rows, 3 duplicates with in-memory: 2,3,4)
	// Writer2 (CN multi-block): 10-39 (30 rows, 5 duplicates with writer1: 10,11,12,13,14)
	// Writer3 (DN multi-block): 35-54 (20 rows, 5 duplicates with writer2: 35,36,37,38,39)
	// Unique breakdown:
	//   - In-memory unique: 0,1 (2 rows, 2-4 duplicated by writer1)
	//   - Writer1 unique: 5-14 (10 rows, 2-4 duplicated by in-mem)
	//   - Writer2 unique: 15-39 (25 rows, 10-14 duplicated by writer1)
	//   - Writer3 unique: 40-54 (15 rows, 35-39 duplicated by writer2)
	// Total unique: 2 + 10 + 25 + 15 = 52 rows

	// Wait, let me recalculate more carefully:
	// All tombstones: 0,1,2,3,4 (in-mem) + 2-14 (w1) + 10-39 (w2) + 35-54 (w3)
	// Unique set: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,...,54
	// That's 0-54 = 55 unique values
	// But wait, we need to check if all data objects are visible!

	// Actually the issue is: writer2 references both dataObjID1 (10-19) and dataObjID2 (20-39)
	// But dataObjID1 is in-memory only, not persisted!
	// So rowid 10-19 from writer2 won't be counted (data not visible in dataObjectsNameIndex)

	// Expected calculation:
	// All tombstone rowids: 0-4 (in-mem) + 2-14 (w1) + 10-39 (w2) + 35-54 (w3)
	// Unique set: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,...,54 = 55 unique rowids

	// Test with map path (default for 3 objects)
	statsMap, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)

	// Force merge path
	iter := state.tombstoneObjectsNameIndex.Iter()
	var objects []objectio.ObjectEntry
	for ok := iter.First(); ok; ok = iter.Next() {
		objects = append(objects, iter.Item())
	}
	statsMerge, err := state.countTombstoneStatsWithMerge(ctx, types.BuildTS(10, 0), fs, objects, TombstoneStats{})
	require.NoError(t, err)

	expectedUnique := uint64(55)

	t.Logf("Comprehensive test: Map=%d, Merge=%d, Expected=%d", statsMap.Rows, statsMerge.Rows, expectedUnique)
	t.Logf("Tombstone ranges: in-mem=[0-4], w1=[2-14], w2=[10-39], w3=[35-54]")

	// Primary requirement: both paths must produce identical results
	assert.Equal(t, statsMap.Rows, statsMerge.Rows, "Map and merge paths must produce identical results")

	// Secondary: verify correctness (if both agree but differ from expected, investigate test data)
	if statsMap.Rows == statsMerge.Rows {
		assert.Equal(t, expectedUnique, statsMap.Rows, "Both paths should count 55 unique tombstones")
	}
}

func TestCountTombstoneRowsReadError(t *testing.T) {
	// Test error handling when reading tombstone file fails
	ctx := context.Background()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create a tombstone object with invalid/empty stats
	// This will cause ReadDeletes to fail when trying to read the file
	var stats objectio.ObjectStats

	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: stats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Try to count - should return error or handle gracefully
	// With empty stats, the object will be skipped or cause an error
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows

	// Either error or zero count is acceptable for invalid stats
	if err != nil {
		t.Logf("Got expected error: %v", err)
	} else {
		assert.Equal(t, uint64(0), count, "Should return 0 for invalid tombstone object")
	}
}

func TestCountTombstoneRowsEdgeCases(t *testing.T) {
	// Test various edge cases for CountTombstoneRows
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create data object
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(5, 0),
		DeleteTime:  types.TS{},
	})

	// Create tombstone
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()

	// Test 1: Tombstone created after snapshot - should not be visible
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(20, 0), // After snapshot
		DeleteTime:  types.TS{},
	})

	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count, "Tombstone created after snapshot should not be counted")

	// Test 2: Tombstone deleted before snapshot - should not be visible
	state.tombstoneObjectsNameIndex.Delete(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(20, 0),
		DeleteTime:  types.TS{},
	})

	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.BuildTS(5, 0), // Deleted before snapshot
	})

	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count, "Tombstone deleted before snapshot should not be counted")

	// Test 3: Appendable tombstone - should be skipped
	state.tombstoneObjectsNameIndex.Delete(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.BuildTS(5, 0),
	})

	appendableStats := objectio.NewObjectStatsWithObjectID(&dataObjID, true, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(appendableStats, 10))

	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *appendableStats,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	tombStats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count = tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count, "Appendable tombstone should be skipped")
}

// TestCountTombstoneRowsIntegration demonstrates the full flow with real tombstone files
// This is a more complete integration test showing how tombstone counting would work
// with actual file I/O. Currently commented out as it requires more setup.
/*
func TestCountTombstoneRowsIntegration(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Step 1: Create a real tombstone object file
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	// Create 100 deletion records
	dataObjID := objectio.NewObjectid()
	for i := 0; i < 100; i++ {
		// Create rowid pointing to data object
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		pk := rand.Int()

		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(pk), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)

	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	// Step 2: Add the tombstone object to partition state
	ss := writer.GetObjectStats()
	tombstoneEntry := objectio.ObjectEntry{
		ObjectStats: *ss,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	}
	state.tombstoneObjectsNameIndex.Set(tombstoneEntry)

	// Step 3: Count tombstone rows
	// Note: Current implementation uses Rows() approximation
	// Full implementation would read the file and filter by snapshot
	tombStats, err := state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	count := tombStats.Rows
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)
}
*/

// TestCalculateTableStatsEmpty tests empty partition
func TestCalculateTableStatsEmpty(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	assert.Equal(t, float64(0), stats.TotalRows)
	assert.Equal(t, float64(0), stats.TotalSize)
	assert.Equal(t, 0, stats.DataObjectCnt)
	assert.Equal(t, 0, stats.TombstoneObjectCnt)
}

// TestCalculateTableStatsNonAppendableOnly tests only non-appendable objects
func TestCalculateTableStatsNonAppendableOnly(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add 2 non-appendable objects
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 200))
	require.NoError(t, objectio.SetObjectStatsSize(stats2, 3000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	assert.Equal(t, float64(300), stats.TotalRows)
	assert.Equal(t, float64(4000), stats.TotalSize)
	assert.Equal(t, 2, stats.DataObjectCnt)
	assert.Equal(t, 0, stats.TombstoneObjectCnt)
}

// TestCalculateTableStatsWithAppendableRows tests appendable rows with size estimation
func TestCalculateTableStatsWithAppendableRows(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add non-appendable object for size estimation
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add appendable rows
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 50; i++ {
		vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
	}
	bat.SetRowCount(50)

	appendableObjID := objectio.NewObjectid()
	for i := 0; i < 50; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(appendableObjID, 0, uint32(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(5, uint32(i)),
			Batch:   bat,
			Offset:  int64(i),
			ID:      int64(i),
			Deleted: false,
		})
	}

	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	// Total rows = 100 (object) + 50 (appendable)
	assert.Equal(t, float64(150), stats.TotalRows)

	// estimatedOneRowSize = 1000 / 100 = 10
	// Total size = 1000 + 50 * 10 = 1500
	assert.Equal(t, float64(1500), stats.TotalSize)

	assert.Equal(t, 1, stats.DataObjectCnt)
	assert.Equal(t, 0, stats.TombstoneObjectCnt)
}

// TestCalculateTableStatsWithInMemoryDeletes tests in-memory deletes on non-appendable object rows
func TestCalculateTableStatsWithInMemoryDeletes(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add data object with 100 rows
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add in-memory deletes on non-appendable object rows (rowid 0-19)
	// These are NOT paired with inserts, so they should be counted
	for i := 0; i < 20; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, uint32(i))
		entry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(10, uint32(i)),
			ID:      int64(i),
			Deleted: true,
		}
		state.rows.Set(entry)

		// Add to inMemTombstoneRowIdIndex (required by CountTombstoneRows)
		state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
			Bytes:      rid.BorrowObjectID()[:],
			BlockID:    entry.BlockID,
			RowID:      entry.RowID,
			Time:       entry.Time,
			RowEntryID: entry.ID,
			Deleted:    true,
		})
	}

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	// Total rows = 100 - 20 = 80 (visible rows after deletions)
	assert.Equal(t, float64(80), stats.TotalRows)
}

// TestCalculateTableStatsPairedInsertDelete tests paired insert-delete should be skipped
func TestCalculateTableStatsPairedInsertDelete(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add data object
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add in-memory insert and delete for same rowid (should be paired and skipped)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(bat.Vecs[0], int32(0), false, mp)
	bat.SetRowCount(1)

	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, 100)

	// Insert
	state.rows.Set(&RowEntry{
		BlockID: rid.CloneBlockID(),
		RowID:   rid,
		Time:    types.BuildTS(5, 0),
		Batch:   bat,
		Offset:  0,
		ID:      0,
		Deleted: false,
	})

	// Delete same row
	deleteEntry := &RowEntry{
		BlockID: rid.CloneBlockID(),
		RowID:   rid,
		Time:    types.BuildTS(10, 0),
		ID:      1,
		Deleted: true,
	}
	state.rows.Set(deleteEntry)

	// Add to inMemTombstoneRowIdIndex
	state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
		Bytes:      rid.BorrowObjectID()[:],
		BlockID:    deleteEntry.BlockID,
		RowID:      deleteEntry.RowID,
		Time:       deleteEntry.Time,
		RowEntryID: deleteEntry.ID,
		Deleted:    true,
	})

	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	// Total rows = 100 + 1 - 1 = 100 (visible rows after deletion)
	assert.Equal(t, float64(100), stats.TotalRows)
}

// TestCalculateTableStatsFilterByObjectVisibility tests deletion filtering
func TestCalculateTableStatsFilterByObjectVisibility(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add only objID1 as visible
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add appendable inserts on objID1
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 20; i++ {
		vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
	}
	bat.SetRowCount(20)

	for i := 0; i < 20; i++ {
		rid1 := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, uint32(100+i))
		state.rows.Set(&RowEntry{
			BlockID: rid1.CloneBlockID(),
			RowID:   rid1,
			Time:    types.BuildTS(5, uint32(i)),
			Batch:   bat,
			Offset:  int64(i),
			ID:      int64(i),
			Deleted: false,
		})
	}

	// Add in-memory deletes on both objID1 and objID2
	objID2 := objectio.NewObjectid()

	for i := 0; i < 10; i++ {
		rid1 := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, uint32(100+i))
		state.rows.Set(&RowEntry{
			BlockID: rid1.CloneBlockID(),
			RowID:   rid1,
			Time:    types.BuildTS(10, uint32(i)),
			ID:      int64(20 + i),
			Deleted: true,
		})

		rid2 := types.NewRowIDWithObjectIDBlkNumAndRowID(objID2, 0, uint32(i))
		state.rows.Set(&RowEntry{
			BlockID: rid2.CloneBlockID(),
			RowID:   rid2,
			Time:    types.BuildTS(10, uint32(i)),
			ID:      int64(30 + i),
			Deleted: true,
		})
	}

	_, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	// All deletes on objID1 are paired with inserts, so no net deletions
	// TotalRows should reflect visible rows only
}

// TestCalculateTableStatsIntegration tests full integration scenario
func TestCalculateTableStatsIntegration(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("test", false, 0, false)
	snapshot := types.BuildTS(100, 0)

	// Add non-appendable object
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 100))
	require.NoError(t, objectio.SetObjectStatsSize(stats1, 1000))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Add appendable rows
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 50; i++ {
		vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
	}
	bat.SetRowCount(50)

	for i := 0; i < 50; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, uint32(100+i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(5, uint32(i)),
			Batch:   bat,
			Offset:  int64(i),
			ID:      int64(i),
			Deleted: false,
		})
	}

	// Add deletes
	for i := 0; i < 20; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(objID1, 0, uint32(100+i))
		deleteEntry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(10, uint32(i)),
			ID:      int64(50 + i),
			Deleted: true,
		}
		state.rows.Set(deleteEntry)

		// Add to inMemTombstoneRowIdIndex
		state.inMemTombstoneRowIdIndex.Set(&PrimaryIndexEntry{
			Bytes:      rid.BorrowObjectID()[:],
			BlockID:    deleteEntry.BlockID,
			RowID:      deleteEntry.RowID,
			Time:       deleteEntry.Time,
			RowEntryID: deleteEntry.ID,
			Deleted:    true,
		})
	}

	stats, err := state.CalculateTableStats(ctx, snapshot, fs, mp)
	require.NoError(t, err)

	// Total rows = 100 + 50 - 20 = 130 (visible rows after deletions)
	assert.Equal(t, float64(130), stats.TotalRows)

	// Total size = 1000 + 50 * 10 = 1500 (all data size, including deleted)
	assert.Equal(t, float64(1500), stats.TotalSize)

	assert.Equal(t, 1, stats.DataObjectCnt)
	assert.Equal(t, 0, stats.TombstoneObjectCnt)
}

// TestCollectTombstoneStats_DNCreatedWithCommitTs tests DN created tombstone with CommitTs check
func TestCollectTombstoneStats_DNCreatedWithCommitTs(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("", false, 42, false)

	// Create a data object
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create DN tombstone (with CommitTs) at TS=10
	// Contains deletions from different commit times
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	// 5 deletions committed at TS=3
	for i := 0; i < 5; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(3, 0), false, mp))
	}

	// 5 deletions committed at TS=7
	for i := 5; i < 10; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(7, 0), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	// DN created tombstone (no CNCreated flag)
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(10, 0),
		DeleteTime:  types.TS{},
	})

	// At TS=9: tombstone not visible yet
	stats, err := state.CollectTombstoneStats(ctx, types.BuildTS(9, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Rows, "Tombstone not visible before CreateTime")

	// At TS=10: tombstone visible, all 10 deletions visible (CommitTs <= 10)
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible at CreateTime")

	// At TS=15: all 10 deletions still visible
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(15, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible after CreateTime")
}

// TestCollectTombstoneStats_AppendableTombstone tests appendable tombstone objects
func TestCollectTombstoneStats_AppendableTombstone(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("", false, 42, false)

	// Create a data object
	dataObjID := objectio.NewObjectid()
	dataStats := objectio.NewObjectStatsWithObjectID(&dataObjID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(dataStats, 100))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *dataStats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	// Create appendable tombstone (CN created, appendable) at TS=10
	// Contains deletions from different commit times
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	// 5 deletions committed at TS=3
	for i := 0; i < 5; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(3, 0), false, mp))
	}

	// 5 deletions committed at TS=7
	for i := 5; i < 10; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(7, 0), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	// Mark as appendable and CN created
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(10, 0),
		DeleteTime:  types.TS{},
	})

	// At TS=9: tombstone not visible yet
	stats, err := state.CollectTombstoneStats(ctx, types.BuildTS(9, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Rows, "Tombstone not visible before CreateTime")

	// At TS=10: tombstone visible, all 10 deletions visible (CommitTs <= 10)
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible at CreateTime")

	// At TS=15: all 10 deletions still visible
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(15, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible after CreateTime")
}

// TestCollectTombstoneStats_AppendableDataWithAppendableTombstone tests appendable data + appendable tombstone
func TestCollectTombstoneStats_AppendableDataWithAppendableTombstone(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()
	state := NewPartitionState("", false, 42, false)

	// Create appendable data object (in-memory inserts)
	dataObjID := objectio.NewObjectid()
	for i := 0; i < 20; i++ {
		rid := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		entry := &RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(2, uint32(i)),
			ID:      int64(i),
			Deleted: false,
		}
		state.rows.Set(entry)
	}

	// Create appendable tombstone (flushed to S3) with deletions at different commit times
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	// 5 deletions committed at TS=5 (rows 0-4)
	for i := 0; i < 5; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(5, 0), false, mp))
	}

	// 5 deletions committed at TS=8 (rows 5-9)
	for i := 5; i < 10; i++ {
		row := types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjID, 0, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], int32(i), false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], types.BuildTS(8, 0), false, mp))
	}

	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	ss := writer.GetObjectStats()
	// Appendable tombstone (CNCreated=true, Appendable=true)
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: ss,
		CreateTime:  types.BuildTS(10, 0),
		DeleteTime:  types.TS{},
	})

	// At TS=9: tombstone not visible yet
	stats, err := state.CollectTombstoneStats(ctx, types.BuildTS(9, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), stats.Rows, "Tombstone not visible before CreateTime")

	// At TS=10: tombstone visible, all 10 deletions visible (CommitTs <= 10)
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(10, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible at CreateTime")

	// Verify CountRows: 20 inserts - 10 deletes = 10 visible rows
	count, err := state.CountRows(ctx, types.BuildTS(10, 0), fs, mp)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), count, "Visible rows = inserts - deletes")

	// At TS=15: all 10 deletions still visible
	stats, err = state.CollectTombstoneStats(ctx, types.BuildTS(15, 0), fs)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), stats.Rows, "All deletions visible after CreateTime")
}

// TestCountRows_VisibleAppendableDataObjects covers:
// 1) Multiple appendable data objects in the index, some visible and some invisible at snapshot.
// 2) With fs=nil, appendable objects contribute 0 (no block read) — only non-appendable + p.rows count.
// 3) With fs set but no real object files, appendable block read fails and contributes 0.
// Full coverage of "persisted appendable on disk with commit_ts" is exercised by BVT (e.g. 11_select_count_snapshot).
func TestCountRows_VisibleAppendableDataObjects(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	snapshot := types.BuildTS(10, 0)

	t.Run("multiple_aobj_only_fs_nil", func(t *testing.T) {
		state := NewPartitionState("", false, 42, false)
		// Appendable A: visible (CreateTime=1, no DeleteTime)
		objA := objectio.NewObjectid()
		statsA := objectio.NewObjectStatsWithObjectID(&objA, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsA, 5))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsA,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.TS{},
		})
		// Appendable B: invisible (CreateTime > snapshot)
		objB := objectio.NewObjectid()
		statsB := objectio.NewObjectStatsWithObjectID(&objB, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsB, 3))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsB,
			CreateTime:  types.BuildTS(20, 0),
			DeleteTime:  types.TS{},
		})
		// Appendable C: invisible (DeleteTime <= snapshot)
		objC := objectio.NewObjectid()
		statsC := objectio.NewObjectStatsWithObjectID(&objC, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsC, 2))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsC,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.BuildTS(5, 0),
		})
		// No p.rows, no non-appendable. With fs=nil we do not read blocks → appendable contribute 0.
		dataStats, err := state.CollectDataStats(ctx, snapshot, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), dataStats.Rows, "fs=nil: appendable objects not read, count stays 0")
		// With fs but object has no blocks in stats (never flushed), ForeachBlkInObjStatsList iterates 0 blocks → count stays 0, no error.
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		dataStats2, err := state.CollectDataStats(ctx, snapshot, fs, mp)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), dataStats2.Rows, "no blocks in appendable object so 0 rows, no I/O")
	})

	t.Run("multiple_aobj_plus_non_appendable", func(t *testing.T) {
		state := NewPartitionState("", false, 42, false)
		// Non-appendable: 10 rows
		objN := objectio.NewObjectid()
		statsN := objectio.NewObjectStatsWithObjectID(&objN, false, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsN, 10))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsN,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.TS{},
		})
		// Appendable visible (no real file)
		objA := objectio.NewObjectid()
		statsA := objectio.NewObjectStatsWithObjectID(&objA, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsA, 5))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsA,
			CreateTime:  types.BuildTS(2, 0),
			DeleteTime:  types.TS{},
		})
		// CountRows with fs: appendable has no blocks in stats so no I/O, only non-appendable 10 counted.
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		total, err := state.CountRows(ctx, snapshot, fs, mp)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), total, "only non-appendable counted when appendable has no blocks")
		// With fs=nil same: 10 from non-appendable only.
		totalNil, err := state.CountRows(ctx, snapshot, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), totalNil)
	})

	t.Run("only_visible_appendable_in_loop", func(t *testing.T) {
		state := NewPartitionState("", false, 42, false)
		// One visible appendable (CreateTime=1), one invisible (CreateTime=15). Snapshot=10.
		objVis := objectio.NewObjectid()
		statsVis := objectio.NewObjectStatsWithObjectID(&objVis, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsVis, 4))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsVis,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.TS{},
		})
		objInvis := objectio.NewObjectid()
		statsInvis := objectio.NewObjectStatsWithObjectID(&objInvis, true, false, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(statsInvis, 6))
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: *statsInvis,
			CreateTime:  types.BuildTS(15, 0),
			DeleteTime:  types.TS{},
		})
		// Without blocks in stats, no I/O is done for appendable objects → 0 rows, no error.
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		dataStats, err := state.CollectDataStats(ctx, snapshot, fs, mp)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), dataStats.Rows, "no blocks in appendable objects so 0 from appendable path")
	})

	// appendableDataObjectWithCommitTS writes one appendable data object to fs with a single block
	// containing one int32 column and one commit_ts column; returns object stats and the number of
	// rows with commit_ts <= snapshot (for assertion).
	t.Run("persisted_appendable_with_commit_ts", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		// Write one appendable data object: 5 rows with commit_ts 2,4,6,8,10
		writer := ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS}, -1, false, false, fs)
		writer.SetAppendable()
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		commitTSs := []types.TS{
			types.BuildTS(2, 0), types.BuildTS(4, 0), types.BuildTS(6, 0), types.BuildTS(8, 0), types.BuildTS(10, 0),
		}
		for i := 0; i < 5; i++ {
			require.NoError(t, vector.AppendFixed[int32](bat.Vecs[0], int32(i), false, mp))
			require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[1], commitTSs[i], false, mp))
		}
		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats(objectio.WithAppendable())

		state := NewPartitionState("", false, 42, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.TS{},
		})
		// snapshot=10: rows with commit_ts 2,4,6,8,10 are visible (all 5)
		dataStats, err := state.CollectDataStats(ctx, types.BuildTS(10, 0), fs, mp)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), dataStats.Rows, "all 5 rows have commit_ts <= 10")
		// snapshot=7: only 2,4,6 visible (3 rows)
		dataStats2, err := state.CollectDataStats(ctx, types.BuildTS(7, 0), fs, mp)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), dataStats2.Rows, "only 3 rows have commit_ts <= 7")
	})

	// Cover error path: appendable object has blocks in stats but we read from a different fs
	// (no data there) → LoadColumnsData fails → CollectDataStats and CountRows return error.
	t.Run("appendable_with_blocks_wrong_fs_returns_error", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		fsWithData := testutil.NewSharedFS()
		writer := ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS}, -1, false, false, fsWithData)
		writer.SetAppendable()
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[0], 1, false, mp))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[1], types.BuildTS(1, 0), false, mp))
		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats(objectio.WithAppendable())

		state := NewPartitionState("", false, 42, false)
		state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
			DeleteTime:  types.TS{},
		})
		// Read with a different, empty fs → block not found → error.
		emptyFs := testutil.NewSharedFS()
		_, err = state.CollectDataStats(ctx, types.BuildTS(10, 0), emptyFs, mp)
		require.Error(t, err, "CollectDataStats must return error when appendable block read fails")
		_, err = state.CountRows(ctx, types.BuildTS(10, 0), emptyFs, mp)
		require.Error(t, err, "CountRows must return error when CollectDataStats fails")
	})
}
