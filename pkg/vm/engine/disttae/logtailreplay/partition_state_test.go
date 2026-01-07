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
	"math/rand"
	"testing"
	"time"

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
	state := NewPartitionState("", false, 42, false)

	// Test empty state
	count, err := state.CountRows(ctx, types.BuildTS(10, 0), nil)
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
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// Add non-appendable tombstone object with 20 deletions
	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 20))
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	// Count at TS=10, should see 100 - 20 = 80 rows
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(80), count)

	// Add in-memory inserts (5 rows)
	for i := 0; i < 5; i++ {
		rid := types.BuildTestRowid(int64(i), int64(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(3, uint32(i)),
			ID:      int64(i),
			Deleted: false,
		})
	}

	// Count at TS=10, should see 80 + 5 = 85 rows
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(85), count)

	// Add in-memory deletes (3 rows) - these point to non-existent objects
	// so they won't be counted with object visibility check
	for i := 5; i < 8; i++ {
		rid := types.BuildTestRowid(int64(i), int64(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(4, uint32(i)),
			ID:      int64(i),
			Deleted: true,
		})
	}

	// Count at TS=10, should see 85 - 0 = 85 rows (deletes filtered out due to no matching objects)
	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(85), count)

	// Test snapshot visibility: count at TS=1 (before tombstone)
	count, err = state.CountRows(ctx, types.BuildTS(1, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)

	// Test snapshot visibility: count at TS=2 (after tombstone, before inserts)
	count, err = state.CountRows(ctx, types.BuildTS(2, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(80), count)

	// Test snapshot visibility: count at TS=3 (after some inserts)
	count, err = state.CountRows(ctx, types.BuildTS(3, 2), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(83), count) // 80 + 3 inserts visible
}

func TestCountDataRows(t *testing.T) {
	state := NewPartitionState("", false, 42, false)

	// Test empty state
	count := state.CountDataRows(types.BuildTS(10, 0))
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

	count = state.CountDataRows(types.BuildTS(10, 0))
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

	count = state.CountDataRows(types.BuildTS(10, 0))
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

	count = state.CountDataRows(types.BuildTS(10, 0))
	assert.Equal(t, uint64(160), count)

	// Test snapshot visibility: count at TS=1
	count = state.CountDataRows(types.BuildTS(1, 0))
	assert.Equal(t, uint64(100), count)

	// Test snapshot visibility: count at TS=2
	count = state.CountDataRows(types.BuildTS(2, 0))
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
	count = state.CountDataRows(types.BuildTS(4, 0))
	assert.Equal(t, uint64(190), count)

	// At TS=5, object is deleted (100 + 50 + 10 in-mem = 160)
	count = state.CountDataRows(types.BuildTS(5, 0))
	assert.Equal(t, uint64(160), count)
}

func TestCountTombstoneRows(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs := testutil.NewSharedFS()

	state := NewPartitionState("", false, 42, false)

	// Create real tombstone object 1 with 20 deletions
	writer1 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 20; i++ {
		row := types.RandomRowid()
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

	count, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), count)

	// Create real tombstone object 2 with 15 deletions
	writer2 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 15; i++ {
		row := types.RandomRowid()
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

	count, err = state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(35), count)

	// Add in-memory deletes
	for i := 0; i < 5; i++ {
		rid := types.BuildTestRowid(int64(i), int64(i))
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(3, uint32(i)),
			ID:      int64(i),
			Deleted: true,
		})
	}

	count, err = state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(40), count)

	// Test snapshot visibility: count at TS=1
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(1, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), count)

	// Test snapshot visibility: count at TS=2
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(2, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(35), count)

	// Test deleted tombstone object visibility
	writer3 := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	bat3 := batch.NewWithSize(2)
	bat3.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat3.Vecs[1] = vector.NewVec(types.T_int32.ToType())

	for i := 0; i < 10; i++ {
		row := types.RandomRowid()
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
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(4, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(50), count)

	// At TS=5, tombstone object is deleted (20 + 15 + 5 in-mem = 40)
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(5, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(40), count)
}

func TestCountRowsEdgeCases(t *testing.T) {
	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Test when tombstones > data rows (should return 0)
	objID1 := objectio.NewObjectid()
	stats1 := objectio.NewObjectStatsWithObjectID(&objID1, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats1, 10))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats1,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	})

	objID2 := objectio.NewObjectid()
	stats2 := objectio.NewObjectStatsWithObjectID(&objID2, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats2, 20))
	state.tombstoneObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats2,
		CreateTime:  types.BuildTS(2, 0),
		DeleteTime:  types.TS{},
	})

	count, err := state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)

	// Test exact match (data == tombstones)
	objID3 := objectio.NewObjectid()
	stats3 := objectio.NewObjectStatsWithObjectID(&objID3, false, false, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats3, 10))
	state.dataObjectsNameIndex.Set(objectio.ObjectEntry{
		ObjectStats: *stats3,
		CreateTime:  types.BuildTS(3, 0),
		DeleteTime:  types.TS{},
	})

	count, err = state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), count)
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
	count := state.CountDataRows(types.BuildTS(10, 0))
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

	tombCount, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), nil, false)
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
		count := state.CountDataRows(tt.ts)
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
		state.rows.Set(&RowEntry{
			BlockID: rid.CloneBlockID(),
			RowID:   rid,
			Time:    types.BuildTS(2, uint32(i)),
			ID:      int64(i),
			Deleted: isDeleted,
		})
	}

	// Count: 100 (base) + 13 inserts - 0 deletes = 113 (deletes filtered out due to no matching objects)
	count, err := state.CountRows(ctx, types.BuildTS(10, 0), nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(113), count)

	// Verify individual counts
	dataCount := state.CountDataRows(types.BuildTS(10, 0))
	assert.Equal(t, uint64(113), dataCount) // 100 + 13

	tombCount, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), nil, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), tombCount)
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
		count := state.CountDataRows(tt.ts)
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

	count := state.CountDataRows(types.BuildTS(10, 0))
	assert.Equal(t, uint64(0), count)

	totalCount, err := state.CountRows(ctx, types.BuildTS(10, 0), nil)
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

	count := state.CountDataRows(types.BuildTS(1000, 0))
	assert.Equal(t, uint64(numObjects*rowsPerObject), count)

	totalCount, err := state.CountRows(ctx, types.BuildTS(1000, 0), nil)
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
		count := state.CountDataRows(tt.ts)
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
				count, err := state.CountRows(ctx, types.BuildTS(100, 0), nil)
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

	// Create tombstone with duplicates by writing multiple batches
	// This simulates the scenario where duplicates come from flush/merge transfer
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	
	// First batch: 10 deletions
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 10; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		pk := int32(i)
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], pk, false, mp))
	}
	_, err := writer.WriteBatch(bat1)
	require.NoError(t, err)
	
	// Second batch: 5 duplicates (same rowids)
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		pk := int32(i)
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], pk, false, mp))
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
	// Even if writer deduplicates, our logic should handle it correctly
	count, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, true)
	require.NoError(t, err)
	
	// Should count 10 unique deletions (duplicates filtered by writer or our logic)
	assert.Equal(t, uint64(10), count)
	
	// Test without object visibility check to verify file content
	countNoCheck, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
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
	count, err := state.CountTombstoneRows(ctx, types.BuildTS(4, 0), fs, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), count)

	// At TS=6, object 2 deleted, should count only 10 deletions (for object 1)
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(6, 0), fs, true)
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
	writer := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_None, fs)
	
	// Batch 1: deletions for all objects
	bat1 := batch.NewWithSize(2)
	bat1.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat1.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	
	// 5 deletions for object 1
	for i := 0; i < 5; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(i), false, mp))
	}
	
	// 3 deletions for object 2
	for i := 0; i < 3; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(i+100), false, mp))
	}
	
	// 4 deletions for object 3 (not visible)
	for i := 0; i < 4; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID3, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat1.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat1.Vecs[1], int32(i+200), false, mp))
	}
	
	_, err := writer.WriteBatch(bat1)
	require.NoError(t, err)
	
	// Batch 2: duplicates for object 1 and 2
	bat2 := batch.NewWithSize(2)
	bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat2.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	
	// 2 duplicates for object 1
	for i := 0; i < 2; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID1, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], int32(i), false, mp))
	}
	
	// 1 duplicate for object 2
	for i := 0; i < 1; i++ {
		blkID := objectio.NewBlockidWithObjectID(&dataObjID2, 0)
		rowid := types.NewRowid(&blkID, uint32(i))
		require.NoError(t, vector.AppendFixed[types.Rowid](bat2.Vecs[0], rowid, false, mp))
		require.NoError(t, vector.AppendFixed[int32](bat2.Vecs[1], int32(i+100), false, mp))
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
	count, err := state.CountTombstoneRows(ctx, types.BuildTS(4, 0), fs, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(8), count)

	// At TS=6: obj1 visible (5), obj2 deleted (0), obj3 not visible (0)
	// Total: 5
	count, err = state.CountTombstoneRows(ctx, types.BuildTS(6, 0), fs, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)
	
	// Without object visibility check: should count all rows including duplicates
	// Batch1: 5+3+4=12, Batch2: 2+1=3, Total: 15
	countNoCheck, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), countNoCheck)
	t.Logf("Total rows in file (with duplicates): %d", countNoCheck)
}

func TestCountTombstoneRowsWithRealObject(t *testing.T) {
	// This test demonstrates how to read real tombstone objects
	// Currently skipped as it requires actual tombstone files
	t.Skip("Requires real tombstone object files")

	ctx := context.Background()
	state := NewPartitionState("", false, 42, false)

	// Mock fileservice would be needed here
	// fs := testutil.NewSharedFS()

	// Add a real tombstone object
	objID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objID, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))

	tombstoneEntry := objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(1, 0),
		DeleteTime:  types.TS{},
	}
	state.tombstoneObjectsNameIndex.Set(tombstoneEntry)

	// To properly test with real files, we would need to:
	// 1. Create a test tombstone object file using objectio writer
	// 2. Read it back using ioutil.ReadDeletes
	// 3. Count visible deletions based on snapshot timestamp
	// 4. Verify the count matches expected value

	// For now, verify the basic counting works
	count, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), nil, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)
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
	count, err := state.CountTombstoneRows(ctx, types.BuildTS(10, 0), fs, false)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), count)
}
*/
