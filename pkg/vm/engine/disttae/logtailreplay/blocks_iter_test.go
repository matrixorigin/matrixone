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

package logtailreplay

import (
	"context"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestPartitionState_CollectObjectsBetweenInProgress(t *testing.T) {
	pState := NewPartitionState("", false, 0x3fff, false)

	//       t1			t2		   t3		  t4           t4         t5		 t5			t6		   t6
	// ---- obj1 ----- obj2 ----- obj3 ----- d-obj1 ----- obj4 ---- d-obj2 ---- obj5 ---- d-obj3 ---- obj6
	ts := types.TimestampToTS(timestamp.Timestamp{
		PhysicalTime: 10,
		LogicalTime:  00,
	})

	var (
		stats objectio.ObjectStats
		t1    = ts
		t2    = ts.Next()
		t3    = t2.Next()
		t4    = t3.Next()
		t5    = t4.Next()
		t6    = t5.Next()

		obj1, obj2, obj3, obj4, obj5, obj6 objectio.ObjectEntry
	)

	// t1: insert obj1
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj1"))
		obj1 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t1,
		}

		pState.dataObjectsNameIndex.Set(obj1)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj1.CreateTime,
			ShortObjName: *obj1.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t2: insert obj2
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj2"))
		obj2 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t2,
		}

		pState.dataObjectsNameIndex.Set(obj2)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj2.CreateTime,
			ShortObjName: *obj2.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t3: insert obj3
	{
		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj3"))
		obj3 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t3,
		}

		pState.dataObjectsNameIndex.Set(obj3)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj3.CreateTime,
			ShortObjName: *obj3.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t4: delete obj1, insert obj4
	{
		obj1.DeleteTime = t4
		pState.dataObjectsNameIndex.Set(obj1)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj1.DeleteTime,
			ShortObjName: *obj1.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj4"))
		obj4 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t4,
		}

		pState.dataObjectsNameIndex.Set(obj4)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj4.CreateTime,
			ShortObjName: *obj4.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// check 1
	{
		inserted, deleted := pState.CollectObjectsBetween(t1, t3)
		require.Nil(t, deleted)

		require.Equal(t, inserted,
			[]objectio.ObjectStats{
				obj1.ObjectStats,
				obj2.ObjectStats,
				obj3.ObjectStats,
			})
	}

	// check 2
	{
		inserted, deleted := pState.CollectObjectsBetween(t1, t4)
		require.Nil(t, deleted)
		require.Equal(t, inserted,
			[]objectio.ObjectStats{obj2.ObjectStats, obj3.ObjectStats, obj4.ObjectStats})
	}

	// check 3
	{
		inserted, deleted := pState.CollectObjectsBetween(t2, t4)
		require.Equal(t, deleted, []objectio.ObjectStats{obj1.ObjectStats})
		require.Equal(t, inserted,
			[]objectio.ObjectStats{obj2.ObjectStats, obj3.ObjectStats, obj4.ObjectStats})
	}

	// t5: delete obj2, insert obj5
	{
		obj2.DeleteTime = t5
		pState.dataObjectsNameIndex.Set(obj2)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj2.DeleteTime,
			ShortObjName: *obj2.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj5"))
		obj5 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t5,
		}

		pState.dataObjectsNameIndex.Set(obj5)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj5.CreateTime,
			ShortObjName: *obj5.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// t6: delete obj3, insert obj6
	{
		obj3.DeleteTime = t6
		pState.dataObjectsNameIndex.Set(obj3)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj3.DeleteTime,
			ShortObjName: *obj3.ObjectShortName(),
			IsDelete:     true,
		})

		objectio.SetObjectStatsObjectName(&stats, objectio.ObjectName("obj6"))
		obj6 = objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  t6,
		}

		pState.dataObjectsNameIndex.Set(obj6)
		pState.dataObjectTSIndex.Set(ObjectIndexByTSEntry{
			Time:         obj6.CreateTime,
			ShortObjName: *obj6.ObjectShortName(),
			IsDelete:     false,
		})
	}

	// random check
	{
		//name := []string{"t1", "t2", "t3", "t4", "t5", "t6"}
		tss := []types.TS{t1, t2, t3, t4, t5, t6}
		for i := 0; i < 100000; i++ {
			x := rand.Int() % (len(tss) - 1)
			y := rand.Int()%(len(tss)-x) + x

			tx := tss[x]
			ty := tss[y]

			require.True(t, ty.GE(&tx))

			inserted, deleted := pState.CollectObjectsBetween(tx, ty)
			for _, ss := range inserted {
				obj, ok := pState.GetObject(*ss.ObjectShortName())
				require.True(t, ok)

				if obj.DeleteTime.IsEmpty() {
					ok = obj.CreateTime.GE(&tx) && obj.CreateTime.LE(&ty)
					require.True(t, ok)
				} else {
					ok = obj.CreateTime.GE(&tx) && obj.CreateTime.LE(&ty) && obj.DeleteTime.GT(&ty)
					require.True(t, ok)
				}
			}

			for _, ss := range deleted {
				obj, ok := pState.GetObject(*ss.ObjectShortName())
				require.True(t, ok)

				require.False(t, obj.DeleteTime.IsEmpty())

				ok = obj.CreateTime.LT(&tx) && obj.DeleteTime.GE(&tx) && obj.DeleteTime.LE(&ty)
				require.True(t, ok)
			}

			//fmt.Println(name[x], name[y], len(inserted), len(deleted))

		}
	}
}

func TestPartitionState_NewBlocksIter(t *testing.T) {
	pState := NewPartitionState("", false, 0x3fff, false)
	pState.start = types.BuildTS(100, 0)
	_, err := pState.NewObjectsIter(types.BuildTS(99, 0), false, true)
	require.Error(t, err)
}

func TestPartitionStateScanVisibleDataObjectsPage(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	snapshot := types.BuildTS(100, 0)
	state.start = types.BuildTS(1, 0)

	entries := make([]objectio.ObjectEntry, 5)
	segmentID := objectio.NewSegmentid()
	for i := range entries {
		stats := objectio.NewObjectStats()
		name := objectio.BuildObjectName(segmentID, uint16(i))
		require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, uint32(i+1)))
		entries[i] = objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(i+1), 0),
		}
		state.dataObjectsNameIndex.Set(entries[i])
	}

	// Deleted before the snapshot is not visible.
	deleted := entries[1]
	deleted.DeleteTime = types.BuildTS(50, 0)
	state.dataObjectsNameIndex.Set(deleted)

	// Appendable objects are not lifecycle retirement candidates.
	appendable := entries[3]
	objectio.SetObjectStatsAppendable(&appendable.ObjectStats, true)
	state.dataObjectsNameIndex.Set(appendable)

	first, err := state.ScanVisibleDataObjectsPage(
		context.Background(), snapshot, nil, 2, 1<<20)
	require.NoError(t, err)
	require.Len(t, first.Objects, 2)
	require.False(t, first.End)
	require.Equal(t, *first.Objects[1].ObjectShortName(), *first.LastObjectName)

	second, err := state.ScanVisibleDataObjectsPage(
		context.Background(), snapshot, first.LastObjectName, 2, 1<<20)
	require.NoError(t, err)
	require.Len(t, second.Objects, 1)
	require.True(t, second.End)
	require.Equal(t, *entries[4].ObjectShortName(), *second.Objects[0].ObjectShortName())

	// The byte limit is also hard. One entry is allowed so forward progress is
	// possible, then the page stops before adding another.
	limited, err := state.ScanVisibleDataObjectsPage(
		context.Background(), snapshot, nil, 100, lifecycleObjectEntryMetaBytes)
	require.NoError(t, err)
	require.Len(t, limited.Objects, 1)
	require.False(t, limited.End)
}

func TestPartitionStateScanVisibleDataObjectsPageLimitsAndCancellation(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	state.start = types.BuildTS(1, 0)

	_, err := state.ScanVisibleDataObjectsPage(
		context.Background(), types.BuildTS(2, 0), nil, 0, 1)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = state.ScanVisibleDataObjectsPage(
		ctx, types.BuildTS(2, 0), nil, 1, lifecycleObjectEntryMetaBytes)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPartitionStateSelectLifecycleTombstonesFailClosed(t *testing.T) {
	state := NewPartitionState("", false, 42, false)
	state.start = types.BuildTS(1, 0)
	snapshot := types.BuildTS(100, 0)
	sourceID := objectio.NewObjectid()
	otherID := objectio.NewObjectid()

	related := makeLifecycleTombstoneEntry(t, sourceID, 1)
	unrelated := makeLifecycleTombstoneEntry(t, otherID, 2)
	unknown := makeLifecycleTombstoneEntry(t, otherID, 3)
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		&unknown.ObjectStats,
		index.NewZM(types.T_Rowid, 0),
	))
	malformed := makeLifecycleTombstoneEntry(t, otherID, 4)
	malformedZoneMap := index.BuildZM(
		types.T_Rowid,
		make([]byte, types.RowidSize),
	)
	// Keep the ZoneMap initialized but corrupt its physical min/max lengths.
	// Lifecycle must conservatively protect this Object without invoking the
	// unsafe RowidPrefixEq decoder.
	malformedZoneMap[30] = 0
	malformedZoneMap[61] = 0
	require.True(t, malformedZoneMap.Valid())
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(
		&malformed.ObjectStats,
		malformedZoneMap,
	))
	state.tombstoneObjectsNameIndex.Set(related)
	state.tombstoneObjectsNameIndex.Set(unrelated)
	state.tombstoneObjectsNameIndex.Set(unknown)
	state.tombstoneObjectsNameIndex.Set(malformed)
	state.tombstoneObjectDTSIndex.Set(related)
	state.tombstoneObjectDTSIndex.Set(unrelated)
	state.tombstoneObjectDTSIndex.Set(unknown)
	state.tombstoneObjectDTSIndex.Set(malformed)

	selected, scanned, err := state.SelectLifecycleTombstoneObjects(
		context.Background(),
		snapshot,
		[]objectio.ObjectId{sourceID},
		LifecycleTombstoneSelectionLimits{
			MaxScannedObjects:  10,
			MaxSelectedObjects: 10,
			MaxMetaBytes:       1 << 20,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 4, scanned)
	require.Len(t, selected, 3)
	require.Equal(t, *related.ObjectShortName(), *selected[0].ObjectShortName())
	require.Equal(t, *unknown.ObjectShortName(), *selected[1].ObjectShortName())
	require.Equal(t, *malformed.ObjectShortName(), *selected[2].ObjectShortName())

	_, _, err = state.SelectLifecycleTombstoneObjects(
		context.Background(),
		snapshot,
		[]objectio.ObjectId{sourceID},
		LifecycleTombstoneSelectionLimits{
			MaxScannedObjects:  2,
			MaxSelectedObjects: 10,
			MaxMetaBytes:       1 << 20,
		},
	)
	require.Error(t, err)
}

func makeLifecycleTombstoneEntry(
	t *testing.T,
	dataObjectID objectio.ObjectId,
	number uint16,
) objectio.ObjectEntry {
	t.Helper()
	tombstoneID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&tombstoneID, false, true, false)
	blockID := objectio.NewBlockidWithObjectID(&dataObjectID, 0)
	first := types.NewRowid(&blockID, 0)
	last := types.NewRowid(&blockID, 100)
	zoneMap := index.NewZM(types.T_Rowid, 0)
	index.UpdateZM(zoneMap, first[:])
	index.UpdateZM(zoneMap, last[:])
	require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, zoneMap))
	segmentID := stats.ObjectName().SegmentId()
	name := objectio.BuildObjectName(&segmentID, number)
	require.NoError(t, objectio.SetObjectStatsObjectName(stats, name))
	return objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(2, 0),
	}
}

func TestFilterBatchCase2InsertThenDelete(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test scenario: For the same primary key:
	// 1. Insert at ts=100 (first operation)
	// 2. Delete at ts=200 (last operation)
	//
	// Expected behavior after fix:
	// - Insert should be removed (it's not the last operation)
	// - Delete should be KEPT (to ensure downstream consistency in CDC restart)

	pkValue := int32(999)
	primarySeqnum := 0
	// Create data batch with 1 insert operation
	dataBatch := batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)

	// Primary key column
	pkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=100
	dataBatch.Vecs[0] = pkVec

	// Data column
	col1Vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp)
	dataBatch.Vecs[1] = col1Vec

	// Timestamp column (last column)
	tsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp) // Insert at ts=100
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(1)

	// Create tombstone batch with 1 delete operation
	tombstoneBatch := batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)

	// Primary key column
	tombstonePkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=200
	tombstoneBatch.Vecs[0] = tombstonePkVec

	// Timestamp column
	tombstoneTsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(200, 0), false, mp) // Delete at ts=200 (last)
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(1)

	// Call filterBatch with skipDeletes=true (CDC scenario)
	err := filterBatch(dataBatch, tombstoneBatch, primarySeqnum, true, true)
	require.NoError(t, err)

	// Verify the fix:
	// OLD behavior (BUG): Both data and tombstone were empty (data=0, tombstone=0)
	// NEW behavior (FIX): Delete is preserved (data=0, tombstone=1)

	// Data batch should be empty (insert is removed because delete is the last operation)
	assert.Equal(t, 0, dataBatch.Vecs[0].Length(),
		"Data batch should be empty because the delete is the last operation")

	// Tombstone batch should contain the delete (THIS IS THE FIX)
	assert.Equal(t, 1, tombstoneBatch.Vecs[0].Length(),
		"Tombstone batch should contain the delete to ensure downstream consistency in CDC restart")

	// Verify the remaining delete has the correct timestamp
	if tombstoneBatch.Vecs[0].Length() > 0 {
		remainingDeleteTs := vector.MustFixedColWithTypeCheck[types.TS](tombstoneBatch.Vecs[1])
		assert.Equal(t, types.BuildTS(200, 0), remainingDeleteTs[0],
			"The remaining delete should be at ts=200")

		remainingDeletePk := vector.MustFixedColWithTypeCheck[int32](tombstoneBatch.Vecs[0])
		assert.Equal(t, pkValue, remainingDeletePk[0],
			"The remaining delete should have the correct primary key")
	}
}

// TestFilterBatchCase2InsertThenDeleteMultiple tests Case 2.2 with multiple operations:
// Insert -> Insert -> Delete -> Delete
// Only the last delete should be preserved.
func TestFilterBatchCase2InsertThenDeleteMultiple(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test scenario: For the same primary key:
	// 1. Insert at ts=100 (first operation)
	// 2. Insert at ts=200 (middle operation)
	// 3. Delete at ts=300 (middle operation)
	// 4. Delete at ts=400 (last operation)
	//
	// Expected behavior after fix:
	// - All inserts should be removed
	// - Only the last delete (ts=400) should be kept

	pkValue := int32(888)
	primarySeqnum := 0

	// Create data batch with 2 insert operations
	dataBatch := batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)

	pkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=100
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=200
	dataBatch.Vecs[0] = pkVec

	col1Vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp)
	vector.AppendFixed(col1Vec, int64(200), false, mp)
	dataBatch.Vecs[1] = col1Vec

	tsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp)
	vector.AppendFixed(tsVec, types.BuildTS(200, 0), false, mp)
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(2)

	// Create tombstone batch with 2 delete operations
	tombstoneBatch := batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)

	tombstonePkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=300
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=400
	tombstoneBatch.Vecs[0] = tombstonePkVec

	tombstoneTsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(300, 0), false, mp)
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(400, 0), false, mp) // Last operation
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(2)

	// Call filterBatch with skipDeletes=true
	err := filterBatch(dataBatch, tombstoneBatch, primarySeqnum, true, true)
	require.NoError(t, err)

	// Verify: data should be empty, tombstone should have 1 row (last delete)
	assert.Equal(t, 0, dataBatch.Vecs[0].Length(),
		"Data batch should be empty")
	assert.Equal(t, 1, tombstoneBatch.Vecs[0].Length(),
		"Tombstone batch should contain only the last delete")

	// Verify the remaining delete is the last one (ts=400)
	if tombstoneBatch.Vecs[0].Length() > 0 {
		remainingDeleteTs := vector.MustFixedColWithTypeCheck[types.TS](tombstoneBatch.Vecs[1])
		assert.Equal(t, types.BuildTS(400, 0), remainingDeleteTs[0],
			"The remaining delete should be at ts=400 (the last one)")
	}
}
