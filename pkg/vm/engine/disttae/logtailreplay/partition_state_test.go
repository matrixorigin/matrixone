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
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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

func TestGetChangedTombstoneObjsBetween(t *testing.T) {
	state := NewPartitionState("", true, 42, false)

	// empty state
	objs := state.GetChangedTombstoneObjsBetween(types.BuildTS(1, 0))
	require.Empty(t, objs)

	roid := func() objectio.ObjectStats {
		nobjid := objectio.NewObjectid()
		return *objectio.NewObjectStatsWithObjectID(&nobjid, false, true, false)
	}

	// obj1: created=5, deleted=8
	// obj2: created=1, deleted=7
	// obj3: created=6, deleted=12
	// obj4: created=15, alive (no delete)
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(5, 0), DeleteTime: types.BuildTS(8, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(7, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(6, 0), DeleteTime: types.BuildTS(12, 0)})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{ObjectStats: roid(), CreateTime: types.BuildTS(15, 0)})

	// from=13: should get obj4 (created=15>=13)
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(13, 0))
	require.Len(t, objs, 1)
	require.True(t, objs[0].CreateTime.Equal(&[]types.TS{types.BuildTS(15, 0)}[0]))

	// from=6: should get obj1(deleted=8>=6), obj2(deleted=7>=6), obj3(created=6>=6, deleted=12>=6), obj4(created=15>=6)
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(6, 0))
	require.Len(t, objs, 4)

	// from=1: all objects match (obj2 created=1>=1, others also match)
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(1, 0))
	require.Len(t, objs, 4)

	// from=20: only nothing matches
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(20, 0))
	require.Empty(t, objs)

	// from=9: obj3(deleted=12>=9), obj4(created=15>=9) = 2
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(9, 0))
	require.Len(t, objs, 2)
}

func TestGetChangedTombstoneObjsBetween_RowCount(t *testing.T) {
	state := NewPartitionState("", true, 42, false)

	roidWithRows := func(rows uint32) objectio.ObjectStats {
		nobjid := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&nobjid, false, true, false)
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, rows))
		return *stats
	}

	// Add tombstone objects with known row counts
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{
		ObjectStats: roidWithRows(30000),
		CreateTime:  types.BuildTS(10, 0),
	})
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{
		ObjectStats: roidWithRows(10000),
		CreateTime:  types.BuildTS(12, 0),
	})

	// Total rows = 40000 < 50000 threshold
	objs := state.GetChangedTombstoneObjsBetween(types.BuildTS(5, 0))
	require.Len(t, objs, 2)
	var totalRows uint32
	for _, obj := range objs {
		totalRows += obj.Rows()
	}
	require.Equal(t, uint32(40000), totalRows)

	// Add one more to exceed threshold
	state.tombstoneObjectDTSIndex.Set(objectio.ObjectEntry{
		ObjectStats: roidWithRows(20000),
		CreateTime:  types.BuildTS(14, 0),
	})
	objs = state.GetChangedTombstoneObjsBetween(types.BuildTS(5, 0))
	require.Len(t, objs, 3)
	totalRows = 0
	for _, obj := range objs {
		totalRows += obj.Rows()
	}
	require.Equal(t, uint32(60000), totalRows)
	// Caller (tombstonePKExistsInRange) would return true conservatively when totalRows > 50000
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
