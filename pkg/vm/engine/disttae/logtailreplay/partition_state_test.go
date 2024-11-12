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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func BenchmarkPartitionStateConcurrentWriteAndIter(b *testing.B) {
	partition := NewPartition("", 42)
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
	partition := NewPartitionState("", true, 42)
	partition.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(2, 0))
	addObject(partition, types.BuildTS(1, 0), types.BuildTS(3, 0))
	addObject(partition, types.BuildTS(1, 0), types.TS{})

	partition.truncate([2]uint64{0, 0}, types.BuildTS(1, 0))
	assert.Equal(t, 5, partition.dataObjectTSIndex.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(2, 0))
	assert.Equal(t, 3, partition.dataObjectTSIndex.Len())

	partition.truncate([2]uint64{0, 0}, types.BuildTS(3, 0))
	assert.Equal(t, 1, partition.dataObjectTSIndex.Len())

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
	state := NewPartitionState("", true, 42)
	require.False(t, state.HasTombstoneChanged(types.BuildTS(13, 0), types.BuildTS(15, 0)))

	roid := func() objectio.ObjectStats {
		return *objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), false, true, false)
	}

	state.tombstoneObjectDTSIndex.Set(ObjectEntry{ObjectInfo{ObjectStats: roid(), CreateTime: types.BuildTS(5, 0), DeleteTime: types.BuildTS(8, 0)}})
	state.tombstoneObjectDTSIndex.Set(ObjectEntry{ObjectInfo{ObjectStats: roid(), CreateTime: types.BuildTS(1, 0), DeleteTime: types.BuildTS(7, 0)}})
	state.tombstoneObjectDTSIndex.Set(ObjectEntry{ObjectInfo{ObjectStats: roid(), CreateTime: types.BuildTS(6, 0), DeleteTime: types.BuildTS(12, 0)}})
	state.tombstoneObjectDTSIndex.Set(ObjectEntry{ObjectInfo{ObjectStats: roid(), CreateTime: types.BuildTS(6, 0), DeleteTime: types.BuildTS(24, 0)}})
	require.True(t, state.HasTombstoneChanged(types.BuildTS(24, 0), types.BuildTS(30, 0)))
	require.False(t, state.HasTombstoneChanged(types.BuildTS(25, 0), types.BuildTS(30, 0)))

	for i := 10; i < 20; i++ {
		state.tombstoneObjectDTSIndex.Set(ObjectEntry{
			ObjectInfo{
				ObjectStats: roid(),
				CreateTime:  types.BuildTS(int64(i), 0),
			},
		})
	}

	require.True(t, state.HasTombstoneChanged(types.BuildTS(13, 0), types.BuildTS(15, 0)))
	require.True(t, state.HasTombstoneChanged(types.BuildTS(9, 0), types.BuildTS(15, 0)))
	require.False(t, state.HasTombstoneChanged(types.BuildTS(25, 0), types.BuildTS(30, 0)))

}
