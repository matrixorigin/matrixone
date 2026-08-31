// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestDeletedObjectFilterUsesExactTransferWindow(t *testing.T) {
	emptyFilter := newDeletedObjectFilter(nil)
	require.False(t, emptyFilter(objectio.MockObjectName().ObjectId()))

	deletedName := objectio.MockObjectName()
	var deletedStats objectio.ObjectStats
	require.NoError(t, objectio.SetObjectStatsObjectName(&deletedStats, deletedName))

	filter := newDeletedObjectFilter([]objectio.ObjectStats{deletedStats})
	require.True(t, filter(deletedName.ObjectId()))

	// A live appendable object has no entry in PartitionState's persisted-object
	// index. It must not be mistaken for an object deleted in this window.
	liveAppendableName := objectio.MockObjectName()
	require.False(t, filter(liveAppendableName.ObjectId()))

	// Nor should an object deleted before a previous transfer window be moved
	// again merely because its tombstone shares a file with an affected object.
	previouslyDeletedName := objectio.MockObjectName()
	require.False(t, filter(previouslyDeletedName.ObjectId()))
}

func TestTransferBatchLimit(t *testing.T) {
	tests := []struct {
		name            string
		rowCount        int
		byteSize        int
		sourceBatchDone bool
		want            bool
	}{
		{name: "empty", byteSize: transferBatchSizeLimit, sourceBatchDone: true},
		{
			name:     "below both limits",
			rowCount: transferBatchRowLimit - 1,
			byteSize: transferBatchSizeLimit - 1,
		},
		{
			name:     "row limit",
			rowCount: transferBatchRowLimit,
			want:     true,
		},
		{
			name:            "size limit after source batch",
			rowCount:        1,
			byteSize:        transferBatchSizeLimit,
			sourceBatchDone: true,
			want:            true,
		},
		{
			name:     "size is not measured per row",
			rowCount: 1,
			byteSize: transferBatchSizeLimit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, transferBatchLimitReached(
				test.rowCount, test.byteSize, test.sourceBatchDone))
		})
	}
}

func TestTransferFlowBatchesAcrossObjectBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()
	objectID := objectio.NewObjectid()
	rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(objectID, 0, 0)
	rowCount := int(objectio.BlockMaxRows)

	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		input.Vecs[0], makeRepeated(rowID, rowCount), nil, mp))
	require.NoError(t, vector.AppendFixedList(
		input.Vecs[1], makeRepeated(int64(1), rowCount), nil, mp))
	input.SetRowCount(rowCount)
	defer input.Clean(mp)

	staged := batch.NewWithSize(2)
	staged.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	staged.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	flow := &TransferFlow{
		isObjectDeletedFn: func(*objectio.ObjectId) bool { return true },
		staged:            staged,
		mp:                mp,
	}
	flow.transferred.objDetails = make(map[string]int)
	defer flow.staged.Clean(mp)

	// The former one-block policy invoked RowID lookup after the first call.
	// Keeping both blocks staged proves the expensive lookup setup is amortized
	// without changing which rows are selected for transfer.
	require.NoError(t, flow.processOneBatch(ctx, input))
	require.Equal(t, rowCount, flow.staged.RowCount())
	require.NoError(t, flow.processOneBatch(ctx, input))
	require.Equal(t, rowCount*2, flow.staged.RowCount())
	require.Equal(t, rowCount*2, flow.transferred.rowCnt)
}

func makeRepeated[T any](value T, count int) []T {
	values := make([]T, count)
	for i := range values {
		values[i] = value
	}
	return values
}

func TestTransferFlowStagesOnlyObjectsDeletedInWindow(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()
	state := logtailreplay.NewPartitionState("", false, 42, false)
	start := types.BuildTS(10, 0)
	end := types.BuildTS(20, 0)

	// Reproduce the root ownership boundary: CN intentionally does not add a
	// live appendable object's create event to the persisted-object index.
	liveAppendableID := objectio.NewObjectid()
	liveAppendableStats := objectio.NewObjectStatsWithObjectID(
		&liveAppendableID, true, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(liveAppendableStats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *liveAppendableStats,
		CreateTime:  types.BuildTS(5, 0),
	}, false))
	_, exists := state.GetObject(*liveAppendableStats.ObjectShortName())
	require.False(t, exists)

	// A persisted source object visible at start is deleted during the window,
	// and a replacement object is created by the same merge.
	deletedID := objectio.NewObjectid()
	deletedStats := objectio.NewObjectStatsWithObjectID(
		&deletedID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(deletedStats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *deletedStats,
		CreateTime:  types.BuildTS(5, 0),
		DeleteTime:  types.BuildTS(15, 0),
	}, false))

	replacementID := objectio.NewObjectid()
	replacementStats := objectio.NewObjectStatsWithObjectID(
		&replacementID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(replacementStats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *replacementStats,
		CreateTime:  types.BuildTS(15, 0),
	}, false))

	newDataObjects, deletedObjects := state.CollectObjectsBetween(start, end)
	require.Len(t, newDataObjects, 1)
	require.Equal(t, replacementID, *newDataObjects[0].ObjectName().ObjectId())
	require.Len(t, deletedObjects, 1)
	require.Equal(t, deletedID, *deletedObjects[0].ObjectName().ObjectId())

	liveAppendableName := liveAppendableStats.ObjectName()
	deletedName := deletedStats.ObjectName()

	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	defer input.Clean(mp)

	liveRowID := types.NewRowIDWithObjectIDBlkNumAndRowID(
		*liveAppendableName.ObjectId(), 0, 0,
	)
	deletedRowID := types.NewRowIDWithObjectIDBlkNumAndRowID(
		*deletedName.ObjectId(), 0, 0,
	)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], liveRowID, false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(1), false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], deletedRowID, false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(2), false, mp))
	input.SetRowCount(2)

	staged := batch.NewWithSize(2)
	staged.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	staged.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	flow := &TransferFlow{
		isObjectDeletedFn: newDeletedObjectFilter(deletedObjects),
		staged:            staged,
		mp:                mp,
	}
	flow.transferred.objDetails = make(map[string]int)
	defer flow.staged.Clean(mp)

	require.NoError(t, flow.processOneBatch(ctx, input))
	require.Equal(t, 1, flow.staged.RowCount())
	require.Equal(t, deletedRowID,
		vector.MustFixedColWithTypeCheck[types.Rowid](flow.staged.Vecs[0])[0])
	require.Equal(t, int64(2),
		vector.MustFixedColWithTypeCheck[int64](flow.staged.Vecs[1])[0])
	require.Equal(t, 1, flow.transferred.rowCnt)
	require.Equal(t, map[string]int{deletedName.ObjectId().ShortStringEx(): 1},
		flow.transferred.objDetails)
}

func BenchmarkDeletedObjectFilter(b *testing.B) {
	for _, objectCount := range []int{1, 16, 256, 4096} {
		deletedObjects := make([]objectio.ObjectStats, objectCount)
		for i := range deletedObjects {
			require.NoError(b, objectio.SetObjectStatsObjectName(
				&deletedObjects[i], objectio.MockObjectName()))
		}

		b.Run(fmt.Sprintf("build/%d", objectCount), func(b *testing.B) {
			b.ReportAllocs()
			var filter func(*objectio.ObjectId) bool
			for i := 0; i < b.N; i++ {
				filter = newDeletedObjectFilter(deletedObjects)
			}
			runtime.KeepAlive(filter)
		})

		filter := newDeletedObjectFilter(deletedObjects)
		objectID := deletedObjects[objectCount-1].ObjectName().ObjectId()
		b.Run(fmt.Sprintf("lookup/%d", objectCount), func(b *testing.B) {
			b.ReportAllocs()
			var deleted bool
			for i := 0; i < b.N; i++ {
				deleted = filter(objectID)
			}
			runtime.KeepAlive(deleted)
		})
	}
}
