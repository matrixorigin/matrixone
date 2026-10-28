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
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
		name      string
		rowCount  int
		byteSize  int
		checkSize bool
		want      bool
	}{
		{name: "empty", byteSize: transferBatchSizeLimit, checkSize: true},
		{
			name:     "below both limits",
			rowCount: transferBatchRowLimit - 1,
			byteSize: transferBatchSizeLimit - 1,
		},
		{
			name:     "above row limit",
			rowCount: transferBatchRowLimit + 1,
			want:     true,
		},
		{
			name:     "row limit",
			rowCount: transferBatchRowLimit,
			want:     true,
		},
		{
			name:      "size limit when checked",
			rowCount:  1,
			byteSize:  transferBatchSizeLimit,
			checkSize: true,
			want:      true,
		},
		{
			name:     "size check deferred",
			rowCount: 1,
			byteSize: transferBatchSizeLimit,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, transferBatchLimitReached(
				test.rowCount, test.byteSize, test.checkSize))
		})
	}
	t.Run("wide input and reset", func(t *testing.T) {
		mp := mpool.MustNewZero()
		func() {
			staged := batch.NewWithSchema(false, []string{"rowid", "pk", "entry", "position"}, []types.Type{
				types.T_Rowid.ToType(), types.T_varchar.ToType(), types.T_int32.ToType(), types.T_int32.ToType(),
			})
			defer staged.Clean(mp)
			key := make([]byte, types.MaxVarcharLen)
			rowid := types.NewRowIDWithObjectIDBlkNumAndRowID(objectio.NewObjectid(), 0, 0)
			lastSize := 0
			for !transferBatchLimitReached(staged.RowCount(), staged.Size(), true) {
				lastSize = staged.Size()
				require.NoError(t, vector.AppendFixed(staged.Vecs[0], rowid, false, mp))
				require.NoError(t, vector.AppendBytes(staged.Vecs[1], key, false, mp))
				for _, v := range staged.Vecs[2:] {
					require.NoError(t, vector.AppendFixed(v, int32(0), false, mp))
				}
				staged.SetRowCount(staged.Vecs[0].Length())
			}
			require.Less(t, lastSize, transferBatchSizeLimit)
			require.GreaterOrEqual(t, staged.Size(), transferBatchSizeLimit)
			require.Less(t, staged.RowCount(), int(transferBatchRowLimit))
			require.LessOrEqual(t, staged.Size()-lastSize, len(key)+types.RowidSize+types.VarlenaSize+8)
			for _, v := range staged.Vecs {
				v.Reset(*v.GetType())
			}
			staged.SetRowCount(0)
			require.Zero(t, staged.Size(), "retained capacity cannot trigger another flush")
			require.False(t, transferBatchLimitReached(staged.RowCount(), staged.Size(), true))
			require.NoError(t, vector.AppendBytes(staged.Vecs[1], []byte("inline"), false, mp))
			require.Less(t, staged.Size(), transferBatchSizeLimit)
		}()
		require.Zero(t, mp.CurrNB())
	})

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
	// Feed a live logtail object-list event, not the checkpoint callback.
	liveBatch := batch.NewWithSize(10)
	defer liveBatch.Clean(mp)
	var err error
	for i := range liveBatch.Vecs {
		liveBatch.Vecs[i], err = vector.NewConstFixed(types.T_TS.ToType(), types.BuildTS(5, 0), 1, mp)
		require.NoError(t, err)
	}
	liveBatch.Vecs[2].Free(mp)
	liveBatch.Vecs[2], err = vector.NewConstBytes(types.T_binary.ToType(), liveAppendableStats.Marshal(), 1, mp)
	require.NoError(t, err)
	liveBatch.Vecs[6].Free(mp)
	liveBatch.Vecs[6], err = vector.NewConstFixed(types.T_TS.ToType(), types.TS{}, 1, mp)
	require.NoError(t, err)
	liveBatch.SetRowCount(1)
	liveProto, err := batch.BatchToProtoBatch(liveBatch)
	require.NoError(t, err)
	state.HandleDataObjectList(ctx, &api.Entry{Bat: liveProto}, nil, mp)
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

// TestTransferFlowPassThroughCarriesVerbatimRows pins the r26 review F3 / D113
// total-replacement property of the pass-through mode: rows whose object is
// NOT deleted travel into the leftover batch verbatim (identical row ids, in
// observed order), so the published replacement covers EVERY consumed input
// row and the caller can retire the superseded source entries; the selective
// mode (passThrough=false) must keep discarding them — that is the
// ordinary-transaction behavior the provenance gate protects.
func TestTransferFlowPassThroughCarriesVerbatimRows(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()

	deletedID := objectio.NewObjectid()
	deletedStats := objectio.NewObjectStatsWithObjectID(&deletedID, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(deletedStats, 1))
	deletedObjects := []objectio.ObjectStats{*deletedStats}

	liveID := objectio.NewObjectid()
	liveRow1 := types.NewRowIDWithObjectIDBlkNumAndRowID(liveID, 0, 0)
	deadRow := types.NewRowIDWithObjectIDBlkNumAndRowID(deletedID, 0, 0)
	liveRow2 := types.NewRowIDWithObjectIDBlkNumAndRowID(liveID, 0, 1)

	newVecs := func() (*batch.Batch, *batch.Batch) {
		input := batch.NewWithSize(2)
		input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		staged := batch.NewWithSize(2)
		staged.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		staged.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		return input, staged
	}
	mkFlow := func(passThrough bool, staged *batch.Batch) *TransferFlow {
		flow := &TransferFlow{
			isObjectDeletedFn: newDeletedObjectFilter(deletedObjects),
			staged:            staged,
			mp:                mp,
			passThrough:       passThrough,
			// The production leftover batch rides the one-schema pool:
			// Close, not the test, owns its release (r27 review F4).
			buffer: containers.NewOneSchemaBatchBuffer(
				mpool.MB,
				[]string{"rowid", "pk"},
				[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()},
				false),
		}
		flow.transferred.objDetails = make(map[string]int)
		return flow
	}
	fillInput := func(input *batch.Batch) {
		// live row, deleted row, live row — interleaved so the per-object
		// memoization switch runs in both directions.
		require.NoError(t, vector.AppendFixed(input.Vecs[0], liveRow1, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(7), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[0], deadRow, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(9), false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[0], liveRow2, false, mp))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(8), false, mp))
		input.SetRowCount(3)
	}

	t.Run("pass-through carries live-object rows verbatim", func(t *testing.T) {
		input, staged := newVecs()
		fillInput(input)
		flow := mkFlow(true, staged)

		require.NoError(t, flow.processOneBatch(ctx, input))
		require.Equal(t, 1, flow.staged.RowCount(),
			"only the deleted-object row stages for re-key")
		require.Equal(t, deadRow,
			vector.MustFixedColWithTypeCheck[types.Rowid](flow.staged.Vecs[0])[0])
		require.Equal(t, 2, flow.leftover.RowCount(),
			"every live-object row reaches the leftover batch")
		leftIDs := vector.MustFixedColWithTypeCheck[types.Rowid](flow.leftover.Vecs[0])
		require.Equal(t, liveRow1, leftIDs[0], "verbatim identity, observed order")
		require.Equal(t, liveRow2, leftIDs[1])
		leftPKs := vector.MustFixedColWithTypeCheck[int64](flow.leftover.Vecs[1])
		require.Equal(t, int64(7), leftPKs[0])
		require.Equal(t, int64(8), leftPKs[1])
		require.Equal(t, 1, flow.transferred.rowCnt,
			"leftover rows never count as transferred")

		// Close owns staged and leftover; no test-side batch cleanup may
		// mask a production leak.
		require.NoError(t, flow.Close())
		require.Nil(t, flow.leftover)
		require.Nil(t, flow.staged)
		require.Nil(t, flow.buffer)
		input.Clean(mp)
	})

	t.Run("selective mode keeps discarding live-object rows", func(t *testing.T) {
		input, staged := newVecs()
		fillInput(input)
		flow := mkFlow(false, staged)

		require.NoError(t, flow.processOneBatch(ctx, input))
		require.Equal(t, 1, flow.staged.RowCount())
		require.Nil(t, flow.leftover,
			"ordinary transactions allocate no leftover batch: zero-cost gate")

		require.NoError(t, flow.Close())
		input.Clean(mp)
	})
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

// --- r27 review F1 (D114): the persisted-tombstone scan must skip the
// retired nil slot instead of dereferencing it ---

// TestCollectCnTombstoneDeleteStatsSkipsRetiredNilSlots pins the r27 review
// F1 contract: a pk-move transfer retirement releases a fully covered
// file-backed DELETE entry's batch IN PLACE while keeping the DELETE type
// and the fileName (the nil-slot lifecycle, txn.go
// releaseWorkspaceEntryBatchLocked). The raw txn.writes scan behind
// ConstructCNTombstoneObjectsTransferFlow dereferenced e.bat.Vecs[0]
// unconditionally, so a second qualifying transfer window inside the same
// transaction panicked. The collector must skip nil/empty slots exactly
// like every other persisted-DELETE scan in the package (txn_table.go,
// table_meta_reader.go, local_disttae_datasource.go, object_list.go all
// guard identically); a rolled-back retirement re-appends a live tail
// entry, which the collector then sees again.
func TestCollectCnTombstoneDeleteStatsSkipsRetiredNilSlots(t *testing.T) {
	mp := mpool.MustNewZero()
	const (
		dbID    = uint64(7)
		tableID = uint64(42)
	)

	statA := pkMoveTestObjectStats(t)
	statB := pkMoveTestObjectStats(t)
	statC := pkMoveTestObjectStats(t)
	statD := pkMoveTestObjectStats(t)
	statE := pkMoveTestObjectStats(t)

	live := pkMoveFileDeleteEntry(t, mp, 1, dbID, tableID, statA, statB)
	defer live.bat.Clean(mp)

	// The in-place release of a fully covered entry: DELETE type and
	// fileName stay, the batch is gone.
	retired := pkMoveFileDeleteEntry(t, mp, 1, dbID, tableID, statC)
	retired.bat.Clean(mp)
	retired.bat = nil

	insertWithFile := Entry{
		typ: INSERT, accountId: 1, databaseId: dbID, tableId: tableID,
		fileName: "insert-file",
	}
	inMemDelete := pkMoveFileDeleteEntry(t, mp, 1, dbID, tableID, statE)
	inMemDelete.fileName = ""
	defer inMemDelete.bat.Clean(mp)
	otherTable := pkMoveFileDeleteEntry(t, mp, 1, dbID, tableID+1, statD)
	defer otherTable.bat.Clean(mp)

	entries := []Entry{live, retired, insertWithFile, inMemDelete, otherTable}
	got := collectCnTombstoneDeleteStats(entries, dbID, tableID)
	require.Len(t, got, 2,
		"only this table's LIVE file-backed DELETE stats; the retired nil slot is skipped, not dereferenced")
	require.Equal(t, statA.Marshal(), got[0].Marshal())
	require.Equal(t, statB.Marshal(), got[1].Marshal())
}

// --- r27 review F4 (D117): Close releases the checked-out leftover batch ---

// transferFlowStubReader feeds one synthetic batch, then reports the
// injected error — the exact abandoned-mid-Process shape the r27 review
// priced: the flow owns a checked-out leftover holding rows at that point.
type transferFlowStubReader struct {
	fill func(out *batch.Batch, mp *mpool.MPool) error
	err  error
	read int
}

func (r *transferFlowStubReader) Read(
	_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch,
) (bool, error) {
	r.read++
	if r.read == 1 && r.fill != nil {
		if err := r.fill(out, mp); err != nil {
			return false, err
		}
		return false, nil
	}
	return false, r.err
}

func (r *transferFlowStubReader) Close() error                         { return nil }
func (r *transferFlowStubReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (r *transferFlowStubReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (r *transferFlowStubReader) SetIndexParam(*plan.IndexReaderParam) {}
func (r *transferFlowStubReader) SetFilterZM(objectio.ZoneMap)         {}

// TestTransferFlowCloseReleasesLeftover pins the r27 review F4 contract: the
// pass-through leftover batch is CHECKED OUT of the one-schema pool, and the
// pool's own Close frees only batches returned to it. An error between
// checkout and orphanLeftover (reader, rekey, sorter, or sinker) used to
// strand the batch's memory because TransferFlow.Close never touched the
// field. Process-then-Close must return the mpool to baseline, and a
// repeated Close must be a safe no-op.
func TestTransferFlowCloseReleasesLeftover(t *testing.T) {
	mp := mpool.MustNewZero()
	ctx := context.Background()

	liveID := objectio.NewObjectid()
	liveRow1 := types.NewRowIDWithObjectIDBlkNumAndRowID(liveID, 0, 0)
	liveRow2 := types.NewRowIDWithObjectIDBlkNumAndRowID(liveID, 0, 1)

	newFlow := func(sourcer engine.Reader) *TransferFlow {
		flow := &TransferFlow{
			isObjectDeletedFn: newDeletedObjectFilter(nil),
			sourcer:           sourcer,
			mp:                mp,
			passThrough:       true,
			buffer: containers.NewOneSchemaBatchBuffer(
				mpool.MB,
				[]string{"rowid", "pk"},
				[]types.Type{types.T_Rowid.ToType(), types.T_int64.ToType()},
				false),
		}
		flow.transferred.objDetails = make(map[string]int)
		return flow
	}

	t.Run("process error with accumulated leftover returns mpool to baseline", func(t *testing.T) {
		injected := errors.New("injected reader failure")
		reader := &transferFlowStubReader{
			fill: func(out *batch.Batch, mp *mpool.MPool) error {
				for i, pair := range [][2]any{{liveRow1, int64(7)}, {liveRow2, int64(8)}} {
					if err := vector.AppendFixed(
						out.Vecs[0], pair[0].(types.Rowid), false, mp); err != nil {
						return err
					}
					if err := vector.AppendFixed(
						out.Vecs[1], pair[1].(int64), false, mp); err != nil {
						return err
					}
					_ = i
				}
				out.SetRowCount(2)
				return nil
			},
			err: injected,
		}
		flow := newFlow(reader)

		err := flow.Process(ctx)
		require.ErrorIs(t, err, injected)
		require.NotNil(t, flow.leftover,
			"the reader error stranded two verbatim rows in the checked-out leftover")
		require.Equal(t, 2, flow.leftover.RowCount())
		require.Greater(t, flow.leftover.Allocated(), 0,
			"the flow still owns resident memory only Close can release")
		mpBase := mp.CurrNB()

		require.NoError(t, flow.Close())
		require.Nil(t, flow.leftover, "Close owns the checked-out leftover")
		require.Nil(t, flow.staged)
		require.Nil(t, flow.buffer,
			"every ownership field clears — nothing references freed batches")
		require.Equal(t, mpBase, mp.CurrNB(),
			"accounted mpool usage returns to baseline")
		require.NoError(t, flow.Close(), "repeated Close is a safe no-op")
	})
}
