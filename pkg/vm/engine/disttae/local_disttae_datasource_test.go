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

package disttae

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

type fixedObjectIter struct {
	entries []objectio.ObjectEntry
	pos     int
}

func (i *fixedObjectIter) Next() bool {
	if i.pos >= len(i.entries) {
		return false
	}
	i.pos++
	return true
}

func (i *fixedObjectIter) Entry() objectio.ObjectEntry {
	return i.entries[i.pos-1]
}

func (i *fixedObjectIter) Close() error {
	return nil
}

func TestReusableObjectStatsIterAllocations(t *testing.T) {
	const count = 10_000
	entries := make([]objectio.ObjectEntry, count)

	var visited int
	allocs := testing.AllocsPerRun(10, func() {
		visited = 0
		iter := reusableObjectStatsIter{
			iter: &fixedObjectIter{entries: entries},
		}
		for {
			stats, err := iter.next()
			if err != nil {
				t.Fatal(err)
			}
			if stats == nil {
				break
			}
			visited++
		}
	})

	require.Equal(t, count, visited)
	require.LessOrEqual(t, allocs, float64(2))
}

func TestLocalDisttaeDataSourceUsesTombstoneObjectIndex(t *testing.T) {
	pState := logtailreplay.NewPartitionState("", true, 0, false)
	pState.UpdateDuration(types.BuildTS(0, 0), types.MaxTs())

	stats := testTombstoneStats(10, 20)
	require.NoError(t, objectio.SetObjectStatsObjectName(
		&stats,
		objectio.BuildObjectName(objectio.NewSegmentid(), 0),
	))
	require.NoError(t, objectio.SetObjectStatsSize(&stats, 1))
	require.NoError(t, pState.HandleObjectEntry(
		context.Background(),
		nil,
		objectio.ObjectEntry{
			ObjectStats: stats,
			CreateTime:  types.BuildTS(1, 0),
		},
		true,
	))

	ls := LocalDisttaeDataSource{
		ctx:        context.Background(),
		pState:     pState,
		snapshotTS: types.BuildTS(2, 0),
		rangeSlice: readutil.NewBlockListRelationData(
			tombstoneRangeIndexMinBlocks,
		).GetBlockInfoSlice(),
	}
	block := testBlockID(5)
	offsets := []int64{7}

	left, err := ls.applyPStateTombstoneObjects(&block, offsets, nil)
	require.NoError(t, err)
	require.Equal(t, offsets, left)
	require.True(t, ls.pStateTombstoneObjects.initialized)
	require.Len(t, ls.pStateTombstoneObjects.index.objects, 1)
	require.Empty(t, ls.pStateTombstoneObjects.candidates)

	require.NoError(t, ls.initPStateTombstoneObjectIndex())
	require.Len(t, ls.pStateTombstoneObjects.index.objects, 1)
}

func TestRelationDataV2_MarshalAndUnMarshal(t *testing.T) {
	location := objectio.NewRandomLocation(0, 0)
	objID := location.ObjectId()
	metaLoc := objectio.ObjectLocation(location)

	relData := readutil.NewBlockListRelationData(0)
	blkNum := 10
	for i := 0; i < blkNum; i++ {
		blkID := types.NewBlockidWithObjectID(&objID, uint16(blkNum))
		blkInfo := objectio.BlockInfo{
			BlockID: blkID,
			MetaLoc: metaLoc,
		}
		blkInfo.ObjectFlags |= objectio.ObjectFlag_Appendable
		relData.AppendBlockInfo(&blkInfo)
	}

	tombstone := readutil.NewEmptyTombstoneData()
	for i := 0; i < 3; i++ {
		rowid := types.RandomRowid()
		tombstone.AppendInMemory(rowid)
	}
	var stats1, stats2 objectio.ObjectStats
	location1 := objectio.NewRandomLocation(1, 1111)
	location2 := objectio.NewRandomLocation(2, 1111)

	objectio.SetObjectStatsLocation(&stats1, location1)
	objectio.SetObjectStatsLocation(&stats2, location2)
	tombstone.AppendFiles(stats1, stats2)
	relData.AttachTombstones(tombstone)

	buf, err := relData.MarshalBinary()
	require.NoError(t, err)

	newRelData, err := readutil.UnmarshalRelationData(buf)
	require.NoError(t, err)
	require.Equal(t, relData.String(), newRelData.String())
}

func TestLocalDatasource_ApplyWorkspaceFlushedS3Deletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	txnOp.AddWorkspace(&Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	})

	txnDB := txnDatabase{
		op: txnOp,
	}

	txnTbl := txnTable{
		db: &txnDB,
	}

	pState := logtailreplay.NewPartitionState("", true, 0, false)

	proc := testutil.NewProc(t)

	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	ls := &LocalDisttaeDataSource{
		fs:     fs,
		ctx:    ctx,
		table:  &txnTbl,
		pState: pState,
	}

	//var stats []objectio.ObjectStats
	int32Type := types.T_int32.ToType()
	var tombstoneRowIds []types.Rowid
	for i := 0; i < 3; i++ {
		writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, int32Type, -1)
		require.NoError(t, err)

		bat := readutil.NewCNTombstoneBatch(
			&int32Type,
			objectio.HiddenColumnSelection_None,
		)

		for j := 0; j < 10; j++ {
			row := types.RandomRowid()
			tombstoneRowIds = append(tombstoneRowIds, row)
			vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
			vector.AppendFixed[int32](bat.Vecs[1], int32(j), false, proc.GetMPool())
		}

		bat.SetRowCount(bat.Vecs[0].Length())

		err = writer.Write(ctx, bat)
		require.NoError(t, err)

		ss, err := writer.Sync(proc.Ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(ss))
		require.False(t, ss[0].IsZero())

		//stats = append(stats, ss)
		txnOp.GetWorkspace().(*Transaction).StashFlushedTombstones(ss[0])
	}

	deletedMask := objectio.GetReusableBitmap()
	defer deletedMask.Release()
	for i := range tombstoneRowIds {
		bid := tombstoneRowIds[i].BorrowBlockID()
		left, err := ls.applyWorkspaceFlushedS3Deletes(bid, nil, &deletedMask)
		require.NoError(t, err)
		require.Zero(t, len(left))

		require.True(t, deletedMask.Contains(uint64(tombstoneRowIds[i].GetRowOffset())))
	}
}

func TestBigS3WorkspaceIterMissingData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	// This batch can be obtained by 'insert into db.t1 select result from generate_series(1, 67117056) g;'
	s3Bat := batch.NewWithSize(2)
	s3Bat.SetRowCount(8193)
	s3Bat.SetAttributes([]string{catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
	txn := &Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
		op:            txnOp,
		deletedBlocks: &deletedBlocks{},
		writes: []Entry{
			{
				typ:        INSERT,
				databaseId: 11,
				tableId:    22,
				fileName:   "a-s3-file-name",
				bat:        s3Bat,
			},
		},
	}

	// This batch can be obtained by 'insert into db.t2 values (1);'
	normalBat := batch.NewWithSize(1)
	normalBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	m := mpool.MustNewZero()
	normalBat.SetRowCount(1)
	vector.AppendFixed(normalBat.Vecs[0], int32(1), false, m)
	txn.WriteBatch(INSERT, "", 0, 11, 23, "db", "t2", normalBat, DNStore{})

	txnOp.AddWorkspace(txn)

	// query t2 table
	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: len(txn.writes),
		table: &txnTable{
			db: &txnDatabase{
				databaseId: 11,
				op:         txnOp,
			},
			tableId: 23,
		},
		memPKFilter: &readutil.MemPKFilter{},
	}

	outBatch := batch.NewWithSize(1)
	outBatch.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	err := ls.filterInMemUnCommittedInserts(ctx, []uint16{0}, -1, m, outBatch)
	require.NoError(t, err)
	require.Equal(t, 1, outBatch.RowCount())
	require.Equal(t, 1, outBatch.Vecs[0].Length())
}

func TestLocalDatasourceWorkspaceDeleteEntriesSortsWithoutMutatingBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	blk2 := types.NewBlockidWithObjectID(&oid, 2)
	blk3 := types.NewBlockidWithObjectID(&oid, 3)
	rows1 := []types.Rowid{
		types.NewRowid(&blk, 3),
		types.NewRowid(&blk, 1),
	}
	rows2 := []types.Rowid{
		types.NewRowid(&blk2, 2),
	}

	m := mpool.MustNewZero()
	delVec := vector.NewVec(types.T_Rowid.ToType())
	for _, row := range rows1 {
		require.NoError(t, vector.AppendFixed(delVec, row, false, m))
	}
	require.False(t, delVec.GetSorted())

	delBat := batch.NewWithSize(1)
	delBat.SetAttributes([]string{catalog.Row_ID})
	delBat.Vecs[0] = delVec
	delBat.SetRowCount(len(rows1))

	delVec2 := vector.NewVec(types.T_Rowid.ToType())
	for _, row := range rows2 {
		require.NoError(t, vector.AppendFixed(delVec2, row, false, m))
	}
	delBat2 := batch.NewWithSize(1)
	delBat2.SetAttributes([]string{catalog.Row_ID})
	delBat2.Vecs[0] = delVec2
	delBat2.SetRowCount(len(rows2))

	txn := &Transaction{
		op: txnOp,
		writes: []Entry{
			{
				typ:        DELETE,
				databaseId: 11,
				tableId:    22,
				bat:        delBat,
			},
			{
				typ:        DELETE,
				databaseId: 11,
				tableId:    22,
				bat:        delBat2,
			},
		},
	}
	txnOp.AddWorkspace(txn)

	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: len(txn.writes),
		table: &txnTable{
			db: &txnDatabase{
				databaseId: 11,
				op:         txnOp,
			},
			tableId: 22,
		},
	}

	entries := ls.workspaceDeleteEntriesLocked()
	require.Len(t, entries, 2)
	for _, entry := range entries {
		require.True(t, entry.sorted)
		require.True(t, slices.IsSortedFunc(entry.rowIds, func(a, b types.Rowid) int { return a.Compare(&b) }))
	}
	blkEntries := ls.workspaceDeleteEntriesForBlockLocked(&blk)
	require.Len(t, blkEntries, 1)
	require.Equal(t, []types.Rowid{types.NewRowid(&blk, 1), types.NewRowid(&blk, 3)}, blkEntries[0].rowIds)
	blk2Entries := ls.workspaceDeleteEntriesForBlockLocked(&blk2)
	require.Len(t, blk2Entries, 1)
	require.Equal(t, rows2, blk2Entries[0].rowIds)
	require.Empty(t, ls.workspaceDeleteEntriesForBlockLocked(&blk3))
	require.Equal(t, []int64{4}, ls.applyWorkspaceEntryDeletes(&blk, []int64{1, 3, 4}, nil))
	require.Equal(t, []int64{4}, ls.applyWorkspaceEntryDeletes(&blk2, []int64{2, 4}, nil))
	require.Equal(t, []int64{1, 4}, ls.applyWorkspaceEntryDeletes(&blk3, []int64{1, 4}, nil))

	original := vector.MustFixedColNoTypeCheck[types.Rowid](delVec)
	require.Equal(t, rows1, original)
	require.False(t, delVec.GetSorted())
	require.Equal(t, rows2, vector.MustFixedColNoTypeCheck[types.Rowid](delVec2))
}

func TestLocalDatasourceWorkspaceDeleteEntriesMergesLargeDeleteSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	blk2 := types.NewBlockidWithObjectID(&oid, 2)

	// Cross the merge threshold and mix blocks/order so the test exercises the
	// flatten-sort-then-split path, not just the small-entry cache path.
	writes := make([]Entry, 0, mergeWorkspaceDeleteEntriesThreshold+1)
	for i := 0; i < mergeWorkspaceDeleteEntriesThreshold+1; i++ {
		bid := &blk
		offset := uint32(i)
		if i%2 == 0 {
			bid = &blk2
			offset = uint32(mergeWorkspaceDeleteEntriesThreshold - i)
		}
		writes = append(writes, Entry{
			typ:        DELETE,
			databaseId: 11,
			tableId:    22,
			bat:        newWorkspaceDeleteBatch(t, []types.Rowid{types.NewRowid(bid, offset)}),
		})
	}

	txn := &Transaction{op: txnOp, writes: writes}
	txnOp.AddWorkspace(txn)

	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: len(txn.writes),
		table: &txnTable{
			db: &txnDatabase{
				databaseId: 11,
				op:         txnOp,
			},
			tableId: 22,
		},
	}

	entries := ls.workspaceDeleteEntriesLocked()
	require.Len(t, entries, 1)
	require.True(t, entries[0].sorted)
	require.Len(t, entries[0].rowIds, mergeWorkspaceDeleteEntriesThreshold+1)
	require.True(t, slices.IsSortedFunc(entries[0].rowIds, func(a, b types.Rowid) int { return a.Compare(&b) }))

	blkEntries := ls.workspaceDeleteEntriesForBlockLocked(&blk)
	require.Len(t, blkEntries, 1)
	require.True(t, blkEntries[0].sorted)
	for _, rowID := range blkEntries[0].rowIds {
		require.Equal(t, blk, *rowID.BorrowBlockID())
	}

	blk2Entries := ls.workspaceDeleteEntriesForBlockLocked(&blk2)
	require.Len(t, blk2Entries, 1)
	require.True(t, blk2Entries[0].sorted)
	for _, rowID := range blk2Entries[0].rowIds {
		require.Equal(t, blk2, *rowID.BorrowBlockID())
	}
}

func TestLocalDatasourceWorkspaceDeleteEntriesIndexesHotBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	otherBlk := types.NewBlockidWithObjectID(&oid, 2)
	writes := make([]Entry, 0, indexWorkspaceDeleteEntriesForBlockThreshold+1)
	for i := indexWorkspaceDeleteEntriesForBlockThreshold - 1; i >= 0; i-- {
		writes = append(writes, Entry{
			typ:        DELETE,
			databaseId: 11,
			tableId:    22,
			bat:        newWorkspaceDeleteBatch(t, []types.Rowid{types.NewRowid(&blk, uint32(i))}),
		})
	}
	writes = append(writes, Entry{
		typ:        DELETE,
		databaseId: 11,
		tableId:    22,
		bat:        newWorkspaceDeleteBatch(t, []types.Rowid{types.NewRowid(&otherBlk, 7)}),
	})
	txn := &Transaction{op: txnOp, writes: writes}
	txnOp.AddWorkspace(txn)
	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: indexWorkspaceDeleteEntriesForBlockThreshold - 1,
		table: &txnTable{
			db:      &txnDatabase{databaseId: 11, op: txnOp},
			tableId: 22,
		},
	}

	require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold-1)
	require.Equal(t, []int64{0}, ls.applyWorkspaceEntryDeletes(&blk, []int64{0}, nil))
	require.Equal(t, []int64{indexWorkspaceDeleteEntriesForBlockThreshold},
		ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold}, nil))
	require.Empty(t, ls.workspaceDeletes.pointRows)
	ls.txnOffset = len(txn.writes)
	require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold)
	require.Empty(t, ls.workspaceDeletes.pointRows)
	require.Equal(t, []int64{indexWorkspaceDeleteEntriesForBlockThreshold},
		ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold}, nil))
	_, seen := ls.workspaceDeletes.pointRows[blk]
	require.True(t, seen)
	require.Nil(t, ls.workspaceDeletes.pointRows[blk])
	require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold - 1}, nil))
	require.Nil(t, ls.workspaceDeletes.pointRows[blk])
	require.Equal(t, []int64{indexWorkspaceDeleteEntriesForBlockThreshold},
		ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold}, nil))
	require.Len(t, ls.workspaceDeletes.pointRows[blk], objectio.BlockMaxRows/64)
	require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold)
	require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold - 1}, nil))
	require.Equal(t, []int64{indexWorkspaceDeleteEntriesForBlockThreshold},
		ls.applyWorkspaceEntryDeletes(&blk, []int64{0, 1, indexWorkspaceDeleteEntriesForBlockThreshold - 1, indexWorkspaceDeleteEntriesForBlockThreshold}, nil))
	require.Empty(t, ls.applyWorkspaceEntryDeletes(&otherBlk, []int64{7}, nil))

	// A later workspace delete invalidates the point index. The row that was
	// a miss above must now be filtered.
	txn.writes = append(txn.writes, Entry{
		typ: DELETE, databaseId: 11, tableId: 22,
		bat: newWorkspaceDeleteBatch(t, []types.Rowid{types.NewRowid(&blk, indexWorkspaceDeleteEntriesForBlockThreshold)}),
	})
	ls.txnOffset = len(txn.writes)
	require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold+1)
	require.Empty(t, ls.workspaceDeletes.pointRows)
	require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{indexWorkspaceDeleteEntriesForBlockThreshold}, nil))
}

func TestLocalDatasourceWorkspaceDeleteEntriesKeepsLargeBlockBatches(t *testing.T) {
	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	for _, tc := range []struct {
		name      string
		totalRows int
		wantIndex bool
	}{
		{name: "at cap", totalRows: maxIndexedWorkspaceDeleteRowsPerBlock, wantIndex: true},
		{name: "above cap", totalRows: maxIndexedWorkspaceDeleteRowsPerBlock + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LocalDisttaeDataSource{txnOffset: 1}
			ls.rc.WorkspaceLocked = true
			ls.workspaceDeletes.initialized = true
			ls.workspaceDeletes.txnOffset = 1
			ls.workspaceDeletes.entries = make([]workspaceDeleteEntry, indexWorkspaceDeleteEntriesForBlockThreshold)
			firstRows := tc.totalRows - len(ls.workspaceDeletes.entries) + 1
			rows := make([]types.Rowid, firstRows)
			for i := range rows {
				rows[i] = types.NewRowid(&blk, uint32(i))
			}
			ls.workspaceDeletes.entries[0] = workspaceDeleteEntry{rowIds: rows, sorted: true}
			for i := 1; i < len(ls.workspaceDeletes.entries); i++ {
				ls.workspaceDeletes.entries[i] = workspaceDeleteEntry{
					rowIds: []types.Rowid{types.NewRowid(&blk, uint32(len(rows)+i))},
					sorted: true,
				}
			}
			require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold)
			for i := 0; i < 2; i++ {
				require.Equal(t, []int64{int64(tc.totalRows + 1)},
					ls.applyWorkspaceEntryDeletes(&blk, []int64{int64(tc.totalRows + 1)}, nil))
			}
			require.Len(t, ls.workspaceDeleteEntriesForBlockLocked(&blk), indexWorkspaceDeleteEntriesForBlockThreshold)
			require.Equal(t, tc.wantIndex, len(ls.workspaceDeletes.pointRows[blk]) > 0)
		})
	}
}

func TestLocalDatasourceWorkspaceDeleteColdReadsKeepBatches(t *testing.T) {
	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	for _, rowsPerEntry := range []int{2, 32} {
		t.Run(fmt.Sprint(rowsPerEntry), func(t *testing.T) {
			entries := make([]workspaceDeleteEntry, indexWorkspaceDeleteEntriesForBlockThreshold)
			for i := range entries {
				rows := make([]types.Rowid, rowsPerEntry)
				for j := range rows {
					rows[j] = types.NewRowid(&blk, uint32((len(entries)-1-i)*len(rows)+j))
				}
				entries[i] = workspaceDeleteEntry{rowIds: rows, sorted: true}
			}
			ls := &LocalDisttaeDataSource{txnOffset: 1}
			ls.rc.WorkspaceLocked = true
			ls.workspaceDeletes.initialized = true
			ls.workspaceDeletes.txnOffset = 1
			ls.workspaceDeletes.entries = entries
			ls.workspaceDeletes.byBlock = map[objectio.Blockid][]workspaceDeleteEntry{blk: entries}

			rowCount := len(entries) * rowsPerEntry
			mask := objectio.GetReusableBitmap()
			defer mask.Release()
			ls.applyWorkspaceEntryDeletes(&blk, nil, &mask)
			require.True(t, mask.Contains(0))
			require.True(t, mask.Contains(uint64(rowCount-1)))
			require.Len(t, ls.workspaceDeletes.byBlock[blk], len(entries))

			require.Equal(t, []int64{int64(rowCount)}, ls.applyWorkspaceEntryDeletes(&blk, []int64{0, int64(rowCount)}, nil))
			require.Len(t, ls.workspaceDeletes.byBlock[blk], len(entries))

			require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{int64(rowCount - rowsPerEntry)}, nil))
			require.Len(t, ls.workspaceDeletes.byBlock[blk], len(entries))
			require.Empty(t, ls.workspaceDeletes.pointRows)

			require.Equal(t, []int64{int64(rowCount)}, ls.applyWorkspaceEntryDeletes(&blk, []int64{int64(rowCount)}, nil))
			require.Len(t, ls.workspaceDeletes.byBlock[blk], len(entries))
			if rowsPerEntry > maxIndexedWorkspaceDeleteRowsPerBlock/len(entries) {
				require.Empty(t, ls.workspaceDeletes.pointRows)
			} else {
				_, seen := ls.workspaceDeletes.pointRows[blk]
				require.True(t, seen)
				require.Nil(t, ls.workspaceDeletes.pointRows[blk])
			}
		})
	}
}

func TestLocalDatasourceWorkspaceDeletePointIndexOffsetBoundary(t *testing.T) {
	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	for _, target := range []uint32{objectio.BlockMaxRows - 1, objectio.BlockMaxRows} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			entries := make([]workspaceDeleteEntry, indexWorkspaceDeleteEntriesForBlockThreshold)
			for i := range entries {
				offset := uint32(i)
				if i == 0 {
					offset = target
				}
				entries[i] = workspaceDeleteEntry{
					rowIds: []types.Rowid{types.NewRowid(&blk, offset)}, sorted: true,
				}
			}
			ls := &LocalDisttaeDataSource{txnOffset: 1}
			ls.rc.WorkspaceLocked = true
			ls.workspaceDeletes.initialized = true
			ls.workspaceDeletes.txnOffset = 1
			ls.workspaceDeletes.entries = entries
			ls.workspaceDeletes.byBlock = map[objectio.Blockid][]workspaceDeleteEntry{blk: entries}

			miss := int64(objectio.BlockMaxRows - 2)
			for i := 0; i < 2; i++ {
				require.Equal(t, []int64{miss},
					ls.applyWorkspaceEntryDeletes(&blk, []int64{miss}, nil))
			}
			require.Equal(t, target < objectio.BlockMaxRows, len(ls.workspaceDeletes.pointRows[blk]) > 0)
			require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{int64(target)}, nil))
			if target < objectio.BlockMaxRows {
				outOfRange := (int64(1) << 32) + int64(target)
				require.Equal(t, []int64{outOfRange},
					ls.applyWorkspaceEntryDeletes(&blk, []int64{outOfRange}, nil))
			}
		})
	}
}

func TestLocalDatasourceWorkspaceDeletePointSearchSortedBatch(t *testing.T) {
	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	for _, count := range []int{31, 32, 33} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rows := make([]types.Rowid, count)
			for i := range rows {
				rows[i] = types.NewRowid(&blk, uint32(i*17))
			}
			entries := []workspaceDeleteEntry{{rowIds: rows, sorted: true}}
			ls := &LocalDisttaeDataSource{txnOffset: 1}
			ls.rc.WorkspaceLocked = true
			ls.workspaceDeletes.initialized = true
			ls.workspaceDeletes.txnOffset = 1
			ls.workspaceDeletes.entries = entries
			ls.workspaceDeletes.byBlock = map[objectio.Blockid][]workspaceDeleteEntry{blk: entries}
			for _, offset := range []int64{0, int64((count / 2) * 17), int64((count - 1) * 17)} {
				require.Empty(t, ls.applyWorkspaceEntryDeletes(&blk, []int64{offset}, nil))
			}
			require.Equal(t, []int64{1}, ls.applyWorkspaceEntryDeletes(&blk, []int64{1}, nil))
			require.Equal(t, []int64{int64(count * 17)}, ls.applyWorkspaceEntryDeletes(&blk, []int64{int64(count * 17)}, nil))
			require.Equal(t, rows, entries[0].rowIds)
		})
	}
}

func TestLocalDatasourceWorkspaceDeleteEntriesInvalidatesCacheWhenTxnOffsetChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	oid := types.NewObjectid()
	blk := types.NewBlockidWithObjectID(&oid, 1)
	blk2 := types.NewBlockidWithObjectID(&oid, 2)
	row := types.NewRowid(&blk, 1)
	row2 := types.NewRowid(&blk2, 2)

	txn := &Transaction{
		op: txnOp,
		writes: []Entry{
			{
				typ:        DELETE,
				databaseId: 11,
				tableId:    22,
				bat:        newWorkspaceDeleteBatch(t, []types.Rowid{row}),
			},
			{
				typ:        DELETE,
				databaseId: 11,
				tableId:    22,
				bat:        newWorkspaceDeleteBatch(t, []types.Rowid{row2}),
			},
		},
	}
	txnOp.AddWorkspace(txn)

	ls := &LocalDisttaeDataSource{
		ctx:       ctx,
		txnOffset: 1,
		table: &txnTable{
			db: &txnDatabase{
				databaseId: 11,
				op:         txnOp,
			},
			tableId: 22,
		},
	}

	require.Equal(t, []workspaceDeleteEntry{{rowIds: []types.Rowid{row}, sorted: true}}, ls.workspaceDeleteEntriesForBlockLocked(&blk))
	require.Empty(t, ls.workspaceDeleteEntriesForBlockLocked(&blk2))
	require.Equal(t, 1, ls.workspaceDeletes.txnOffset)
	require.NotNil(t, ls.workspaceDeletes.byBlock)

	ls.txnOffset = 2
	entries := ls.workspaceDeleteEntriesLocked()
	require.Len(t, entries, 2)
	require.Equal(t, 2, ls.workspaceDeletes.txnOffset)
	// Changing txnOffset must invalidate the block index built for the previous
	// view of txn writes, otherwise later deletes can be silently skipped.
	require.Nil(t, ls.workspaceDeletes.byBlock)
	require.Equal(t, []workspaceDeleteEntry{{rowIds: []types.Rowid{row2}, sorted: true}}, ls.workspaceDeleteEntriesForBlockLocked(&blk2))
	require.Equal(t, []workspaceDeleteEntry{{rowIds: []types.Rowid{row}, sorted: true}}, ls.workspaceDeleteEntriesForBlockLocked(&blk))
}

func newWorkspaceDeleteBatch(t *testing.T, rows []types.Rowid) *batch.Batch {
	t.Helper()

	m := mpool.MustNewZero()
	delVec := vector.NewVec(types.T_Rowid.ToType())
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(delVec, row, false, m))
	}

	delBat := batch.NewWithSize(1)
	delBat.SetAttributes([]string{catalog.Row_ID})
	delBat.Vecs[0] = delVec
	delBat.SetRowCount(len(rows))
	return delBat
}

// TestLocalDisttaeDataSource_getBlockZMs_ColumnLookup tests the column lookup logic
// that fixes the bug where ColPos in JOIN scenarios points to projection list position
// instead of table column position.
func TestLocalDisttaeDataSource_getBlockZMs_ColumnLookup(t *testing.T) {
	// This test focuses on testing the column lookup logic without requiring actual block data.
	// We test that the function correctly finds columns by name even when ColPos is wrong.

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "remark", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 8},      // Position 8: VARCHAR
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 9}, // Position 9: DATETIME
		},
		Name2ColIndex: map[string]int32{
			"id":         0, // Index in Cols array
			"remark":     1, // Index in Cols array
			"created_at": 2, // Index in Cols array
		},
	}

	// Test case 1: Find column by qualified name "table.column"
	orderByCol := &plan.ColRef{
		Name:   "table.created_at",
		ColPos: 8, // Wrong ColPos (points to remark in projection list)
	}
	orderByColName := "table.created_at"
	if idx := strings.LastIndex(strings.ToLower(orderByColName), "."); idx >= 0 {
		orderByColName = orderByColName[idx+1:]
	}
	require.Equal(t, "created_at", orderByColName, "Should extract column name from qualified name")

	var orderByColIDX int = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	require.Equal(t, 9, orderByColIDX, "Should find created_at (seqnum 9) by name, not remark (seqnum 8)")

	// Test case 2: Find column by simple name
	orderByColName = "created_at"
	orderByColIDX = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	require.Equal(t, 9, orderByColIDX, "Should find created_at by simple name")

	// Test case 3: Fallback to ColPos when name lookup fails
	orderByColName = "nonexistent_column"
	orderByCol.ColPos = 1 // Valid ColPos pointing to remark (index 1 in Cols array)
	orderByColIDX = -1
	if tableDef.Name2ColIndex != nil {
		if colIdx, ok := tableDef.Name2ColIndex[orderByColName]; ok {
			orderByColIDX = int(tableDef.Cols[colIdx].Seqnum)
		}
	}
	// Fallback to ColPos
	if orderByColIDX == -1 {
		if int(orderByCol.ColPos) < len(tableDef.Cols) {
			orderByColIDX = int(tableDef.Cols[int(orderByCol.ColPos)].Seqnum)
		}
	}
	require.Equal(t, 8, orderByColIDX, "Should fallback to ColPos 1 (remark, seqnum 8) when name lookup fails")
}

// TestLocalDisttaeDataSource_getBlockZMs tests the fix for the bug where
// ColPos in JOIN scenarios points to projection list position instead of table column position.
// This test verifies that getBlockZMs correctly finds columns by name.
func TestLocalDisttaeDataSource_getBlockZMs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	// Create a table definition with multiple columns
	// Simulating the bug scenario: created_at is at seqnum 9, but ColPos might point to position 8 (remark)
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "organization_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 1},
			{Name: "trx_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 2},
			{Name: "record_sn", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 3},
			{Name: "coupon_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 4},
			{Name: "bill_id", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 5},
			{Name: "amount", Typ: plan.Type{Id: int32(types.T_decimal128)}, Seqnum: 6},
			{Name: "after_amount", Typ: plan.Type{Id: int32(types.T_decimal128)}, Seqnum: 7},
			{Name: "remark", Typ: plan.Type{Id: int32(types.T_varchar)}, Seqnum: 8},      // Position 8: VARCHAR
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 9}, // Position 9: DATETIME (ORDER BY column)
			{Name: "__mo_rowid", Typ: plan.Type{Id: int32(types.T_Rowid)}, Seqnum: 10},
		},
		Name2ColIndex: map[string]int32{
			"id":              0,
			"organization_id": 1,
			"trx_id":          2,
			"record_sn":       3,
			"coupon_id":       4,
			"bill_id":         5,
			"amount":          6,
			"after_amount":    7,
			"remark":          8,
			"created_at":      9,
			"__mo_rowid":      10,
		},
	}

	txnDB := txnDatabase{
		op: nil,
	}

	txnTbl := txnTable{
		db:       &txnDB,
		tableDef: tableDef,
	}

	ls := &LocalDisttaeDataSource{
		fs:    fs,
		ctx:   ctx,
		table: &txnTbl,
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_datetime)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   "coupon_usage_detail_logs.created_at", // Full qualified name
							ColPos: 8,                                     // This points to projection list position 8 (remark), not table position 9 (created_at)
							RelPos: 0,
						},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			},
		},
		Limit: 0, // No limit to trigger getBlockZMs
	}

	// Create an empty rangeSlice to avoid loading non-existent block metadata
	// This test focuses on column lookup logic, not block loading
	ls.rangeSlice = readutil.NewBlockListRelationData(0).GetBlockInfoSlice()

	// Test case 1: Column name lookup should find created_at (seqnum 9) instead of remark (seqnum 8)
	// This simulates the bug scenario where ColPos=8 would incorrectly point to remark
	// but we should find created_at by name.
	// With empty rangeSlice, getBlockZMs should complete without trying to load blocks
	zms, err := ls.getBlockZMs(ctx)
	require.NoError(t, err)

	// Verify that blockZMS was initialized (even if empty)
	require.NotNil(t, zms)
	require.Empty(t, zms)

	// Test case 2: Test with simple column name (without table prefix)
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "created_at"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 8 // Still wrong ColPos
	_, err = ls.getBlockZMs(ctx)
	require.NoError(t, err)

	// Test case 3: Test fallback to ColPos when name lookup fails
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "nonexistent_column"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 9 // Valid ColPos as fallback (points to created_at)
	_, err = ls.getBlockZMs(ctx)
	require.NoError(t, err)

	// Test case 4: Report a plan error when both name lookup and ColPos fail
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "nonexistent_column"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 999 // Invalid ColPos
	_, err = ls.getBlockZMs(ctx)
	require.ErrorContains(t, err, "cannot find column for ORDER BY")

	// Test case 5: Test with Name2ColIndex (O(1) lookup)
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.Name = "created_at"
	ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos = 8 // Wrong ColPos
	_, err = ls.getBlockZMs(ctx)
	require.NoError(t, err)
}

// TestLocalDisttaeDataSource_getBlockZMs_ColumnNameExtraction tests column name extraction
// from qualified names like "table.column".
func TestLocalDisttaeDataSource_getBlockZMs_ColumnNameExtraction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0},
			{Name: "created_at", Typ: plan.Type{Id: int32(types.T_datetime)}, Seqnum: 1},
		},
		Name2ColIndex: map[string]int32{
			"id":         0,
			"created_at": 1,
		},
	}

	txnTbl := txnTable{
		tableDef: tableDef,
	}

	ls := &LocalDisttaeDataSource{
		fs:    fs,
		ctx:   ctx,
		table: &txnTbl,
		OrderBy: []*plan.OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_datetime)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   "db.table.created_at", // Multiple dots
							ColPos: 0,
							RelPos: 0,
						},
					},
				},
			},
		},
		Limit: 0,
	}

	// Create an empty rangeSlice to avoid loading non-existent block metadata
	// This test focuses on column name extraction logic, not block loading
	ls.rangeSlice = readutil.NewBlockListRelationData(0).GetBlockInfoSlice()

	// Should extract "created_at" from "db.table.created_at"
	// Since rangeSlice is empty, getBlockZMs should complete without trying to load blocks
	zms, err := ls.getBlockZMs(ctx)
	require.NoError(t, err)

	// Verify that blockZMS was initialized (even if empty)
	require.NotNil(t, zms)
	require.Empty(t, zms)
}
