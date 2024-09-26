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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTombstoneData1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	blockio.Start("")
	defer blockio.Stop("")

	proc := testutil.NewProc()

	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	var stats []objectio.ObjectStats

	var tombstoneRowIds []types.Rowid
	int32Type := types.T_int32.ToType()
	for i := 0; i < 3; i++ {
		writer, err := colexec.NewS3TombstoneWriter()
		require.NoError(t, err)
		bat := engine_util.NewCNTombstoneBatch(
			&int32Type,
		)
		for j := 0; j < 10; j++ {
			row := types.RandomRowid()
			tombstoneRowIds = append(tombstoneRowIds, row)
			vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
			vector.AppendFixed[int32](bat.Vecs[1], int32(j), false, proc.GetMPool())
		}

		bat.SetRowCount(bat.Vecs[0].Length())

		writer.StashBatch(proc, bat)

		_, ss, err := writer.SortAndSync(proc)
		require.NoError(t, err)
		require.False(t, ss.IsZero())

		stats = append(stats, ss)
	}

	var stats1, stats2, stats3 objectio.ObjectStats
	stats1 = stats[0]
	stats2 = stats[1]
	stats3 = stats[2]

	obj1 := objectio.NewObjectid()
	obj2 := objectio.NewObjectid()
	blk1_0 := objectio.NewBlockidWithObjectID(obj1, 0)
	blk1_1 := objectio.NewBlockidWithObjectID(obj1, 1)
	blk1_2 := objectio.NewBlockidWithObjectID(obj1, 2)
	blk2_0 := objectio.NewBlockidWithObjectID(obj2, 0)

	rowids := make([]types.Rowid, 0)
	for i := 0; i < 4; i++ {
		rowid := types.NewRowid(blk1_0, uint32(i))
		rowids = append(rowids, *rowid)
		rowid = types.NewRowid(blk2_0, uint32(i))
		rowids = append(rowids, *rowid)
	}

	// Test AppendInMemory and AppendFiles and SortInMemory
	tombstones1 := NewEmptyTombstoneData()
	err = tombstones1.AppendInMemory(rowids...)
	require.Nil(t, err)
	err = tombstones1.AppendFiles(stats1, stats2)
	require.Nil(t, err)

	deleteMask := nulls.Nulls{}
	tombstones1.PrefetchTombstones("", fs, nil)
	for i := range tombstoneRowIds {
		sIdx := i / int(stats1.Rows())
		if sIdx == 2 {
			continue
		}

		bid := tombstoneRowIds[i].BorrowBlockID()
		exist, err := tombstones1.HasBlockTombstone(ctx, bid, fs)
		require.NoError(t, err)
		require.True(t, exist)

		maxTS := types.MaxTs()
		left, err := tombstones1.ApplyPersistedTombstones(
			ctx, fs, &maxTS, bid,
			[]int64{int64(tombstoneRowIds[i].GetRowOffset())},
			&deleteMask)

		require.NoError(t, err)
		require.Equal(t, 0, len(left))
		require.Equal(t, 1, deleteMask.Count())
		deleteMask.Reset()
	}

	tombstones1.SortInMemory()
	last := tombstones1.rowids[0]
	for i := 1; i < len(tombstones1.rowids); i++ {
		require.True(t, last.LE(&tombstones1.rowids[i]))
	}

	tombstones2 := NewEmptyTombstoneData()
	rowids = rowids[:0]
	for i := 0; i < 3; i++ {
		rowid := types.NewRowid(blk1_1, uint32(i))
		rowids = append(rowids, *rowid)
		rowid = types.NewRowid(blk1_2, uint32(i))
		rowids = append(rowids, *rowid)
	}
	err = tombstones2.AppendInMemory(rowids...)
	require.Nil(t, err)
	err = tombstones2.AppendFiles(stats3)
	require.Nil(t, err)
	tombstones2.SortInMemory()
	last = tombstones2.rowids[0]
	for i := 1; i < len(tombstones2.rowids); i++ {
		require.True(t, last.LE(&tombstones2.rowids[i]))
	}

	// Test Merge
	tombstones1.Merge(tombstones2)
	tombstones1.SortInMemory()
	last = tombstones1.rowids[0]
	for i := 1; i < len(tombstones1.rowids); i++ {
		require.True(t, last.LE(&tombstones1.rowids[i]))
	}

	// Test MarshalBinary and UnmarshalBinary
	var w bytes.Buffer
	err = tombstones1.MarshalBinaryWithBuffer(&w)
	require.NoError(t, err)

	tombstonesCopy, err := UnmarshalTombstoneData(w.Bytes())
	require.NoError(t, err)
	require.Equal(t, tombstones1.Type(), tombstonesCopy.Type())

	require.Equal(t, tombstones1.String(), tombstonesCopy.String())

	// Test AppendInMemory
	// inMemTombstones:
	//    blk1_0: [0, 1, 2, 3],
	//    blk1_1: [0, 1, 2],
	//    blk1_2: [0, 1, 2],
	//    blk2_0: [0, 1, 2, 3]

	// case 1: target is blk1_3 and rowsOffset is [0, 1, 2, 3]
	// expect: left is [0, 1, 2, 3]. no rows are deleted
	target := types.NewBlockidWithObjectID(obj1, 3)
	rowsOffset := []int64{0, 1, 2, 3}
	left := tombstones1.ApplyInMemTombstones(
		target,
		rowsOffset,
		nil,
	)
	require.Equal(t, left, rowsOffset)

	// case 2: target is blk1_3 and deleted rows is [5]
	// expect: left is [], deleted rows is [5]. no rows are deleted
	deleted := nulls.NewWithSize(0)
	deleted.Add(5)
	left = tombstones1.ApplyInMemTombstones(
		target,
		nil,
		deleted,
	)
	require.Equal(t, 0, len(left))
	require.True(t, deleted.Contains(5))
	require.True(t, deleted.Count() == 1)

	// case 3: target is blk2_0 and rowsOffset is [2, 3, 4]
	// expect: left is [4]. [2, 3] are deleted
	target = types.NewBlockidWithObjectID(obj2, 0)
	rowsOffset = []int64{2, 3, 4}
	left = tombstones1.ApplyInMemTombstones(
		target,
		rowsOffset,
		nil,
	)
	require.Equal(t, []int64{4}, left)

	// case 4: target is blk1_1 and deleted rows is [4]
	// expect: left is [], deleted rows is [0,1,2,4].
	target = types.NewBlockidWithObjectID(obj1, 1)
	deleted = nulls.NewWithSize(0)
	deleted.Add(4)
	left = tombstones1.ApplyInMemTombstones(
		target,
		nil,
		deleted,
	)
	require.Equal(t, 0, len(left))
	require.True(t, deleted.Contains(0))
	require.True(t, deleted.Contains(1))
	require.True(t, deleted.Contains(2))
	require.True(t, deleted.Contains(4))
	require.Equal(t, 4, deleted.Count())
}

func TestRelationDataV2_MarshalAndUnMarshal(t *testing.T) {
	location := objectio.NewRandomLocation(0, 0)
	objID := location.ObjectId()
	metaLoc := objectio.ObjectLocation(location)

	relData := NewBlockListRelationData(0)
	blkNum := 10
	for i := 0; i < blkNum; i++ {
		blkID := types.NewBlockidWithObjectID(&objID, uint16(blkNum))
		blkInfo := objectio.BlockInfo{
			BlockID:      *blkID,
			MetaLoc:      metaLoc,
			PartitionNum: int16(i),
		}
		blkInfo.ObjectFlags |= objectio.ObjectFlag_Appendable
		relData.AppendBlockInfo(&blkInfo)
	}

	tombstone := NewEmptyTombstoneData()
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

	newRelData, err := UnmarshalRelationData(buf)
	require.NoError(t, err)
	require.Equal(t, relData.String(), newRelData.String())
}

func TestLocalDatasource_ApplyWorkspaceFlushedS3Deletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	txnOp, closeFunc := client.NewTestTxnOperator(ctx)
	defer closeFunc()

	txnOp.AddWorkspace(&Transaction{})

	txnDB := txnDatabase{
		op: txnOp,
	}

	txnTbl := txnTable{
		db: &txnDB,
	}

	pState := logtailreplay.NewPartitionState("", true, 0)

	proc := testutil.NewProc()

	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	ls := &LocalDataSource{
		fs:     fs,
		ctx:    ctx,
		table:  &txnTbl,
		pState: pState,
	}

	//var stats []objectio.ObjectStats
	int32Type := types.T_int32.ToType()
	var tombstoneRowIds []types.Rowid
	for i := 0; i < 3; i++ {
		writer, err := colexec.NewS3TombstoneWriter()
		require.NoError(t, err)

		bat := engine_util.NewCNTombstoneBatch(
			&int32Type,
		)

		for j := 0; j < 10; j++ {
			row := types.RandomRowid()
			tombstoneRowIds = append(tombstoneRowIds, row)
			vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, proc.GetMPool())
			vector.AppendFixed[int32](bat.Vecs[1], int32(j), false, proc.GetMPool())
		}

		bat.SetRowCount(bat.Vecs[0].Length())

		writer.StashBatch(proc, bat)

		_, ss, err := writer.SortAndSync(proc)
		require.NoError(t, err)
		require.False(t, ss.IsZero())

		//stats = append(stats, ss)
		txnOp.GetWorkspace().(*Transaction).StashFlushedTombstones(ss)
	}

	deletedMask := &nulls.Nulls{}
	for i := range tombstoneRowIds {
		bid := tombstoneRowIds[i].BorrowBlockID()
		left, err := ls.applyWorkspaceFlushedS3Deletes(bid, nil, deletedMask)
		require.NoError(t, err)
		require.Zero(t, len(left))

		require.True(t, deletedMask.Contains(uint64(tombstoneRowIds[i].GetRowOffset())))
	}
}
