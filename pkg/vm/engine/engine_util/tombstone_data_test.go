// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine_util

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
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
		bat := NewCNTombstoneBatch(
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

		writer.StashBatch(proc, bat)

		_, ss, err := writer.SortAndSync(ctx, proc)
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

	deleteMask := objectio.GetReusableBitmap()
	defer deleteMask.Release()
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
		deleteMask.Clear()
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
	deleted := objectio.GetReusableBitmap()
	defer deleted.Release()
	deleted.Add(5)
	left = tombstones1.ApplyInMemTombstones(
		target,
		nil,
		&deleted,
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
	deleted.Clear()
	deleted.Add(4)
	left = tombstones1.ApplyInMemTombstones(
		target,
		nil,
		&deleted,
	)
	require.Equal(t, 0, len(left))
	require.True(t, deleted.Contains(0))
	require.True(t, deleted.Contains(1))
	require.True(t, deleted.Contains(2))
	require.True(t, deleted.Contains(4))
	require.Equal(t, 4, deleted.Count())
}

func TestRowIdsToOffset(t *testing.T) {

	objId := types.NewObjectid()
	blkId := types.NewBlockidWithObjectID(objId, 1)

	rowIds := make([]types.Rowid, 0, 10)
	for i := 0; i < 10; i++ {
		row := types.NewRowid(blkId, uint32(i))
		rowIds = append(rowIds, *row)
	}

	skipMask := objectio.GetReusableBitmap()
	skipMask.Add(1)
	skipMask.Add(3)

	left1 := RowIdsToOffset(rowIds, int32(0), skipMask).([]int32)
	left2 := RowIdsToOffset(rowIds, uint32(0), skipMask).([]uint32)
	left3 := RowIdsToOffset(rowIds, uint64(0), skipMask).([]uint64)

	expect := []int{0, 2, 4, 5, 6, 7, 8, 9}

	require.Equal(t, len(expect), len(left1))
	require.Equal(t, len(expect), len(left2))
	require.Equal(t, len(expect), len(left3))

	for i := 0; i < len(expect); i++ {
		require.Equal(t, expect[i], int(left1[i]))
		require.Equal(t, expect[i], int(left2[i]))
		require.Equal(t, expect[i], int(left3[i]))
	}
}
