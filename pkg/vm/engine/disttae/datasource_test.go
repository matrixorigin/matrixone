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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func TestTombstoneData1(t *testing.T) {
	location1 := objectio.NewRandomLocation(1, 1111)
	location2 := objectio.NewRandomLocation(2, 2222)
	location3 := objectio.NewRandomLocation(3, 3333)

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
	err := tombstones1.AppendInMemory(rowids...)
	require.Nil(t, err)
	err = tombstones1.AppendFiles(location1, location2)
	require.Nil(t, err)

	tombstones1.SortInMemory()
	last := tombstones1.rowids[0]
	for i := 1; i < len(tombstones1.rowids); i++ {
		require.True(t, last.Le(tombstones1.rowids[i]))
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
	err = tombstones2.AppendFiles(location3)
	require.Nil(t, err)
	tombstones2.SortInMemory()
	last = tombstones2.rowids[0]
	for i := 1; i < len(tombstones2.rowids); i++ {
		require.True(t, last.Le(tombstones2.rowids[i]))
	}

	// Test Merge
	tombstones1.Merge(tombstones2)
	tombstones1.SortInMemory()
	last = tombstones1.rowids[0]
	for i := 1; i < len(tombstones1.rowids); i++ {
		require.True(t, last.Le(tombstones1.rowids[i]))
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
		*target,
		rowsOffset,
		nil,
	)
	require.Equal(t, left, rowsOffset)

	// case 2: target is blk1_3 and deleted rows is [5]
	// expect: left is [], deleted rows is [5]. no rows are deleted
	deleted := nulls.NewWithSize(0)
	deleted.Add(5)
	left = tombstones1.ApplyInMemTombstones(
		*target,
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
		*target,
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
		*target,
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

func TestRelationDataV1_MarshalAndUnMarshal(t *testing.T) {

	location := objectio.NewRandomLocation(0, 0)
	objID := location.ObjectId()
	metaLoc := objectio.ObjectLocation(location)
	cts := types.BuildTSForTest(1, 1)

	relData := NewEmptyBlockListRelationData()
	blkNum := 10
	for i := 0; i < blkNum; i++ {
		blkID := types.NewBlockidWithObjectID(&objID, uint16(blkNum))
		blkInfo := objectio.BlockInfo{
			BlockID:      *blkID,
			EntryState:   true,
			Sorted:       false,
			MetaLoc:      metaLoc,
			CommitTs:     *cts,
			PartitionNum: int16(i),
		}
		relData.AppendBlockInfo(blkInfo)
	}

	buildTombstoner := func() *tombstoneDataWithDeltaLoc {
		tombstoner := NewEmptyTombstoneWithDeltaLoc()

		for i := 0; i < 10; i++ {
			bid := types.BuildTestBlockid(int64(i), 1)
			for j := 0; j < 10; j++ {
				tombstoner.inMemTombstones[bid] = append(tombstoner.inMemTombstones[bid], int32(i))
				loc := objectio.MockLocation(objectio.MockObjectName())
				tombstoner.blk2UncommitLoc[bid] = append(tombstoner.blk2UncommitLoc[bid], loc)
			}
			tombstoner.blk2CommitLoc[bid] = logtailreplay.BlockDeltaInfo{
				Cts: *types.BuildTSForTest(1, 1),
				Loc: objectio.MockLocation(objectio.MockObjectName()),
			}
		}
		return tombstoner
	}

	tombstoner := buildTombstoner()

	relData.AttachTombstones(tombstoner)
	t.Log(relData.String())
	buf, err := relData.MarshalBinary()
	require.Nil(t, err)

	newRelData, err := UnmarshalRelationData(buf)
	require.Nil(t, err)

	tomIsEqual := func(t1 *tombstoneDataWithDeltaLoc, t2 *tombstoneDataWithDeltaLoc) bool {
		if t1.Type() != t2.Type() ||
			len(t1.inMemTombstones) != len(t2.inMemTombstones) ||
			len(t1.blk2UncommitLoc) != len(t2.blk2UncommitLoc) ||
			len(t1.blk2CommitLoc) != len(t2.blk2CommitLoc) {
			return false
		}

		for bid, offsets1 := range t1.inMemTombstones {
			if _, ok := t2.inMemTombstones[bid]; !ok {
				return false
			}
			offsets2 := t2.inMemTombstones[bid]
			if len(offsets1) != len(offsets2) {
				return false
			}
			for i := 0; i < len(offsets1); i++ {
				if offsets1[i] != offsets2[i] {
					return false
				}
			}

		}

		for bid, locs1 := range t1.blk2UncommitLoc {
			if _, ok := t2.blk2UncommitLoc[bid]; !ok {
				return false
			}
			locs2 := t2.blk2UncommitLoc[bid]
			if len(locs1) != len(locs2) {
				return false
			}
			for i := 0; i < len(locs1); i++ {
				if !bytes.Equal(locs1[i], locs2[i]) {
					return false
				}
			}

		}

		for bid, info1 := range t1.blk2CommitLoc {
			if _, ok := t2.blk2CommitLoc[bid]; !ok {
				return false
			}
			info2 := t2.blk2CommitLoc[bid]
			if info1.Cts != info2.Cts {
				return false
			}
			if !bytes.Equal(info1.Loc, info2.Loc) {
				return false
			}
		}
		return true
	}

	isEqual := func(rd1 *blockListRelData, rd2 *blockListRelData) bool {

		if rd1.GetType() != rd2.GetType() || rd1.DataCnt() != rd2.DataCnt() {
			return false
		}

		if !bytes.Equal(rd1.blklist, rd2.blklist) {
			return false
		}

		return tomIsEqual(rd1.tombstones.(*tombstoneDataWithDeltaLoc),
			rd2.tombstones.(*tombstoneDataWithDeltaLoc))

	}
	require.True(t, isEqual(relData, newRelData.(*blockListRelData)))

}
