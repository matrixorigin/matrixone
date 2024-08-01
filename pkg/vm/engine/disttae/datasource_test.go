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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

func TestRelationDataV1_MarshalAndUnMarshal(t *testing.T) {

	objID := types.NewObjectid()
	objName := objectio.BuildObjectNameWithObjectID(objID)

	extent := objectio.NewExtent(0x1f, 0x2f, 0x3f, 0x4f)
	delLoc := objectio.BuildLocation(objName, extent, 0, 0)
	metaLoc := objectio.ObjectLocation(delLoc)
	cts := types.BuildTSForTest(1, 1)

	relData := buildBlockListRelationData()
	blkNum := 10
	for i := 0; i < blkNum; i++ {
		blkID := types.NewBlockidWithObjectID(objID, uint16(blkNum))
		blkInfo := objectio.BlockInfoInProgress{
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
		tombstoner := buildTombstoneWithDeltaLoc()

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
