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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/stretchr/testify/require"
)

func TestRelationDataV2_MarshalAndUnMarshal(t *testing.T) {
	location := objectio.NewRandomLocation(0, 0)
	objID := location.ObjectId()
	metaLoc := objectio.ObjectLocation(location)

	relData := engine_util.NewBlockListRelationData(0)
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

	tombstone := engine_util.NewEmptyTombstoneData()
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

	newRelData, err := engine_util.UnmarshalRelationData(buf)
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
		writer, err := colexec.NewS3TombstoneWriter()
		require.NoError(t, err)

		bat := engine_util.NewCNTombstoneBatch(
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
