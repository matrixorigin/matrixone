// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCountSealedBlocksMatchesOrdinaryBlockRead(t *testing.T) {
	ctx := t.Context()
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	t.Cleanup(proc.Free)
	fs, err := fileservice.NewMemoryFS("count-sealed-blocks", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	typ := types.T_int64.ToType()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(typ, mp)
	defer bat.Clean(mp)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	// A short non-final block makes the range's size estimate unsuitable for COUNT.
	for _, n := range []int{1, 2} {
		bat.CleanOnlyData()
		for row := 0; row < n; row++ {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row), false, mp))
		}
		bat.SetRowCount(n)
		_, err = writer.Write(bat)
		require.NoError(t, err)
	}
	written, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	blockFor := func(i uint16) objectio.BlockInfo {
		var blk objectio.BlockInfo
		blk.BlockID = types.NewBlockidWithObjectID(&id, i)
		blk.SetMetaLocation(objectio.BuildLocation(name, written[i].GetExtent(), objectio.BlockMaxRows, i))
		return blk
	}
	first, second := blockFor(0), blockFor(1)

	deletes := batch.NewWithSize(3)
	deletes.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
	deletes.Vecs[1] = vector.NewVec(typ, mp)
	deletes.Vecs[2] = vector.NewVec(types.T_TS.ToType(), mp)
	defer deletes.Clean(mp)
	for _, entry := range []struct {
		block *types.Blockid
		ts    int64
	}{
		{&first.BlockID, 4},
		{&first.BlockID, 4},
		{&second.BlockID, 6},
	} {
		require.NoError(t, vector.AppendFixed(deletes.Vecs[0], types.NewRowid(entry.block, 0), false, mp))
		require.NoError(t, vector.AppendFixed(deletes.Vecs[1], int64(0), false, mp))
		require.NoError(t, vector.AppendFixed(deletes.Vecs[2], types.BuildTS(entry.ts, 0), false, mp))
	}
	deletes.SetRowCount(deletes.Vecs[0].Length())
	tombWriter := ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
	_, err = tombWriter.WriteBatch(deletes)
	require.NoError(t, err)
	_, _, err = tombWriter.Sync(ctx)
	require.NoError(t, err)
	tomb := tombWriter.GetObjectStats()
	state := logtailreplay.NewPartitionState("", true, 0, false)
	state.UpdateDuration(types.BuildTS(1, 0), types.MaxTs())
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: tomb, CreateTime: types.BuildTS(3, 0),
	}, true))

	var dataReads int
	injected := errors.New("tombstone read failed")
	failTombstones := false
	countFS := tombstoneReadFS{FileService: fs, read: func(ctx context.Context, vec *fileservice.IOVector) error {
		if failTombstones && vec.FilePath == tomb.ObjectName().String() {
			return injected
		}
		if vec.FilePath == name.String() {
			for _, entry := range vec.Entries {
				if entry.Offset < int64(written[0].GetExtent().Offset()) {
					dataReads++
				}
			}
		}
		return fs.Read(ctx, vec)
	}}

	txn := &Transaction{
		engine:        &Engine{fs: countFS},
		deletedBlocks: &deletedBlocks{offsets: make(map[uint64]map[types.Blockid][]int64)},
	}
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().SnapshotTS().Return(types.BuildTS(5, 0).ToTimestamp()).AnyTimes()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	op.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	tbl := &txnTable{db: &txnDatabase{op: op}}
	tbl.proc.Store(proc)
	ranges := readutil.NewBlockListRelationData(0, readutil.WithPartitionState(state))
	ranges.AppendBlockInfo(&objectio.EmptyBlockInfo)
	ranges.AppendBlockInfo(&first)
	ranges.AppendBlockInfo(&second)
	appendable := objectio.BlockInfo{ObjectFlags: objectio.ObjectFlag_Appendable}
	ranges.AppendBlockInfo(&appendable)

	analyzer := process.NewAnalyzer(0, true, false, "test_scan")
	count, remaining, err := CountSealedBlocks(ctx, &txnTableDelegate{origin: tbl}, ranges, 0, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.Zero(t, dataReads, "COUNT must not load ordinary columns")
	require.Equal(t, 2, remaining.DataCnt())
	require.Equal(t, objectio.EmptyBlockInfo, remaining.GetBlockInfo(0))
	require.Equal(t, appendable, remaining.GetBlockInfo(1))

	source, err := tbl.buildLocalDataSource(ctx, 0, ranges, engine.Policy_CheckAll, engine.GeneralLocalDataSource)
	require.NoError(t, err)
	defer source.Close()
	var ordinaryCount int64
	for _, blk := range []objectio.BlockInfo{first, second} {
		bat.CleanOnlyData()
		require.NoError(t, blockio.BlockDataRead(ctx, &blk, source,
			[]uint16{0}, []types.Type{typ}, -1, op.SnapshotTS(), nil, nil,
			objectio.BlockReadFilter{}, nil, 0, "count_test", bat, containers.NewVectors(3), mp, countFS))
		ordinaryCount += int64(bat.RowCount())
	}
	require.Equal(t, ordinaryCount, count)
	require.Positive(t, dataReads, "control must load an ordinary column")

	// A local workspace delete is visible. An offset past the end of the
	// block must not contribute to its cardinality.
	txn.deletedBlocks.addDeletedBlocks(tbl.tableId, &second.BlockID, []int64{1, 9})
	count, _, err = CountSealedBlocks(ctx, tbl, ranges, 0, analyzer)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	txn.deletedBlocks.addDeletedBlocks(tbl.tableId, &second.BlockID, []int64{0})
	count, _, err = CountSealedBlocks(ctx, tbl, ranges, 0, analyzer)
	require.NoError(t, err)
	require.Zero(t, count)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, remaining, err = CountSealedBlocks(cancelled, tbl, ranges, 0, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, remaining)

	failTombstones = true
	_, remaining, err = CountSealedBlocks(ctx, tbl, ranges, 0, analyzer)
	require.ErrorIs(t, err, injected)
	require.Nil(t, remaining)
}
