// Copyright 2026 Matrix Origin
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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestPartitionChangesHandleCloseWithTypedNil(t *testing.T) {
	var handle engine.ChangesHandle = (*PartitionChangesHandle)(nil)
	require.NoError(t, handle.Close())
}

func TestPartitionChangesHandleClose_CleansBufferedBatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(1), false, mp))
	data.SetRowCount(1)

	tombstone := batch.NewWithSize(1)
	tombstone.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(tombstone.Vecs[0], int64(2), false, mp))
	tombstone.SetRowCount(1)

	stub := &stubChangesHandle{}
	handle := &PartitionChangesHandle{
		mp:                  mp,
		currentChangeHandle: stub,
		bufferedBatches: []queuedChangeBatch{{
			data:      data,
			tombstone: tombstone,
		}},
	}

	require.NoError(t, handle.Close())
	require.True(t, stub.closed)
	require.Nil(t, handle.currentChangeHandle)
	require.Nil(t, handle.bufferedBatches)
}

func TestPartitionChangesHandleNextWithSnapshotRecovery_UsesBufferedBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, mp))
	data.SetRowCount(1)
	defer data.Clean(mp)

	handle := &PartitionChangesHandle{
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		bufferedBatches: []queuedChangeBatch{{
			data: data,
			hint: engine.ChangesHandle_Snapshot,
		}},
	}

	gotData, gotTombstone, hint, err := handle.Next(context.Background(), mp)
	require.NoError(t, err)
	require.Same(t, data, gotData)
	require.Nil(t, gotTombstone)
	require.Equal(t, engine.ChangesHandle_Snapshot, hint)
	require.Empty(t, handle.bufferedBatches)
}

func TestGetTableCreationCommitTSFromCatalogCache_RejectsNonDisttaeEngine(t *testing.T) {
	_, err := GetTableCreationCommitTSFromCatalogCache(&engine.EntireEngine{}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not *disttae.Engine")
}

func TestPartitionChangesHandleLoadCheckpointEntries(t *testing.T) {
	tbl, _ := newSnapshotScanTxnTable(t)
	handle := &PartitionChangesHandle{tbl: tbl}

	t.Run("empty response", func(t *testing.T) {
		ssStub := gostub.Stub(&RequestSnapshotRead, func(context.Context, *txnTable, *types.TS) (any, error) {
			return &cmd_util.SnapshotReadResp{Succeed: false}, nil
		})
		defer ssStub.Reset()

		entries, minTS, maxTS, err := handle.loadCheckpointEntries(context.Background(), types.BuildTS(10, 0))
		require.NoError(t, err)
		require.Nil(t, entries)
		require.Equal(t, types.MaxTs(), minTS)
		require.Equal(t, types.TS{}, maxTS)
	})

	t.Run("success", func(t *testing.T) {
		start := types.BuildTS(10, 0)
		end := types.BuildTS(20, 0)
		startTS := start.ToTimestamp()
		endTS := end.ToTimestamp()
		ssStub := gostub.Stub(&RequestSnapshotRead, func(context.Context, *txnTable, *types.TS) (any, error) {
			return &cmd_util.SnapshotReadResp{
				Succeed: true,
				Entries: []*cmd_util.CheckpointEntryResp{{
					Start:     &startTS,
					End:       &endTS,
					Location1: []byte("loc1"),
					Location2: []byte("loc2"),
					EntryType: 1,
				}},
			}, nil
		})
		defer ssStub.Reset()

		entries, minTS, maxTS, err := handle.loadCheckpointEntries(context.Background(), start)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, start, minTS)
		require.Equal(t, end, maxTS)
	})
}

func TestPartitionChangesHandleGetNextChangeHandle(t *testing.T) {
	t.Run("returns stale read after snapshot replay fallback", func(t *testing.T) {
		handle := newRealPartitionStateHandleForTest(t)

		chStub := gostub.Stub(&NewPartitionStateChangesHandler, func(
			context.Context,
			*logtailreplay.PartitionState,
			types.TS,
			types.TS,
			bool,
			uint32,
			int,
			*mpool.MPool,
			fileservice.FileService,
		) (*logtailreplay.ChangeHandler, error) {
			return nil, moerr.NewFileNotFoundNoCtx("missing partition object")
		})
		defer chStub.Reset()

		start := types.BuildTS(15, 0)
		end := types.BuildTS(20, 0)
		startTS := start.ToTimestamp()
		endTS := end.ToTimestamp()
		ssStub := gostub.Stub(&RequestSnapshotRead, func(context.Context, *txnTable, *types.TS) (any, error) {
			return &cmd_util.SnapshotReadResp{
				Succeed: true,
				Entries: []*cmd_util.CheckpointEntryResp{{
					Start:     &startTS,
					End:       &endTS,
					Location1: []byte("loc1"),
					Location2: []byte("loc2"),
					EntryType: 1,
				}},
			}, nil
		})
		defer ssStub.Reset()

		_, err := handle.getNextChangeHandle(context.Background())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead))
	})
}

func TestPartitionChangesHandleSnapshotStateRecoveryHelpers(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)

	t.Run("get txn table at", func(t *testing.T) {
		handle := newSnapshotRecoveryHandleForTest(t)
		got, err := handle.getTxnTableAt(ctx, types.BuildTS(20, 0))
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, handle.tbl.tableId, got.tableId)
	})

	t.Run("swap to snapshot state range closes previous handle", func(t *testing.T) {
		handle := newSnapshotRecoveryHandleForTest(t)
		handle.currentPSFrom = types.BuildTS(10, 0)
		handle.currentPSTo = types.BuildTS(20, 0)

		prev := &stubChangesHandle{}
		handle.currentChangeHandle = prev

		err := handle.swapCurrentHandleToSnapshotStateRange(ctx)
		require.NoError(t, err)
		require.True(t, prev.closed)
		require.NotNil(t, handle.currentChangeHandle)
	})

	t.Run("non visible policy is noop", func(t *testing.T) {
		handle := newSnapshotRecoveryHandleForTest(t)
		handle.snapshotReadPolicy = engine.SnapshotReadPolicyCheckpointReplay
		err := handle.swapCurrentHandleToSnapshotStateRange(ctx)
		require.NoError(t, err)
		require.Nil(t, handle.currentChangeHandle)
	})
}

func TestPartitionChangesHandleBufferCurrentRange_DropsQueuedBatchesOnRecovery(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	handle := newSnapshotRecoveryHandleForTest(t)
	handle.currentPSFrom = types.BuildTS(10, 0)
	handle.currentPSTo = types.BuildTS(20, 0)

	data := batch.NewWithSize(1)
	data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(7), false, handle.mp))
	data.SetRowCount(1)

	handle.currentChangeHandle = &stubChangesHandle{
		responses: []stubChangesHandleResponse{
			{
				data: data,
				hint: engine.ChangesHandle_Tail_wip,
			},
			{
				err: moerr.NewFileNotFoundNoCtx("missing object"),
			},
		},
	}

	err := handle.bufferCurrentRange(ctx, handle.mp)
	require.NoError(t, err)
	require.Empty(t, handle.bufferedBatches)
	require.True(t, handle.currentRangeDrained)
}

type stubChangesHandle struct {
	closed    bool
	responses []stubChangesHandleResponse
	idx       int
}

type stubChangesHandleResponse struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
	err       error
}

func (s *stubChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if s.idx >= len(s.responses) {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	resp := s.responses[s.idx]
	s.idx++
	return resp.data, resp.tombstone, resp.hint, resp.err
}

func (s *stubChangesHandle) Close() error {
	s.closed = true
	return nil
}

func newSnapshotRecoveryHandleForTest(t *testing.T) *PartitionChangesHandle {
	t.Helper()

	snapshotTbl, fs := newSnapshotScanTxnTable(t)
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockEng := mock_frontend.NewMockEngine(ctrl)
	mockEng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), snapshotTbl.tableId).
		Return(snapshotTbl.db.databaseName, snapshotTbl.tableName, snapshotTbl, nil).
		AnyTimes()

	baseOp := newTxnOperatorForTest(t)
	baseOp.EXPECT().CloneSnapshotOp(gomock.Any()).Return(baseOp).AnyTimes()
	baseOp.EXPECT().SnapshotTS().Return(types.BuildTS(20, 0).ToTimestamp()).AnyTimes()

	baseTbl := &txnTable{
		accountId:     snapshotTbl.accountId,
		tableId:       snapshotTbl.tableId,
		tableName:     snapshotTbl.tableName,
		primarySeqnum: snapshotTbl.primarySeqnum,
		eng:           mockEng,
		db: &txnDatabase{
			op:           baseOp,
			databaseId:   snapshotTbl.db.databaseId,
			databaseName: snapshotTbl.db.databaseName,
		},
	}

	return &PartitionChangesHandle{
		tbl:                baseTbl,
		fromTs:             types.BuildTS(10, 0),
		toTs:               types.BuildTS(20, 0),
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		primarySeqnum:      snapshotTbl.primarySeqnum,
		mp:                 snapshotTbl.proc.Load().Mp(),
		fs:                 fs,
	}
}

func newRealPartitionStateHandleForTest(t *testing.T) *PartitionChangesHandle {
	t.Helper()

	tbl, fs := newSnapshotScanTxnTable(t)
	return &PartitionChangesHandle{
		tbl:                tbl,
		fromTs:             types.BuildTS(10, 0),
		toTs:               types.BuildTS(20, 0),
		snapshotReadPolicy: engine.SnapshotReadPolicyCheckpointReplay,
		primarySeqnum:      tbl.primarySeqnum,
		mp:                 tbl.proc.Load().Mp(),
		fs:                 fs,
	}
}
