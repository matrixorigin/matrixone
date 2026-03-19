// Copyright 2021 - 2024 Matrix Origin
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
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func Test_GetUncommittedS3Tombstone(t *testing.T) {
	var statsList []objectio.ObjectStats
	for i := 0; i < 3; i++ {
		row := types.RandomRowid()
		stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), false, false, true)
		objectio.SetObjectStatsRowCnt(stats, uint32(10+i*10))
		statsList = append(statsList, *stats)
	}

	txn := &Transaction{
		cn_flushed_s3_tombstone_object_stats_list: new(sync.Map),
	}

	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[0], nil)
	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[1], nil)
	txn.cn_flushed_s3_tombstone_object_stats_list.Store(statsList[2], nil)

	objectSlice := objectio.ObjectStatsSlice{}

	require.NoError(t, txn.getUncommittedS3Tombstone(func(stats *objectio.ObjectStats) {
		objectSlice.Append(stats[:])
	}))
	require.Equal(t, len(statsList), objectSlice.Len())

	txn.cn_flushed_s3_tombstone_object_stats_list.Range(func(key, value any) bool {
		ss := key.(objectio.ObjectStats)
		found := false
		for i := range objectSlice.Len() {
			if bytes.Equal(ss[:], objectSlice.Get(i)[:]) {
				found = true
				break
			}
		}

		require.True(t, found)
		return true
	})
}

func Test_BatchAllocNewRowIds(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("A", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		for i := 0; i < 10; i++ {
			ll := rand.Intn(100) + 1
			vec, err := txn.batchAllocNewRowIds(ll)
			require.NoError(t, err)
			require.Equal(t, ll, vec.Length())

			rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
			require.Equal(t, int(0), int(rowIds[0].GetRowOffset()))
			require.Equal(t, int(ll-1), int(rowIds[len(rowIds)-1].GetRowOffset()))

			vec.Free(common.DefaultAllocator)
		}
	})

	t.Run("B", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := options.DefaultBlockMaxRows*11 + 1
		mm1 := make(map[types.Blockid]struct{})
		mm2 := make(map[types.Objectid]struct{})

		vec, err := txn.batchAllocNewRowIds(ll)
		require.NoError(t, err)
		require.Equal(t, ll, vec.Length())

		rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](vec)
		for i := range rowIds {
			if i%options.DefaultBlockMaxRows == 0 {
				require.Equal(t, 0, int(rowIds[i].GetRowOffset()))
				if i > 0 {
					require.Equal(t, int(rowIds[i-1].GetBlockOffset()+1), int(rowIds[i].GetBlockOffset()))
					require.Equal(t, int(options.DefaultBlockMaxRows-1), int(rowIds[i-1].GetRowOffset()))
				}
			}

			mm1[*rowIds[i].BorrowBlockID()] = struct{}{}
			mm2[*rowIds[i].BorrowObjectID()] = struct{}{}
		}

		require.Equal(t, 12, len(mm1))
		require.Equal(t, 1, len(mm2))

		vec.Free(common.DefaultAllocator)
	})

	t.Run("C", func(t *testing.T) {
		txn := Transaction{
			proc: proc,
		}

		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)

		ll := math.MaxUint16
		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrObj()
			require.NoError(t, err)
		}

		for i := 0; i < ll; i++ {
			err := txn.currentRowId.IncrBlk()
			require.NoError(t, err)
		}

		_, err := txn.batchAllocNewRowIds(1)
		require.Error(t, err)
	})

	var deletedBlocks *deletedBlocks
	require.Equal(t, 0, deletedBlocks.size())
}

func TestWriteBatchRecordsPKCheckState(t *testing.T) {
	proc := testutil.NewProc(t)
	op := newTxnOperatorForTest(t)

	t.Run("insert", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op}
		bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 0, 1, 2, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.True(t, txn.writes[0].pkCheckReady)
		require.Equal(t, -1, txn.writes[0].pkCheckPos)

		bat.Clean(proc.Mp())
	})

	t.Run("delete", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op}
		bat := newDeleteBatchForTest(t, proc, []int64{1})

		_, err := txn.WriteBatch(DELETE, "", 0, 1, 2, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.True(t, txn.writes[0].pkCheckReady)
		require.Equal(t, -1, txn.writes[0].pkCheckPos)

		bat.Clean(proc.Mp())
	})
}

func TestTransactionCheckDupUsesWriteEntryPKMetadata(t *testing.T) {
	t.Run("insert duplicate", func(t *testing.T) {
		txn := &Transaction{
			op:          newTxnOperatorForTest(t),
			tableOps:    newTableOps(),
			databaseOps: newDbOps(),
			writes: []Entry{
				{
					typ:          INSERT,
					tableId:      42,
					databaseId:   7,
					tableName:    "tbl",
					databaseName: "db",
					bat:          newInt64BatchForTest(t, testutil.NewProc(t), []string{"pk"}, []int64{1, 1}),
					pkCheckPos:   0,
					pkCheckReady: true,
				},
			},
		}

		err := txn.checkDup()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("delete duplicate", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := &Transaction{
			op:          newTxnOperatorForTest(t),
			tableOps:    newTableOps(),
			databaseOps: newDbOps(),
			writes: []Entry{
				{
					typ:          DELETE,
					tableId:      42,
					databaseId:   7,
					tableName:    "tbl",
					databaseName: "db",
					bat:          newDeleteBatchForTest(t, proc, []int64{3, 3}),
					pkCheckPos:   1,
					pkCheckReady: true,
				},
			},
		}

		err := txn.checkDup()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("out of range and unique", func(t *testing.T) {
		proc := testutil.NewProc(t)
		txn := &Transaction{
			op:          newTxnOperatorForTest(t),
			tableOps:    newTableOps(),
			databaseOps: newDbOps(),
			writes: []Entry{
				{
					typ:          INSERT,
					tableId:      42,
					databaseId:   7,
					tableName:    "tbl",
					databaseName: "db",
					bat:          newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2}),
					pkCheckPos:   3,
					pkCheckReady: true,
				},
				{
					typ:          DELETE,
					tableId:      42,
					databaseId:   7,
					tableName:    "tbl",
					databaseName: "db",
					bat:          newDeleteBatchForTest(t, proc, []int64{4, 5}),
					pkCheckPos:   1,
					pkCheckReady: true,
				},
			},
		}

		require.NoError(t, txn.checkDup())
	})
}

func TestTransactionGetTableNilGuards(t *testing.T) {
	txn := &Transaction{}

	_, err := txn.getTable(0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disttae txn engine is nil")

	txn.engine = &Engine{}
	_, err = txn.getTable(0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "disttae txn operator is nil")
}

func TestResolvePKCheckPosForWriteEarlyExit(t *testing.T) {
	txn := &Transaction{}

	pos, err := txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", 1, nil)
	require.NoError(t, err)
	require.Equal(t, -1, pos)

	proc := testutil.NewProc(t)
	bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})

	pos, err = txn.resolvePKCheckPosForWrite(ALTER, 0, "db", "tbl", 1, bat)
	require.NoError(t, err)
	require.Equal(t, -1, pos)

	pos, err = txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", catalog.MO_TABLES_ID, bat)
	require.NoError(t, err)
	require.Equal(t, -1, pos)

	pos, err = txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", 1, bat)
	require.NoError(t, err)
	require.Equal(t, -1, pos)
}

func TestWriteFileLockedMarksPKCheckReady(t *testing.T) {
	proc := testutil.NewProc(t)
	txn := &Transaction{proc: proc}
	bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})

	err := txn.WriteFileLocked(ALTER, 0, 1, 2, "db", "tbl", "file", bat, DNStore{})
	require.NoError(t, err)
	require.Len(t, txn.writes, 1)
	require.True(t, txn.writes[0].pkCheckReady)
	require.Equal(t, -1, txn.writes[0].pkCheckPos)

	bat.Clean(proc.Mp())
	txn.writes[0].bat.Clean(proc.Mp())
}

func newTxnOperatorForTest(t *testing.T) *mock_frontend.MockTxnOperator {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn-test")}).AnyTimes()
	op.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
	return op
}

func newInt64BatchForTest(
	t *testing.T,
	proc *process.Process,
	attrs []string,
	cols ...[]int64,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(len(cols))
	bat.SetAttributes(attrs)
	for i, vals := range cols {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vec, vals, nil, proc.Mp()))
		bat.Vecs[i] = vec
	}
	bat.SetRowCount(len(cols[0]))
	return bat
}

func newDeleteBatchForTest(
	t *testing.T,
	proc *process.Process,
	pks []int64,
) *batch.Batch {
	t.Helper()
	rowids := make([]types.Rowid, len(pks))
	for i := range rowids {
		rowids[i] = types.RandomRowid()
	}

	bat := batch.NewWithSize(2)
	bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "pk"})

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixedList(rowidVec, rowids, nil, proc.Mp()))
	bat.Vecs[0] = rowidVec

	pkVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(pkVec, pks, nil, proc.Mp()))
	bat.Vecs[1] = pkVec

	bat.SetRowCount(len(pks))
	return bat
}
