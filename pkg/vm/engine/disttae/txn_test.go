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
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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

	t.Run("insert without engine falls back", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op}
		bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 0, 1, 42, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.False(t, txn.writes[0].pkCheckReady)
		require.Equal(t, -1, txn.writes[0].pkCheckPos)

		bat.Clean(proc.Mp())
	})

	t.Run("delete without engine falls back", func(t *testing.T) {
		txn := &Transaction{proc: proc, op: op}
		bat := newDeleteBatchForTest(t, proc, []int64{1})

		_, err := txn.WriteBatch(DELETE, "", 0, 1, 42, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.False(t, txn.writes[0].pkCheckReady)
		require.Equal(t, -1, txn.writes[0].pkCheckPos)

		bat.Clean(proc.Mp())
	})

	t.Run("insert with active pk table resolves position", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		bat := newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.True(t, txn.writes[0].pkCheckReady)
		require.Equal(t, 1, txn.writes[0].pkCheckPos)

		bat.Clean(txn.proc.Mp())
	})

	t.Run("missing pk attr keeps legacy fallback", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		bat := newInt64BatchForTest(t, txn.proc, []string{"other"}, []int64{1, 2})

		_, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", bat, DNStore{})
		require.NoError(t, err)
		require.Len(t, txn.writes, 1)
		require.False(t, txn.writes[0].pkCheckReady)
		require.Equal(t, -1, txn.writes[0].pkCheckPos)

		bat.Clean(txn.proc.Mp())
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

	t.Run("no pk check and unique", func(t *testing.T) {
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
					pkCheckPos:   -1,
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

	t.Run("out of range falls back to legacy", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		txn.writes = []Entry{
			{
				typ:          INSERT,
				accountId:    1,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{9, 9}),
				pkCheckPos:   3,
				pkCheckReady: true,
			},
		}

		err := txn.checkDup()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("legacy insert with rowid duplicate", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		txn.writes = []Entry{
			{
				typ:          INSERT,
				accountId:    1,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInsertBatchWithRowIDForTest(t, txn.proc, []int64{8, 8}),
				pkCheckReady: false,
			},
		}

		err := txn.checkDup()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("legacy delete duplicate", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		txn.writes = []Entry{
			{
				typ:          DELETE,
				accountId:    1,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newDeleteBatchForTest(t, txn.proc, []int64{6, 6}),
				pkCheckReady: false,
			},
		}

		err := txn.checkDup()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	})

	t.Run("legacy delete without pk vector", func(t *testing.T) {
		txn := newTransactionWithActivePKTableForTest(t, "pk")
		txn.writes = []Entry{
			{
				typ:          DELETE,
				accountId:    1,
				tableId:      42,
				databaseId:   7,
				tableName:    "tbl",
				databaseName: "db",
				bat:          newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1}),
				pkCheckReady: false,
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

	pos, ready, err := txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", 1, nil)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, -1, pos)

	proc := testutil.NewProc(t)
	bat := newInt64BatchForTest(t, proc, []string{"pk"}, []int64{1})

	pos, ready, err = txn.resolvePKCheckPosForWrite(ALTER, 0, "db", "tbl", 1, bat)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, -1, pos)

	pos, ready, err = txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", catalog.MO_TABLES_ID, bat)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, -1, pos)

	pos, ready, err = txn.resolvePKCheckPosForWrite(INSERT, 0, "db", "tbl", 42, bat)
	require.NoError(t, err)
	require.False(t, ready)
	require.Equal(t, -1, pos)
}

func TestResolvePKCheckPosForWriteWithActiveTxnTable(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "pk")

	pos, ready, err := txn.resolvePKCheckPosForWrite(
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 0, pos)

	pos, ready, err = txn.resolvePKCheckPosForWrite(
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"PK"}, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 0, pos)

	pos, ready, err = txn.resolvePKCheckPosForWrite(
		DELETE,
		1,
		"db",
		"tbl",
		42,
		newDeleteBatchForTest(t, txn.proc, []int64{1}),
	)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, pos)

	pos, ready, err = txn.resolvePKCheckPosForWrite(
		INSERT,
		1,
		"db",
		"tbl",
		42,
		newInt64BatchForTest(t, txn.proc, []string{"other"}, []int64{1}),
	)
	require.NoError(t, err)
	require.False(t, ready)
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
	return newTxnOperatorForTestWithWorkspace(t, nil)
}

func newTxnOperatorForTestWithWorkspace(
	t *testing.T,
	workspace client.Workspace,
) *mock_frontend.MockTxnOperator {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn-test")}).AnyTimes()
	op.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	return op
}

func newTransactionWithActivePKTableForTest(
	t *testing.T,
	pkName string,
) *Transaction {
	t.Helper()
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:        proc,
		engine:      &Engine{},
		tableOps:    newTableOps(),
		databaseOps: newDbOps(),
	}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	txn.op = op

	db := &txnDatabase{
		op:           op,
		databaseId:   7,
		databaseName: "db",
	}
	txn.databaseOps.addCreateDatabase(genDatabaseKey(1, "db"), 0, db)
	txn.tableOps.addCreateTable(
		genTableKey(1, "tbl", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   42,
			tableName: "tbl",
			db:        db,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: pkName},
				},
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: pkName,
				},
			},
		},
	)
	return txn
}

// TestCheckPKDupSkipsNulls verifies that checkPKDup correctly skips NULL
// values per SQL standard (NULL != NULL), preventing false duplicate errors.
func TestCheckPKDupSkipsNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	t.Run("int64_all_nulls_no_dup", func(t *testing.T) {
		// All NULLs should never produce a duplicate
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "all-NULL rows must not report duplicate")
		require.Empty(t, m, "NULL rows must not be added to the map")
		pk.Free(mp)
	})

	t.Run("int64_mixed_nulls_and_values", func(t *testing.T) {
		// Two NULLs + two distinct values: no duplicate
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 4)
		require.False(t, dup, "NULLs should be skipped, 1 and 2 are distinct")
		require.Len(t, m, 2, "only non-NULL values should be in the map")
		pk.Free(mp)
	})

	t.Run("int64_real_dup_among_nulls", func(t *testing.T) {
		// Real duplicate among NULLs should still be caught
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp))  // NULL
		require.NoError(t, vector.AppendFixed(pk, int64(1), false, mp)) // dup!

		m := make(map[any]bool)
		dup, entry := checkPKDup(m, pk, 0, 3)
		require.True(t, dup, "real duplicate 1 must be caught")
		require.Contains(t, entry, "1")
		pk.Free(mp)
	})

	t.Run("varchar_nulls_no_dup", func(t *testing.T) {
		// String type NULLs should be skipped
		pk := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(pk, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(pk, []byte("world"), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 4)
		require.False(t, dup, "NULLs should be skipped for varchar")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("varchar_null_empty_string_no_collision", func(t *testing.T) {
		// NULL and empty string "" are different: NULL is skipped, "" is a value
		pk := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp))         // NULL
		require.NoError(t, vector.AppendBytes(pk, []byte(""), false, mp)) // empty string
		require.NoError(t, vector.AppendBytes(pk, nil, true, mp))         // NULL

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "NULL and empty string must not collide")
		require.Len(t, m, 1, "only the empty string should be in the map")
		pk.Free(mp)
	})

	t.Run("partial_range_with_nulls", func(t *testing.T) {
		// Test start/count range with NULLs
		pk := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(pk, int64(10), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL at pos 1
		require.NoError(t, vector.AppendFixed(pk, int64(20), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int64(0), true, mp)) // NULL at pos 3

		// Check only range [1,3) — NULL at 1, value 20 at 2
		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 1, 2)
		require.False(t, dup)
		require.Len(t, m, 1, "only pos 2 (value 20) should be in map")
		pk.Free(mp)
	})

	t.Run("array_float32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, vector.AppendArray(pk, []float32{1.0, 2.0}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))         // NULL
		require.NoError(t, vector.AppendArray(pk, []float32{1.0, 2.0}, false, mp)) // dup!

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.True(t, dup, "real duplicate array should be caught")
		pk.Free(mp)
	})

	t.Run("array_float32_all_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float32.ToType())
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))
		require.NoError(t, vector.AppendArray(pk, []float32{0}, true, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 2)
		require.False(t, dup, "all-NULL arrays must not report duplicate")
		require.Empty(t, m)
		pk.Free(mp)
	})

	t.Run("array_float64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_array_float64.ToType())
		require.NoError(t, vector.AppendArray(pk, []float64{1.0}, false, mp))
		require.NoError(t, vector.AppendArray(pk, []float64{0}, true, mp)) // NULL
		require.NoError(t, vector.AppendArray(pk, []float64{2.0}, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup, "NULLs should be skipped for float64 arrays")
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("bool_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixed(pk, true, false, mp))
		require.NoError(t, vector.AppendFixed(pk, false, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, false, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int8_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(pk, int8(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int8(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int8(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int16_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixed(pk, int16(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int16(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int16(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("int32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(pk, int32(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, int32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, int32(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint8_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint8(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint8(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint8(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint16_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint16(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint16(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint16(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint32(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint32(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uint64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint64(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("float32_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_float32.ToType())
		require.NoError(t, vector.AppendFixed(pk, float32(1.0), false, mp))
		require.NoError(t, vector.AppendFixed(pk, float32(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, float32(2.0), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("float64_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(pk, float64(1.0), false, mp))
		require.NoError(t, vector.AppendFixed(pk, float64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, float64(2.0), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("date_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_date.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Date(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Date(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Date(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("datetime_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_datetime.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Datetime(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("uuid_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_uuid.ToType())
		u1 := types.Uuid{1}
		u2 := types.Uuid{2}
		require.NoError(t, vector.AppendFixed(pk, u1, false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Uuid{}, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, u2, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("decimal64_nulls", func(t *testing.T) {
		tp := types.T_decimal64.ToType()
		tp.Scale = 2
		pk := vector.NewVec(tp)
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Decimal64(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("decimal128_nulls", func(t *testing.T) {
		tp := types.T_decimal128.ToType()
		tp.Scale = 2
		pk := vector.NewVec(tp)
		d1 := types.Decimal128{B0_63: 100, B64_127: 0}
		d2 := types.Decimal128{B0_63: 200, B64_127: 0}
		require.NoError(t, vector.AppendFixed(pk, d1, false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Decimal128{}, true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, d2, false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("timestamp_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_timestamp.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Timestamp(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("time_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_time.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Time(100), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Time(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Time(200), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("enum_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_enum.ToType())
		require.NoError(t, vector.AppendFixed(pk, types.Enum(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, types.Enum(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, types.Enum(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})

	t.Run("bit_nulls", func(t *testing.T) {
		pk := vector.NewVec(types.T_bit.ToType())
		require.NoError(t, vector.AppendFixed(pk, uint64(1), false, mp))
		require.NoError(t, vector.AppendFixed(pk, uint64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(pk, uint64(2), false, mp))

		m := make(map[any]bool)
		dup, _ := checkPKDup(m, pk, 0, 3)
		require.False(t, dup)
		require.Len(t, m, 2)
		pk.Free(mp)
	})
}

// TestDupVectorWithoutNulls tests the extracted helper that filters NULLs
// and duplicates the vector for safe InplaceSort.
func TestDupVectorWithoutNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	t.Run("no_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(2), false, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("some_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp)) // NULL
		require.NoError(t, vector.AppendFixed(v, int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp)) // NULL

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("all_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp))
		require.NoError(t, vector.AppendFixed(v, int64(0), true, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 0, out.Length())
		out.Free(mp)
		v.Free(mp)
	})

	t.Run("varchar_with_nulls", func(t *testing.T) {
		v := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(v, []byte("hello"), false, mp))
		require.NoError(t, vector.AppendBytes(v, nil, true, mp)) // NULL
		require.NoError(t, vector.AppendBytes(v, []byte("world"), false, mp))

		out, err := dupVectorWithoutNulls(v, mp)
		require.NoError(t, err)
		require.Equal(t, 2, out.Length())
		require.False(t, out.HasNull())
		out.Free(mp)
		v.Free(mp)
	})
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

func newInsertBatchWithRowIDForTest(
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

// TestConcurrentCheckPKDup verifies that checkPKDup works correctly when
// called concurrently with different vectors, each containing NULLs.
// This simulates the real production path where multiple INSERT txns
// perform PK duplicate checking concurrently.
func TestConcurrentCheckPKDup(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	const numGoroutines = 8
	const rowsPerGoroutine = 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)
	results := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			pk := vector.NewVec(types.T_int64.ToType())
			for i := 0; i < rowsPerGoroutine; i++ {
				isNull := (i % 5) == 0 // 20% NULLs
				if isNull {
					vector.AppendFixed(pk, int64(0), true, mp)
				} else {
					// Unique values per goroutine
					vector.AppendFixed(pk, int64(goroutineID*1000+i), false, mp)
				}
			}
			defer pk.Free(mp)

			m := make(map[any]bool)
			dup, _ := checkPKDup(m, pk, 0, rowsPerGoroutine)
			results <- dup
			if dup {
				errors <- moerr.NewInternalErrorNoCtxf("unexpected duplicate in goroutine %d", goroutineID)
			}
		}(g)
	}

	wg.Wait()
	close(errors)
	close(results)

	for err := range errors {
		t.Errorf("concurrent checkPKDup error: %v", err)
	}

	// All goroutines should report no duplicates
	for dup := range results {
		require.False(t, dup, "no duplicates expected with unique values per goroutine")
	}
}

// TestConcurrentCheckPKDup_RealDupWithNulls ensures that concurrent
// checkPKDup calls correctly detect real duplicates even when NULLs
// are present, but never flag NULLs as duplicates of each other.
func TestConcurrentCheckPKDup_RealDupWithNulls(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	const numGoroutines = 4
	var wg sync.WaitGroup
	dupDetected := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			pk := vector.NewVec(types.T_int64.ToType())
			// [1, NULL, 2, NULL, 1] — contains real dup (1 appears twice)
			vector.AppendFixed(pk, int64(1), false, mp)
			vector.AppendFixed(pk, int64(0), true, mp)
			vector.AppendFixed(pk, int64(2), false, mp)
			vector.AppendFixed(pk, int64(0), true, mp)
			vector.AppendFixed(pk, int64(1), false, mp) // dup!
			defer pk.Free(mp)

			m := make(map[any]bool)
			dup, _ := checkPKDup(m, pk, 0, 5)
			dupDetected <- dup
		}()
	}

	wg.Wait()
	close(dupDetected)

	for dup := range dupDetected {
		require.True(t, dup, "real duplicate (value=1) must be detected even with NULLs present")
	}
}

// TestDupVectorWithoutNulls_ConcurrentSafety verifies that dupVectorWithoutNulls
// produces independent copies safe for concurrent InplaceSort.
func TestDupVectorWithoutNulls_ConcurrentSafety(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	// Original vector with NULLs
	orig := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(orig, int64(30), false, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(10), false, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(0), true, mp))
	require.NoError(t, vector.AppendFixed(orig, int64(20), false, mp))
	defer orig.Free(mp)

	const numGoroutines = 4
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			dup, err := dupVectorWithoutNulls(orig, mp)
			require.NoError(t, err)
			defer dup.Free(mp)

			require.Equal(t, 3, dup.Length(), "should have 3 non-NULL values")
			require.False(t, dup.HasNull(), "filtered vector should have no NULLs")

			// InplaceSort on the copy should not corrupt original
			dup.InplaceSort()
		}()
	}

	wg.Wait()

	// Original should be unchanged
	require.Equal(t, 5, orig.Length())
	require.True(t, orig.HasNull())
}
