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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

var errDumpSecondWrite = errors.New("injected second object write failure")

type dumpFailSecondWriteFS struct {
	fileservice.FileService
	writes  atomic.Int64
	deleted chan error
}

func (fs *dumpFailSecondWriteFS) Write(ctx context.Context, data fileservice.IOVector) error {
	if fs.writes.Add(1) == 2 {
		return errDumpSecondWrite
	}
	return fs.FileService.Write(ctx, data)
}

func (fs *dumpFailSecondWriteFS) Delete(ctx context.Context, names ...string) error {
	err := fs.FileService.Delete(ctx, names...)
	fs.deleted <- err
	return err
}

// A later table can fail to flush after an earlier table has produced an
// object. Rollback must discard that object and preserve the prior statement.
func TestDumpInsertFailureRollsBackFlushedObjects(t *testing.T) {
	colexec.NewServer("")
	txn, tbl := newDumpInsertTxnForTest(t)
	initialBytes := txn.proc.Mp().CurrNB()
	second := &txnTable{
		accountId: 1, tableId: 43, tableName: "tbl2", db: tbl.db,
		tableDef: &pbplan.TableDef{
			Cols: []*pbplan.ColDef{{Name: "pk", Typ: pbplan.Type{Id: int32(types.T_int64)}}},
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"},
		},
	}
	txn.tableOps.addCreateTable(genTableKey(1, "tbl2", 7, "db"), 0, second)
	txn.tableCache = new(sync.Map)
	op := txn.op.(*mock_frontend.MockTxnOperator)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	prior := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{10, 11})
	a := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{1, 2, 3, 4})
	b := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{5, 6, 7, 8})
	for i, bat := range []*batch.Batch{prior, a, b} {
		id, name := uint64(42), "tbl"
		if i == 2 {
			id, name = 43, "tbl2"
		}
		txn.appendWorkspaceEntryLocked(Entry{
			typ: INSERT, accountId: 1, databaseId: 7, tableId: id,
			databaseName: "db", tableName: name, bat: bat,
		})
	}
	t.Cleanup(func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		require.Equal(t, initialBytes, txn.proc.Mp().CurrNB())
	})
	txn.addBatchSelectionsLocked(a, []int64{0, 2})
	txn.addBatchSelectionsLocked(b, []int64{1, 3})
	txn.statementID = 1
	txn.offsets = []int{1}
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	fail := &dumpFailSecondWriteFS{FileService: fs, deleted: make(chan error, 1)}
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	txn.engine.fs = fail
	txn.engine.gcPool = pool
	var pkCount int
	txn.Lock()
	err = txn.dumpInsertBatchLocked(context.Background(), fail, 1, &pkCount)
	txn.Unlock()
	require.ErrorIs(t, err, errDumpSecondWrite)
	require.EqualValues(t, 2, fail.writes.Load())
	require.Equal(t, 8, pkCount)
	require.Len(t, txn.writes, 2)
	require.Same(t, prior, txn.writes[0].bat)
	stats := objectio.ObjectStats(txn.writes[1].bat.Vecs[1].GetBytesAt(0))
	name := stats.ObjectName().String()
	_, err = fs.StatFile(context.Background(), name)
	require.NoError(t, err)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.NoError(t, txn.RollbackLastStatement(context.Background()))
	select {
	case err = <-fail.deleted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("rollback did not schedule GC")
	}
	_, err = fs.StatFile(context.Background(), name)
	require.Error(t, err)
	require.Len(t, txn.writes, 1)
	require.Same(t, prior, txn.writes[0].bat)
	require.Equal(t, []int64{10, 11}, vector.MustFixedColWithTypeCheck[int64](prior.Vecs[1]))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
}
