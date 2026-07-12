// Copyright 2023 Matrix Origin
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
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTxnTableForTest() *txnTable {
	engine := &Engine{
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
	}
	engine.catalog.Store(cache.NewCatalog())
	var tnStore DNStore
	txn := &Transaction{
		engine:   engine,
		tnStores: []DNStore{tnStore},
	}
	rt := runtime.DefaultRuntime()
	s, err := rpc.NewSender(rpc.Config{}, rt)
	if err != nil {
		panic(err)
	}
	c := client.NewTxnClient("", s)
	c.Resume()
	op, _ := c.New(context.Background(), timestamp.Timestamp{})
	op.AddWorkspace(txn)

	db := &txnDatabase{
		op: op,
	}
	table := &txnTable{
		db:         db,
		primaryIdx: 0,
		eng:        engine,
	}
	return table
}

func makeBatchForTest(
	mp *mpool.MPool,
	ints ...int64,
) *batch.Batch {
	bat := batch.New([]string{"a"})
	vec := vector.NewVec(types.T_int64.ToType())
	for _, n := range ints {
		vector.AppendFixed(vec, n, false, mp)
	}
	bat.SetVector(0, vec)
	bat.SetRowCount(len(ints))
	return bat
}

func newResetTxnForTest(t *testing.T, eng *Engine) (client.TxnOperator, *Transaction) {
	t.Helper()

	op, closeFn := client.NewTestTxnOperator(context.Background())
	t.Cleanup(closeFn)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	txn := &Transaction{
		op:          op,
		proc:        proc,
		engine:      eng,
		tableCache:  new(sync.Map),
		tableOps:    newTableOps(),
		databaseOps: newDbOps(),
	}
	op.AddWorkspace(txn)
	return op, txn
}

func insertCatalogTableForResetTest(
	t *testing.T,
	eng *Engine,
	txn *Transaction,
	accountID uint32,
	databaseID, tableID uint64,
	databaseName, tableName string,
) {
	t.Helper()

	packer := types.NewPacker()
	defer packer.Close()
	bat, err := catalog.GenCreateTableTuple(catalog.Table{
		AccountId:    accountID,
		DatabaseId:   databaseID,
		DatabaseName: databaseName,
		TableId:      tableID,
		TableName:    tableName,
	}, txn.proc.Mp(), packer)
	require.NoError(t, err)
	defer bat.Clean(txn.proc.Mp())
	_, err = fillRandomRowidAndZeroTs(bat, txn.proc.Mp())
	require.NoError(t, err)
	eng.GetLatestCatalogCache().InsertTable(bat)
}

func TestReusableRelationHandleResetDoesNotMutateSharedTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 0
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 0
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"

	shared := &txnTableDelegate{origin: oldCanonical}
	shared.isLocal = shared.isLocalFunc
	require.Error(t, shared.Reset(oldCanonical.db.op))

	handle1 := shared.NewRelationHandle().(*txnTableDelegate)
	handle2 := shared.NewRelationHandle().(*txnTableDelegate)
	require.NotSame(t, shared, handle1)
	require.Same(t, oldCanonical, handle1.origin)
	require.Error(t, handle1.Reset(nil))
	require.Error(t, oldCanonical.Reset(oldCanonical.db.op))

	newOp, newTxn := newResetTxnForTest(t, oldCanonical.eng.(*Engine))
	newProc := newTxn.proc

	newDB := &txnDatabase{
		accountId:    0,
		databaseId:   10,
		databaseName: "db",
		op:           newOp,
	}
	newCanonical := &txnTable{
		accountId: 0,
		tableId:   42,
		tableName: "t",
		db:        newDB,
		eng:       oldCanonical.eng,
		lastTS:    newOp.SnapshotTS(),
	}
	newCanonical.proc.Store(newProc)
	newShared := &txnTableDelegate{origin: newCanonical}
	newShared.isLocal = newShared.isLocalFunc
	newShared.parent = shared
	newShared.combined.is = true
	combinedPrimary := &txnTable{
		tableId:   84,
		tableName: "t_partition",
		db:        newDB,
		eng:       oldCanonical.eng,
	}
	combinedPrimary.proc.Store(newProc)
	newShared.combined.tbl = &combinedTxnTable{primary: combinedPrimary}
	newTxn.tableCache.Store(genTableKey(0, "t", 10, "db"), newShared)
	require.ErrorContains(t, newShared.combined.tbl.Reset(newOp), "cannot reset a shared combined relation")

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, handle := range []*txnTableDelegate{handle1, handle2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- handle.Reset(newOp)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Same(t, oldCanonical, shared.origin)
	require.Same(t, newCanonical, handle1.origin)
	require.Same(t, newCanonical, handle2.origin)
	require.Same(t, newCanonical, newShared.origin)
	require.Same(t, shared, handle1.parent)
	require.True(t, handle1.combined.is)
	require.Same(t, newShared.combined.tbl, handle1.combined.tbl)
	require.Equal(t, uint64(84), handle1.GetTableID(context.Background()))
	require.Equal(t, "t_partition", handle1.GetTableName())

	var resetErr error
	allocs := testing.AllocsPerRun(100, func() {
		resetErr = handle1.Reset(newOp)
	})
	require.NoError(t, resetErr)
	require.Zero(t, allocs, "steady-state relation handle reset must not allocate")
}

func TestReusableRelationHandleResetFromCatalogCacheMiss(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 7
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 7
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	eng := oldCanonical.eng.(*Engine)
	newOp, newTxn := newResetTxnForTest(t, eng)
	key := genTableKey(7, "t", 10, "db")
	_, cached := newTxn.tableCache.Load(key)
	require.False(t, cached)
	require.Nil(t, newTxn.tableOps.existAndActive(key))
	insertCatalogTableForResetTest(t, eng, newTxn, 7, 10, 84, "db", "t")

	require.NoError(t, handle.Reset(newOp))
	require.NotSame(t, oldCanonical, handle.origin)
	require.Equal(t, uint64(84), handle.GetTableID(context.Background()))
	require.Same(t, newOp, handle.origin.db.op)
	require.Same(t, newTxn.proc, handle.origin.proc.Load())
	require.Same(t, newTxn, handle.origin.db.getTxn())

	value, cached := newTxn.tableCache.Load(key)
	require.True(t, cached)
	require.Same(t, handle.origin, value.(*txnTableDelegate).origin)
}

func TestReusableRelationHandleResetFromCatalogMissingTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 7
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 7
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	eng := oldCanonical.eng.(*Engine)
	newOp, _ := newResetTxnForTest(t, eng)
	eng.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())

	err := handle.Reset(newOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Same(t, oldCanonical, handle.origin)
}

func TestReusableRelationHandleResetRejectsDeletedTable(t *testing.T) {
	oldCanonical := newTxnTableForTest()
	oldCanonical.accountId = 0
	oldCanonical.tableId = 42
	oldCanonical.tableName = "t"
	oldCanonical.db.accountId = 0
	oldCanonical.db.databaseId = 10
	oldCanonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: oldCanonical}).NewRelationHandle().(*txnTableDelegate)

	newOp, newTxn := newResetTxnForTest(t, oldCanonical.eng.(*Engine))
	key := genTableKey(0, "t", 10, "db")
	newTxn.tableOps.addDeleteTable(key, 0, oldCanonical.tableId)
	// A txn-local DROP must win even if a stale canonical relation is cached.
	newTxn.tableCache.Store(key, &txnTableDelegate{origin: oldCanonical})

	err := handle.Reset(newOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), err)
	require.Same(t, oldCanonical, handle.origin)
}

func TestReusableRelationHandleRejectsNonDisttaeWorkspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	canonical := newTxnTableForTest()
	canonical.tableName = "t"
	canonical.db.databaseName = "db"
	handle := (&txnTableDelegate{origin: canonical}).NewRelationHandle()

	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().GetWorkspace().Return(mock_frontend.NewMockWorkspace(ctrl))
	op.EXPECT().Status().Return(txn.TxnStatus_Active)

	require.ErrorContains(t, handle.Reset(op), "disttae transaction workspace")
}

// func TestPrimaryKeyCheck(t *testing.T) {
// 	ctx := context.Background()
// 	mp := mpool.MustNewZero()

// 	getRowIDsBatch := func(table *txnTable) *batch.Batch {
// 		bat := batch.New(false, []string{catalog.Row_ID})
// 		vec := vector.NewVec(types.T_Rowid.ToType())
// 		iter := table.localState.NewRowsIter(
// 			types.TimestampToTS(table.nextLocalTS()),
// 			nil,
// 			false,
// 		)
// 		l := 0
// 		for iter.Next() {
// 			entry := iter.Entry()
// 			vector.AppendFixed(vec, entry.RowID, false, mp)
// 			l++
// 		}
// 		iter.Close()
// 		bat.SetVector(0, vec)
// 		bat.SetZs(l, mp)
// 		return bat
// 	}

// 	table := newTxnTableForTest(mp)

// 	// insert
// 	err := table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)

// 	// // insert duplicated
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 1),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	// insert no duplicated
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 2, 3),
// 	)
// 	assert.Nil(t, err)

// 	// duplicated in same batch
// 	// we check duplicated in pipeline runing now
// 	// err = table.Write(
// 	// 	ctx,
// 	// 	makeBatchForTest(mp, 4, 4),
// 	// )
// 	// assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

// 	table = newTxnTableForTest(mp)

// 	// insert, delete then insert
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 1),
// 	)
// 	assert.Nil(t, err)
// 	err = table.Delete(
// 		ctx,
// 		getRowIDsBatch(table),
// 		catalog.Row_ID,
// 	)
// 	assert.Nil(t, err)
// 	err = table.Write(
// 		ctx,
// 		makeBatchForTest(mp, 5),
// 	)
// 	assert.Nil(t, err)

// }

func BenchmarkTxnTableInsert(b *testing.B) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	table := newTxnTableForTest()
	for i, max := int64(0), int64(b.N); i < max; i++ {
		err := table.Write(
			ctx,
			makeBatchForTest(mp, i),
		)
		assert.Nil(b, err)
	}
}
