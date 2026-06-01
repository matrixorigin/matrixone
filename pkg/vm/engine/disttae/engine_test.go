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

/*
type testTxnOperator struct {
	meta txn.TxnMeta
}

func TestCache(t *testing.T) {
	db := new(DB)
	ctx := context.Background()
	ts := newTimestamp(rand.Int63())
	_ = db.Update(ctx, nil, 0, 0, ts)
	_ = db.BlockList(ctx, nil, 0, 0, ts, nil)
	_, _ = db.NewReader(ctx, 0, nil, nil, 0, 0, ts, nil)
}

func TestEngine(t *testing.T) {
	ctx := context.Background()
	getClusterDetails := func() (details logservice.ClusterDetails, err error) {
		return
	}
	txnOp := newTestTxnOperator()
	m := mheap.New(guest.New(1<<20, host.New(1<<20)))
	e := New(m, ctx, nil, getClusterDetails)
	err := e.New(ctx, txnOp)
	require.NoError(t, err)
	err = e.Create(ctx, "test", txnOp)
	require.NoError(t, err)
	err = e.Delete(ctx, "test", txnOp)
	require.NoError(t, err)
	err = e.Commit(ctx, txnOp)
	require.NoError(t, err)
	err = e.Rollback(ctx, txnOp)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	_, err = e.Nodes()
	require.NoError(t, err)
	hints := e.Hints()
	require.Equal(t, time.Minute*5, hints.CommitOrRollbackTimeout)
}

func TestTransaction(t *testing.T) {
	txn := &Transaction{
		readOnly: false,
		meta:     newTxnMeta(rand.Int63()),
		fileMap:  make(map[string]uint64),
	}
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
	ro := txn.ReadOnly()
	require.Equal(t, false, ro)
	err := txn.WriteBatch(INSERT, 0, 0, "test", "test", batch.NewOffHeapEmpty())
	require.NoError(t, err)
	txn.IncStatementId()
	txn.RegisterFile("test")
	err = txn.WriteFile(DELETE, 0, 0, "test", "test", "test")
	require.NoError(t, err)
	ctx := context.TODO()

	bm := makeBlockMetaForTest()
	_, err = blockWrite(ctx, bm, testutil.NewBatch([]types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}, true, 20, testutil.NewMheap()), testutil.NewFS())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// fmt.Printf("%v", blks)

	_, _ = txn.getRow(ctx, 0, 0, nil, nil, MakeFunctionExprForTest(">", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64ConstExprWithType(20),
	}), nil)

	_, _ = txn.getRows(ctx, 0, 0, nil, nil, MakeFunctionExprForTest(">", []*plan.Expr{
		MakeColExprForTest(0, types.T_int64),
		plan2.MakePlan2Int64ConstExprWithType(20),
	}), nil)

}

func TestTable(t *testing.T) {
	tbl := new(table)
	ctx := context.TODO()
	_, _ = tbl.Rows(ctx)
	_, _ = tbl.Size(ctx, "test")
	_, _ = tbl.Ranges(ctx)
	_, _ = tbl.TableDefs(ctx)
	_, _ = tbl.GetPrimaryKeys(ctx)
	_, _ = tbl.GetHideKeys(ctx)
	_ = tbl.Write(ctx, nil)
	_ = tbl.Update(ctx, nil)
	_ = tbl.Delete(ctx, nil, "test")
	_, _ = tbl.Truncate(ctx)
	_ = tbl.AddTableDef(ctx, nil)
	_ = tbl.DelTableDef(ctx, nil)
	_ = tbl.GetTableID(ctx)
	_, _ = tbl.NewReader(ctx, 0, nil, nil)
}

func TestTools(t *testing.T) {
	_ = genCreateTableTuple("test")
	_ = genCreateColumnTuple(nil)
	_ = genDropTableTuple("test")
	_ = genDropColumnsTuple("test")
	_ = genDatabaseIdExpr("test")
	_ = genTableIdExpr(0, "test")
}

func newTestTxnOperator() *testTxnOperator {
	return &testTxnOperator{
		meta: newTxnMeta(rand.Int63()),
	}
}

func (op *testTxnOperator) Txn() txn.TxnMeta {
	return op.meta
}

func (op *testTxnOperator) Snapshot() ([]byte, error) {
	return nil, nil
}

func (op *testTxnOperator) ApplySnapshot(data []byte) error {
	return nil
}

func (op *testTxnOperator) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (op *testTxnOperator) Commit(ctx context.Context) error {
	return nil
}

func (op *testTxnOperator) Rollback(ctx context.Context) error {
	return nil
}

func newTimestamp(v int64) timestamp.Timestamp {
	return timestamp.Timestamp{PhysicalTime: v}
}

func newTxnMeta(snapshotTS int64) txn.TxnMeta {
	id := uuid.New()
	return txn.TxnMeta{
		ID:         id[:],
		Status:     txn.TxnStatus_Active,
		SnapshotTS: newTimestamp(snapshotTS),
	}
}
*/

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestFilterDeleteDatabaseRelationsSkipsAlreadyDeletedRelation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()
	txnOp.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()

	const (
		accountID    = uint32(7)
		databaseID   = uint64(11)
		dbName       = "acc_test02"
		deletedTable = "aff01"
		activeTable  = "pri01"
		tableID      = uint64(13)
	)
	db := &txnDatabase{
		accountId:    accountID,
		databaseId:   databaseID,
		databaseName: dbName,
		op:           txnOp,
	}
	txn.tableOps.addDeleteTable(genTableKey(accountID, deletedTable, databaseID, dbName), 0, tableID)

	rels := filterDeleteDatabaseRelations(db, []string{deletedTable, activeTable}, dbName, txnOp)
	require.Equal(t, []string{activeTable}, rels)
}

func TestIsDeleteDatabaseRelationDeletedInTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()

	const (
		accountID  = uint32(7)
		databaseID = uint64(11)
		dbName     = "acc_test02"
		tableName  = "aff01"
		tableID    = uint64(13)
	)
	db := &txnDatabase{
		databaseId:   databaseID,
		databaseName: dbName,
		op:           txnOp,
	}

	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, tableName))

	txn.tableOps.addDeleteTable(genTableKey(accountID, tableName, databaseID, dbName), 0, tableID)
	require.True(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, tableName))

	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID+1, tableName))
	require.False(t, isDeleteDatabaseRelationDeletedInTxn(db, accountID, "other_table"))
}

func TestIsDeleteDatabaseRelationDeletedInTxnUsesSystemAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txn := &Transaction{tableOps: newTableOps()}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(txn).AnyTimes()

	const tableName = catalog.MO_TABLES
	db := &txnDatabase{
		databaseId:   catalog.MO_CATALOG_ID,
		databaseName: catalog.MO_CATALOG,
		op:           txnOp,
	}

	txn.tableOps.addDeleteTable(genTableKey(catalog.System_Account, tableName, catalog.MO_CATALOG_ID, catalog.MO_CATALOG), 0, catalog.MO_TABLES_ID)
	require.True(t, isDeleteDatabaseRelationDeletedInTxn(db, uint32(99), tableName))
}
