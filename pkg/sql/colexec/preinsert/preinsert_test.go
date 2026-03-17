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

package preinsert

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

var (
	i64typ     = plan.Type{Id: int32(types.T_int64)}
	i32typ     = plan.Type{Id: int32(types.T_int64), AutoIncr: true}
	varchartyp = plan.Type{Id: int32(types.T_varchar)}
)

func TestPreInsertNormal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Base.SessionInfo.StorageEngine = eng
	argument1 := PreInsert{
		ctr:        container{},
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column", Typ: i64typ},
				{Name: "scalar_int64", Typ: i64typ},
				{Name: "varchar_column", Typ: varchartyp},
				{Name: "scalar_varchar", Typ: varchartyp},
				{Name: "int64_column", Typ: i64typ},
			},
			Pkey: &plan.PrimaryKeyDef{},
		},
		Attrs:       []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		IsOldUpdate: false,
		HasAutoCol:  false,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}
	resetChildren(&argument1)
	err := argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	argument1.Reset(proc, false, nil)
	resetChildren(&argument1)
	err = argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	argument1.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPreInsertNullCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.SessionInfo.StorageEngine = eng
	name2ColIndex := make(map[string]int32, 1)
	name2ColIndex["int64_column_primary"] = 0
	argument2 := PreInsert{
		ctr:        container{},
		SchemaName: "testDb",
		Attrs:      []string{"int64_column_primary"},
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column_primary", Primary: true, Typ: i32typ,
					Default: &plan.Default{
						NullAbility: false,
					},
				},
			},
			Pkey: &plan.PrimaryKeyDef{
				PkeyColName: "int64_column_primary",
			},
			Name2ColIndex: name2ColIndex,
		},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     1,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	resetChildren(&argument2)
	err2 := argument2.Prepare(proc)
	require.NoError(t, err2)
	_, err2 = vm.Exec(&argument2, proc)
	require.Error(t, err2, "should return error when insert null into primary key column")
	argument2.Reset(proc, false, nil)
	resetChildren(&argument2)
	err2 = argument2.Prepare(proc)
	require.NoError(t, err2)
	_, err2 = vm.Exec(&argument2, proc)
	require.Error(t, err2, "should return error when insert null into primary key column")
	argument2.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPreInsertHasAutoCol(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	incrService := mock_frontend.NewMockAutoIncrementService(ctrl)
	incrService.EXPECT().InsertValues(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(111111), nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Base.IncrService = incrService
	proc.Base.SessionInfo.StorageEngine = eng
	argument1 := PreInsert{
		ctr:        container{},
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column", Typ: i64typ},
				{Name: "scalar_int64", Typ: i64typ},
				{Name: "varchar_column", Typ: varchartyp},
				{Name: "scalar_varchar", Typ: varchartyp},
				{Name: "int64_column", Typ: i64typ},
			},
			Pkey: &plan.PrimaryKeyDef{},
		},
		Attrs:       []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		IsOldUpdate: false,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	resetChildren(&argument1)
	err := argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	argument1.Reset(proc, false, nil)
	resetChildren(&argument1)
	err = argument1.Prepare(proc)
	require.NoError(t, err)
	_, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	argument1.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPreInsertIsUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	incrService := mock_frontend.NewMockAutoIncrementService(ctrl)
	incrService.EXPECT().InsertValues(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(111111), nil).AnyTimes()

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Base.IncrService = incrService
	proc.Base.SessionInfo.StorageEngine = eng
	argument1 := PreInsert{
		ctr:         container{},
		IsOldUpdate: true,
		SchemaName:  "testDb",
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "int64_column", Typ: i64typ},
				{Name: "scalar_int64", Typ: i64typ},
				{Name: "varchar_column", Typ: varchartyp},
				{Name: "scalar_varchar", Typ: varchartyp},
				{Name: "int64_column", Typ: i64typ},
			},
			Pkey: &plan.PrimaryKeyDef{},
		},
		Attrs: []string{"int64_column", "scalar_int64", "varchar_column", "scalar_varchar", "int64_column"},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx:     0,
				IsFirst: false,
				IsLast:  false,
			},
		},
	}

	resetChildren2(&argument1)
	err := argument1.Prepare(proc)
	require.NoError(t, err)
	res, err := vm.Exec(&argument1, proc)
	vecsNum1 := len(res.Batch.Vecs)
	require.NoError(t, err)
	res, err = vm.Exec(&argument1, proc)
	vecsNum2 := len(res.Batch.Vecs)
	require.NoError(t, err)
	require.Equal(t, vecsNum1, vecsNum2)

	argument1.Reset(proc, false, nil)
	resetChildren2(&argument1)
	err = argument1.Prepare(proc)
	require.NoError(t, err)
	res, err = vm.Exec(&argument1, proc)
	require.NoError(t, err)
	require.Equal(t, vecsNum1, len(res.Batch.Vecs))
	argument1.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func TestPreInsertPrepareRefreshesAutoIncrementTableID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "testDb", txnOperator).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "idx_tbl", nil).Return(rel, nil)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(200))

	proc := testutil.NewProc(t)
	proc.Ctx = ctx
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Base.SessionInfo.StorageEngine = eng

	argument := PreInsert{
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Name:  "idx_tbl",
			TblId: 100,
			Cols: []*plan.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: i32typ},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		Attrs: []string{catalog.FakePrimaryKeyColName},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}

	err := argument.Prepare(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(200), argument.TableDef.TblId)
}

func TestPreInsertPrepareSkipsTemporaryTableRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)

	proc := testutil.NewProc(t)
	proc.Ctx = ctx
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Base.SessionInfo.StorageEngine = eng

	argument := PreInsert{
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Name:        "temp_idx_tbl",
			TblId:       100,
			IsTemporary: true,
			Cols: []*plan.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: i32typ},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		Attrs: []string{catalog.FakePrimaryKeyColName},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{
				Idx: 0,
			},
		},
	}

	err := argument.Prepare(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(100), argument.TableDef.TblId)
}

func TestGenAutoIncrColRefreshesStaleTableID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "testDb", txnOperator).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "idx_tbl", nil).Return(rel, nil)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(200))

	incrService := mock_frontend.NewMockAutoIncrementService(ctrl)
	gomock.InOrder(
		incrService.EXPECT().InsertValues(gomock.Any(), uint64(100), gomock.Any(), 1, int64(1)).
			Return(uint64(0), moerr.NewNoSuchTableNoCtx("", "100")),
		incrService.EXPECT().InsertValues(gomock.Any(), uint64(200), gomock.Any(), 1, int64(1)).
			Return(uint64(111111), nil),
	)

	proc := testutil.NewProc(t)
	proc.Ctx = ctx
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Base.IncrService = incrService
	proc.Base.SessionInfo.StorageEngine = eng

	preInsert := &PreInsert{
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Name:  "idx_tbl",
			TblId: 100,
			Cols: []*plan.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: i32typ},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		Attrs:             []string{catalog.FakePrimaryKeyColName},
		EstimatedRowCount: 1,
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{0}, nil)
	bat.SetRowCount(1)

	err := genAutoIncrCol(bat, proc, preInsert)
	require.NoError(t, err)
	require.Equal(t, uint64(200), preInsert.TableDef.TblId)
}

func TestGenAutoIncrColReturnsRetryWhenDefinitionStillChanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	eng.EXPECT().Database(gomock.Any(), "testDb", txnOperator).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "idx_tbl", nil).Return(rel, nil)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(100))

	incrService := mock_frontend.NewMockAutoIncrementService(ctrl)
	incrService.EXPECT().InsertValues(gomock.Any(), uint64(100), gomock.Any(), 1, int64(1)).
		Return(uint64(0), moerr.NewNoSuchTableNoCtx("", "100"))

	proc := testutil.NewProc(t)
	proc.Ctx = ctx
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Base.IncrService = incrService
	proc.Base.SessionInfo.StorageEngine = eng

	preInsert := &PreInsert{
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Name:  "idx_tbl",
			TblId: 100,
			Cols: []*plan.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: i32typ},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		Attrs:             []string{catalog.FakePrimaryKeyColName},
		EstimatedRowCount: 1,
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{0}, nil)
	bat.SetRowCount(1)

	err := genAutoIncrCol(bat, proc, preInsert)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
}

func TestGenAutoIncrColKeepsTemporaryTableBehavior(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	incrService := mock_frontend.NewMockAutoIncrementService(ctrl)
	incrService.EXPECT().InsertValues(gomock.Any(), uint64(100), gomock.Any(), 1, int64(1)).
		Return(uint64(0), moerr.NewNoSuchTableNoCtx("", "100"))

	proc := testutil.NewProc(t)
	proc.Ctx = ctx
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.Base.IncrService = incrService
	proc.Base.SessionInfo.StorageEngine = eng

	preInsert := &PreInsert{
		HasAutoCol: true,
		SchemaName: "testDb",
		TableDef: &plan.TableDef{
			Name:        "temp_idx_tbl",
			TblId:       100,
			IsTemporary: true,
			Cols: []*plan.ColDef{
				{Name: catalog.FakePrimaryKeyColName, Typ: i32typ},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		Attrs:             []string{catalog.FakePrimaryKeyColName},
		EstimatedRowCount: 1,
	}

	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{0}, nil)
	bat.SetRowCount(1)

	err := genAutoIncrCol(bat, proc, preInsert)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoSuchTable))
}

func resetChildren(arg *PreInsert) {
	bat := colexec.MakeMockBatchsWithNullVec()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func resetChildren2(arg *PreInsert) {
	bat1 := colexec.MakeMockBatchs()
	bat2 := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat2})
	arg.Children = nil
	arg.AppendChild(op)
}
