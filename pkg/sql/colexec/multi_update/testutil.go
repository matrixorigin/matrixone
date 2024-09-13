// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

var (
	i64typ     = plan.Type{Id: int32(types.T_int64)}
	i32typ     = plan.Type{Id: int32(types.T_int32)}
	varcharTyp = plan.Type{Id: int32(types.T_varchar), Width: 8192}
	rowIdTyp   = plan.Type{Id: int32(types.T_Rowid)}
)

type testCase struct {
	op           *MultiUpdate
	inputBatchs  []*batch.Batch
	expectErr    bool
	affectedRows uint64
}

func runTestCases(t *testing.T, proc *process.Process, tcs []*testCase) {
	var err error
	var res vm.CallResult
	resetChildren := func(multiUpdate *MultiUpdate, bats []*batch.Batch) {
		op := colexec.NewMockOperator().WithBatchs(bats)
		multiUpdate.Children = nil
		multiUpdate.AppendChild(op)
	}

	for _, tc := range tcs {
		resetChildren(tc.op, tc.inputBatchs)
		err = tc.op.Prepare(proc)
		require.NoError(t, err)
		for {
			res, err = tc.op.Call(proc)
			if tc.expectErr {
				require.Error(t, err)
				break
			}
			if res.Batch == nil || res.Status == vm.ExecStop {
				break
			}
		}

		// if expect error.  only run one time
		if tc.expectErr {
			tc.op.Free(proc, true, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.op.ctr.affectedRows, tc.affectedRows)

		tc.op.Reset(proc, false, nil)

		resetChildren(tc.op, tc.inputBatchs)
		err = tc.op.Prepare(proc)
		require.NoError(t, err)
		for {
			res, err = tc.op.Call(proc)
			if res.Batch == nil || res.Status == vm.ExecStop {
				break
			}
		}
		require.NoError(t, err)

		tc.op.Free(proc, false, nil)
	}

	proc.Free()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}

func prepareTestCtx(t *testing.T) (context.Context, *gomock.Controller, *process.Process) {
	ctrl := gomock.NewController(t)

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	proc := testutil.NewProc()
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx

	return ctx, ctrl, proc
}

func prepareTestEng(ctrl *gomock.Controller) engine.Engine {
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	database := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(database, nil).AnyTimes()

	relation := mock_frontend.NewMockRelation(ctrl)
	relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

	return eng
}

func prepareTestDeleteBatchs(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) ([]*batch.Batch, uint64) {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
	affectRows := 0
	bat1ColumnA := []int64{1, 2, 3}
	bat1ColumnRowID := []string{"1a", "2b", "3c"}

	bat2ColumnA := []int64{1, 2, 3}
	bat2ColumnRowID := []string{"1a", "2b", "3c"}
	attrs := []string{"a", "main_row_id"}

	bat1 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector(bat1ColumnA, nil),
			testutil.MakeVarcharVector(bat1ColumnRowID, nil),
		},
		Attrs: attrs,
		Cnt:   1,
	}
	bat1.SetRowCount(bat1.Vecs[0].Length())
	affectRows += bat1.RowCount()

	bat2 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector(bat2ColumnA, nil),
			testutil.MakeVarcharVector(bat2ColumnRowID, nil),
		},
		Attrs: attrs,
		Cnt:   1,
	}
	bat2.SetRowCount(bat2.Vecs[0].Length())
	affectRows += bat2.RowCount()

	if hasUniqueKey {
		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_uk_1", "bat1_uk_2", "bat1_uk_3"}, nil))
		bat1.Attrs = append(bat1.Attrs, "bat1_uk_pk")

		bat2.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat2_uk_1", "bat2_uk_2", "bat2_uk_3"}, nil))
		bat2.Attrs = append(bat1.Attrs, "bat2_uk_pk")
	}

	if hasSecondaryKey {
		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_sk_1", "bat1_sk_2", "bat1_sk_3"}, nil))
		bat1.Attrs = append(bat1.Attrs, "bat1_sk_pk")

		bat2.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat2_sk_1", "bat2_sk_2", "bat2_sk_3"}, nil))
		bat2.Attrs = append(bat1.Attrs, "bat2_sk_pk")
	}

	if isPartition {
		//todo
	}

	return []*batch.Batch{bat1, bat2}, uint64(affectRows)
}

func prepareTestInsertBatchs(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) ([]*batch.Batch, uint64) {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
	affectRows := 0
	bat1ColumnA := []int64{1, 2, 3}
	bat1ColumnB := []string{"1a", "2b", "3c"}
	bat1ColumnC := []int32{11, 12, 13}
	bat1ColumnD := []int32{21, 22, 23}

	bat2ColumnA := []int64{4, 5, 6}
	bat2ColumnB := []string{"4a", "5b", "6c"}
	bat2ColumnC := []int32{14, 15, 16}
	bat2ColumnD := []int32{24, 25, 26}
	attrs := []string{"a", "b", "c"}

	bat1 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector(bat1ColumnA, nil),
			testutil.MakeVarcharVector(bat1ColumnB, nil),
			testutil.MakeInt32Vector(bat1ColumnC, nil),
			testutil.MakeInt32Vector(bat1ColumnD, nil),
		},
		Attrs: attrs,
		Cnt:   1,
	}
	bat1.SetRowCount(bat1.Vecs[0].Length())
	affectRows += bat1.RowCount()

	bat2 := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector(bat2ColumnA, nil),
			testutil.MakeVarcharVector(bat2ColumnB, nil),
			testutil.MakeInt32Vector(bat2ColumnC, nil),
			testutil.MakeInt32Vector(bat2ColumnD, nil),
		},
		Attrs: attrs,
		Cnt:   1,
	}
	bat2.SetRowCount(bat2.Vecs[0].Length())
	affectRows += bat2.RowCount()

	if hasUniqueKey {
		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_uk_1", "bat1_uk_2", "bat1_uk_3"}, nil))
		bat1.Attrs = append(bat1.Attrs, "bat1_uk_pk")

		bat2.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat2_uk_1", "bat2_uk_2", "bat2_uk_3"}, nil))
		bat2.Attrs = append(bat1.Attrs, "bat2_uk_pk")
	}

	if hasSecondaryKey {
		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_sk_1", "bat1_sk_2", "bat1_sk_3"}, nil))
		bat1.Attrs = append(bat1.Attrs, "bat1_sk_pk")

		bat2.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat2_sk_1", "bat2_sk_2", "bat2_sk_3"}, nil))
		bat2.Attrs = append(bat1.Attrs, "bat2_sk_pk")
	}

	if isPartition {
		//todo
	}

	return []*batch.Batch{bat1, bat2}, uint64(affectRows)
}

func prepareTestMultiUpdateCtx(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) []*MultiUpdateCtx {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
	// if has uniqueKey : t1(a big int primary key, b varchar(10) not null, c int unique key, d int);
	// if has secondaryKey : t1(a big int primary key, b varchar(10) not null, c int, d int, key(d));
	objRef, tableDef := getTestMainTable()

	updateCtx := &MultiUpdateCtx{
		ref:        objRef,
		tableDef:   tableDef,
		tableType:  MainTable,
		insertCols: []int{0, 1, 2, 3},
	}
	updateCtxs := []*MultiUpdateCtx{updateCtx}

	if hasUniqueKey {
		uniqueTblName, _ := util.BuildIndexTableName(context.TODO(), true)

		tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
			IdxId:          "1",
			IndexName:      "c",
			Parts:          []string{"c"},
			Unique:         true,
			IndexTableName: uniqueTblName,
			TableExist:     true,
			Visible:        true,
		})

		uniqueObjRef, uniqueTableDef := getTestUniqueIndexTable(uniqueTblName)

		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        uniqueObjRef,
			tableDef:   uniqueTableDef,
			tableType:  UniqueIndexTable,
			insertCols: []int{4, 0},
		})
	}

	if hasSecondaryKey {
		secondaryIdxTblName, _ := util.BuildIndexTableName(context.TODO(), false)
		tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
			IdxId:          "2",
			IndexName:      "d",
			Parts:          []string{"d"},
			Unique:         false,
			IndexTableName: secondaryIdxTblName,
			TableExist:     true,
			Visible:        true,
		})

		secondaryIdxObjRef, secondaryIdxTableDef := getTestSecondaryIndexTable(secondaryIdxTblName)

		secondaryPkPos := 4
		if hasUniqueKey {
			secondaryPkPos += 1
		}
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        secondaryIdxObjRef,
			tableDef:   secondaryIdxTableDef,
			tableType:  SecondaryIndexTable,
			insertCols: []int{secondaryPkPos, 0},
		})
	}

	if isPartition {
		//todo
	}

	return updateCtxs
}

func getTestMainTable() (*plan.ObjectRef, *plan.TableDef) {
	objRef := &plan.ObjectRef{Schema: 1, Obj: 1, SchemaName: "test", ObjName: "t1"}

	tableDef := &plan.TableDef{
		TblId:  0,
		Name:   "t1",
		Hidden: false,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Typ: i64typ, NotNull: true, Primary: true},
			{ColId: 1, Name: "b", Typ: varcharTyp, NotNull: true},
			{ColId: 2, Name: "c", Typ: i32typ},
			{ColId: 3, Name: "d", Typ: i32typ},
			{ColId: 4, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: "a",
			Names:       []string{"a"},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	tableDef.Name2ColIndex["a"] = 0
	tableDef.Name2ColIndex["b"] = 1
	tableDef.Name2ColIndex["c"] = 2
	tableDef.Name2ColIndex["d"] = 3
	tableDef.Name2ColIndex[catalog.Row_ID] = 4

	return objRef, tableDef
}

func getTestUniqueIndexTable(uniqueTblName string) (*plan.ObjectRef, *plan.TableDef) {
	uniqueObjRef := &plan.ObjectRef{Schema: 1, Obj: 2, SchemaName: "test", ObjName: uniqueTblName}
	uniqueTableDef := &plan.TableDef{
		TblId:  1,
		Name:   uniqueTblName,
		Hidden: true,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, Typ: varcharTyp, NotNull: true, Primary: true},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, Typ: varcharTyp, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: catalog.IndexTableIndexColName,
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	uniqueTableDef.Name2ColIndex[catalog.IndexTableIndexColName] = 0
	uniqueTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName] = 1
	uniqueTableDef.Name2ColIndex[catalog.Row_ID] = 2

	return uniqueObjRef, uniqueTableDef
}

func getTestSecondaryIndexTable(secondaryIdxTblName string) (*plan.ObjectRef, *plan.TableDef) {
	uniqueObjRef := &plan.ObjectRef{Schema: 1, Obj: 2, SchemaName: "test", ObjName: secondaryIdxTblName}
	uniqueTableDef := &plan.TableDef{
		TblId:  1,
		Name:   secondaryIdxTblName,
		Hidden: true,
		Cols: []*plan.ColDef{
			{ColId: 0, Name: catalog.IndexTableIndexColName, Typ: varcharTyp, NotNull: true, Primary: true},
			{ColId: 1, Name: catalog.IndexTablePrimaryColName, Typ: varcharTyp, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Typ: rowIdTyp},
		},
		TableType: catalog.SystemOrdinaryRel,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: catalog.IndexTableIndexColName,
			Names:       []string{catalog.IndexTableIndexColName},
		},
		Name2ColIndex: make(map[string]int32),
		DbName:        "test",
	}
	uniqueTableDef.Name2ColIndex[catalog.IndexTableIndexColName] = 0
	uniqueTableDef.Name2ColIndex[catalog.IndexTablePrimaryColName] = 1
	uniqueTableDef.Name2ColIndex[catalog.Row_ID] = 2

	return uniqueObjRef, uniqueTableDef
}
