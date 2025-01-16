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
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// test simple interface
func TestSimpleInterface(t *testing.T) {
	op := NewArgument()

	_ = op.TypeName()
	require.Equal(t, op.OpType(), vm.MultiUpdate)
	buf := new(bytes.Buffer)
	op.String(buf)

	op.Release()
}

func TestUpdateSingleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateTableWithUniqueKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := false

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

// update table s3
func TestUpdateS3SingleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildUpdateS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateS3TableWithUniqueKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true

	proc, case1 := buildUpdateS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

// ----- util function ----
func buildUpdateTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareUpdateTestBatchs(proc.GetMPool(), 3, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteTable
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, action)
	return proc, retCase
}

func buildUpdateS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl)

	batchs, _ := prepareUpdateTestBatchs(proc.GetMPool(), 10, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteS3
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, 0, action)
	return proc, retCase
}

func prepareUpdateTestBatchs(mp *mpool.MPool, size int, hasUniqueKey bool, hasSecondaryKey bool) ([]*batch.Batch, uint64) {
	var bats = make([]*batch.Batch, size)
	affectRows := 0
	mainObjectID := types.NewObjectid()
	uniqueObjectID := types.NewObjectid()
	secondaryObjectID := types.NewObjectid()
	for i := 0; i < size; i++ {
		rowCount := colexec.DefaultBatchSize
		if i == size-1 {
			rowCount = rowCount / 2
		}

		rows := makeTestPkArray(int64(affectRows), rowCount)
		columnA := testutil.MakeInt64Vector(rows, nil)
		columnB := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil)
		columnC := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil)
		columnD := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil)
		columnRowID := makeTestRowIDVector(mp, mainObjectID, uint16(i), rowCount)

		attrs := []string{"a", "b", "c", "d", catalog.Row_ID}
		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnA, columnB, columnC, columnD, columnRowID},
			Attrs: attrs,
		}

		if hasUniqueKey {
			columnRowID := makeTestRowIDVector(mp, uniqueObjectID, uint16(i), rowCount)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnOldPk)
			bat.Vecs = append(bat.Vecs, columnNewPk)
			bat.Attrs = append(bat.Attrs, "uk_rowid", "uk_old_pk", "uk_new_pk")
		}

		if hasSecondaryKey {
			columnRowID := makeTestRowIDVector(mp, secondaryObjectID, uint16(i), rowCount)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnOldPk)
			bat.Vecs = append(bat.Vecs, columnNewPk)
			bat.Attrs = append(bat.Attrs, "sk_rowid", "sk_old_pk", "sk_new_pk")
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		bats[i] = bat
		affectRows = affectRows + rowCount
	}

	return bats, uint64(affectRows)
}

func prepareTestUpdateMultiUpdateCtx(hasUniqueKey bool, hasSecondaryKey bool) []*MultiUpdateCtx {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);

	// only test: update t1 set c = 10
	objRef, tableDef := getTestMainTable()

	// if only update main table, attrs = ["a","b","new_c","d","row_id"]
	updateCtx := &MultiUpdateCtx{
		ObjRef:     objRef,
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2, 3}, //a, b, new_c, d
		DeleteCols: []int{4, 0},       //row_id, a
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

		// if update main table with uk, attrs = ["a","b","new_c","d","row_id","uk_del_rowid","uk_del_pk","uk_new_pk"]
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ObjRef:     uniqueObjRef,
			TableDef:   uniqueTableDef,
			InsertCols: []int{7, 0}, //uk_pk & main_tbl_pk
			DeleteCols: []int{5, 6}, //del_row_id & del_pk
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

		// if update main table with sk, attrs = ["a","b","new_c","d","row_id","sk_del_rowid","sk_del_pk","sk_new_pk"]
		deleteCols := []int{5, 6}
		insertCols := []int{7, 0}
		if hasUniqueKey {
			deleteCols[0] = deleteCols[0] + 3
			deleteCols[1] = deleteCols[1] + 3
			insertCols[0] = insertCols[0] + 3
		}
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ObjRef:     secondaryIdxObjRef,
			TableDef:   secondaryIdxTableDef,
			InsertCols: insertCols,
			DeleteCols: deleteCols,
		})
	}

	return updateCtxs
}
