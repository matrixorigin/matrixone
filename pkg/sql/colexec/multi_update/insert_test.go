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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestInsertSimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

func TestInsertSpecialTable(t *testing.T) {
	//case 1: pk has null, throw err
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)

	oldBat := case1.inputBatchs[0]
	bat1RowCount := oldBat.RowCount()
	rows := makeTestPkArray(0, bat1RowCount)
	// pk have null
	columnA := testutil.MakeInt64Vector(rows, []uint64{0, 1, 2})
	oldBat.Vecs[0].Free(proc.GetMPool())
	oldBat.ReplaceVector(oldBat.Vecs[0], columnA, 0)
	case1.expectErr = true
	runTestCases(t, proc, []*testCase{case1})

	//case 2: unique key has null, that will be ok
	hasUniqueKey = true
	hasSecondaryKey = false

	proc, case1 = buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	oldBat = case1.inputBatchs[0]
	bat1RowCount = oldBat.RowCount()
	pkArr := makeTestVarcharArray(bat1RowCount)
	uniquePk := testutil.MakeVarcharVector(pkArr, []uint64{0, 1, 2})
	oldBat.Vecs[4].Free(proc.GetMPool())
	oldBat.ReplaceVector(oldBat.Vecs[4], uniquePk, 0)
	runTestCases(t, proc, []*testCase{case1})
}

func TestInsertTableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true

	proc, case1 := buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

// insert s3
func TestInsertS3SimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildInsertS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

func TestInsertS3TableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true

	proc, case1 := buildInsertS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCases(t, proc, []*testCase{case1})
}

func TestFlushS3Info(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl)

	batchs, rowCount := buildFlushS3InfoBatch(proc.GetMPool(), hasUniqueKey, hasSecondaryKey)

	multiUpdateCtxs := prepareTestInsertMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateFlushS3Info
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, rowCount, action)

	runTestCases(t, proc, []*testCase{retCase})
}

// ----- util function ----
func buildInsertTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareTestInsertBatchs(proc.GetMPool(), 2, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestInsertMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteTable
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, action)
	return proc, retCase
}

func buildInsertS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl)

	batchs, _ := prepareTestInsertBatchs(proc.GetMPool(), 10, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestInsertMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteS3
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, 0, action)
	return proc, retCase
}

func prepareTestInsertBatchs(mp *mpool.MPool, size int, hasUniqueKey bool, hasSecondaryKey bool) ([]*batch.Batch, uint64) {
	var bats = make([]*batch.Batch, size)
	affectRows := 0
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

		attrs := []string{"a", "b", "c", "d"}
		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnA, columnB, columnC, columnD},
			Attrs: attrs,
		}

		if hasUniqueKey {
			bat.Vecs = append(bat.Vecs, testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil))
			bat.Attrs = append(bat.Attrs, "uk_pk")
		}

		if hasSecondaryKey {
			bat.Vecs = append(bat.Vecs, testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil))
			bat.Attrs = append(bat.Attrs, "sk_pk")
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		bats[i] = bat
		affectRows = affectRows + rowCount
	}

	return bats, uint64(affectRows)
}

func prepareTestInsertMultiUpdateCtx(hasUniqueKey bool, hasSecondaryKey bool) []*MultiUpdateCtx {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
	// if has uniqueKey : t1(a big int primary key, b varchar(10) not null, c int unique key, d int);
	// if has secondaryKey : t1(a big int primary key, b varchar(10) not null, c int, d int, key(d));
	objRef, tableDef := getTestMainTable()

	updateCtx := &MultiUpdateCtx{
		ObjRef:     objRef,
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2, 3},
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
			ObjRef:     uniqueObjRef,
			TableDef:   uniqueTableDef,
			InsertCols: []int{4, 0},
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
			ObjRef:     secondaryIdxObjRef,
			TableDef:   secondaryIdxTableDef,
			InsertCols: []int{secondaryPkPos, 0},
		})
	}

	return updateCtxs
}

func buildFlushS3InfoBatch(mp *mpool.MPool, hasUniqueKey bool, hasSecondaryKey bool) ([]*batch.Batch, uint64) {
	insertBats, _ := prepareTestInsertBatchs(mp, 5, hasUniqueKey, hasSecondaryKey)
	retBat := batch.NewWithSize(6)
	action := uint8(actionInsert)

	retBat.Vecs[0] = testutil.NewUInt8Vector(5, types.T_uint8.ToType(), mp, false, []uint8{action, action, action, action, action})
	retBat.Vecs[1] = testutil.NewUInt16Vector(5, types.T_uint16.ToType(), mp, false, []uint16{0, 0, 0, 0, 0})       //idx
	retBat.Vecs[2] = testutil.NewUInt16Vector(5, types.T_uint16.ToType(), mp, false, []uint16{0, 0, 0, 0, 0})       //partIdx
	retBat.Vecs[3] = vector.NewVec(types.T_uint64.ToType())                                                         //rowCount
	retBat.Vecs[4] = testutil.NewStringVector(5, types.T_varchar.ToType(), mp, false, []string{"", "", "", "", ""}) //name
	retBat.Vecs[5] = vector.NewVec(types.T_text.ToType())                                                           //batch bytes

	totalRowCount := 0
	for _, bat := range insertBats {
		totalRowCount += bat.RowCount()
		_ = vector.AppendFixed(retBat.Vecs[3], bat.RowCount(), false, mp)

		val, _ := bat.MarshalBinary()
		_ = vector.AppendBytes(retBat.Vecs[5], val, false, mp)

		bat.Clean(mp)
	}

	retBat.SetRowCount(retBat.Vecs[0].Length())
	return []*batch.Batch{retBat}, uint64(totalRowCount)
}
