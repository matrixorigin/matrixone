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
	isPartition := false

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateTableWithUniqueKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := false
	isPartition := false

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateParitionTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

// update table s3
func TestUpdateS3SingleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := false

	proc, case1 := buildUpdateS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateS3TableWithUniqueKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true
	isPartition := false

	proc, case1 := buildUpdateS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateS3ParitionTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	proc, case1 := buildUpdateS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

// ----- util function ----
func buildUpdateTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareUpdateTestBatchs(proc.GetMPool(), 3, hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)
	toWriteS3 := false
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, toWriteS3)
	return proc, retCase
}

func buildUpdateS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareUpdateTestBatchs(proc.GetMPool(), 500, hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)
	toWriteS3 := true
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, toWriteS3)
	return proc, retCase
}

func prepareUpdateTestBatchs(mp *mpool.MPool, size int, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) ([]*batch.Batch, uint64) {
	var bats = make([]*batch.Batch, size)
	affectRows := 0
	partitionCount := 3
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
		columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)

		attrs := []string{"a", "b", "c", "d", catalog.Row_ID}
		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnA, columnB, columnC, columnD, columnRowID},
			Attrs: attrs,
			Cnt:   1,
		}

		if hasUniqueKey {
			columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnOldPk)
			bat.Vecs = append(bat.Vecs, columnNewPk)
			bat.Attrs = append(bat.Attrs, "uk_rowid", "uk_old_pk", "uk_new_pk")
		}

		if hasSecondaryKey {
			columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnOldPk)
			bat.Vecs = append(bat.Vecs, columnNewPk)
			bat.Attrs = append(bat.Attrs, "sk_rowid", "sk_old_pk", "sk_new_pk")
		}

		if isPartition {
			rows := makeTestPartitionArray(rowCount, partitionCount)
			bat.Vecs = append(bat.Vecs, testutil.MakeInt32Vector(rows, nil))
			bat.Attrs = append(bat.Attrs, "part_idx")
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		bats[i] = bat
		affectRows = affectRows + rowCount
	}

	return bats, uint64(affectRows)
}

// func prepareUpdateTestBatchs(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) ([]*batch.Batch, uint64) {
// 	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
// 	// update t1 set c = 10
// 	affectRows := 0
// 	segmentID := objectio.NewSegmentid()

// 	//old data
// 	blkId1 := objectio.NewBlockid(segmentID, 0, 1)
// 	bat1ColumnA := []int64{1, 2, 3}
// 	bat1ColumnB := []string{"1a", "2b", "3c"}
// 	bat1ColumnC := []int32{10, 10, 10} // set c = 10
// 	bat1ColumnD := []int32{21, 22, 23}
// 	bat1ColumnRowID := []types.Rowid{*objectio.NewRowid(blkId1, 0), *objectio.NewRowid(blkId1, 1), *objectio.NewRowid(blkId1, 2)}

// 	blkId2 := objectio.NewBlockid(segmentID, 0, 2)
// 	bat2ColumnA := []int64{4, 5, 6}
// 	bat2ColumnB := []string{"4a", "5b", "6c"}
// 	bat2ColumnC := []int32{10, 10, 10} // set c = 10
// 	bat2ColumnD := []int32{24, 25, 26}
// 	bat2ColumnRowID := []types.Rowid{*objectio.NewRowid(blkId2, 0), *objectio.NewRowid(blkId2, 1), *objectio.NewRowid(blkId2, 2)}
// 	attrs := []string{"a", "b", "c", "d", "new_c", catalog.Row_ID}

// 	bat1 := &batch.Batch{
// 		Vecs: []*vector.Vector{
// 			testutil.MakeInt64Vector(bat1ColumnA, nil),
// 			testutil.MakeVarcharVector(bat1ColumnB, nil),
// 			testutil.MakeInt32Vector(bat1ColumnC, nil),
// 			testutil.MakeInt32Vector(bat1ColumnD, nil),
// 			testutil.MakeRowIdVector(bat1ColumnRowID, nil),
// 		},
// 		Attrs: attrs,
// 		Cnt:   1,
// 	}
// 	bat1.SetRowCount(bat1.Vecs[0].Length())
// 	affectRows += bat1.RowCount()

// 	bat2 := &batch.Batch{
// 		Vecs: []*vector.Vector{
// 			testutil.MakeInt64Vector(bat2ColumnA, nil),
// 			testutil.MakeVarcharVector(bat2ColumnB, nil),
// 			testutil.MakeInt32Vector(bat2ColumnC, nil),
// 			testutil.MakeInt32Vector(bat2ColumnD, nil),
// 			testutil.MakeRowIdVector(bat2ColumnRowID, nil),
// 		},
// 		Attrs: attrs,
// 		Cnt:   1,
// 	}
// 	bat2.SetRowCount(bat2.Vecs[0].Length())
// 	affectRows += bat2.RowCount()

// 	if hasUniqueKey {
// 		blkId3 := objectio.NewBlockid(segmentID, 0, 3)
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeRowIdVector([]types.Rowid{*objectio.NewRowid(blkId3, 0), *objectio.NewRowid(blkId3, 1), *objectio.NewRowid(blkId3, 2)}, nil))
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_uk_1", "bat1_uk_2", "bat1_uk_3"}, nil))
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_uk_11", "bat1_uk_22", "bat1_uk_33"}, nil))
// 		bat1.Attrs = append(bat1.Attrs, "bat1_uk_rowid", "bat1_uk_old_pk", "bat1_uk_new_pk")

// 		blkId4 := objectio.NewBlockid(segmentID, 0, 4)
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeRowIdVector([]types.Rowid{*objectio.NewRowid(blkId4, 0), *objectio.NewRowid(blkId4, 1), *objectio.NewRowid(blkId4, 2)}, nil))
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeVarcharVector([]string{"bat2_uk_1", "bat2_uk_2", "bat2_uk_3"}, nil))
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeVarcharVector([]string{"bat2_uk_11", "bat2_uk_22", "bat2_uk_33"}, nil))
// 		bat2.Attrs = append(bat2.Attrs, "bat2_uk_rowid", "bat2_uk_old_pk", "bat2_uk_new_pk")
// 	}

// 	if hasSecondaryKey {
// 		blkId5 := objectio.NewBlockid(segmentID, 0, 5)
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeRowIdVector([]types.Rowid{*objectio.NewRowid(blkId5, 0), *objectio.NewRowid(blkId5, 1), *objectio.NewRowid(blkId5, 2)}, nil))
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_sk_1", "bat1_sk_2", "bat1_sk_3"}, nil))
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeVarcharVector([]string{"bat1_sk_11", "bat1_sk_22", "bat1_sk_33"}, nil))
// 		bat1.Attrs = append(bat1.Attrs, "bat1_sk_pk", "bat1_sk_old_pk", "bat1_sk_new_pk")

// 		blkId6 := objectio.NewBlockid(segmentID, 0, 6)
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeRowIdVector([]types.Rowid{*objectio.NewRowid(blkId6, 0), *objectio.NewRowid(blkId6, 1), *objectio.NewRowid(blkId6, 2)}, nil))
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeVarcharVector([]string{"bat2_sk_1", "bat2_sk_2", "bat2_sk_3"}, nil))
// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeVarcharVector([]string{"bat2_sk_11", "bat2_sk_22", "bat2_sk_33"}, nil))
// 		bat2.Attrs = append(bat2.Attrs, "bat2_sk_pk", "bat1_sk_old_pk", "bat1_sk_new_pk")
// 	}

// 	if isPartition {
// 		bat1.Vecs = append(bat1.Vecs, testutil.MakeInt32Vector([]int32{0, 1, 2}, nil))
// 		bat1.Attrs = append(bat1.Attrs, "bat1_part_idx")

// 		bat2.Vecs = append(bat2.Vecs, testutil.MakeInt32Vector([]int32{0, 1, 2}, nil))
// 		bat2.Attrs = append(bat2.Attrs, "bat2_part_idx")
// 	}

// 	return []*batch.Batch{bat1, bat2}, uint64(affectRows)
// }

func prepareTestUpdateMultiUpdateCtx(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) []*MultiUpdateCtx {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);

	// only test: update t1 set c = 10
	objRef, tableDef := getTestMainTable(isPartition)

	// if only update main table, attrs = ["a","b","new_c","d","row_id"]
	updateCtx := &MultiUpdateCtx{
		ref:        objRef,
		tableDef:   tableDef,
		tableType:  updateMainTable,
		insertCols: []int{0, 1, 2, 3}, //a, b, new_c, d
		deleteCols: []int{4, 0},       //row_id, a
	}
	updateCtxs := []*MultiUpdateCtx{updateCtx}
	colCount := 5

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

		uniqueObjRef, uniqueTableDef := getTestUniqueIndexTable(uniqueTblName, isPartition)

		// if update main table with uk, attrs = ["a","b","new_c","d","row_id","uk_del_rowid","uk_del_pk","uk_new_pk"]
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        uniqueObjRef,
			tableDef:   uniqueTableDef,
			tableType:  updateUniqueIndexTable,
			insertCols: []int{7, 0}, //uk_pk & main_tbl_pk
			deleteCols: []int{5, 6}, //del_row_id & del_pk
		})
		colCount += 3
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

		secondaryIdxObjRef, secondaryIdxTableDef := getTestSecondaryIndexTable(secondaryIdxTblName, isPartition)

		// if update main table with sk, attrs = ["a","b","new_c","d","row_id","sk_del_rowid","sk_del_pk","sk_new_pk"]
		deleteCols := []int{5, 6}
		insertCols := []int{7, 0}
		if hasUniqueKey {
			deleteCols[0] = deleteCols[0] + 3
			deleteCols[1] = deleteCols[1] + 3
			insertCols[0] = insertCols[0] + 3
		}
		colCount += 3
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        secondaryIdxObjRef,
			tableDef:   secondaryIdxTableDef,
			tableType:  updateSecondaryIndexTable,
			insertCols: insertCols,
			deleteCols: deleteCols,
		})
	}

	if isPartition {
		for i, updateCtx := range updateCtxs {
			partTblIDs := make([]int32, len(tableDef.Partition.PartitionTableNames))
			for j := range tableDef.Partition.PartitionTableNames {
				partTblIDs[j] = int32(i*1000 + j)
			}
			updateCtx.partitionIdx = colCount
			updateCtx.partitionTableIDs = partTblIDs
			updateCtx.partitionTableNames = tableDef.Partition.PartitionTableNames
		}
	}

	return updateCtxs
}
