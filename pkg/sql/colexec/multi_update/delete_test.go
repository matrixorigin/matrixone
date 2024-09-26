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

func TestDeleteSimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := false

	proc, case1 := buildDeleteTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestDeleteTableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true
	isPartition := false

	proc, case1 := buildDeleteTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestDeletePartitionTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	proc, case1 := buildDeleteTestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

// delete s3
func TestDeleteS3SimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := false

	proc, case1 := buildDeleteS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestDeleteS3TableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true
	isPartition := false

	proc, case1 := buildDeleteS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)
	runTestCases(t, proc, []*testCase{case1})
}

func TestDeleteS3PartitionTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false
	isPartition := true

	proc, case1 := buildDeleteS3TestCase(t, hasUniqueKey, hasSecondaryKey, isPartition)

	runTestCases(t, proc, []*testCase{case1})
}

// multi delete

// ----- util function ----
func buildDeleteTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareTestDeleteBatchs(proc.GetMPool(), 2, hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestDeleteMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)
	toWriteS3 := false
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, toWriteS3)
	return proc, retCase
}

func buildDeleteS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl)

	batchs, affectRows := prepareTestDeleteBatchs(proc.GetMPool(), 200, hasUniqueKey, hasSecondaryKey, isPartition)
	multiUpdateCtxs := prepareTestDeleteMultiUpdateCtx(hasUniqueKey, hasSecondaryKey, isPartition)
	toWriteS3 := true
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, toWriteS3)
	return proc, retCase
}

func prepareTestDeleteBatchs(mp *mpool.MPool, size int, hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) ([]*batch.Batch, uint64) {
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
		columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)
		attrs := []string{"main_rowid", "a"}

		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnRowID, columnA},
			Attrs: attrs,
			Cnt:   1,
		}

		if hasUniqueKey {
			columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)
			columnPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnPk)
			bat.Attrs = append(bat.Attrs, "uk_rowid", "uk_pk")
		}

		if hasSecondaryKey {
			columnRowID := testutil.NewRowidVector(rowCount, types.T_Rowid.ToType(), mp, false, nil)
			columnPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnPk)
			bat.Attrs = append(bat.Attrs, "sk_rowid", "sk_pk")
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

func prepareTestDeleteMultiUpdateCtx(hasUniqueKey bool, hasSecondaryKey bool, isPartition bool) []*MultiUpdateCtx {
	// create table t1(a big int primary key, b varchar(10) not null, c int, d int);
	// if has uniqueKey : t1(a big int primary key, b varchar(10) not null, c int unique key, d int);
	// if has secondaryKey : t1(a big int primary key, b varchar(10) not null, c int, d int, key(d));
	objRef, tableDef := getTestMainTable(isPartition)

	updateCtx := &MultiUpdateCtx{
		ref:        objRef,
		tableDef:   tableDef,
		tableType:  updateMainTable,
		deleteCols: []int{0, 1}, //row_id & pk
	}
	updateCtxs := []*MultiUpdateCtx{updateCtx}
	colCount := 2

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

		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        uniqueObjRef,
			tableDef:   uniqueTableDef,
			tableType:  updateUniqueIndexTable,
			deleteCols: []int{2, 3}, //row_id & pk
		})
		colCount += 2
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

		secondaryPkPos := []int{2, 3}
		if hasUniqueKey {
			secondaryPkPos[0] = secondaryPkPos[0] + 2
			secondaryPkPos[1] = secondaryPkPos[1] + 2
		}
		colCount += 2
		updateCtxs = append(updateCtxs, &MultiUpdateCtx{
			ref:        secondaryIdxObjRef,
			tableDef:   secondaryIdxTableDef,
			tableType:  updateSecondaryIndexTable,
			deleteCols: secondaryPkPos,
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
