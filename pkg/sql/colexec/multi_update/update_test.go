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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, false)
	runTestCases(t, proc, []*testCase{case1})

	proc, case1 = buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, true)
	runTestCases(t, proc, []*testCase{case1})
}

func TestUpdateTableWithUniqueKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := false

	proc, case1 := buildUpdateTestCase(t, hasUniqueKey, hasSecondaryKey, false)
	runTestCases(t, proc, []*testCase{case1})
}

func TestFilterTargetRowsKeepsIndependentWholeRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	bat := batch.NewWithSize(7)
	bat.Vecs[0] = testutil.MakeRowIdVector(
		[]types.Rowid{
			types.BuildTestRowid(1, 1),
			types.BuildTestRowid(1, 1),
			types.BuildTestRowid(1, 2),
			types.BuildTestRowid(1, 3),
		},
		[]uint64{3},
		mp,
	)
	bat.Vecs[1] = testutil.NewInt64Vector(
		4,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{1, 2, 1, 1},
	)
	bat.Vecs[2] = testutil.MakeRowIdVector(
		[]types.Rowid{
			types.BuildTestRowid(2, 1),
			types.BuildTestRowid(2, 2),
			types.BuildTestRowid(2, 2),
			types.BuildTestRowid(2, 3),
		},
		nil,
		mp,
	)
	bat.Vecs[3] = testutil.NewInt64Vector(
		4,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{1, 1, 2, 1},
	)
	bat.Vecs[4] = testutil.NewInt32Vector(
		4,
		types.T_int32.ToType(),
		mp,
		false,
		nil,
		[]int32{10, 20, 30, 40},
	)
	bat.Vecs[5] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, true, true, true},
	)
	bat.Vecs[6] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, false, true, true},
	)
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	first, clean, duplicateRows, err := filterTargetRows(proc, &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 1},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 4, 1, 5},
	}, bat, nil)
	require.NoError(t, err)
	require.True(t, clean)
	require.Zero(t, duplicateRows)
	defer first.Clean(mp)
	require.Equal(t, []int32{10, 30}, vector.MustFixedColWithTypeCheck[int32](first.Vecs[4]))

	second, clean, duplicateRows, err := filterTargetRows(proc, &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 2},
		DedupByTargetRowID: true,
		DeleteCols:         []int{2, 4, 3, 6},
	}, bat, nil)
	require.NoError(t, err)
	require.True(t, clean)
	require.Zero(t, duplicateRows)
	defer second.Clean(mp)
	require.Equal(t, []int32{10, 40}, vector.MustFixedColWithTypeCheck[int32](second.Vecs[4]))
}

func TestFilterTargetRowsCrossesAllocationAccountBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<20)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	bat := batch.NewOffHeapWithSize(4)
	for idx, typ := range []types.Type{
		types.T_Rowid.ToType(), types.T_int64.ToType(),
		types.T_int32.ToType(), types.T_bool.ToType(),
	} {
		bat.Vecs[idx], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
		require.NoError(t, err)
	}
	require.NoError(t, vector.AppendFixedList(
		bat.Vecs[0], []types.Rowid{types.BuildTestRowid(1, 1)}, nil, mp))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []int64{1}, nil, mp))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[2], []int32{10}, nil, mp))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[3], []bool{true}, nil, mp))
	bat.SetRowCount(1)

	filtered, clean, _, err := filterTargetRows(proc, &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 1},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 2, 1, 3},
	}, bat, nil)
	require.NoError(t, err)
	require.True(t, clean)
	require.Nil(t, filtered.AllocationAccountSelection())
	for _, vec := range filtered.Vecs {
		require.Nil(t, vec.AllocationAccountSelection())
	}
	filtered.Clean(mp)
	bat.Clean(mp)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestFilterTargetRowsDedupsAliasesAcrossBatchesWithAccountedHashMap(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	rowID1 := types.BuildTestRowid(1, 1)
	rowID2 := types.BuildTestRowid(1, 2)
	makeBatch := func() *batch.Batch {
		bat := batch.NewWithSize(4)
		bat.Vecs[0] = testutil.MakeRowIdVector([]types.Rowid{rowID1, rowID2}, nil, mp)
		bat.Vecs[1] = testutil.NewInt64Vector(
			2,
			types.T_int64.ToType(),
			mp,
			false,
			nil,
			[]int64{1, 1},
		)
		bat.Vecs[2] = testutil.NewInt32Vector(
			2,
			types.T_int32.ToType(),
			mp,
			false,
			nil,
			[]int32{10, 20},
		)
		bat.Vecs[3] = testutil.NewBoolVector(
			2, types.T_bool.ToType(), mp, false, nil, []bool{true, true},
		)
		bat.SetRowCount(2)
		return bat
	}
	updateCtx := &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 42},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 2, 1, 3},
	}
	seen, err := hashmap.NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer seen.Free()

	firstInput := makeBatch()
	defer firstInput.Clean(mp)
	first, clean, duplicateRows, err := filterTargetRows(proc, updateCtx, firstInput, seen)
	require.NoError(t, err)
	require.True(t, clean)
	defer first.Clean(mp)
	require.Equal(t, 2, first.RowCount())
	require.Zero(t, duplicateRows)

	secondInput := makeBatch()
	defer secondInput.Clean(mp)
	second, clean, duplicateRows, err := filterTargetRows(proc, updateCtx, secondInput, seen)
	require.NoError(t, err)
	require.True(t, clean)
	defer second.Clean(mp)
	require.Zero(t, second.RowCount())
	require.Equal(t, uint64(2), duplicateRows)
	require.Positive(t, seen.Size())
}

func TestFilterTargetRowsDedupsRepeatedRowIDsWithinOneChunk(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	rowIDA := types.BuildTestRowid(1, 1)
	rowIDB := types.BuildTestRowid(1, 2)
	bat := batch.NewWithSize(5)
	bat.Vecs[0] = testutil.MakeRowIdVector(
		[]types.Rowid{rowIDA, rowIDA, rowIDB, rowIDA}, nil, mp)
	bat.Vecs[1] = testutil.NewInt64Vector(
		4, types.T_int64.ToType(), mp, false, nil, []int64{1, 1, 1, 1})
	bat.Vecs[2] = testutil.NewInt32Vector(
		4, types.T_int32.ToType(), mp, false, nil, []int32{10, 11, 20, 12})
	bat.Vecs[3] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, true, true, true})
	bat.Vecs[4] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, false, true, true})
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	seen, err := hashmap.NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer seen.Free()
	changedRowsCol := 4
	filtered, owned, duplicateRows, err := filterTargetRows(proc, &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 42},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 2, 1, 3},
		ChangedRowsCol:     &changedRowsCol,
	}, bat, seen)
	require.NoError(t, err)
	require.True(t, owned)
	defer filtered.Clean(mp)
	require.Zero(t, duplicateRows)
	require.Equal(t, []int32{10, 20}, vector.MustFixedColWithTypeCheck[int32](filtered.Vecs[2]))
}

func TestFilterTargetRowsCountsActiveAliasesWithoutRepeatingPhysicalWrites(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	bat := batch.NewWithSize(6)
	bat.Vecs[0] = testutil.MakeRowIdVector(
		[]types.Rowid{
			types.BuildTestRowid(1, 1),
			types.BuildTestRowid(1, 2),
			types.BuildTestRowid(1, 3),
			types.BuildTestRowid(1, 4),
		},
		nil,
		mp,
	)
	bat.Vecs[1] = testutil.NewInt64Vector(
		4, types.T_int64.ToType(), mp, false, nil, []int64{1, 1, 1, 1},
	)
	bat.Vecs[2] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, true, false, false},
	)
	bat.Vecs[3] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, false, true, false},
	)
	bat.Vecs[4] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, true, true, false},
	)
	bat.Vecs[5] = testutil.NewInt32Vector(
		4, types.T_int32.ToType(), mp, false, nil, []int32{10, 20, 30, 40},
	)
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	filtered, clean, semanticAffectedRows, err := filterTargetRows(proc, &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 42},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 5, 1, 4},
		AffectedRowsCols:   []int{2, 3},
	}, bat, nil)
	require.NoError(t, err)
	require.True(t, clean)
	defer filtered.Clean(mp)
	require.Equal(t, []int32{10, 20, 30}, vector.MustFixedColWithTypeCheck[int32](filtered.Vecs[5]))
	require.Equal(t, uint64(4), semanticAffectedRows)
}

func TestFilterTargetRowsKeepsSelfCascadeRowsWithoutCountingThem(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	bat := batch.NewWithSize(5)
	bat.Vecs[0] = testutil.MakeRowIdVector([]types.Rowid{
		types.BuildTestRowid(1, 1),
		types.BuildTestRowid(1, 2),
		types.BuildTestRowid(2, 1),
	}, nil, mp)
	bat.Vecs[1] = testutil.NewInt64Vector(
		3, types.T_int64.ToType(), mp, false, nil, []int64{1, 1, 1})
	bat.Vecs[2] = testutil.NewBoolVector(
		3, types.T_bool.ToType(), mp, false, nil, []bool{true, true, false})
	bat.Vecs[3] = testutil.NewBoolVector(
		3, types.T_bool.ToType(), mp, false, nil, []bool{true, false, false})
	bat.Vecs[4] = testutil.NewInt32Vector(
		3, types.T_int32.ToType(), mp, false, nil, []int32{10, 20, 30})
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	updateCtx := &MultiUpdateCtx{
		TableDef:           &plan.TableDef{TblId: 42},
		DedupByTargetRowID: true,
		DeleteCols:         []int{0, 4, 1, 2},
		AffectedRowsCols:   []int{3},
	}
	filtered, clean, semanticAffectedRows, err := filterTargetRows(proc, updateCtx, bat, nil)
	require.NoError(t, err)
	require.True(t, clean)
	defer filtered.Clean(mp)
	require.Equal(t, []int32{10, 20}, vector.MustFixedColWithTypeCheck[int32](filtered.Vecs[4]))
	require.Equal(t, uint64(1), semanticAffectedRows)
	require.Zero(t, physicalInsertAffectedRows(updateCtx, uint64(filtered.RowCount())))
}

func TestFilterTargetRowsCountsSelectorsWithoutDroppingCascadeRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.NewInt32Vector(
		4, types.T_int32.ToType(), mp, false, nil, []int32{10, 20, 30, 40},
	)
	bat.Vecs[1] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, false, true, false},
	)
	bat.Vecs[2] = testutil.NewBoolVector(
		4, types.T_bool.ToType(), mp, false, nil, []bool{true, true, false, true},
	)
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	changedRowsCol := 2
	updateCtx := &MultiUpdateCtx{
		AffectedRowsCols: []int{1},
		ChangedRowsCol:   &changedRowsCol,
	}
	filtered, clean, semanticAffectedRows, err := filterTargetRows(proc, updateCtx, bat, nil)
	require.NoError(t, err)
	require.False(t, clean)
	require.Same(t, bat, filtered)
	require.Equal(t, 4, filtered.RowCount(), "cascade rows remain in the physical write batch")
	require.Equal(t, uint64(1), semanticAffectedRows,
		"only explicit roots whose final row image changed contribute")
	require.Zero(t, physicalInsertAffectedRows(updateCtx, uint64(filtered.RowCount())))
}

func TestPhysicalInsertAffectedRowsPreservesDeduplicatedTargetRows(t *testing.T) {
	updateCtx := &MultiUpdateCtx{
		DedupByTargetRowID: true,
		AffectedRowsCols:   []int{2, 3},
	}

	require.Zero(t, physicalInsertAffectedRows(updateCtx, 2),
		"semantic selectors own affected-row accounting independently of physical writes")
}

func TestS3WriterRefreshSelectorState(t *testing.T) {
	update := &MultiUpdate{
		ctr: container{seenTargetRows: map[uint64]*hashmap.StrHashMap{}},
	}
	oldSeen, err := hashmap.NewStrHashMap(false, mpool.MustNewZero())
	require.NoError(t, err)
	defer oldSeen.Free()
	writer := &s3WriterDelegate{seenTargetRows: map[uint64]*hashmap.StrHashMap{1: oldSeen}}
	update.ctr.s3Writer = writer

	newSeen, err := hashmap.NewStrHashMap(false, mpool.MustNewZero())
	require.NoError(t, err)
	defer newSeen.Free()
	update.ctr.seenTargetRows = map[uint64]*hashmap.StrHashMap{1: newSeen}
	update.addAffectedRowsFunc = update.doAddAffectedRows
	writer.refreshSelectorState(update)

	require.Same(t, newSeen, writer.seenTargetRows[1])
	require.NotSame(t, oldSeen, writer.seenTargetRows[1])
	require.NotNil(t, writer.admitSeenGrowth)
	require.NotNil(t, writer.addAffectedRows)
}

func TestUpdateCtxKeySeparatesCrossDatabaseSameNameTables(t *testing.T) {
	left := &MultiUpdateCtx{
		ObjRef:   &plan.ObjectRef{Db: 1, Obj: 10, SchemaName: "db_a", ObjName: "t"},
		TableDef: &plan.TableDef{TblId: 100, DbName: "db_a", Name: "t"},
	}
	right := &MultiUpdateCtx{
		ObjRef:   &plan.ObjectRef{Db: 2, Obj: 20, SchemaName: "db_b", ObjName: "t"},
		TableDef: &plan.TableDef{TblId: 200, DbName: "db_b", Name: "t"},
	}
	require.NotEqual(t, updateCtxKey(left), updateCtxKey(right))

	leftInfo := &updateCtxInfo{}
	rightInfo := &updateCtxInfo{}
	infos := map[string]*updateCtxInfo{
		updateCtxKey(left):  leftInfo,
		updateCtxKey(right): rightInfo,
	}
	require.Same(t, leftInfo, lookupUpdateCtxInfo(infos, left))
	require.Same(t, rightInfo, lookupUpdateCtxInfo(infos, right))
}

type testSeenRowsThrottler struct {
	available int64
	acquired  int64
	released  int64
}

func (m *testSeenRowsThrottler) Refresh()    {}
func (m *testSeenRowsThrottler) PrintUsage() {}
func (m *testSeenRowsThrottler) Available() int64 {
	return m.available
}
func (m *testSeenRowsThrottler) Acquire(size int64) (int64, bool) {
	if size > m.available {
		return m.available, false
	}
	m.available -= size
	m.acquired += size
	return m.available, true
}
func (m *testSeenRowsThrottler) Release(size int64) int64 {
	m.available += size
	m.released += size
	return m.available
}

func TestSeenTargetRowsGrowthUsesS3MemoryAdmission(t *testing.T) {
	throttler := &testSeenRowsThrottler{available: 128}
	update := &MultiUpdate{}
	update.ctr.seenRowsRSC = throttler

	require.NoError(t, update.admitSeenTargetRowsGrowth(96))
	require.Equal(t, int64(96), update.ctr.seenRowsGrant)
	require.Equal(t, int64(96), throttler.acquired)
	require.Error(t, update.admitSeenTargetRowsGrowth(64))
	require.Equal(t, int64(96), update.ctr.seenRowsGrant)

	update.freeSeenTargetRows()
	require.Equal(t, int64(96), throttler.released)
	require.Zero(t, update.ctr.seenRowsGrant)
	require.Nil(t, update.ctr.seenRowsRSC)
}

func TestRetainedS3InputColsCountsEveryContextCopy(t *testing.T) {
	updateCtxs := []*MultiUpdateCtx{
		{
			InsertCols: []int{0, 1, 2},
			DeleteCols: []int{3, 4, 9},
		},
		{
			InsertCols: []int{0, 5},
			DeleteCols: []int{6, 7},
		},
		{
			InsertCols: []int{8, 2},
			DeleteCols: []int{10, 11},
		},
	}

	require.Equal(
		t,
		[]int{0, 1, 2, 3, 4, 0, 5, 6, 7, 8, 2, 10, 11},
		retainedS3InputCols(updateCtxs, actionUpdate),
	)
	require.Equal(
		t,
		[]int{0, 1, 2, 0, 5, 8, 2},
		retainedS3InputCols(updateCtxs, actionInsert),
	)
	require.Equal(
		t,
		[]int{3, 4, 6, 7, 10, 11},
		retainedS3InputCols(updateCtxs, actionDelete),
	)
}

func TestS3WriterActionDoesNotRequireMainTableContext(t *testing.T) {
	tests := []struct {
		name       string
		updateCtxs []*MultiUpdateCtx
		want       actionType
	}{
		{
			name: "index backfill insert",
			updateCtxs: []*MultiUpdateCtx{
				{InsertCols: []int{0, 1}},
				{InsertCols: []int{2, 3}},
			},
			want: actionInsert,
		},
		{
			name:       "delete only",
			updateCtxs: []*MultiUpdateCtx{{DeleteCols: []int{0, 1}}},
			want:       actionDelete,
		},
		{
			name: "mixed contexts",
			updateCtxs: []*MultiUpdateCtx{
				{InsertCols: []int{0, 1}},
				{DeleteCols: []int{2, 3}},
			},
			want: actionUpdate,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, s3WriterAction(test.updateCtxs))
		})
	}
}

func TestNewS3WriterAllowsIndexOnlyContext(t *testing.T) {
	_, _, proc := prepareTestCtx(t, false)
	objRef, tableDef := getTestSecondaryIndexTable("index_backfill")
	updateCtx := &MultiUpdateCtx{
		ObjRef:     objRef,
		TableDef:   tableDef,
		InsertCols: []int{0, 1},
	}
	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{updateCtx}}
	update.resetMultiUpdateCtxs()

	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	require.Equal(t, actionInsert, writer.action)
	require.Equal(t, InsertWriteS3Threshold, writer.flushThreshold)
	require.Equal(t, []int{0, 1}, writer.checkSizeCols)
	require.NoError(t, writer.free(proc))
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
func buildUpdateTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool, relResetExpectErr bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl, relResetExpectErr)

	batchs, affectRows := prepareUpdateTestBatchs(proc.GetMPool(), 3, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteTable
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, action, relResetExpectErr)
	return proc, retCase
}

func buildUpdateS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl, false)

	batchs, _ := prepareUpdateTestBatchs(proc.GetMPool(), 10, hasUniqueKey, hasSecondaryKey)
	multiUpdateCtxs := prepareTestUpdateMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteS3
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, 0, action, false)
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
		columnA := testutil.MakeInt64Vector(rows, nil, mp)
		columnB := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil, nil)
		columnC := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
		columnD := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
		columnRowID := makeTestRowIDVector(mp, &mainObjectID, uint16(i), rowCount)

		attrs := []string{"a", "b", "c", "d", catalog.Row_ID}
		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnA, columnB, columnC, columnD, columnRowID},
			Attrs: attrs,
		}

		if hasUniqueKey {
			columnRowID := makeTestRowIDVector(mp, &uniqueObjectID, uint16(i), rowCount)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil, nil)
			bat.Vecs = append(bat.Vecs, columnRowID)
			bat.Vecs = append(bat.Vecs, columnOldPk)
			bat.Vecs = append(bat.Vecs, columnNewPk)
			bat.Attrs = append(bat.Attrs, "uk_rowid", "uk_old_pk", "uk_new_pk")
		}

		if hasSecondaryKey {
			columnRowID := makeTestRowIDVector(mp, &secondaryObjectID, uint16(i), rowCount)
			columnOldPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil, nil)
			columnNewPk := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, true, nil, nil)
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
