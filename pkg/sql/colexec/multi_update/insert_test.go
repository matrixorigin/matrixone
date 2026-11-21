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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestInsertSimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCase(t, proc, case1)
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
	columnA := testutil.MakeInt64Vector(rows, []uint64{0, 1, 2}, proc.GetMPool())
	oldBat.Vecs[0].Free(proc.GetMPool())
	oldBat.ReplaceVector(oldBat.Vecs[0], columnA, 0)
	case1.expectErr = true
	runTestCase(t, proc, case1)

	//case 2: unique key has null, that will be ok
	hasUniqueKey = true
	hasSecondaryKey = false

	proc, case1 = buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	oldBat = case1.inputBatchs[0]
	bat1RowCount = oldBat.RowCount()
	pkArr := makeTestVarcharArray(bat1RowCount)
	uniquePk := testutil.MakeVarcharVector(pkArr, []uint64{0, 1, 2}, proc.GetMPool())
	oldBat.Vecs[4].Free(proc.GetMPool())
	oldBat.ReplaceVector(oldBat.Vecs[4], uniquePk, 0)
	runTestCase(t, proc, case1)
}

func TestInsertTableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true

	proc, case1 := buildInsertTestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCaseAfterReset(t, proc, case1)
}

// insert s3
func TestInsertS3SimpleTable(t *testing.T) {
	hasUniqueKey := false
	hasSecondaryKey := false

	proc, case1 := buildInsertS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCase(t, proc, case1)
}

func TestInsertS3TableWithUniqueKeyAndSecondaryKey(t *testing.T) {
	hasUniqueKey := true
	hasSecondaryKey := true

	proc, case1 := buildInsertS3TestCase(t, hasUniqueKey, hasSecondaryKey)
	runTestCaseAfterReset(t, proc, case1)
}

func TestInsertRejectsZeroTemporalInStrictMode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*testing.T, bool, bool) (*process.Process, *testCase)
	}{
		{name: "write table", build: buildInsertTestCase},
		{name: "write s3", build: buildInsertS3TestCase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, c := tc.build(t, false, false)
			c.op.RejectZeroTemporal = true
			c.op.MultiUpdateCtx[0].TableDef.Cols[3].Typ = plan.Type{Id: int32(types.T_date)}
			for _, bat := range c.inputBatchs {
				zeroDates := vector.NewVec(types.T_date.ToType())
				for i := 0; i < bat.RowCount(); i++ {
					require.NoError(t, vector.AppendFixed(zeroDates, types.ZeroDate, false, proc.Mp()))
				}
				bat.Vecs[3].Free(proc.Mp())
				bat.Vecs[3] = zeroDates
			}
			c.expectErr = true
			runTestCase(t, proc, c)
		})
	}
}

func TestCheckTemporalWriteRejectsTimestampBelowMinimum(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	for _, tc := range []struct {
		name    string
		value   types.Timestamp
		wantErr bool
	}{
		{name: "below minimum", value: types.TimestampMinValue - 1, wantErr: true},
		{name: "exact minimum", value: types.TimestampMinValue},
		{name: "zero follows zero temporal policy", value: types.ZeroTimestamp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixed(vec, tc.value, false, proc.Mp()))
			bat := &batch.Batch{Vecs: []*vector.Vector{vec}}
			bat.SetRowCount(1)
			defer bat.Clean(proc.Mp())

			err := checkZeroTemporalInStrictMode(false, proc, bat)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestInsertRejectsTimestampBelowMinimum(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*testing.T, bool, bool) (*process.Process, *testCase)
	}{
		{name: "write table", build: buildInsertTestCase},
		{name: "write s3", build: buildInsertS3TestCase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, c := tc.build(t, false, false)
			c.op.MultiUpdateCtx[0].TableDef.Cols[3].Typ = plan.Type{Id: int32(types.T_timestamp)}
			for _, bat := range c.inputBatchs {
				timestamps := vector.NewVec(types.T_timestamp.ToType())
				for i := 0; i < bat.RowCount(); i++ {
					require.NoError(t, vector.AppendFixed(timestamps, types.TimestampMinValue-1, false, proc.Mp()))
				}
				bat.Vecs[3].Free(proc.Mp())
				bat.Vecs[3] = timestamps
			}
			c.expectErr = true
			runTestCase(t, proc, c)
		})
	}
}

func TestMultiUpdatePreparePreservesCompiledZeroTemporalPolicy(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, false)
	defer ctrl.Finish()
	defer proc.Free()
	op := &MultiUpdate{
		MultiUpdateCtx:     prepareTestInsertMultiUpdateCtx(false, false),
		Action:             UpdateWriteTable,
		Engine:             prepareTestEng(ctrl, false),
		RejectZeroTemporal: true,
	}
	require.NoError(t, op.Prepare(proc))
	require.True(t, op.RejectZeroTemporal)
}

func TestMultiUpdateSetRejectZeroTemporalUpdatesCachedS3Writer(t *testing.T) {
	op := &MultiUpdate{
		RejectZeroTemporal: true,
		ctr: container{
			s3Writer: &s3WriterDelegate{rejectZeroTemporal: true},
		},
	}
	op.SetRejectZeroTemporal(false)
	require.False(t, op.RejectZeroTemporal)
	require.False(t, op.ctr.s3Writer.rejectZeroTemporal)
}

// ----- util function ----
func buildInsertTestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, false)
	eng := prepareTestEng(ctrl, false)

	batchs, affectRows := prepareTestInsertBatchs(
		proc.GetMPool(), 2, colexec.DefaultBatchSize, hasUniqueKey, hasSecondaryKey,
	)
	multiUpdateCtxs := prepareTestInsertMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteTable
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, affectRows, action, false)
	return proc, retCase
}

func buildInsertS3TestCase(t *testing.T, hasUniqueKey bool, hasSecondaryKey bool) (*process.Process, *testCase) {
	_, ctrl, proc := prepareTestCtx(t, true)
	eng := prepareTestEng(ctrl, false)

	batchs, _ := prepareTestInsertBatchs(
		proc.GetMPool(), testS3BatchCount, testS3BatchRows, hasUniqueKey, hasSecondaryKey,
	)
	multiUpdateCtxs := prepareTestInsertMultiUpdateCtx(hasUniqueKey, hasSecondaryKey)
	action := UpdateWriteS3
	retCase := buildTestCase(multiUpdateCtxs, eng, batchs, 0, action, false)
	return proc, retCase
}

func prepareTestInsertBatchs(
	mp *mpool.MPool,
	size int,
	rowsPerBatch int,
	hasUniqueKey bool,
	hasSecondaryKey bool,
) ([]*batch.Batch, uint64) {
	var bats = make([]*batch.Batch, size)
	affectRows := 0
	for i := 0; i < size; i++ {
		rowCount := rowsPerBatch
		if i == size-1 {
			rowCount = rowCount / 2
		}
		rows := makeTestPkArray(int64(affectRows), rowCount)
		columnA := testutil.MakeInt64Vector(rows, nil, mp)
		columnB := testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil, nil)
		columnC := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)
		columnD := testutil.NewInt32Vector(rowCount, types.T_int32.ToType(), mp, false, nil, nil)

		attrs := []string{"a", "b", "c", "d"}
		bat := &batch.Batch{
			Vecs:  []*vector.Vector{columnA, columnB, columnC, columnD},
			Attrs: attrs,
		}

		if hasUniqueKey {
			bat.Vecs = append(bat.Vecs, testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil, nil))
			bat.Attrs = append(bat.Attrs, "uk_pk")
		}

		if hasSecondaryKey {
			bat.Vecs = append(bat.Vecs, testutil.NewStringVector(rowCount, types.T_varchar.ToType(), mp, false, nil, nil))
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

func TestMainTableWriteObservationScopeAndReset(t *testing.T) {
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	ctx := context.Background()
	point := "fj/sql/main_table_write/observer_db/observer_table"
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "echo", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, "observer_count", ":::", "getcount", 0, point, false))
	proc := testutil.NewProcess(t)
	observeMainTableWrite(proc, &MultiUpdateCtx{ObjRef: &planpb.ObjectRef{SchemaName: "other_db", ObjName: "observer_table"}})
	count, _, ok := fault.TriggerFault("observer_count")
	require.True(t, ok)
	require.Zero(t, count)
	observeMainTableWrite(proc, &MultiUpdateCtx{ObjRef: &planpb.ObjectRef{SchemaName: "observer_db", ObjName: "observer_table"}})
	count, _, _ = fault.TriggerFault("observer_count")
	require.Equal(t, int64(1), count)
	_, err := fault.RemoveFaultPoint(ctx, point)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "echo", 0, "", false))
	count, _, _ = fault.TriggerFault("observer_count")
	require.Zero(t, count)
	// A deliberately blocking observation must respect statement cancellation.
	_, err = fault.RemoveFaultPoint(ctx, point)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "wait", 0, "", false))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	proc.Ctx = cancelled
	observeMainTableWrite(proc, &MultiUpdateCtx{ObjRef: &planpb.ObjectRef{SchemaName: "observer_db", ObjName: "observer_table"}})
	require.NoError(t, fault.AddFaultPoint(ctx, "observer_waiters", ":::", "getwaiters", 0, point, false))
	waiters, _, ok := fault.TriggerFault("observer_waiters")
	require.True(t, ok)
	require.Zero(t, waiters, "cancelled statements must not retain an observation waiter")
	require.True(t, fault.Disable())
	observeMainTableWrite(proc, &MultiUpdateCtx{})
}

func TestMainTableWriteEnteredWaitCancellationAndNewGeneration(t *testing.T) {
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	ctx := context.Background()
	point := "fj/sql/main_table_write/entered_wait/table"
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "wait", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, "entered_wait_waiters", ":::", "getwaiters", 0, point, false))
	require.NoError(t, fault.AddFaultPoint(ctx, "entered_wait_count", ":::", "getcount", 0, point, false))
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(ctx)
	t.Cleanup(func() { proc.Cancel(context.Canceled) })
	updateCtx := &MultiUpdateCtx{ObjRef: &planpb.ObjectRef{SchemaName: "entered_wait", ObjName: "table"}}
	done := make(chan struct{})
	go func() {
		observeMainTableWrite(proc, updateCtx)
		close(done)
	}()
	// Registration is the schedule barrier. Elapsed time alone is not proof
	// that the observation entered WAIT.
	require.Eventually(t, func() bool {
		waiters, _, ok := fault.TriggerFault("entered_wait_waiters")
		return ok && waiters == 1
	}, 10*time.Second, time.Millisecond)
	proc.Cancel(context.Canceled)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("entered observation did not return after pipeline cancellation")
	}
	waiters, _, ok := fault.TriggerFault("entered_wait_waiters")
	require.True(t, ok)
	require.Zero(t, waiters)
	_, err := fault.RemoveFaultPoint(ctx, point)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "echo", 0, "", false))
	count, _, ok := fault.TriggerFault("entered_wait_count")
	require.True(t, ok)
	require.Zero(t, count)
	proc.BuildPipelineContext(ctx)
	observeMainTableWrite(proc, updateCtx)
	count, _, _ = fault.TriggerFault("entered_wait_count")
	require.Equal(t, int64(1), count)
	waiters, _, _ = fault.TriggerFault("entered_wait_waiters")
	require.Zero(t, waiters)
}
