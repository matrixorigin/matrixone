// Copyright 2026 Matrix Origin
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

package icebergwrite

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestIcebergWriteLifecycleAbortsOpenCoordinator(t *testing.T) {
	coord := &testCoordinator{}
	op := NewArgument(AppendRequest{
		Ref:      &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef: &plan.TableDef{Name: "gold_orders"},
		Attrs:    []string{"id"},
	}).WithCoordinator(coord)
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	proc.Ctx = context.Background()

	require.NoError(t, op.Prepare(proc))
	require.Equal(t, 1, coord.beginCalls)
	op.Reset(proc, true, context.Canceled)
	require.Equal(t, 1, coord.abortCalls)
}

func TestIcebergWritePassesSessionTimeZoneToCoordinatorFactory(t *testing.T) {
	proc := testutil.NewProc(t)
	loc := time.FixedZone("UTC+03", 3*60*60)
	proc.GetSessionInfo().TimeZone = loc
	var got AppendRequest
	op := NewArgument(AppendRequest{Operation: OperationAppend})
	op.Factory = CoordinatorFactoryFunc(func(ctx context.Context, req AppendRequest) (Coordinator, error) {
		got = req
		return &testCoordinator{}, nil
	})

	require.NoError(t, op.Prepare(proc))
	require.Same(t, loc, got.TimeZone)
}

func TestIcebergWriteAbortsCoordinatorWhenBeginFails(t *testing.T) {
	beginErr := errors.New("begin failed")
	coord := &testCoordinator{beginErr: beginErr}
	op := NewArgument(AppendRequest{Operation: OperationAppend}).WithCoordinator(coord)
	proc := testutil.NewProc(t)

	err := op.Prepare(proc)
	require.ErrorIs(t, err, beginErr)
	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, 1, coord.abortCalls)
	require.ErrorIs(t, coord.abortCause, beginErr)

	op.Free(proc, true, err)
	require.Equal(t, 1, coord.abortCalls, "Free must not repeat rollback after a failed Begin")
}

func TestIcebergWriteReleasesRetainedObjectIORef(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-write-object-io", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	ref, err := icebergio.RegisterEphemeralObjectIOProvider(ctx, icebergio.ScopedProvider{FileService: fs}, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			StorageLocation: location,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "writer-test",
		}
	}, 0)
	require.NoError(t, err)
	op := NewArgument(AppendRequest{DMLScan: DMLScanMetadata{ObjectIORef: ref}})
	require.NoError(t, op.RetainObjectIORef(ctx))
	require.NoError(t, op.RetainObjectIORef(ctx), "retaining the same operator ref must be idempotent")
	_, _, err = icebergio.ResolveObjectIORef(ctx, ref, "s3://warehouse/orders/data/a.parquet")
	require.NoError(t, err)

	op.Free(testutil.NewProc(t), false, nil)
	_, _, err = icebergio.ResolveObjectIORef(ctx, ref, "s3://warehouse/orders/data/a.parquet")
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrObjectIO))
	op.Free(testutil.NewProc(t), false, nil)
}

func TestUnsupportedCoordinatorFailsOnDataAndCommit(t *testing.T) {
	coord := unsupportedCoordinator{}
	err := coord.Append(context.Background(), batch.EmptyBatch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data path is not implemented")

	err = coord.Commit(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit path is not implemented")
}

func TestUnsupportedCoordinatorReportsDeleteOperation(t *testing.T) {
	coord := unsupportedCoordinator{operation: OperationDelete}
	err := coord.Append(context.Background(), batch.EmptyBatch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Iceberg DELETE writer data path is not implemented")

	err = coord.Commit(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Iceberg DELETE writer commit path is not implemented")
}

func TestIcebergWriteAppendsMultipleBatchesAndCommitsOnLast(t *testing.T) {
	proc := testutil.NewProc(t)
	coord := &testCoordinator{}
	op := NewArgument(AppendRequest{
		Ref:         &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef:    &plan.TableDef{Name: "gold_orders"},
		Attrs:       []string{"id"},
		Operation:   OperationAppend,
		StatementID: "stmt-1",
	}).WithCoordinator(coord)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		testBatchWithRows(proc, 2),
		batch.EmptyBatch,
		testBatchWithRows(proc, 3),
		lastBatch(),
	}))

	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.True(t, result.Batch.IsEmpty())
	require.Equal(t, vm.ExecNext, result.Status)
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.True(t, result.Batch.IsEmpty())
	require.Equal(t, vm.ExecNext, result.Status)
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.True(t, result.Batch.IsEmpty())
	require.Equal(t, vm.ExecNext, result.Status)
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)

	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, 2, coord.appendCalls)
	require.Equal(t, []int{2, 3}, coord.appendRows)
	require.Equal(t, 1, coord.commitCalls)
	op.Reset(proc, false, nil)
	require.Zero(t, coord.abortCalls)
}

func TestIcebergWriteCommitsOnNilInputEOF(t *testing.T) {
	proc := testutil.NewProc(t)
	coord := &testCoordinator{}
	op := NewArgument(AppendRequest{
		Ref:         &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef:    &plan.TableDef{Name: "gold_orders"},
		Attrs:       []string{"id"},
		Operation:   OperationAppend,
		StatementID: "stmt-nil-eof",
	}).WithCoordinator(coord)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		testBatchWithRows(proc, 2),
	}))

	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.True(t, result.Batch.IsEmpty())
	result, err = op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)

	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, []int{2}, coord.appendRows)
	require.Equal(t, 1, coord.commitCalls)
	op.Free(proc, false, nil)
	require.Zero(t, coord.abortCalls)
}

func TestIcebergWriteAppendsNonEmptyLastBatchBeforeCommit(t *testing.T) {
	proc := testutil.NewProc(t)
	coord := &testProcessAwareCoordinator{}
	op := NewArgument(AppendRequest{
		Ref:         &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef:    &plan.TableDef{Name: "gold_orders"},
		Attrs:       []string{"id"},
		Operation:   OperationDelete,
		StatementID: "stmt-last-data",
	}).WithCoordinator(coord)
	lastData := testBatchWithRows(proc, 1)
	lastData.SetLast()
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{lastData}))

	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)

	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, 1, coord.appendWithProcessCalls)
	require.Equal(t, []int{1}, coord.appendRows)
	require.Equal(t, 1, coord.commitCalls)
	op.Free(proc, false, nil)
	require.Zero(t, coord.abortCalls)
}

func TestIcebergWriteCommitsWhenChildStopsWithDataBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	coord := &testCoordinator{}
	op := NewArgument(AppendRequest{
		Ref:         &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef:    &plan.TableDef{Name: "gold_orders"},
		Attrs:       []string{"id"},
		Operation:   OperationAppend,
		StatementID: "stmt-stop-with-data",
	}).WithCoordinator(coord)
	child := &stopWithDataOperator{bat: testBatchWithRows(proc, 10)}
	child.OpAnalyzer = process.NewAnalyzer(0, false, false, "stop-with-data")
	op.AppendChild(child)

	require.NoError(t, op.Prepare(proc))
	result, err := op.Call(proc)
	require.NoError(t, err)
	require.Nil(t, result.Batch)
	require.Equal(t, vm.ExecStop, result.Status)

	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, []int{10}, coord.appendRows)
	require.Equal(t, 1, coord.commitCalls)
	op.Free(proc, false, nil)
	require.Zero(t, coord.abortCalls)
}

func TestIcebergWriteUsesCoordinatorFactoryAndProcessAwareAppend(t *testing.T) {
	proc := testutil.NewProc(t)
	coord := &testProcessAwareCoordinator{}
	var captured AppendRequest
	op := NewArgument(AppendRequest{
		Ref:         &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef:    &plan.TableDef{Name: "gold_orders"},
		Attrs:       []string{"id"},
		Operation:   OperationUpdate,
		StatementID: "stmt-2",
	}).WithCoordinatorFactory(CoordinatorFactoryFunc(func(ctx context.Context, req AppendRequest) (Coordinator, error) {
		captured = req
		return coord, nil
	}))
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		testBatchWithRows(proc, 1),
		lastBatch(),
	}))

	require.NoError(t, op.Prepare(proc))
	_, err := op.Call(proc)
	require.NoError(t, err)
	_, err = op.Call(proc)
	require.NoError(t, err)

	require.Equal(t, "stmt-2", captured.StatementID)
	require.Equal(t, OperationUpdate, captured.Operation)
	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, 0, coord.appendCalls)
	require.Equal(t, 1, coord.appendWithProcessCalls)
	require.Same(t, proc, coord.lastProc)
	require.Equal(t, 1, coord.commitCalls)
}

func TestIcebergWriteAbortsOpenCoordinatorAfterAppendFailure(t *testing.T) {
	proc := testutil.NewProc(t)
	appendErr := errors.New("append failed")
	coord := &testCoordinator{appendErr: appendErr}
	op := NewArgument(AppendRequest{
		Ref:      &plan.ObjectRef{ObjName: "gold_orders"},
		TableDef: &plan.TableDef{Name: "gold_orders"},
		Attrs:    []string{"id"},
	}).WithCoordinator(coord)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{testBatchWithRows(proc, 1)}))

	require.NoError(t, op.Prepare(proc))
	_, err := op.Call(proc)
	require.ErrorIs(t, err, appendErr)
	op.Free(proc, true, err)

	require.Equal(t, 1, coord.beginCalls)
	require.Equal(t, 1, coord.appendCalls)
	require.Zero(t, coord.commitCalls)
	require.Equal(t, 1, coord.abortCalls)
	require.ErrorIs(t, coord.abortCause, appendErr)
}

func TestUnsupportedCoordinatorOperationMessages(t *testing.T) {
	cases := []struct {
		operation  string
		wantData   string
		wantCommit string
	}{
		{OperationAppend, "append writer data path", "append writer commit path"},
		{OperationDelete, "DELETE writer data path", "DELETE writer commit path"},
		{OperationUpdate, "UPDATE writer data path", "UPDATE writer commit path"},
		{OperationMerge, "MERGE writer data path", "MERGE writer commit path"},
		{OperationOverwrite, "OVERWRITE writer data path", "OVERWRITE writer commit path"},
	}
	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			coord := unsupportedCoordinator{operation: tc.operation}
			err := coord.Append(context.Background(), batch.EmptyBatch)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantData)
			err = coord.Commit(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantCommit)
		})
	}
}

type testCoordinator struct {
	beginCalls  int
	appendCalls int
	commitCalls int
	abortCalls  int
	appendRows  []int
	appendErr   error
	commitErr   error
	abortErr    error
	abortCause  error
	beginErr    error
}

func (c *testCoordinator) Begin(ctx context.Context, req AppendRequest) error {
	c.beginCalls++
	return c.beginErr
}

func (c *testCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	c.appendCalls++
	c.appendRows = append(c.appendRows, bat.RowCount())
	return c.appendErr
}

func (c *testCoordinator) Commit(ctx context.Context) error {
	c.commitCalls++
	return c.commitErr
}

func (c *testCoordinator) Abort(ctx context.Context, cause error) error {
	c.abortCalls++
	c.abortCause = cause
	return c.abortErr
}

type testProcessAwareCoordinator struct {
	testCoordinator
	appendWithProcessCalls int
	lastProc               *process.Process
}

func (c *testProcessAwareCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	c.appendWithProcessCalls++
	c.lastProc = proc
	c.appendRows = append(c.appendRows, bat.RowCount())
	return c.appendErr
}

func testBatchWithRows(proc *process.Process, rows int) *batch.Batch {
	bat := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_int64.ToType()})
	for i := 0; i < rows; i++ {
		if err := vector.AppendFixed[int64](bat.Vecs[0], int64(i+1), false, proc.Mp()); err != nil {
			panic(err)
		}
	}
	bat.SetRowCount(rows)
	return bat
}

func lastBatch() *batch.Batch {
	bat := batch.NewWithSchema(false, []string{"id"}, []types.Type{types.T_int64.ToType()})
	bat.SetLast()
	return bat
}

type stopWithDataOperator struct {
	bat  *batch.Batch
	done bool
	vm.OperatorBase
}

func (op *stopWithDataOperator) String(buf *bytes.Buffer) {
	buf.WriteString("stop-with-data")
}

func (op *stopWithDataOperator) OpType() vm.OpType {
	return vm.Mock
}

func (op *stopWithDataOperator) GetOperatorBase() *vm.OperatorBase {
	return &op.OperatorBase
}

func (op *stopWithDataOperator) Prepare(proc *process.Process) error {
	if op.OpAnalyzer == nil {
		op.OpAnalyzer = process.NewAnalyzer(0, false, false, "stop-with-data")
	} else {
		op.OpAnalyzer.Reset()
	}
	return nil
}

func (op *stopWithDataOperator) Call(proc *process.Process) (vm.CallResult, error) {
	if op.done {
		return vm.CancelResult, nil
	}
	op.done = true
	return vm.CallResult{Status: vm.ExecStop, Batch: op.bat}, nil
}

func (op *stopWithDataOperator) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (op *stopWithDataOperator) Reset(proc *process.Process, pipelineFailed bool, err error) {}

func (op *stopWithDataOperator) Free(proc *process.Process, pipelineFailed bool, err error) {}

func (op *stopWithDataOperator) Release() {}
