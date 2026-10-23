// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package multi_update

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCommittedDeleteZeroDataCreditsUsesBoundedWorkspace(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	const unit = 16 << 10
	ioBytes, err := ioutil.IOEnvelope([][]types.Type{{types.T_Rowid.ToType(), types.T_int64.ToType()}}, unit, types.RowidSize+8)
	require.NoError(t, err)
	outputBytes, err := OutputEnvelope(unit, types.RowidSize+8, 1)
	require.NoError(t, err)
	owner := rscthrottler.NewMemThrottler("delete-working-set", 1, rscthrottler.WithConstLimit(1<<30))
	ws, err := rscthrottler.ReserveWorkingSet(proc.Ctx, owner, rscthrottler.WorkingSetSpec{
		UnitBytes: unit,
		Inputs:    []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 4 * unit}, {Name: "output", Count: 1, Bytes: outputBytes}},
		IOBytes:   ioBytes,
	})
	require.NoError(t, err)
	t.Cleanup(ws.Close)
	proc.Base.ImportMemory = &process.ImportMemory{WorkingSet: ws, UnitBytes: unit, SourceBytes: 4 * unit, OutputBytes: outputBytes, IOBytes: ioBytes}
	contexts := prepareTestUpdateMultiUpdateCtx(false, false)
	contexts[0].InsertCols = nil
	contexts[0].DeleteCols = []int{0, 1}
	contexts[0].TableDef.TblId = 99
	tc := buildTestCase(contexts, prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
	t.Cleanup(func() { tc.op.Free(proc, false, nil) })
	require.NoError(t, tc.op.Prepare(proc))
	w := tc.op.ctr.s3Writer
	deny := &admissionThrottler{}
	deny.beforeWait = func(*admissionThrottler) { t.Fatal("W must progress without waiting for D") }
	w.memController.throttler = deny
	var observed int
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		observed++
		require.NotNil(t, w.activeWorkspace)
		var actual int64
		for _, block := range w.deleteBlockMap[0] {
			actual += int64(nulls.Allocated(block.bitmap)) + deleteBlockObjectsBytes
		}
		require.LessOrEqual(t, actual, int64(unit), "distinct-block bookkeeping is bounded by the materialization unit")
		actual += w.activeMP.AllocatedBytes()
		require.LessOrEqual(t, actual, w.activeWorkspace.Capacity())
		return txn.TxnMeta{ID: []byte{1}}
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp
	require.NoError(t, w.append(proc, tc.op.OpAnalyzer, committedDeleteInput(t, proc, 1, 32)))
	require.Equal(t, 32, observed)
	require.Zero(t, deny.granted)
	require.Zero(t, w.chargedBytes())
	var rows uint64
	for {
		require.NoError(t, w.flushTailAndWriteToOutput(proc, tc.op.OpAnalyzer))
		if w.outputBat == nil {
			break
		}
		for _, count := range vector.MustFixedColNoTypeCheck[uint64](w.outputBat.Vecs[2]) {
			rows += count
		}
	}
	require.Equal(t, uint64(32), rows)
	require.NoError(t, w.reset(proc))
	lease, err := ws.AcquireIO(proc.Ctx, ioBytes)
	require.NoError(t, err)
	lease.Release()
}

func TestDeleteAppendPredictionCoversExistingVarlenaTailReplacement(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	credits := &admissionThrottler{capacity: 1 << 30}
	w := deleteContractWriter(credits)
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	w.deleteBatches[0] = batch.NewBatchSet(8192)
	// One large old area followed by tiny appends forces replacement of an
	// existing tail. Admission must reserve the complete new area, not merely
	// multiply the tiny input's from-zero allocation.
	for row := 0; row < 16; row++ {
		input := committedDeleteInput(t, proc, row, 1)
		input.Vecs[1].Free(proc.Mp())
		value := strings.Repeat("z", 17)
		if row == 0 {
			value = strings.Repeat("x", 8<<10)
		}
		input.Vecs[1] = testutil.MakeVarcharVector([]string{value}, nil, proc.Mp())
		ask := w.deleteAppendAllocation(0, input, []int{0, 1})
		before := w.memoryPool(proc).AllocatedBytes()
		_, err := w.deleteBatches[0].Extend(w.memoryPool(proc), input, nil)
		require.NoError(t, err)
		actual := w.memoryPool(proc).AllocatedBytes() - before
		require.LessOrEqual(t, actual, ask)
	}
}

func TestDeleteFutureCreditIsLinearInNewWindow(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	input := committedDeleteInput(t, proc, 1, 32)
	whole := deletePreparationBytes(input, 0, 32, []int{0, 1})
	var windows int64
	for row := 0; row < 32; row++ {
		windows += deletePreparationBytes(input, row, row+1, []int{0, 1})
	}
	// Capacity rounding may add one small quantum per column/window, but a
	// tiny append cannot reserve every old row or old vector capacity again.
	require.LessOrEqual(t, windows, whole+32*64)
}
