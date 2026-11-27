// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
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
	ws, err := owner.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(proc.Ctx, []rscthrottler.WorkingSetSpec{{
		UnitBytes: unit,
		Inputs:    []rscthrottler.InputSlotSpec{{Name: "csv", Count: 1, Bytes: 4 * unit}, {Name: "output", Count: 1, Bytes: outputBytes}},
		IOBytes:   ioBytes,
	}})
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

func TestDeletePreparationCreditsFollowBlockRuns(t *testing.T) {
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	const rows = 32
	b := committedDeleteInput(t, proc, 1, rows)
	cols := []int{0, 1}
	scattered := deletePreparationBytes(b, 0, rows, cols)
	block := types.BuildTestBlockid(1, 9)
	ids := vector.MustFixedColWithTypeCheck[types.Rowid](b.Vecs[0])
	for i := range ids {
		ids[i] = types.NewRowid(&block, uint32(i))
	}
	dense := deletePreparationBytes(b, 0, rows, cols)
	perBlock := int64(options.DefaultBlockMaxRows/8) + deleteBlockObjectsBytes
	require.Equal(t, (rows-1)*perBlock, scattered-dense)
	other := types.BuildTestBlockid(1, 11)
	ids[7] = types.NewRowid(&other, 0)
	b.Vecs[0].GetNulls().Add(7)
	require.Equal(t, dense+2*perBlock, deletePreparationBytes(b, 0, rows, cols))
	require.Greater(t, deletePreparationBytes(b, 0, 8, cols)+deletePreparationBytes(b, 8, rows, cols), dense)
	require.Zero(t, deletePreparationBytes(b, 8, 8, cols))
	b.Vecs[0].Free(proc.Mp())
	var err error
	b.Vecs[0], err = vector.NewConstFixed(types.T_Rowid.ToType(), types.NewRowid(&block, 0), rows, proc.Mp())
	require.NoError(t, err)
	require.Equal(t, dense, deletePreparationBytes(b, 0, rows, cols))
	b.Vecs[0].Free(proc.Mp())
	b.Vecs[0] = vector.NewConstNull(types.T_Rowid.ToType(), rows, proc.Mp())
	require.Zero(t, deletePreparationBytes(b, 3, rows, cols))
}

func TestDenseDeletePreparationStaysAdmitted(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	credits := &admissionThrottler{capacity: 1 << 30}
	w := deleteContractWriter(credits)
	w.action = actionDelete
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	input := committedDeleteInput(t, proc, 1, 512)
	block := types.BuildTestBlockid(1, 9)
	ids := vector.MustFixedColWithTypeCheck[types.Rowid](input.Vecs[0])
	for i := range ids {
		ids[i] = types.NewRowid(&block, uint32(i/2))
	}
	input.Vecs[0].GetNulls().Add(0, 1)
	require.NoError(t, w.append(proc, process.NewTempAnalyzer(), input))
	require.Equal(t, w.chargedBytes(), credits.reserved)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		require.LessOrEqual(t, w.retainedBytes(), credits.reserved)
		return txn.TxnMeta{ID: []byte{1}}
	}).Times(510)
	proc.Base.TxnOperator = op
	acquires := credits.acquires
	bats, err := w.prepareDeleteBatches(proc, 0, w.deleteBatches[0].TakeBatches(), true)
	defer func() {
		for _, b := range bats {
			b.Clean(w.memoryPool(proc))
		}
	}()
	require.NoError(t, err)
	require.Len(t, bats, 1)
	require.Equal(t, 255, bats[0].RowCount())
	require.Equal(t, acquires, credits.acquires)
	require.LessOrEqual(t, w.retainedBytes(), credits.reserved)
}

func TestUpdateFlushesByProjectedBytes(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	tc := buildTestCase(prepareTestUpdateMultiUpdateCtx(false, false), prepareTestEng(ctrl, false), nil, 0, UpdateWriteS3, false)
	t.Cleanup(func() { tc.op.Free(proc, false, nil) })
	require.NoError(t, tc.op.Prepare(proc))
	proc.Base.TxnOperator = nil
	w := tc.op.ctr.s3Writer
	analyzer := tc.op.OpAnalyzer
	credits := &admissionThrottler{capacity: 1 << 30}
	w.memController.throttler = credits
	w.flushThreshold = 100 << 10
	input, total := prepareUpdateTestBatchs(proc.Mp(), 4, false, false)
	for _, b := range input {
		t.Cleanup(func() { b.Clean(proc.Mp()) })
		for start := 0; start < b.RowCount(); start += 512 {
			part, err := b.Window(start, min(start+512, b.RowCount()))
			require.NoError(t, err)
			func() {
				defer part.Clean(proc.Mp())
				require.NoError(t, w.append(proc, analyzer, part))
				if b == input[0] && start == 0 {
					require.Equal(t, uint64(w.projectionBytes(part, 0, part.RowCount())), w.batchSize)
					require.Positive(t, w.deleteFutureBytes[0])
				}
			}()
			require.LessOrEqual(t, w.chargedBytes(), credits.reserved)
		}
	}
	objects := map[actionType]int{}
	rows := map[actionType]uint64{}
	for {
		require.NoError(t, w.flushTailAndWriteToOutput(proc, analyzer))
		if w.outputBat == nil {
			break
		}
		kind := actionType(vector.GetFixedAtNoTypeCheck[uint8](w.outputBat.Vecs[0], 0))
		if kind == actionDelete {
			require.Zero(t, objects[actionInsert])
		}
		func() {
			b := batch.NewOffHeapEmpty()
			defer b.Clean(proc.Mp())
			require.NoError(t, b.UnmarshalBinaryWithAnyMp(w.outputBat.Vecs[4].GetBytesAt(0), proc.Mp()))
			stats := b.Vecs[len(b.Vecs)-1]
			for i := range stats.Length() {
				st := objectio.ObjectStats(stats.GetBytesAt(i))
				objects[kind]++
				rows[kind] += uint64(st.Rows())
			}
		}()
	}
	require.Equal(t, total, rows[actionInsert])
	require.Equal(t, total, rows[actionDelete])
	// Each 512-row projection has 36 KiB. Three windows cross the 100 KiB
	// threshold; EOF flushes any remainder. Verify the persisted object stats.
	wantObjects := int((total + 3*512 - 1) / (3 * 512))
	require.Equal(t, wantObjects, objects[actionDelete])
	require.Equal(t, wantObjects, objects[actionInsert])
	require.Zero(t, credits.reserved)
}
