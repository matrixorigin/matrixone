// Copyright 2021 Matrix Origin
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

package mergeorder

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type orderTestCase struct {
	arg   *MergeOrder
	types []types.Type
	proc  *process.Process
}

type int8Row struct {
	value  int8
	isNull bool
}

type pairRow struct {
	first  int8
	second int64
}

type countingColumnExecutor struct {
	col      int
	maxCalls int
	calls    int
}

func (e *countingColumnExecutor) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return e.EvalWithoutResultReusing(nil, batches, nil)
}

func (e *countingColumnExecutor) EvalWithoutResultReusing(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls > e.maxCalls {
		return nil, moerr.NewInternalErrorNoCtx("order executor reevaluated spilled keys")
	}
	return batches[0].Vecs[e.col], nil
}

func (e *countingColumnExecutor) ResetForNextQuery() {}
func (e *countingColumnExecutor) Free()              {}
func (e *countingColumnExecutor) IsColumnExpr() bool { return true }
func (e *countingColumnExecutor) TypeName() string   { return "countingColumnExecutor" }

type negatingInt8Executor struct {
	col   int
	calls int
}

func (e *negatingInt8Executor) Eval(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return e.EvalWithoutResultReusing(proc, batches, nil)
}

func (e *negatingInt8Executor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	e.calls++
	src := vector.MustFixedColWithTypeCheck[int8](batches[0].Vecs[e.col])
	values := make([]int8, len(src))
	for i := range src {
		values[i] = -src[i]
	}
	return testutil.NewVector(len(values), types.T_int8.ToType(), proc.Mp(), false, values), nil
}

func (e *negatingInt8Executor) ResetForNextQuery() {}
func (e *negatingInt8Executor) Free()              {}
func (e *negatingInt8Executor) IsColumnExpr() bool { return false }
func (e *negatingInt8Executor) TypeName() string   { return "negatingInt8Executor" }

type resetTrackingExecutor struct {
	resetCalls int
}

func (e *resetTrackingExecutor) Eval(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return nil, nil
}

func (e *resetTrackingExecutor) EvalWithoutResultReusing(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return nil, nil
}

func (e *resetTrackingExecutor) ResetForNextQuery() { e.resetCalls++ }
func (e *resetTrackingExecutor) Free()              {}
func (e *resetTrackingExecutor) IsColumnExpr() bool { return true }
func (e *resetTrackingExecutor) TypeName() string   { return "resetTrackingExecutor" }

type failingExecutor struct{}

func (e *failingExecutor) Eval(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("test executor failure")
}

func (e *failingExecutor) EvalWithoutResultReusing(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	return nil, moerr.NewInternalErrorNoCtx("test executor failure")
}

func (e *failingExecutor) ResetForNextQuery() {}
func (e *failingExecutor) Free()              {}
func (e *failingExecutor) IsColumnExpr() bool { return false }
func (e *failingExecutor) TypeName() string   { return "failingExecutor" }

type failWriter struct{}

func (failWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

type cancelAfterWriteWriter struct {
	cancel context.CancelFunc
	writes int
}

func (w *cancelAfterWriteWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.cancel()
	}
	return len(p), nil
}

type callbackEOFReader struct {
	callback func()
	reads    int
}

func (r *callbackEOFReader) Read([]byte) (int, error) {
	r.reads++
	if r.callback != nil {
		r.callback()
	}
	return 0, io.EOF
}

type cancelAfterReadReader struct {
	reader io.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *cancelAfterReadReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.reads++
	if r.reads == 1 {
		r.cancel()
	}
	return n, err
}

// cancelOnDoneCheckContext deterministically cancels on the Nth observation of
// Done. The spill paths are synchronous, so this lets tests select an exact
// phase boundary without sleeps or scheduler races.
type cancelOnDoneCheckContext struct {
	context.Context
	remaining int
	done      chan struct{}
}

func newCancelOnDoneCheckContext(parent context.Context, checks int) *cancelOnDoneCheckContext {
	return &cancelOnDoneCheckContext{
		Context:   parent,
		remaining: checks,
		done:      make(chan struct{}),
	}
}

func (ctx *cancelOnDoneCheckContext) Done() <-chan struct{} {
	if ctx.remaining > 0 {
		ctx.remaining--
		if ctx.remaining == 0 {
			close(ctx.done)
		}
	}
	return ctx.done
}

func (ctx *cancelOnDoneCheckContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

type trackingMutableFileService struct {
	fileservice.MutableFileService
	files []*os.File
}

func (fs *trackingMutableFileService) CreateAndRemoveFile(ctx context.Context, filePath string) (*os.File, error) {
	file, err := fs.MutableFileService.CreateAndRemoveFile(ctx, filePath)
	if err == nil {
		fs.files = append(fs.files, file)
	}
	return file, err
}

func makeTestCases(t *testing.T) []orderTestCase {
	return []orderTestCase{
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(1, types.T_int64), Flag: 0}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		newTestCase(t, []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}, {Expr: newExpression(1, types.T_int64), Flag: 0}}),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOrder(t *testing.T) {
	for tci, tc := range makeTestCases(t) {
		bats := []*batch.Batch{newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs), batch.EmptyBatch, newIntBatch(tc.types, tc.proc, Rows, tc.arg.OrderBySpecs)}
		resetChildren(tc.arg, bats)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		var bat *batch.Batch
		for {
			ok, err := vm.Exec(tc.arg, tc.proc)
			if ok.Batch != nil {
				bat = ok.Batch
				continue
			}
			require.NoError(t, err)
			// do the result check
			if len(tc.arg.OrderBySpecs) > 0 {
				desc := tc.arg.OrderBySpecs[0].Flag&plan.OrderBySpec_DESC != 0
				index := tc.arg.OrderBySpecs[0].Expr.Expr.(*plan.Expr_Col).Col.ColPos
				vec := bat.Vecs[index]
				if vec.GetType().Oid == types.T_int8 {
					i8c := vector.MustFixedColWithTypeCheck[int8](vec)
					if desc {
						for j := range i8c {
							if j > 0 {
								require.True(t, i8c[j] <= i8c[j-1], fmt.Sprintf("tc %d require desc, but get %v", tci, i8c))
							}
						}
					} else {
						for j := range i8c {
							if j > 0 {
								require.True(t, i8c[j] >= i8c[j-1])
							}
						}
					}
				} else if vec.GetType().Oid == types.T_int64 {
					i64c := vector.MustFixedColWithTypeCheck[int64](vec)
					if desc {
						for j := range i64c {
							if j > 0 {
								require.True(t, i64c[j] <= i64c[j-1])
							}
						}
					} else {
						for j := range i64c {
							if j > 0 {
								require.True(t, i64c[j] >= i64c[j-1])
							}
						}
					}
				}
			}

			break
		}
		tc.arg.Children[0].Free(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestMergeOrderFloatNaNLastAndPeerTieBreak(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{
			{Expr: newExpression(0, types.T_float64)},
			{Expr: newExpression(1, types.T_int64)},
		},
		OperatorBase: vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	makeBatch := func(values []float64, ids []int64) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, proc.Mp()))
		require.NoError(t, vector.AppendFixedList(bat.Vecs[1], ids, nil, proc.Mp()))
		bat.SetRowCount(len(values))
		return bat
	}
	resetChildren(arg, []*batch.Batch{
		makeBatch([]float64{math.Inf(-1), 1, math.Float64frombits(0x7ff8000000000002)}, []int64{10, 40, 1}),
		makeBatch([]float64{-1, math.Inf(1), math.Float64frombits(0x7ff8000000000001)}, []int64{20, 60, 2}),
	})
	require.NoError(t, arg.Prepare(proc))

	var got []int64
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			got = append(got, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])...)
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, []int64{10, 20, 40, 60, 1, 2}, got)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestOrderSpill(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{1, 4, 7}),
		newValuesBatch(proc, []int8{2, 5, 8}),
		batch.EmptyBatch,
		newValuesBatch(proc, []int8{3, 6, 9}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{1, 2, 3, 4, 5, 6, 7, 8, 9}, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillFinalMergeHonorsCancellationAfterInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newValuesBatch(proc, []int8{1, 4, 7}),
		newValuesBatch(proc, []int8{2, 5, 8}),
		newValuesBatch(proc, []int8{3, 6, 9}),
	}).WithEndOfDataCallback(cancel)
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(arg, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Empty(t, arg.ctr.spillReaders)

	arg.Reset(proc, true, context.Canceled)
	require.Empty(t, arg.ctr.spillRuns)
	require.Empty(t, arg.ctr.spillReaders)
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newValuesBatch(proc, []int8{3}),
		newValuesBatch(proc, []int8{1}),
		newValuesBatch(proc, []int8{2}),
	})
	arg.Children = nil
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, []int8{1, 2, 3}, collectInt8Results(t, arg, proc, 0))
}

func TestOrderSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{newValuesBatch(proc, []int8{3, 1, 2})}).
		WithBatchCallback(func(int) { cancel() })
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		arg.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(arg, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Empty(t, arg.ctr.spillRuns)
	require.Nil(t, arg.ctr.spillActiveRun)
}

func TestWriteSpillBatchStopsAtBatchBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	bat := newValuesBatch(proc, []int8{1, 2, 3})
	writer := &cancelAfterWriteWriter{cancel: cancel}
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-write")
	var writeBuf bytes.Buffer

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		bat.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	_, _, err := writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, writer, &writeBuf, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)

	_, _, err = writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, writer, &writeBuf, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)

	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 2)
	var unwritten bytes.Buffer
	_, _, err = writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, &unwritten, &writeBuf, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, unwritten.Len())
}

func TestCleanupSpillDiscardsUncommittedActiveRun(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	file, err := os.CreateTemp(t.TempDir(), "mergeorder-uncommitted-*")
	require.NoError(t, err)
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		_ = file.Close()
		proc.Free()
	})
	written := &bytes.Buffer{}
	activeWriter := bufio.NewWriterSize(written, 1024)
	_, err = activeWriter.Write([]byte("uncommitted spill payload"))
	require.NoError(t, err)
	require.Zero(t, written.Len())

	ctr := &container{
		spillActiveRun:    &spillRun{file: file},
		spillActiveWriter: activeWriter,
	}
	ctr.cleanupSpill(proc)
	require.Zero(t, written.Len())
	require.Nil(t, ctr.spillActiveRun)
	require.Nil(t, ctr.spillActiveWriter)
	_, err = file.Stat()
	require.Error(t, err)

	proc.Free()
	cleaned = true
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSpillCancellationEntryCheckpoints(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-entry")
	t.Cleanup(func() {
		proc.Ctx = baseCtx
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	t.Run("open readers", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		err := (&container{}).openSpillReaders(proc, []*spillRun{{}})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("spill cached runs", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		require.ErrorIs(t, (&container{}).spillCachedRuns(proc, analyzer), context.Canceled)
	})

	t.Run("spill cached run boundary", func(t *testing.T) {
		bat := newValuesBatch(proc, []int8{1})
		ctr := &container{batchList: []*batch.Batch{bat}}
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 2)
		require.ErrorIs(t, ctr.spillCachedRuns(proc, analyzer), context.Canceled)
		require.Same(t, bat, ctr.batchList[0])
		bat.Clean(proc.Mp())
	})

	t.Run("spill append", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		err := (&container{}).spillBatchWithAppend(proc, nil, nil, nil, analyzer)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("finalize active run entry", func(t *testing.T) {
		proc.Ctx = baseCtx
		ctr := &container{}
		require.NoError(t, ctr.ensureActiveSpillRun(proc))
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		require.ErrorIs(t, ctr.finalizeActiveSpillRun(proc, true), context.Canceled)
		require.NotNil(t, ctr.spillActiveRun)
		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
	})

	t.Run("finalize active run after flush", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "mergeorder-finalize-cancel-*")
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(baseCtx)
		writer := &cancelAfterWriteWriter{cancel: cancel}
		buffered := bufio.NewWriterSize(writer, 1024)
		_, err = buffered.Write([]byte("buffered spill payload"))
		require.NoError(t, err)
		require.Zero(t, writer.writes)
		ctr := &container{
			spillActiveRun:    &spillRun{file: file},
			spillActiveWriter: buffered,
		}
		proc.Ctx = ctx
		require.ErrorIs(t, ctr.finalizeActiveSpillRun(proc, true), context.Canceled)
		require.Equal(t, 1, writer.writes)
		require.Nil(t, ctr.spillActiveRun)
		require.Empty(t, ctr.spillRuns)
		_, err = file.Stat()
		require.Error(t, err)
	})

	t.Run("merge runs", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		merged, err := (&container{}).mergeRunsToSpill(proc, []*spillRun{{}, {}}, analyzer)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, merged)
	})

	t.Run("reduce runs", func(t *testing.T) {
		ctr := &container{spillRuns: make([]*spillRun, spillMergeFanIn+1)}
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		require.ErrorIs(t, ctr.reduceSpillRuns(proc, analyzer), context.Canceled)
	})

	t.Run("prepare final merge", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		err := (&container{}).prepareSpillFinalMerge(proc, nil, analyzer)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("send result", func(t *testing.T) {
		proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 1)
		var result vm.CallResult
		done, err := (&container{}).sendSpillResult(proc, &result)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, done)
		require.Nil(t, result.Batch)
	})
}

func TestMergeRunsToSpillCancellationCheckpoints(t *testing.T) {
	const (
		inputRunCount = 2
		// Merge entry, then open-loop/read-entry/read-completion for each
		// source reader, followed by the outer materialization check.
		beforeMaterializationCheck = 1 + inputRunCount*3 + 1
		// Each single-batch source then observes EOF at its next-read entry,
		// followed by the completed-chunk check immediately before spill write.
		beforeSpillWriteCheck = beforeMaterializationCheck + inputRunCount + 1
	)
	tests := []struct {
		name          string
		cancelAtCheck int
	}{
		{name: "before materialization", cancelAtCheck: beforeMaterializationCheck},
		{name: "before spill write", cancelAtCheck: beforeSpillWriteCheck},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			ctr := &container{
				compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
				executors:       make([]colexec.ExpressionExecutor, 1),
				spillColPos:     []int32{-1},
				spillKeyIndexes: []int{0},
			}
			analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-merge-runs")
			runs := []*spillRun{
				newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{1, 3}),
				newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{2, 4}),
			}
			baseCtx := proc.Ctx

			t.Cleanup(func() {
				proc.Ctx = baseCtx
				ctr.cleanupSpill(proc)
				closeSpillRuns(runs)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})

			proc.Ctx = newCancelOnDoneCheckContext(baseCtx, test.cancelAtCheck)
			merged, err := ctr.mergeRunsToSpill(proc, runs, analyzer)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, merged)
			for _, run := range runs {
				require.Nil(t, run.file)
			}
		})
	}
}

func TestMergeRunsToSpillStopsAtChunkBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	baseFS, err := proc.GetSpillFileService()
	require.NoError(t, err)
	trackingFS := &trackingMutableFileService{MutableFileService: baseFS}
	ctr := &container{
		compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors:       make([]colexec.ExpressionExecutor, 1),
		spillColPos:     []int32{-1},
		spillKeyIndexes: []int{0},
		spillFS:         trackingFS,
	}
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-merge-chunk")
	firstValues := make([]int8, maxWinnerChunkRows*2)
	for i := range firstValues {
		firstValues[i] = 1
	}
	runs := []*spillRun{
		newCancellationTestSpillRun(t, proc, ctr, analyzer, firstValues),
		newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{2}),
	}

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
		closeSpillRuns(runs)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// Entry, both reader open/read/read-completion triplets, and the outer
	// materialization check pass.
	// Cancellation is observed after the first 64-row winner chunk, before
	// the remaining rows are copied or an output batch is written.
	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 9)
	merged, err := ctr.mergeRunsToSpill(proc, runs, analyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, merged)
	require.Len(t, trackingFS.files, 3)
	_, statErr := trackingFS.files[2].Stat()
	require.Error(t, statErr)
	for _, run := range runs {
		require.Nil(t, run.file)
	}
}

func TestOpenSpillReadersCancellationPreservesOwnership(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctr := &container{
		compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors:       make([]colexec.ExpressionExecutor, 1),
		spillColPos:     []int32{-1},
		spillKeyIndexes: []int{0},
	}
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-open-readers")
	runs := []*spillRun{
		newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{1}),
		newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{2}),
	}
	firstFile, secondFile := runs[0].file, runs[1].file
	baseCtx := proc.Ctx

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
		closeSpillRuns(runs)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// The first reader uses checks 1-3 for open, read entry, and successful
	// read completion. Cancel at the second run's open-loop ownership boundary.
	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 4)
	err := ctr.openSpillReaders(proc, runs)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, ctr.spillReaders, 1)
	require.Nil(t, runs[0].file)
	require.Same(t, secondFile, runs[1].file)
	_, err = firstFile.Stat()
	require.NoError(t, err)
	_, err = secondFile.Stat()
	require.NoError(t, err)

	ctr.cleanupSpill(proc)
	_, err = firstFile.Stat()
	require.Error(t, err)
	_, err = secondFile.Stat()
	require.NoError(t, err)
	closeSpillRuns(runs)
	_, err = secondFile.Stat()
	require.Error(t, err)
}

func TestSpillReaderRolloverCancellationBoundaries(t *testing.T) {
	t.Run("before multi-batch refill", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		baseCtx := proc.Ctx
		ctr := &container{
			compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
			executors:       make([]colexec.ExpressionExecutor, 1),
			spillColPos:     []int32{-1},
			spillKeyIndexes: []int{0},
		}
		analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-rollover")
		run := newCancellationTestSpillRunBatches(
			t, proc, ctr, analyzer,
			[]int8{1, 2},
			[]int8{3, 4},
		)
		file := run.file
		require.Equal(t, 2, run.batchCount)
		require.NoError(t, ctr.openSpillReaders(proc, []*spillRun{run}))
		require.Nil(t, run.file)
		require.Len(t, ctr.spillReaders, 1)
		reader := ctr.spillReaders[0]
		require.Equal(t, []int8{1, 2}, vector.MustFixedColWithTypeCheck[int8](reader.batch.Vecs[0]))
		bufferedBefore := reader.reader.Buffered()
		require.Positive(t, bufferedBefore)

		t.Cleanup(func() {
			proc.Ctx = baseCtx
			ctr.cleanupSpill(proc)
			closeSpillRuns([]*spillRun{run})
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})

		ctx, cancel := context.WithCancel(baseCtx)
		proc.Ctx = ctx
		cancel()
		err := ctr.advanceSpillReaderByChunk(proc, 0, reader.batch.RowCount())
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, bufferedBefore, reader.reader.Buffered())
		require.Equal(t, []int8{1, 2}, vector.MustFixedColWithTypeCheck[int8](reader.batch.Vecs[0]))
		require.Equal(t, int64(2), reader.rowIdx)
		require.Len(t, ctr.spillReaders, 1)

		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
		_, err = file.Stat()
		require.Error(t, err)
	})

	t.Run("during refill IO", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		baseCtx := proc.Ctx
		ctr := &container{
			executors:       make([]colexec.ExpressionExecutor, 1),
			spillColPos:     []int32{-1},
			spillKeyIndexes: []int{0},
		}
		analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-refill-io")
		bat := newValuesBatch(proc, []int8{3, 4})
		var payload bytes.Buffer
		var writeBuf bytes.Buffer
		_, _, err := writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, &payload, &writeBuf, analyzer)
		require.NoError(t, err)
		bat.Clean(proc.Mp())

		ctx, cancel := context.WithCancel(baseCtx)
		proc.Ctx = ctx
		source := &cancelAfterReadReader{reader: bytes.NewReader(payload.Bytes()), cancel: cancel}
		reader := &spillRunReader{reader: bufio.NewReaderSize(source, spillIOBufferSize)}
		t.Cleanup(func() {
			proc.Ctx = baseCtx
			reader.close(proc)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})

		ok, err := reader.readNextBatch(proc, ctr)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ok)
		require.Equal(t, 1, source.reads)
	})
}

func TestReduceSpillRunsCleansCompletedRuns(t *testing.T) {
	const inputRunCount = spillMergeFanIn + 2

	tests := []struct {
		name             string
		cancelAfterFirst bool
		corruptLaterRun  bool
		wantError        error
	}{
		{
			name:             "cancellation after completed fan-in group",
			cancelAfterFirst: true,
			wantError:        context.Canceled,
		},
		{
			name:            "later merge read error",
			corruptLaterRun: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			baseCtx := proc.Ctx
			baseFS, err := proc.GetSpillFileService()
			require.NoError(t, err)
			trackingFS := &trackingMutableFileService{MutableFileService: baseFS}
			ctr := &container{
				compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
				executors:       make([]colexec.ExpressionExecutor, 1),
				spillColPos:     []int32{-1},
				spillKeyIndexes: []int{0},
				spillFS:         trackingFS,
			}
			analyzer := process.NewAnalyzer(0, false, false, "mergeorder-reduce-ownership")
			runs := make([]*spillRun, 0, inputRunCount)
			for i := range inputRunCount {
				runs = append(runs, newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{int8(i)}))
			}
			ctr.spillRuns = runs
			if test.cancelAfterFirst {
				// Reduction check, merge entry, three open/read checks per
				// source reader, chunk start, one EOF read-entry check per
				// single-batch reader, chunk completion, three write boundaries,
				// then the next fan-in group check.
				proc.Ctx = newCancelOnDoneCheckContext(baseCtx, spillMergeFanIn*4+8)
			}
			if test.corruptLaterRun {
				require.NoError(t, runs[spillMergeFanIn].file.Truncate(4))
				_, err = runs[spillMergeFanIn].file.Seek(0, io.SeekStart)
				require.NoError(t, err)
			}

			t.Cleanup(func() {
				proc.Ctx = baseCtx
				ctr.cleanupSpill(proc)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})

			err = ctr.reduceSpillRuns(proc, analyzer)
			if test.wantError != nil {
				require.ErrorIs(t, err, test.wantError)
			} else {
				require.Error(t, err)
			}
			require.Len(t, trackingFS.files, inputRunCount+1)
			completedRunFile := trackingFS.files[inputRunCount]
			_, statErr := completedRunFile.Stat()
			require.Error(t, statErr)

			for i := 0; i < spillMergeFanIn; i++ {
				require.Nil(t, runs[i].file)
			}
			if test.wantError == nil {
				// The failing run transferred to its reader before the read;
				// only the not-yet-opened final run remains source-owned.
				require.Nil(t, runs[spillMergeFanIn].file)
			}
			require.NotNil(t, runs[spillMergeFanIn+1].file)
			_, statErr = runs[spillMergeFanIn+1].file.Stat()
			require.NoError(t, statErr)
		})
	}
}

func TestSendSpillResultHonorsCancellationBeforePublish(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctr := &container{
		compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors:       make([]colexec.ExpressionExecutor, 1),
		spillColPos:     []int32{-1},
		spillKeyIndexes: []int{0},
	}
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-cancel-send")
	run := newCancellationTestSpillRun(t, proc, ctr, analyzer, []int8{1, 2})
	require.NoError(t, ctr.openSpillReaders(proc, []*spillRun{run}))
	baseCtx := proc.Ctx

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
		closeSpillRuns([]*spillRun{run})
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// Send entry, the exhausted reader's next-read entry, then result publish.
	proc.Ctx = newCancelOnDoneCheckContext(baseCtx, 3)
	var result vm.CallResult
	done, err := ctr.sendSpillResult(proc, &result)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, done)
	require.Nil(t, result.Batch)
}

func TestSendSpillResultStopsAtChunkBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx

	firstValues := make([]int8, batchSizeCheckInterval)
	for i := range firstValues {
		firstValues[i] = int8(i)
	}
	firstBatch := newValuesBatch(proc, firstValues)
	secondBatch := newValuesBatch(proc, []int8{100})
	firstEOF := &callbackEOFReader{callback: cancel}
	secondEOF := &callbackEOFReader{}
	firstReader := &spillRunReader{
		reader:    bufio.NewReader(firstEOF),
		batch:     firstBatch,
		orderCols: []*vector.Vector{firstBatch.Vecs[0]},
		heapIdx:   0,
	}
	secondReader := &spillRunReader{
		reader:    bufio.NewReader(secondEOF),
		batch:     secondBatch,
		orderCols: []*vector.Vector{secondBatch.Vecs[0]},
		heapIdx:   1,
	}
	firstReader.refreshDrainProfile()
	secondReader.refreshDrainProfile()
	ctr := &container{
		compares:     []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		spillReaders: []*spillRunReader{firstReader, secondReader},
	}
	heap.Init(ctr)

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		ctr.cleanupSpill(proc)
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	var result vm.CallResult
	done, err := ctr.sendSpillResult(proc, &result)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, done)
	require.Nil(t, result.Batch)
	require.Equal(t, 1, firstEOF.reads)
	require.Zero(t, secondEOF.reads)
}

func TestOrderSpillMultiPass(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := make([]*batch.Batch, 0, spillMergeFanIn+8)
	for i := spillMergeFanIn + 8; i >= 1; i-- {
		bats = append(bats, newValuesBatch(proc, []int8{int8(i)}))
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	expected := make([]int8, 0, spillMergeFanIn+8)
	for i := 1; i <= spillMergeFanIn+8; i++ {
		expected = append(expected, int8(i))
	}
	require.Equal(t, expected, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillSkipsColumnKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	exec := &countingColumnExecutor{col: 0}
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	arg.ctr.executors = []colexec.ExpressionExecutor{exec}

	bats := make([]*batch.Batch, 0, spillMergeFanIn+8)
	for i := spillMergeFanIn + 8; i >= 1; i-- {
		bats = append(bats, newValuesBatch(proc, []int8{int8(i)}))
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	expected := make([]int8, 0, spillMergeFanIn+8)
	for i := 1; i <= spillMergeFanIn+8; i++ {
		expected = append(expected, int8(i))
	}
	require.Equal(t, expected, values)
	require.Equal(t, 0, exec.calls)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillPersistsComputedKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	exec := &negatingInt8Executor{col: 0}
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	arg.ctr.executors = []colexec.ExpressionExecutor{exec}

	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{3}),
		newValuesBatch(proc, []int8{1}),
		newValuesBatch(proc, []int8{2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{3, 2, 1}, values)
	require.Equal(t, 3, exec.calls)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillDesc(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: plan.OrderBySpec_DESC}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newValuesBatch(proc, []int8{9, 6, 3}),
		newValuesBatch(proc, []int8{8, 5, 2}),
		newValuesBatch(proc, []int8{7, 4, 1}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	values := collectInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8{9, 8, 7, 6, 5, 4, 3, 2, 1}, values)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillMultiKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{
			{Expr: newExpression(0, types.T_int8), Flag: 0},
			{Expr: newExpression(1, types.T_int64), Flag: plan.OrderBySpec_DESC},
		},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newPairBatch(proc, []int8{1, 2, 3}, []int64{10, 9, 8}),
		newPairBatch(proc, []int8{1, 2, 3}, []int64{7, 6, 5}),
		newPairBatch(proc, []int8{1, 2, 3}, []int64{4, 3, 2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectPairResults(t, arg, proc)
	require.Equal(t, []pairRow{
		{first: 1, second: 10},
		{first: 1, second: 7},
		{first: 1, second: 4},
		{first: 2, second: 9},
		{first: 2, second: 6},
		{first: 2, second: 3},
		{first: 3, second: 8},
		{first: 3, second: 5},
		{first: 3, second: 2},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillNullsLast(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs:   []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: plan.OrderBySpec_NULLS_LAST}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newNullableValuesBatch(proc, []int8{1, 4, 0}, []uint64{2}),
		newNullableValuesBatch(proc, []int8{2, 5, 0}, []uint64{2}),
		newNullableValuesBatch(proc, []int8{3, 6, 0}, []uint64{2}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectNullableInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8Row{
		{value: 1},
		{value: 2},
		{value: 3},
		{value: 4},
		{value: 5},
		{value: 6},
		{isNull: true},
		{isNull: true},
		{isNull: true},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderSpillDescNullsFirst(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{{
			Expr: newExpression(0, types.T_int8),
			Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_FIRST,
		}},
		SpillThreshold: 1,
		OperatorBase:   vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}},
	}
	bats := []*batch.Batch{
		newNullableValuesBatch(proc, []int8{0, 9, 6}, []uint64{0}),
		newNullableValuesBatch(proc, []int8{0, 8, 5}, []uint64{0}),
		newNullableValuesBatch(proc, []int8{0, 7, 4}, []uint64{0}),
	}
	resetChildren(arg, bats)
	err := arg.Prepare(proc)
	require.NoError(t, err)

	rows := collectNullableInt8Results(t, arg, proc, 0)
	require.Equal(t, []int8Row{
		{isNull: true},
		{isNull: true},
		{isNull: true},
		{value: 9},
		{value: 8},
		{value: 7},
		{value: 6},
		{value: 5},
		{value: 4},
	}, rows)

	arg.Children[0].Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestSpillAppendPolicy(t *testing.T) {
	var ctr container

	ctr.setSpillThreshold(8 * common.MiB)
	require.False(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(0), ctr.spillAppendTarget)

	ctr.setSpillThreshold(64 * common.MiB)
	require.True(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(32*common.MiB), ctr.spillAppendTarget)

	ctr.setSpillThreshold(2 * common.GiB)
	require.True(t, ctr.spillAppendEnabled)
	require.Equal(t, int64(128*common.MiB), ctr.spillAppendTarget)
}

func TestCanAppendToActiveRun(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors: make([]colexec.ExpressionExecutor, 1),
	}
	defer func() {
		for i := range ctr.spillTailCols {
			if ctr.spillTailCols[i] != nil {
				ctr.spillTailCols[i].Free(proc.Mp())
			}
		}
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	tail := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	incomingEqual := testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false, []int8{5, 6})
	incomingLess := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{4})
	defer tail.Free(proc.Mp())
	defer incomingEqual.Free(proc.Mp())
	defer incomingLess.Free(proc.Mp())

	ctr.spillTailCols = []*vector.Vector{vector.NewOffHeapVecWithType(types.T_int8.ToType())}
	ctr.spillTailReady = false
	require.NoError(t, ctr.updateActiveRunTail(proc, []*vector.Vector{tail}, 0))

	require.True(t, ctr.canAppendToActiveRun([]*vector.Vector{incomingEqual}))
	require.False(t, ctr.canAppendToActiveRun([]*vector.Vector{incomingLess}))
}

func TestComputeDrainChunkFixedWidth(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_int64.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	reader := &spillRunReader{batch: src}
	reader.refreshDrainProfile()

	require.Equal(t, 100, computeDrainChunk(reader, 0))
	require.Equal(t, 1, computeDrainChunk(reader, maxBatchSizeToSend-1))
}

func TestComputeDrainChunkVarlen(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprintf("row-%d", i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_varchar.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	reader := &spillRunReader{batch: src}
	reader.refreshDrainProfile()

	chunk := computeDrainChunk(reader, 0)
	require.Greater(t, chunk, 0)
	require.LessOrEqual(t, chunk, maxVarlenDrainChunkRows)
}

func TestComputeWinnerChunkDominant(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	rootVec := testutil.NewVector(4, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3, 10})
	secondVec := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	defer rootVec.Free(proc.Mp())
	defer secondVec.Free(proc.Mp())

	ctr := &container{
		compares: []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
	}
	rootBatch := batch.NewWithSize(1)
	rootBatch.Vecs[0] = rootVec
	rootBatch.SetRowCount(4)
	secondBatch := batch.NewWithSize(1)
	secondBatch.Vecs[0] = secondVec
	secondBatch.SetRowCount(1)
	root := &spillRunReader{batch: rootBatch, orderCols: []*vector.Vector{rootVec}, rowIdx: 0}
	second := &spillRunReader{batch: secondBatch, orderCols: []*vector.Vector{secondVec}, rowIdx: 0}
	require.Equal(t, 3, ctr.computeWinnerChunk(root, second, 4))
}

func TestComputeWinnerChunkBudgetLimited(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	rootVec := testutil.NewVector(4, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3, 4})
	secondVec := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{9})
	defer rootVec.Free(proc.Mp())
	defer secondVec.Free(proc.Mp())

	ctr := &container{
		compares: []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
	}
	rootBatch := batch.NewWithSize(1)
	rootBatch.Vecs[0] = rootVec
	rootBatch.SetRowCount(4)
	secondBatch := batch.NewWithSize(1)
	secondBatch.Vecs[0] = secondVec
	secondBatch.SetRowCount(1)
	root := &spillRunReader{batch: rootBatch, orderCols: []*vector.Vector{rootVec}, rowIdx: 0}
	second := &spillRunReader{batch: secondBatch, orderCols: []*vector.Vector{secondVec}, rowIdx: 0}
	require.Equal(t, 2, ctr.computeWinnerChunk(root, second, 2))
}

func TestComputeWinnerChunkFallbackToOne(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	rootVec := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 6, 7})
	secondVec := testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	defer rootVec.Free(proc.Mp())
	defer secondVec.Free(proc.Mp())

	ctr := &container{
		compares: []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
	}
	rootBatch := batch.NewWithSize(1)
	rootBatch.Vecs[0] = rootVec
	rootBatch.SetRowCount(3)
	secondBatch := batch.NewWithSize(1)
	secondBatch.Vecs[0] = secondVec
	secondBatch.SetRowCount(1)
	root := &spillRunReader{batch: rootBatch, orderCols: []*vector.Vector{rootVec}, rowIdx: 0}
	second := &spillRunReader{batch: secondBatch, orderCols: []*vector.Vector{secondVec}, rowIdx: 0}
	require.Equal(t, 1, ctr.computeWinnerChunk(root, second, 3))
}

func TestComputeBatchDrainChunkFixedWidth(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_int64.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	require.Equal(t, 100, computeBatchDrainChunk(src, 0, 0, calcFixedRowBytes(src.Vecs)))
	require.Equal(t, 1, computeBatchDrainChunk(src, 0, maxBatchSizeToSend-1, calcFixedRowBytes(src.Vecs)))
}

func TestComputeBatchDrainChunkVarlen(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src := batch.NewWithSize(1)
	values := make([]string, 100)
	for i := range values {
		values[i] = fmt.Sprintf("v-%d", i)
	}
	src.Vecs[0] = testutil.NewVector(100, types.T_varchar.ToType(), proc.Mp(), false, values)
	src.SetRowCount(100)
	defer src.Clean(proc.Mp())

	chunk := computeBatchDrainChunk(src, 0, 0, calcFixedRowBytes(src.Vecs))
	require.Greater(t, chunk, 0)
	require.LessOrEqual(t, chunk, maxVarlenDrainChunkRows)
}

func TestComputeInMemoryWinnerChunkThreeRuns(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	left := batch.NewWithSize(1)
	left.Vecs[0] = testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 6})
	left.SetRowCount(3)
	mid := batch.NewWithSize(1)
	mid.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{4})
	mid.SetRowCount(1)
	right := batch.NewWithSize(1)
	right.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	right.SetRowCount(1)
	defer left.Clean(proc.Mp())
	defer mid.Clean(proc.Mp())
	defer right.Clean(proc.Mp())

	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		batchList: []*batch.Batch{left, mid, right},
		orderCols: [][]*vector.Vector{{left.Vecs[0]}, {mid.Vecs[0]}, {right.Vecs[0]}},
		indexList: []int64{0, 0, 0},
	}
	require.Equal(t, 2, ctr.computeInMemoryWinnerChunk(0, 1, 3))
}

func TestComputeInMemoryWinnerChunkDominant(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	left := batch.NewWithSize(1)
	left.Vecs[0] = testutil.NewVector(4, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3, 10})
	left.SetRowCount(4)
	right := batch.NewWithSize(1)
	right.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	right.SetRowCount(1)
	defer left.Clean(proc.Mp())
	defer right.Clean(proc.Mp())

	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		batchList: []*batch.Batch{left, right},
		orderCols: [][]*vector.Vector{{left.Vecs[0]}, {right.Vecs[0]}},
		indexList: []int64{0, 0},
	}
	require.Equal(t, 3, ctr.computeInMemoryWinnerChunk(0, 1, 4))
	require.Equal(t, 2, ctr.computeInMemoryWinnerChunk(0, 1, 2))
}

func TestComputeInMemoryWinnerChunkFallbackToOne(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	left := batch.NewWithSize(1)
	left.Vecs[0] = testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 6, 7})
	left.SetRowCount(3)
	right := batch.NewWithSize(1)
	right.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{5})
	right.SetRowCount(1)
	defer left.Clean(proc.Mp())
	defer right.Clean(proc.Mp())

	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		batchList: []*batch.Batch{left, right},
		orderCols: [][]*vector.Vector{{left.Vecs[0]}, {right.Vecs[0]}},
		indexList: []int64{0, 0},
	}
	require.Equal(t, 1, ctr.computeInMemoryWinnerChunk(0, 1, 3))
}

func TestInMemoryHeapAdvance(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	left := batch.NewWithSize(1)
	left.Vecs[0] = testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false, []int8{5, 7})
	left.SetRowCount(2)
	mid := batch.NewWithSize(1)
	mid.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{1})
	mid.SetRowCount(1)
	right := batch.NewWithSize(1)
	right.Vecs[0] = testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{3})
	right.SetRowCount(1)
	defer left.Clean(proc.Mp())
	defer mid.Clean(proc.Mp())
	defer right.Clean(proc.Mp())

	ctr := &container{
		compares:  []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		batchList: []*batch.Batch{left, mid, right},
		orderCols: [][]*vector.Vector{{left.Vecs[0]}, {mid.Vecs[0]}, {right.Vecs[0]}},
		indexList: []int64{0, 0, 0},
	}
	ctr.initInMemoryHeap()
	require.NotNil(t, ctr.inMemoryHeap)
	require.Equal(t, 1, ctr.inMemoryHeap.items[0])

	require.NoError(t, ctr.advanceInMemoryBatchByChunk(proc, 1, 1))
	require.Equal(t, 2, ctr.inMemoryHeap.Len())
	require.Equal(t, 2, ctr.inMemoryHeap.items[0])
}

func TestFillSpillIncomingOrderColumns(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	data := newPairBatch(proc, []int8{1, 2}, []int64{10, 20})
	defer data.Clean(proc.Mp())
	key := testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false, []int8{9, 8})
	defer key.Free(proc.Mp())

	ctr := &container{
		executors: make([]colexec.ExpressionExecutor, 2),
		spillColPos: []int32{
			0,
			-1,
		},
	}
	cols, err := ctr.fillSpillIncomingOrderColumns(proc, data, []*vector.Vector{key})
	require.NoError(t, err)
	require.Len(t, cols, 2)
	require.Same(t, data.Vecs[0], cols[0])
	require.Same(t, key, cols[1])

	ctr.spillColPos = []int32{2}
	ctr.executors = make([]colexec.ExpressionExecutor, 1)
	_, err = ctr.fillSpillIncomingOrderColumns(proc, data, nil)
	require.Error(t, err)

	ctr.spillColPos = []int32{-1}
	_, err = ctr.fillSpillIncomingOrderColumns(proc, data, nil)
	require.Error(t, err)

	ctr.spillColPos = []int32{0}
	_, err = ctr.fillSpillIncomingOrderColumns(proc, data, []*vector.Vector{key})
	require.Error(t, err)
}

func TestAppendContiguousOrderRows(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	src0 := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{1, 2, 3})
	src1 := testutil.NewVector(3, types.T_int8.ToType(), proc.Mp(), false, []int8{4, 5, 6})
	defer src0.Free(proc.Mp())
	defer src1.Free(proc.Mp())

	dst := batch.NewOffHeapWithSize(2)
	dst.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	dst.Vecs[1] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	defer dst.Clean(proc.Mp())

	err := appendContiguousOrderRows(dst, []*vector.Vector{src0, src1}, []int{1, 0}, 1, 2, proc)
	require.NoError(t, err)
	require.Equal(t, []int8{5, 6}, vector.MustFixedColWithTypeCheck[int8](dst.Vecs[0]))
	require.Equal(t, []int8{2, 3}, vector.MustFixedColWithTypeCheck[int8](dst.Vecs[1]))
}

func TestSpillAppendPathAndSendResult(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	ctr := &container{
		compares:           []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors:          make([]colexec.ExpressionExecutor, 1),
		spillColPos:        []int32{0},
		spillAppendEnabled: true,
		spillAppendTarget:  1 << 30,
		batchList: []*batch.Batch{
			newValuesBatch(proc, []int8{10, 11}),
			newValuesBatch(proc, []int8{5, 6}),
		},
		orderCols: make([][]*vector.Vector, 2),
	}
	defer func() {
		ctr.cleanupSpill(proc)
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-test")
	require.NoError(t, ctr.spillCachedRuns(proc, analyzer))
	require.True(t, ctr.spilling)
	require.Len(t, ctr.spillRuns, 1)
	require.NotNil(t, ctr.spillActiveRun)

	require.NoError(t, ctr.prepareSpillFinalMerge(proc, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}, analyzer))
	require.Greater(t, len(ctr.spillReaders), 0)

	var got []int8
	for {
		var result vm.CallResult
		sendOver, err := ctr.sendSpillResult(proc, &result)
		require.NoError(t, err)
		if result.Batch != nil {
			got = append(got, vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[0])...)
		}
		if sendOver {
			break
		}
	}
	require.Equal(t, []int8{5, 6, 10, 11}, got)
}

func TestMergeRunsToSpillWithStoredKeys(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	ctr := &container{
		compares:        []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		executors:       make([]colexec.ExpressionExecutor, 1),
		spillColPos:     []int32{-1},
		spillKeyIndexes: []int{0},
	}
	defer func() {
		ctr.cleanupSpill(proc)
		if ctr.buf != nil {
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		}
	}()

	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-merge-runs")
	mkRun := func(values []int8) *spillRun {
		bat := newValuesBatch(proc, values)
		run, err := ctr.createSpillRun(proc)
		require.NoError(t, err)

		writer := bufio.NewWriterSize(run.file, spillIOBufferSize)
		_, _, err = writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, writer, &ctr.spillWriteBuf, analyzer)
		require.NoError(t, err)
		require.NoError(t, writer.Flush())
		_, err = run.file.Seek(0, 0)
		require.NoError(t, err)

		run.batchCount = 1
		run.rowCount = int64(bat.RowCount())
		bat.Clean(proc.Mp())
		return run
	}

	runs := []*spillRun{
		mkRun([]int8{1, 4}),
		mkRun([]int8{2, 5}),
		mkRun([]int8{3, 6}),
	}
	defer func() {
		for _, run := range runs {
			if run != nil && run.file != nil {
				_ = run.file.Close()
				run.file = nil
			}
		}
	}()

	merged, err := ctr.mergeRunsToSpill(proc, runs, analyzer)
	require.NoError(t, err)
	require.NotNil(t, merged)

	require.NoError(t, ctr.openSpillReaders(proc, []*spillRun{merged}))
	var got []int8
	for {
		var result vm.CallResult
		sendOver, err := ctr.sendSpillResult(proc, &result)
		require.NoError(t, err)
		if result.Batch != nil {
			got = append(got, vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[0])...)
		}
		if sendOver {
			break
		}
	}
	require.Equal(t, []int8{1, 2, 3, 4, 5, 6}, got)
}

func TestFinalizeActiveSpillRunError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	ctr := &container{}
	require.NoError(t, ctr.ensureActiveSpillRun(proc))
	require.NotNil(t, ctr.spillActiveRun)
	require.NoError(t, ctr.spillActiveRun.file.Close())

	err := ctr.finalizeActiveSpillRun(proc, true)
	require.Error(t, err)
	ctr.cleanupSpill(proc)
}

func TestFixSpillHeapAfterAdvanceTwoReaders(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	left := newValuesBatch(proc, []int8{5})
	right := newValuesBatch(proc, []int8{1})
	defer left.Clean(proc.Mp())
	defer right.Clean(proc.Mp())

	leftReader := &spillRunReader{batch: left, orderCols: []*vector.Vector{left.Vecs[0]}, heapIdx: 0}
	rightReader := &spillRunReader{batch: right, orderCols: []*vector.Vector{right.Vecs[0]}, heapIdx: 1}
	ctr := &container{
		compares:     []compare.Compare{compare.New(types.T_int8.ToType(), false, false)},
		spillReaders: []*spillRunReader{leftReader, rightReader},
	}

	ctr.fixSpillHeapAfterAdvance(0)
	require.Same(t, rightReader, ctr.spillReaders[0])
	require.Equal(t, 0, rightReader.heapIdx)
	require.Equal(t, 1, leftReader.heapIdx)

	ctr.spillReaders = ctr.spillReaders[:1]
	ctr.fixSpillHeapAfterAdvance(0)
	ctr.spillReaders = nil
	ctr.fixSpillHeapAfterAdvance(0)
}

func TestSpillHeapPush(t *testing.T) {
	ctr := &container{}
	reader := &spillRunReader{}
	heap.Push(ctr, reader)
	require.Len(t, ctr.spillReaders, 1)
	require.Equal(t, 0, reader.heapIdx)
}

func TestMergeOrderResetAndOpType(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	exec := &resetTrackingExecutor{}
	arg := &MergeOrder{}
	arg.ctr.executors = []colexec.ExpressionExecutor{exec}
	arg.ctr.batchList = []*batch.Batch{newValuesBatch(proc, []int8{1, 2})}
	arg.ctr.orderCols = [][]*vector.Vector{{arg.ctr.batchList[0].Vecs[0]}}
	arg.ctr.buf = batch.NewOffHeapWithSize(1)
	arg.ctr.buf.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	arg.ctr.spillTailCols = []*vector.Vector{testutil.NewVector(1, types.T_int8.ToType(), proc.Mp(), false, []int8{9})}
	require.NoError(t, arg.ctr.ensureActiveSpillRun(proc))
	require.Equal(t, vm.MergeOrder, arg.OpType())

	arg.Reset(proc, false, nil)
	require.Equal(t, 1, exec.resetCalls)
	require.Equal(t, receiving, arg.ctr.status)
	require.Len(t, arg.ctr.batchList, 0)
	require.Nil(t, arg.ctr.spillActiveRun)

	arg.Free(proc, false, nil)
}

func TestMergeOrderResetReleasesAccountedResult(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	result := batch.NewOffHeapWithSize(1)
	result.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, result.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(result.Vecs[0], int64(1), false, proc.Mp()))
	result.SetRowCount(1)
	require.Positive(t, account.Snapshot().Used)

	arg := &MergeOrder{}
	arg.ctr.buf = result
	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.buf)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)

	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestRemoveInMemoryBatchReleasesAccountedBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)

	input := batch.NewOffHeapWithSize(2)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	}
	require.NoError(t, input.SetAllocationAccount(selection))
	for i := range 17 {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(i), false, proc.Mp()))
	}
	input.SetRowCount(17)
	require.Positive(t, account.Snapshot().Used)

	ctr := container{
		batchList:     []*batch.Batch{input},
		orderCols:     [][]*vector.Vector{{input.Vecs[0]}},
		indexList:     []int64{17},
		spillMemUsage: int64(input.Size()),
	}
	require.NoError(t, ctr.removeInMemoryBatch(proc, 0))
	require.Nil(t, ctr.batchList[0])
	require.Nil(t, ctr.orderCols[0])
	require.Zero(t, ctr.spillMemUsage)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)

	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestSpillHelperBranches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	ctr := &container{}
	require.False(t, ctr.shouldSpill(1))
	ctr.spillThreshold = 10
	require.False(t, ctr.shouldSpill(1))
	ctr.spilling = true
	require.True(t, ctr.shouldSpill(0))

	emptyBatch := batch.NewWithSize(1)
	emptyBatch.Vecs[0] = testutil.NewVector(0, types.T_int8.ToType(), proc.Mp(), false, []int8{})
	emptyBatch.SetRowCount(0)
	defer emptyBatch.Clean(proc.Mp())
	reader := &spillRunReader{batch: emptyBatch, rowIdx: 0}
	require.Equal(t, 0, computeDrainChunk(reader, 0))

	bat := newValuesBatch(proc, []int8{1, 2, 3})
	defer bat.Clean(proc.Mp())
	ctrErr := &container{
		executors: []colexec.ExpressionExecutor{&failingExecutor{}},
	}
	_, err := ctrErr.evaluateOrderColumns(proc, bat)
	require.Error(t, err)
	ctrErr.spillKeyIndexes = []int{0}
	_, err = ctrErr.buildSpillKeyColumns(proc, bat)
	require.Error(t, err)

	var writeBuf bytes.Buffer
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-helper-branches")
	_, _, err = writeSpillBatch(proc, bat, nil, failWriter{}, &writeBuf, analyzer)
	require.Error(t, err)

	reuse := batch.NewOffHeapWithSize(1)
	reuse.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	defer reuse.Clean(proc.Mp())
	reuseKey := batch.NewOffHeapWithSize(1)
	reuseKey.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	defer reuseKey.Clean(proc.Mp())

	_, err = readSpillPayload(proc, bufio.NewReader(bytes.NewReader(nil)), reuse)
	require.Error(t, err)
	_, _, err = readSpillBatches(proc, bufio.NewReader(bytes.NewReader(nil)), reuse, reuseKey)
	require.ErrorIs(t, err, io.EOF)

	var partial bytes.Buffer
	cnt := int64(bat.RowCount())
	partial.Write(types.EncodeInt64(&cnt))
	require.NoError(t, appendSpillPayload(&partial, bat))
	keyCount := int64(1)
	partial.Write(types.EncodeInt64(&keyCount))
	_, _, err = readSpillBatches(proc, bufio.NewReader(bytes.NewReader(partial.Bytes())), reuse, reuseKey)
	require.Error(t, err)

	ctrRestore := &container{
		executors:   make([]colexec.ExpressionExecutor, 1),
		spillColPos: []int32{1},
	}
	_, err = ctrRestore.restoreSpillOrderColumns(proc, bat, nil, nil)
	require.Error(t, err)
	ctrRestore.spillColPos = []int32{-1}
	_, err = ctrRestore.restoreSpillOrderColumns(proc, bat, nil, nil)
	require.Error(t, err)
	keyMismatch := batch.NewOffHeapWithSize(2)
	keyMismatch.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	keyMismatch.Vecs[1] = vector.NewOffHeapVecWithType(types.T_int8.ToType())
	defer keyMismatch.Clean(proc.Mp())
	_, err = ctrRestore.restoreSpillOrderColumns(proc, bat, keyMismatch, nil)
	require.Error(t, err)

	oneRunCtr := &container{}
	run := &spillRun{file: nil}
	mergedRun, err := oneRunCtr.mergeRunsToSpill(proc, []*spillRun{run}, analyzer)
	require.NoError(t, err)
	require.Same(t, run, mergedRun)

	noRunCtr := &container{}
	require.NoError(t, noRunCtr.prepareSpillFinalMerge(proc, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}, analyzer))
	var result vm.CallResult
	sendOver, err := noRunCtr.sendSpillResult(proc, &result)
	require.NoError(t, err)
	require.True(t, sendOver)

	file, err := os.CreateTemp("", "mergeorder-openerr-*")
	require.NoError(t, err)
	defer os.Remove(file.Name())
	require.NoError(t, file.Close())
	openErrCtr := &container{}
	err = openErrCtr.openSpillReaders(proc, []*spillRun{{file: file}})
	require.Error(t, err)
}

func TestInMemoryHeapPushAndSingleInit(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	ctr := &container{
		batchList: []*batch.Batch{newValuesBatch(proc, []int8{1})},
	}
	defer ctr.batchList[0].Clean(proc.Mp())

	ctr.initInMemoryHeap()
	require.Nil(t, ctr.inMemoryHeap)
	require.Nil(t, ctr.inMemoryHeapPos)

	ctr.inMemoryHeapPos = make([]int, 3)
	h := &inMemoryMergeHeap{ctr: ctr}
	heap.Push(h, 2)
	require.Equal(t, 0, ctr.inMemoryHeapPos[2])
	heap.Pop(h)
	require.Equal(t, -1, ctr.inMemoryHeapPos[2])
}

func TestSpillReaderLifecycleAndCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	file, err := os.CreateTemp("", "mergeorder-reader-*")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	reader := &spillRunReader{}
	reader.reset(file)
	reader.reset(file)

	emptyFixed := batch.NewWithSize(0)
	emptyFixed.SetRowCount(5)
	reader.batch = emptyFixed
	reader.refreshDrainProfile()
	require.True(t, reader.fixedWidth)
	require.Equal(t, 1, reader.rowBytes)

	varlen := batch.NewWithSize(1)
	varlen.Vecs[0] = testutil.NewVector(1, types.T_varchar.ToType(), proc.Mp(), false, []string{""})
	varlen.SetRowCount(1000)
	reader.batch = varlen
	reader.refreshDrainProfile()
	require.False(t, reader.fixedWidth)
	require.Equal(t, 1, reader.avgRowBytes)
	varlen.Clean(proc.Mp())

	reader.reader = bufio.NewReader(bytes.NewReader([]byte{1, 2, 3}))
	ok, err := reader.readNextBatch(proc, &container{})
	require.False(t, ok)
	require.Error(t, err)

	valid := newValuesBatch(proc, []int8{1})
	defer valid.Clean(proc.Mp())
	var payload bytes.Buffer
	var wb bytes.Buffer
	analyzer := process.NewAnalyzer(0, false, false, "mergeorder-reader-lifecycle")
	_, _, err = writeSpillBatch(proc, valid, nil, &payload, &wb, analyzer)
	require.NoError(t, err)
	reader.reader = bufio.NewReader(bytes.NewReader(payload.Bytes()))
	ctrBad := &container{
		executors:   make([]colexec.ExpressionExecutor, 1),
		spillColPos: []int32{1},
	}
	ok, err = reader.readNextBatch(proc, ctrBad)
	require.False(t, ok)
	require.Error(t, err)
	reader.close(proc)

	spillReaderFile, err := os.CreateTemp("", "mergeorder-spill-reader-*")
	require.NoError(t, err)
	defer os.Remove(spillReaderFile.Name())
	spillRunFile, err := os.CreateTemp("", "mergeorder-spill-run-*")
	require.NoError(t, err)
	defer os.Remove(spillRunFile.Name())

	ctrCleanup := &container{
		spillReaders: []*spillRunReader{{
			file:     spillReaderFile,
			batch:    newValuesBatch(proc, []int8{1}),
			keyBatch: newValuesBatch(proc, []int8{1}),
		}},
		spillRuns: []*spillRun{{
			file: spillRunFile,
		}},
	}
	ctrCleanup.cleanupSpill(proc)
	require.Nil(t, ctrCleanup.spillReaders)
	require.Nil(t, ctrCleanup.spillRuns)
}

func TestAdditionalCoverageBranches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	// prepareInMemoryMerge early return (len <= 1) and pickAndSend empty-heap branch.
	ctr := &container{
		batchList: []*batch.Batch{newValuesBatch(proc, []int8{1})},
	}
	defer ctr.batchList[0].Clean(proc.Mp())
	require.NoError(t, ctr.prepareInMemoryMerge(proc, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}))
	var result vm.CallResult
	sendOver, err := ctr.pickAndSend(proc, &result)
	require.NoError(t, err)
	require.True(t, sendOver)
	if ctr.buf != nil {
		ctr.buf.Clean(proc.Mp())
		ctr.buf = nil
	}

	// setSpillThreshold branch for low computed memory and setSpillAppendPolicy target > hardCap branch.
	oldCache := fileservice.GlobalMemoryCacheSizeHint.Load()
	fileservice.GlobalMemoryCacheSizeHint.Store(int64(^uint64(0) >> 1))
	defer fileservice.GlobalMemoryCacheSizeHint.Store(oldCache)
	var spillCtr container
	spillCtr.setSpillThreshold(0)
	require.Equal(t, int64(128*common.MiB), spillCtr.spillThreshold)
	spillCtr.spillThreshold = 24 * common.MiB
	spillCtr.setSpillAppendPolicy()
	require.True(t, spillCtr.spillAppendEnabled)
	require.Equal(t, int64(24*common.MiB), spillCtr.spillAppendTarget)

	// freeOrderColumns skip branch when vector belongs to the batch.
	bat := newValuesBatch(proc, []int8{1, 2})
	defer bat.Clean(proc.Mp())
	freeOrderColumns(proc.Mp(), bat, []*vector.Vector{bat.Vecs[0]})

	// spillRunReader.close nil receiver branch.
	var nilReader *spillRunReader
	nilReader.close(proc)
}

func TestPrepareInMemoryMergeAndHeapEdgeBranches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()

	b1 := newValuesBatch(proc, []int8{1})
	b2 := newValuesBatch(proc, []int8{2})
	defer b1.Clean(proc.Mp())
	defer b2.Clean(proc.Mp())

	ctr := &container{
		batchList: []*batch.Batch{b1, b2},
		orderCols: [][]*vector.Vector{{b1.Vecs[0]}, {b2.Vecs[0]}},
	}
	require.NoError(t, ctr.prepareInMemoryMerge(proc, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}))

	ctr2 := &container{
		batchList: []*batch.Batch{b1, nil},
	}
	ctr2.initInMemoryHeap()
	require.NotNil(t, ctr2.inMemoryHeap)
}

func BenchmarkOrder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tcs := []orderTestCase{
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 0}}),
			newTestCase(b, []types.Type{types.T_int8.ToType()}, []*plan.OrderBySpec{{Expr: newExpression(0, types.T_int8), Flag: 2}}),
		}
		t := new(testing.T)
		for _, tc := range tcs {
			bats := []*batch.Batch{newRandomBatch(tc.types, tc.proc, BenchmarkRows), batch.EmptyBatch, newRandomBatch(tc.types, tc.proc, BenchmarkRows)}
			resetChildren(tc.arg, bats)
			err := tc.arg.Prepare(tc.proc)
			require.NoError(t, err)
			for {
				ok, err := vm.Exec(tc.arg, tc.proc)
				if ok.Status == vm.ExecStop || err != nil {
					break
				}
			}
		}
	}
}

func newTestCase(t testing.TB, ts []types.Type, fs []*plan.OrderBySpec) orderTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	return orderTestCase{
		types: ts,
		proc:  proc,
		arg: &MergeOrder{
			OrderBySpecs: fs,
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
	}
}

func newExpression(pos int32, typeID types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
		Typ: plan.Type{
			Id: int32(typeID),
		},
	}
}

func newIntBatch(ts []types.Type, proc *process.Process, rows int64, fs []*plan.OrderBySpec) *batch.Batch {
	vs := make([]*vector.Vector, len(ts))
	for i, t := range ts {
		generateDescData := false
		for _, f := range fs {
			if f.Flag == plan.OrderBySpec_DESC {
				index := f.Expr.Expr.(*plan.Expr_Col).Col.ColPos
				if int(index) == i {
					generateDescData = true
				}
			}
		}

		if t.Oid == types.T_int8 {
			values := make([]int8, rows)
			if generateDescData {
				for j := range values {
					values[j] = int8(-j)
				}
			} else {
				for j := range values {
					values[j] = int8(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		} else if t.Oid == types.T_int64 {
			values := make([]int64, rows)
			if generateDescData {
				for j := range values {
					values[j] = int64(-j)
				}
			} else {
				for j := range values {
					values[j] = int64(j + 1)
				}
			}
			vs[i] = testutil.NewVector(int(rows), t, proc.Mp(), false, values)
		}
	}
	zs := make([]int64, rows)
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors(vs, zs)
}

func newRandomBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}

func newValuesBatch(proc *process.Process, values []int8) *batch.Batch {
	vec := testutil.NewVector(len(values), types.T_int8.ToType(), proc.Mp(), false, values)
	zs := make([]int64, len(values))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{vec}, zs)
}

func newCancellationTestSpillRun(
	t testing.TB,
	proc *process.Process,
	ctr *container,
	analyzer process.Analyzer,
	values []int8,
) *spillRun {
	return newCancellationTestSpillRunBatches(t, proc, ctr, analyzer, values)
}

func newCancellationTestSpillRunBatches(
	t testing.TB,
	proc *process.Process,
	ctr *container,
	analyzer process.Analyzer,
	valueBatches ...[]int8,
) *spillRun {
	t.Helper()
	run, err := ctr.createSpillRun(proc)
	require.NoError(t, err)
	writer := bufio.NewWriterSize(run.file, spillIOBufferSize)
	for _, values := range valueBatches {
		bat := newValuesBatch(proc, values)
		rows, _, writeErr := writeSpillBatch(proc, bat, []*vector.Vector{bat.Vecs[0]}, writer, &ctr.spillWriteBuf, analyzer)
		bat.Clean(proc.Mp())
		require.NoError(t, writeErr)
		run.batchCount++
		run.rowCount += rows
	}
	require.NoError(t, writer.Flush())
	_, err = run.file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	return run
}

func newNullableValuesBatch(proc *process.Process, values []int8, nulls []uint64) *batch.Batch {
	vec := testutil.MakeInt8Vector(values, nulls, proc.Mp())
	zs := make([]int64, len(values))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{vec}, zs)
}

func newPairBatch(proc *process.Process, first []int8, second []int64) *batch.Batch {
	zs := make([]int64, len(first))
	for i := range zs {
		zs[i] = 1
	}
	return testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.MakeInt8Vector(first, nil, proc.Mp()),
		testutil.MakeInt64Vector(second, nil, proc.Mp()),
	}, zs)
}

func collectInt8Results(t *testing.T, arg *MergeOrder, proc *process.Process, col int) []int8 {
	var values []int8
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			values = append(values, vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[col])...)
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return values
}

func collectNullableInt8Results(t *testing.T, arg *MergeOrder, proc *process.Process, col int) []int8Row {
	var rows []int8Row
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			vec := result.Batch.Vecs[col]
			values := vector.MustFixedColWithTypeCheck[int8](vec)
			for i := range values {
				rows = append(rows, int8Row{
					value:  values[i],
					isNull: vec.GetNulls().Contains(uint64(i)),
				})
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return rows
}

func collectPairResults(t *testing.T, arg *MergeOrder, proc *process.Process) []pairRow {
	var rows []pairRow
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			first := vector.MustFixedColWithTypeCheck[int8](result.Batch.Vecs[0])
			second := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
			for i := range first {
				rows = append(rows, pairRow{first: first[i], second: second[i]})
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	return rows
}

func resetChildren(arg *MergeOrder, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
