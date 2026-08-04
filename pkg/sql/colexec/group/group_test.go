// Copyright 2024 Matrix Origin
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

package group

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// mock batch schema: (a int32, b uuid, c varchar, d json, e datetime)
// col 0 = a int32

func colExpr(pos int32, t types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(t)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func sumAgg(pos int32) aggexec.AggFuncExecExpression {
	e, _ := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	return aggexec.MakeAggFunctionExpression(e.GetEncodedOverloadID(), false, []*plan.Expr{colExpr(pos, types.T_int32)}, nil)
}

func countStarAgg() aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
}

func orderedGroupConcatAgg(distinct bool) aggexec.AggFuncExecExpression {
	config := []byte{2}
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = append(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = binary.BigEndian.AppendUint32(config, 1)
	config = append(config, '|')
	return aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfGroupConcat,
		distinct,
		[]*plan.Expr{colExpr(1, types.T_varchar), colExpr(2, types.T_int64)},
		config,
		plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
	)
}

func newGroupOp(proc *process.Process, groupBy []*plan.Expr, aggs []aggexec.AggFuncExecExpression) *Group {
	g := NewArgument()
	g.GroupBy = groupBy
	g.Aggs = aggs
	g.NeedEval = true
	g.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return g
}

func newMergeGroupOp(aggs []aggexec.AggFuncExecExpression) *MergeGroup {
	mg := NewArgumentMergeGroup()
	mg.Aggs = aggs
	mg.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return mg
}

type cancelOnDoneCheckContext struct {
	context.Context
	remaining int
	done      chan struct{}
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

func resetChildren(g *Group, proc *process.Process) {
	bat := colexec.MakeMockBatchs(proc.Mp())
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	g.Children = nil
	g.AppendChild(op)
}

func collectBatches(t *testing.T, op vm.Operator, proc *process.Process) []*batch.Batch {
	t.Helper()

	var result []*batch.Batch
	for {
		ret, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if ret.Status == vm.ExecStop || ret.Batch == nil {
			return result
		}
		result = append(result, ret.Batch)
	}
}

func cloneBatch(t *testing.T, proc *process.Process, bat *batch.Batch) *batch.Batch {
	t.Helper()

	cloned, err := bat.Dup(proc.Mp())
	require.NoError(t, err)
	cloned.ExtraBuf = append(cloned.ExtraBuf[:0], bat.ExtraBuf...)
	return cloned
}

func buildPartialGroupBatches(t *testing.T, proc *process.Process, sources []*batch.Batch, forceGroupTypesNotNull bool) []*batch.Batch {
	t.Helper()

	groupBy := []*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)}
	partialBatches := make([]*batch.Batch, 0, len(sources))
	for _, source := range sources {
		partial := newGroupOp(proc, groupBy, []aggexec.AggFuncExecExpression{countStarAgg()})
		partial.NeedEval = false
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		require.NoError(t, partial.Prepare(proc))
		rawPartialBatches := collectBatches(t, partial, proc)
		require.Len(t, rawPartialBatches, 1)
		for _, bat := range rawPartialBatches {
			cloned := cloneBatch(t, proc, bat)
			if forceGroupTypesNotNull {
				cloned.Vecs[0].GetType().SetNotNull(true)
				cloned.Vecs[1].GetType().SetNotNull(true)
			}
			partialBatches = append(partialBatches, cloned)
		}
		partial.Free(proc, false, nil)
	}
	return partialBatches
}

func assertMergedTicketCounts(t *testing.T, finals []*batch.Batch, wantNull, wantNonNull int64) {
	t.Helper()

	var nullCount, nonNullCount int64
	totalRows := 0
	for _, final := range finals {
		if final == nil || final.RowCount() == 0 || len(final.Vecs) == 0 {
			continue
		}
		require.Len(t, final.Vecs, 3)

		tickets := vector.MustFixedColNoTypeCheck[int32](final.Vecs[0])
		customers := vector.MustFixedColNoTypeCheck[int32](final.Vecs[1])
		counts := vector.MustFixedColNoTypeCheck[int64](final.Vecs[2])
		totalRows += final.RowCount()

		for i := 0; i < final.RowCount(); i++ {
			require.Equal(t, int32(1), tickets[i])
			if final.Vecs[1].GetNulls().Contains(uint64(i)) {
				nullCount = counts[i]
				continue
			}
			require.Equal(t, int32(10), customers[i])
			nonNullCount = counts[i]
		}
	}

	require.Equal(t, 2, totalRows)
	require.Equal(t, wantNull, nullCount)
	require.Equal(t, wantNonNull, nonNullCount)
}

func TestGroupString(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	buf := new(bytes.Buffer)
	g.String(buf)
	require.NotEmpty(t, buf.String())
}

func TestGroupPrepare(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))
	g.Free(proc, false, nil)
}

// TestGroupByWithSum: GROUP BY a, SUM(a) — two distinct rows → two groups.
func TestGroupByWithSum(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount, execCalls int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		execCalls++
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	// mock batch has 2 rows with distinct values (1, 1000) → 2 groups
	require.Equal(t, 2, rowCount)
	require.Equal(t, execCalls, g.OpAnalyzer.GetOpStats().CallNum)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestGroupNoGroupBy: no GROUP BY, just COUNT(*) → single row result.
func TestGroupNoGroupBy(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countStarAgg()})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	require.Equal(t, 1, rowCount)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGroupSpillReloadKeepsPreallocationBounded(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const rows = 65536
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)

	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	// Values below 10K are interpreted as a group-count spill threshold.
	// One large input batch therefore establishes a 65,536-group high-water
	// mark, while each of the 32 reload buckets is only about 2K groups.
	g.SpillMem = 4096
	g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, g.Prepare(proc))

	var outputRows int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		outputRows += result.Batch.RowCount()
	}

	require.Equal(t, rows, outputRows)
	require.Equal(t, uint64(rows), g.ctr.spillHashPreAllocSize)
	extra := g.OpAnalyzer.GetOpStats().ExtraStats
	require.Positive(t, extra["GroupSpillWriteCalls"])
	require.Positive(t, extra["GroupSpillWriteNanos"])
	require.Positive(t, extra["GroupSpillSerializedBytes"])
	require.Positive(t, extra["GroupSpillAggChunkHeadersOmitted"])
	require.Positive(t, extra["GroupSpillReloadBuckets"])
	require.Positive(t, extra["GroupSpillReloadRecords"])
	require.Positive(t, extra["GroupSpillAggExecReuseRecords"])
	require.Equal(t, int64(rows), extra["GroupSpillReloadRows"])
	require.Equal(t, int64(rows), extra["GroupSpillMaxGroups"])
	require.Greater(t, extra["GroupSpillPreallocRows"], int64(aggHtPreAllocSize))
	require.Positive(t, extra["GroupSpillReloadNanos"])
	require.Equal(t, int64(1), extra["GroupHashBuildGrowthBatches"])
	require.Positive(t, extra["GroupHashBuildGrowthBytes"])
	g.Free(proc, false, nil)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestGroupSpillReloadHonorsCancellationAfterInput(t *testing.T) {
	proc := testutil.NewProcess(t)

	const rows = 65536
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}).WithEndOfDataCallback(cancel)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 4096
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
}

func TestGroupSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 1, 3, 2}, nil, proc.Mp())
	input.SetRowCount(4)
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{input}).
		WithBatchCallback(func(int) { cancel() })
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	require.Nil(t, g.ctr.currentSpillBkt)
}

func TestGroupSpillWriteStopsAtBucketBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	require.NoError(t, g.Prepare(proc))

	const rows = 1024
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	input.SetRowCount(rows)
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		proc.Ctx = baseCtx
		g.Free(proc, true, context.Canceled)
		input.Clean(proc.Mp())
		proc.Free()
	})
	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)

	hashCodes := make([]uint64, g.ctr.hr.Hash.GroupCount())
	hashCodes = g.ctr.hr.Hash.FillGroupHashes(hashCodes)
	g.ctr.computeBucketIndex(hashCodes, 1)
	usedBuckets := make(map[int]struct{})
	firstBucket := spillNumBuckets
	for _, hashCode := range hashCodes {
		bucketIndex := int(hashCode & (spillNumBuckets - 1))
		usedBuckets[bucketIndex] = struct{}{}
		firstBucket = min(firstBucket, bucketIndex)
	}
	require.Greater(t, len(usedBuckets), 1)

	g.ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
	for i := range g.ctr.currentSpillBkt {
		g.ctr.currentSpillBkt[i] = &spillBucket{lv: 1, name: fmt.Sprintf("cancel-boundary-%d", i)}
	}
	file, err := os.CreateTemp(t.TempDir(), "group-cancel-boundary-*")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(baseCtx)
	writer := &cancelAfterWriteWriter{cancel: cancel}
	g.ctr.currentSpillBkt[firstBucket].file = file
	g.ctr.currentSpillBkt[firstBucket].writer = bufio.NewWriterSize(writer, 1)
	proc.Ctx = ctx

	_, _, err = g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)
	require.Equal(t, int64(1), g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])

	proc.Ctx = baseCtx
	g.Free(proc, true, context.Canceled)
	input.Clean(proc.Mp())
	proc.Free()
	cleaned = true
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupSpillWriteStopsAfterLastBucket(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 1
	require.NoError(t, g.Prepare(proc))

	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
	input.SetRowCount(1)
	t.Cleanup(func() {
		proc.Ctx = baseCtx
		if g.ctr.mp != nil {
			g.Free(proc, true, context.Canceled)
		}
		input.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	needSpill, err := g.buildOneBatch(proc, input)
	require.NoError(t, err)
	require.True(t, needSpill)
	hashCodes := make([]uint64, g.ctr.hr.Hash.GroupCount())
	hashCodes = g.ctr.hr.Hash.FillGroupHashes(hashCodes)
	g.ctr.computeBucketIndex(hashCodes, 1)
	require.Len(t, hashCodes, 1)
	bucketIndex := int(hashCodes[0] & (spillNumBuckets - 1))

	g.ctr.currentSpillBkt = make([]*spillBucket, spillNumBuckets)
	for i := range g.ctr.currentSpillBkt {
		g.ctr.currentSpillBkt[i] = &spillBucket{lv: 1, name: fmt.Sprintf("cancel-last-%d", i)}
	}
	file, err := os.CreateTemp(t.TempDir(), "group-cancel-last-*")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(baseCtx)
	writer := &cancelAfterWriteWriter{cancel: cancel}
	g.ctr.currentSpillBkt[bucketIndex].file = file
	g.ctr.currentSpillBkt[bucketIndex].writer = bufio.NewWriterSize(writer, 1)
	proc.Ctx = ctx

	_, _, err = g.ctr.spillDataToDisk(proc, g.OpAnalyzer, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, writer.writes)
	require.Equal(t, int64(1), g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
}

func TestGroupSpillReloadCancellationCleansAndReuses(t *testing.T) {
	const (
		cancelAtLoadEntry = iota
		cancelDuringBucketTransfer
		cancelAfterBucketTransfer
		cancelAfterFirstRecord
	)

	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx

	const rows = 65536
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{countStarAgg()})
	g.SpillMem = 4096
	var spillFiles []*os.File
	var child *colexec.MockOperator
	var nonEmptyBuckets int
	installSpillInput := func(cancelPoint int) {
		values := make([]int32, rows)
		for i := range values {
			values[i] = int32(i)
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(rows)
		spillFiles = nil
		nonEmptyBuckets = 0
		child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}).WithEndOfDataCallback(func() {
			for _, bkt := range g.ctr.currentSpillBkt {
				if bkt.file != nil {
					spillFiles = append(spillFiles, bkt.file)
				}
				if bkt.cnt > 0 {
					nonEmptyBuckets++
				}
			}
			checksAfterEOF := 3 // EOF boundary, final empty spill, load entry.
			switch cancelPoint {
			case cancelDuringBucketTransfer:
				// Pass the first bucket-flush check and cancel before the second.
				checksAfterEOF = 5
			case cancelAfterBucketTransfer:
				checksAfterEOF = nonEmptyBuckets + 4
			case cancelAfterFirstRecord:
				// Also pass every bucket flush, the post-transfer boundary, and
				// the first record checkpoint; cancel before the second record.
				checksAfterEOF = nonEmptyBuckets + 6
			}
			proc.Ctx = newCancelOnDoneCheckContext(baseCtx, checksAfterEOF)
		})
		g.Children = nil
		g.AppendChild(child)
	}
	installSpillInput(cancelAtLoadEntry)
	require.NoError(t, g.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	// Cancel exactly at loadSpilledData entry. Current buckets have not
	// transferred yet and Reset remains their sole cleanup owner.
	result, err := g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])
	require.NotEmpty(t, g.ctr.currentSpillBkt)
	require.NotEmpty(t, spillFiles)
	entryFiles := append([]*os.File(nil), spillFiles...)

	g.Reset(proc, true, context.Canceled)
	for _, file := range entryFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelDuringBucketTransfer)
	require.NoError(t, g.Prepare(proc))

	// One bucket has transferred to spillBkts; all remaining bucket files stay
	// uniquely owned by currentSpillBkt.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Equal(t, spillNumBuckets, nonEmptyBuckets)
	require.NotNil(t, g.ctr.spillBkts)
	require.Equal(t, 1, g.ctr.spillBkts.Len())
	require.NotNil(t, g.ctr.currentSpillBkt)
	transferredSlots := 0
	for _, bkt := range g.ctr.currentSpillBkt {
		if bkt == nil {
			transferredSlots++
		}
	}
	require.Equal(t, 1, transferredSlots)

	transferFiles := append([]*os.File(nil), spillFiles...)
	g.Reset(proc, true, context.Canceled)
	for _, file := range transferFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelAfterBucketTransfer)
	require.NoError(t, g.Prepare(proc))

	// All buckets have transferred to spillBkts, but cancellation is observed
	// before a bucket is popped for reload.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Nil(t, g.ctr.currentSpillBkt)
	require.NotNil(t, g.ctr.spillBkts)
	require.Equal(t, nonEmptyBuckets, g.ctr.spillBkts.Len())
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadBuckets"])

	postTransferFiles := append([]*os.File(nil), spillFiles...)
	g.Reset(proc, true, context.Canceled)
	for _, file := range postTransferFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	installSpillInput(cancelAfterFirstRecord)
	require.NoError(t, g.Prepare(proc))

	// After EOF: pass the phase boundaries and every bucket ownership transfer,
	// then process one record and cancel at the next per-record checkpoint.
	result, err = g.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadRecords"])
	require.NotEmpty(t, spillFiles)
	require.NotNil(t, g.ctr.spillBkts)
	require.Positive(t, g.ctr.spillBkts.Len())

	g.Reset(proc, true, context.Canceled)
	require.Nil(t, g.ctr.mp)
	require.Nil(t, g.ctr.aggList)
	require.Nil(t, g.ctr.spillAggList)
	require.Nil(t, g.ctr.currentSpillBkt)
	for _, file := range spillFiles {
		_, statErr := file.Stat()
		require.Error(t, statErr)
	}
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	fresh := batch.NewWithSize(1)
	fresh.Vecs[0] = testutil.MakeInt32Vector([]int32{4, 1, 3, 2}, nil, proc.Mp())
	fresh.SetRowCount(4)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{fresh})
	g.Children = nil
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))

	var outputRows int
	for _, output := range collectBatches(t, g, proc) {
		outputRows += output.RowCount()
	}
	require.Equal(t, 4, outputRows)
}

func TestGroupedOrderedGroupConcatComposesWithGenericSpill(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprintf("distinct=%t", distinct), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()

			const rows = 512
			groups := make([]int32, rows)
			values := make([]string, rows)
			orderKeys := make([]int64, rows)
			for i := range rows {
				groups[i] = 1
				values[i] = fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 256))
				orderKeys[i] = int64(rows - i)
			}
			input := batch.NewWithSize(3)
			input.Vecs[0] = testutil.MakeInt32Vector(groups, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeVarcharVector(values, nil, proc.Mp())
			input.Vecs[2] = testutil.MakeInt64Vector(orderKeys, nil, proc.Mp())
			input.SetRowCount(rows)

			g := newGroupOp(
				proc,
				[]*plan.Expr{colExpr(0, types.T_int32)},
				[]aggexec.AggFuncExecExpression{orderedGroupConcatAgg(distinct), countStarAgg()},
			)
			g.SpillMem = 64 << 10
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, g.Prepare(proc))

			results := collectBatches(t, g, proc)
			require.Len(t, results, 1)
			require.Equal(t, 1, results[0].RowCount())
			require.Equal(t, int64(rows), vector.MustFixedColNoTypeCheck[int64](results[0].Vecs[2])[0])
			parts := strings.Split(string(results[0].Vecs[1].GetBytesAt(0)), "|")
			require.Len(t, parts, rows)
			require.Equal(t, values[rows-1], parts[0])
			require.Equal(t, values[0], parts[rows-1])

			extra := g.OpAnalyzer.GetOpStats().ExtraStats
			require.Equal(t, int64(spillMaxPass), extra["GroupSpillMaxLevel"])
			require.Positive(t, extra["GroupSpillRespills"])
			g.Free(proc, false, nil)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestSpillReloadPreallocationRespectsByteLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	const requested = uint64(1 << 20)
	ctr := container{
		mp:                    proc.Mp(),
		mtyp:                  H8,
		spillMem:              1 << 20,
		spillHashPreAllocSize: requested,
	}
	got := ctr.boundedSpillReloadPreAlloc(int64(requested))
	require.Less(t, got, requested)
	require.LessOrEqual(t,
		hashtable.Int64HashMapInitialAllocationBytes()+hashtable.EstimateInt64HashMapSize(got),
		uint64(ctr.spillMem))

	// The sub-10K test mode is a group-count threshold rather than a byte
	// budget, but the proven high-water cap still applies.
	ctr.spillMem = 4096
	ctr.spillHashPreAllocSize = 2048
	require.Equal(t, uint64(2048), ctr.boundedSpillReloadPreAlloc(8192))
}

// TestGroupResetAndReuse: verify Reset allows the operator to be reused correctly.
func TestGroupResetAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})

	for i := 0; i < 2; i++ {
		resetChildren(g, proc)
		require.NoError(t, g.Prepare(proc))
		for {
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			if result.Status == vm.ExecStop || result.Batch == nil {
				break
			}
		}
		g.Reset(proc, false, nil)
	}

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMergeGroupPreservesLateNullableGroupKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 10}, nil, proc.Mp())
	first.SetRowCount(2)

	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{0, 0}, []uint64{0, 1}, proc.Mp())
	second.SetRowCount(2)

	partialBatches := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, true)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partialBatches))
	require.NoError(t, merge.Prepare(proc))
	finalBatches := collectBatches(t, merge, proc)
	require.Len(t, finalBatches, 1)
	require.Equal(t, len(finalBatches)+1, merge.OpAnalyzer.GetOpStats().CallNum)
	assertMergedTicketCounts(t, finalBatches, 2, 2)
	merge.Free(proc, false, nil)
}

func TestMergeGroupHonorsCancellationAfterInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	partial := batch.NewWithSize(1)
	partial.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	partial.SetRowCount(3)

	var extra bytes.Buffer
	mtyp := int32(H8)
	nullable := false
	nAggs := int32(0)
	extra.Write(types.EncodeInt32(&mtyp))
	extra.Write(types.EncodeBool(&nullable))
	extra.Write(types.EncodeInt32(&nAggs))
	partial.ExtraBuf = extra.Bytes()

	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{partial}).WithEndOfDataCallback(cancel)
	merge := newMergeGroupOp(nil)
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		merge.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(merge, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	merge.Reset(proc, true, context.Canceled)
	require.Nil(t, merge.ctr.mp)
	child.Free(proc, true, context.Canceled)

	proc.Ctx = baseCtx
	freshPartial := batch.NewWithSize(1)
	freshPartial.Vecs[0] = testutil.MakeInt32Vector([]int32{3, 1, 2}, nil, proc.Mp())
	freshPartial.SetRowCount(3)
	freshPartial.ExtraBuf = append(freshPartial.ExtraBuf[:0], extra.Bytes()...)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{freshPartial})
	merge.Children = nil
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	outputs := collectBatches(t, merge, proc)
	require.Len(t, outputs, 1)
	require.Equal(t, 3, outputs[0].RowCount())
}

func TestMergeGroupSpillWriteHonorsCancellationAfterInputBatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	baseCtx := proc.Ctx
	partial := batch.NewWithSize(1)
	partial.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	partial.SetRowCount(3)

	var extra bytes.Buffer
	mtyp := int32(H8)
	nullable := false
	nAggs := int32(0)
	extra.Write(types.EncodeInt32(&mtyp))
	extra.Write(types.EncodeBool(&nullable))
	extra.Write(types.EncodeInt32(&nAggs))
	partial.ExtraBuf = extra.Bytes()

	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{partial}).
		WithBatchCallback(func(int) { cancel() })
	merge := newMergeGroupOp(nil)
	merge.SpillMem = 1
	merge.AppendChild(child)
	require.NoError(t, merge.Prepare(proc))

	t.Cleanup(func() {
		proc.Ctx = baseCtx
		merge.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	result, err := vm.Exec(merge, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Zero(t, merge.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
	require.Nil(t, merge.ctr.currentSpillBkt)
}

func TestMergeGroupFreesSpillAggListAfterBatchMerge(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 10}, nil, proc.Mp())
	first.SetRowCount(2)

	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 2}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 20}, nil, proc.Mp())
	second.SetRowCount(2)

	partialBatches := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, false)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	require.NoError(t, merge.Prepare(proc))
	defer merge.Free(proc, false, nil)

	for _, partial := range partialBatches {
		_, err := merge.buildOneBatch(proc, partial)
		require.NoError(t, err)
		require.Nil(t, merge.ctr.spillAggList)
	}
}

func TestFreeAggListPartial(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 3)
	for i := 0; i < 3; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggListPartial(aggList, 2)
	freeAggListPartial(aggList, 3)
}

func TestFreeAggList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 2)
	for i := 0; i < 2; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggList(aggList)
}

func TestFreeAggListPartialWithNilEntries(t *testing.T) {
	aggList := make([]aggexec.AggFuncExec, 3)

	freeAggListPartial(aggList, 3)
	freeAggList(aggList)
}

func TestMakeAggListFreesPartialOnCreationError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(-1, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil),
	})
	require.Error(t, err)
}

func TestMakeAggListFreesPartialOnExtraConfigError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin,
			false,
			[]*plan.Expr{colExpr(0, types.T_int32)},
			[]byte("bad-config"),
		),
	})
	require.Error(t, err)
}
