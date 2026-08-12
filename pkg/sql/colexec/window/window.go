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

package window

import (
	"bytes"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "window"

// A Window call can spend a long time inside one frame evaluation, especially
// for running frames whose aggregate work is quadratic in the partition size.
// Keep the cancellation polling overhead bounded while still allowing KILL
// QUERY / KILL CONNECTION to interrupt that work promptly.
const cancellationCheckInterval = 1024

func checkCanceled(proc *process.Process, iteration int) error {
	if iteration&(cancellationCheckInterval-1) != 0 {
		return nil
	}
	if err, canceled := vm.CancelCheck(proc); canceled {
		return err
	}
	return nil
}

func (window *Window) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": window")
}

func (window *Window) OpType() vm.OpType {
	return vm.Window
}

func (window *Window) Prepare(proc *process.Process) (err error) {
	window.ctr.prepareParamKind.Reset(window.Aggs)
	if window.OpAnalyzer == nil {
		window.OpAnalyzer = process.NewAnalyzer(window.GetIdx(), window.IsFirst, window.IsLast, "window")
	} else {
		window.OpAnalyzer.Reset()
	}

	ctr := &window.ctr

	// Runtime frames belong to one Prepare generation. Build them off to the
	// side and publish only after the rest of Prepare succeeds, so neither a
	// bound-evaluation error nor a later setup error exposes partial state.
	ctr.runtimeFrames = nil
	runtimeFrames := make([]*plan.FrameClause, len(window.WinSpecList))
	for i, expr := range window.WinSpecList {
		if expr == nil || expr.GetW() == nil {
			continue
		}
		runtimeFrames[i], err = materializeRowsFrame(proc, expr.GetW().Frame)
		if err != nil {
			return err
		}
	}

	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]colexec.ExprEvalVector, len(window.Aggs))
		for i, ag := range window.Aggs {
			expressions := ag.GetArgExpressions()
			if ctr.aggVecs[i], err = colexec.MakeEvalVector(proc, expressions); err != nil {
				return err
			}
		}
	}

	w := window.WinSpecList[0].Expr.(*plan.Expr_W).W
	if len(w.PartitionBy) == 0 {
		ctr.status = receiveAll
	}

	ctr.runtimeFrames = runtimeFrames
	return nil
}

func materializeRowsFrame(proc *process.Process, planned *plan.FrameClause) (*plan.FrameClause, error) {
	if planned == nil {
		return nil, nil
	}

	runtimeFrame := &plan.FrameClause{Type: planned.Type}
	var err error
	if planned.Type == plan.FrameClause_ROWS {
		runtimeFrame.Start, err = materializeRowsBound(proc, planned.Start)
		if err != nil {
			return nil, err
		}
		runtimeFrame.End, err = materializeRowsBound(proc, planned.End)
		if err != nil {
			return nil, err
		}
		return runtimeFrame, nil
	}

	runtimeFrame.Start = cloneFrameBound(planned.Start)
	runtimeFrame.End = cloneFrameBound(planned.End)
	return runtimeFrame, nil
}

func cloneFrameBound(planned *plan.FrameBound) *plan.FrameBound {
	if planned == nil {
		return nil
	}
	return &plan.FrameBound{
		Type:      planned.Type,
		UnBounded: planned.UnBounded,
		Val:       planned.Val,
	}
}

func materializeRowsBound(proc *process.Process, planned *plan.FrameBound) (*plan.FrameBound, error) {
	runtimeBound := cloneFrameBound(planned)
	if planned == nil || planned.Val == nil || planned.Val.GetLit() != nil {
		return runtimeBound, nil
	}
	if proc == nil || proc.GetPrepareParams() == nil {
		return nil, moerr.NewInvalidInputNoCtx("window frame bound parameter is missing")
	}

	executor, err := colexec.NewExpressionExecutor(proc, planned.Val)
	if err != nil {
		return nil, err
	}
	defer executor.Free()

	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	if vec == nil || vec.Length() != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "window frame bound must evaluate to exactly one value")
	}
	if vec.IsNull(0) {
		return nil, moerr.NewInvalidInput(proc.Ctx, "window frame bound cannot be NULL")
	}
	if vec.GetType().Oid != types.T_uint64 {
		return nil, moerr.NewInvalidInputf(
			proc.Ctx,
			"window frame bound must evaluate to uint64, got %s",
			vec.GetType().String(),
		)
	}

	runtimeBound.Val = &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedColWithTypeCheck[uint64](vec)[0],
			},
		}},
	}
	return runtimeBound, nil
}

func (ctr *container) frameAt(idx int, planned *plan.FrameClause) *plan.FrameClause {
	if idx >= 0 && idx < len(ctr.runtimeFrames) && ctr.runtimeFrames[idx] != nil {
		return ctr.runtimeFrames[idx]
	}
	return planned
}

func (window *Window) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := window.OpAnalyzer

	var err error
	ctr := &window.ctr
	// A returned batch is valid through the caller's processing of it. Once the
	// next Call begins, release its borrowed input windows and owned result
	// vector before reusing any operator state.
	ctr.cleanOutput(proc.Mp())

	for {
		if err, canceled := vm.CancelCheck(proc); canceled {
			return vm.CancelResult, err
		}
		switch ctr.status {
		case receiveAll:
			for {
				result, err := vm.ChildrenCall(window.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				if result.Batch == nil {
					if ctr.bat != nil {
						ctr.status = eval
					} else {
						ctr.status = done
					}
					break
				}
				if result.Batch.IsEmpty() {
					continue
				}
				ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
				if err != nil {
					return result, err
				}
			}
		case receive:
			result, err := vm.ChildrenCall(window.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.status = done
			} else if result.Batch.IsEmpty() {
				continue
			} else {
				ctr.status = eval
				if ctr.bat != nil {
					ctr.bat.CleanOnlyData()
				}
				ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
				if err != nil {
					return result, err
				}
			}
		case eval:
			result := vm.NewCallResult()
			// A new materialized input batch starts a new aggregate generation.
			// Normally the previous generation is released after its last chunk;
			// this also closes reuse after an interrupted or failed generation.
			ctr.freeRunningAgg()
			if err = ctr.evalAggVector(ctr.bat, proc); err != nil {
				return result, err
			}
			for i := range window.Aggs {
				if i < len(ctr.aggVecs) && len(ctr.aggVecs[i].Vec) > 0 {
					arg := ctr.aggVecs[i].Vec[0]
					if arg.Length() > 0 && !arg.AllNull() {
						ctr.prepareParamKind.Observe(i, arg.GetPrepareParamKind())
					}
				}
			}

			// Query planning creates one Window operator per window expression.
			// Keep the materialized and sorted logical partition, then evaluate it
			// lazily in bounded output chunks. A LIMIT consumer can stop after the
			// first chunk without forcing the remaining frame calculations.
			ctr.ps = nil
			ctr.os = nil
			ctr.sels = nil
			w := window.WinSpecList[0]
			if window.Fs = makeOrderBy(w); window.Fs != nil {
				if len(ctr.orderVecs) == 0 {
					ctr.orderVecs = make([]colexec.ExprEvalVector, len(window.Fs))
					for j := range ctr.orderVecs {
						ctr.orderVecs[j], err = colexec.MakeEvalVector(proc, []*plan.Expr{window.Fs[j].Expr})
						if err != nil {
							return result, err
						}
					}
				}

				if _, err = ctr.processOrder(0, window, ctr.bat, proc); err != nil {
					return result, err
				}
			}
			ctr.emitOffset = 0
			ctr.status = emit

		case emit:
			result := vm.NewCallResult()
			start := ctr.emitOffset
			end := min(start+colexec.DefaultBatchSize, ctr.bat.RowCount())
			vec, err := ctr.processFuncRange(0, window, proc, analyzer, start, end)
			if err != nil {
				return result, err
			}
			result.Batch, err = ctr.makeResultBatch(start, end, vec)
			if err != nil {
				vec.Free(proc.Mp())
				return result, err
			}

			ctr.emitOffset = end
			if end == ctr.bat.RowCount() {
				if len(window.WinSpecList[0].Expr.(*plan.Expr_W).W.PartitionBy) == 0 {
					ctr.status = done
				} else {
					ctr.status = receive
				}
			}

			result.Status = vm.ExecNext
			return result, nil
		case done:
			result := vm.NewCallResult()
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) makeResultBatch(start, end int, vec *vector.Vector) (*batch.Batch, error) {
	if vec == nil || vec.Length() != end-start {
		return nil, moerr.NewInternalErrorNoCtx("window result length does not match output chunk")
	}
	rBat, err := ctr.bat.Window(start, end)
	if err != nil {
		return nil, err
	}
	rBat.Vecs = append(rBat.Vecs, vec)
	rBat.SetRowCount(end - start)
	ctr.rBat = rBat
	return rBat, nil
}

// processFunc retains the full-range helper used by focused cancellation
// tests. Production Call uses processFuncRange with bounded output ranges.
func (ctr *container) processFunc(idx int, ap *Window, proc *process.Process, analyzer process.Analyzer) error {
	vec, err := ctr.processFuncRange(idx, ap, proc, analyzer, 0, ctr.bat.RowCount())
	if vec != nil {
		vec.Free(proc.Mp())
	}
	return err
}

func (ctr *container) processFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	analyzer process.Analyzer,
	outputStart int,
	outputEnd int,
) (*vector.Vector, error) {
	if outputStart < 0 || outputEnd <= outputStart || outputEnd > ctr.bat.RowCount() {
		return nil, moerr.NewInternalErrorNoCtx("invalid window output range")
	}

	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	funcName := w.Name
	var (
		vec *vector.Vector
		err error
	)
	switch {
	case function.GetFunctionIsWinValueFunByName(funcName):
		// Value window functions use direct source-row lookup and therefore can
		// materialize only the rows requested by this output chunk.
		vec, err = ctr.processValueFuncRange(idx, ap, proc, outputStart, outputEnd)
	case function.GetFunctionIsWinOrderFunByName(funcName):
		// Rank-family functions are derived directly from the sorted peer
		// boundaries. This avoids building one aggregate group per input row.
		vec, err = ctr.processOrderFuncRange(idx, ap, proc, outputStart, outputEnd)
	default:
		vec, err = ctr.processAggregateFuncRange(idx, ap, proc, outputStart, outputEnd)
	}
	if err != nil {
		return nil, err
	}
	if !vec.HasPrepareParamKind() {
		vec.SetPrepareParamKind(ctr.prepareParamKind.Get(idx))
	}
	analyzer.Alloc(int64(vec.Size()))
	return vec, nil
}

func (ctr *container) makeAggregateExecutor(
	idx int,
	ap *Window,
	proc *process.Process,
	groupCount int,
) error {
	ctr.freeAggFun()
	ctr.batAggs = make([]aggexec.AggFuncExec, len(ap.Aggs))
	exec, err := ctr.newAggregateExecutor(idx, ap, proc, groupCount)
	if err != nil {
		ctr.batAggs = nil
		return err
	}
	ctr.batAggs[idx] = exec
	return nil
}

func (ctr *container) newAggregateExecutor(
	idx int,
	ap *Window,
	proc *process.Process,
	groupCount int,
) (aggexec.AggFuncExec, error) {
	ag := ap.Aggs[idx]
	// Derive one argument type per aggregate argument so multi-argument
	// window aggregates (for example json_objectagg) match Group's contract.
	argExprs := ag.GetArgExpressions()
	argTypes := make([]types.Type, len(argExprs))
	for j, arg := range argExprs {
		argTypes[j] = types.NewWithCharset(
			types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale, uint8(arg.Typ.Charset),
		)
	}

	exec, err := aggexec.MakeAgg(proc.Mp(), ag.GetAggID(), ag.IsDistinct(), argTypes...)
	if err != nil {
		return nil, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			exec.Free()
		}
	}()
	if config := ag.GetExtraInformation(); config != nil {
		if err = exec.SetExtraInformation(config, 0); err != nil {
			return nil, err
		}
	}
	if err = exec.GroupGrow(groupCount); err != nil {
		return nil, err
	}
	succeeded = true
	return exec, nil
}

func (ctr *container) processAggregateFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
) (*vector.Vector, error) {
	if err := ctr.makeAggregateExecutor(idx, ap, proc, outputEnd-outputStart); err != nil {
		return nil, err
	}
	defer ctr.freeAggFun()

	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	frame := ctr.frameAt(idx, w.Frame)
	if cumulativeRowsFrame(frame, ctr.ps, ctr.bat.RowCount()) &&
		aggexec.MergePreservesSource(ctr.batAggs[idx]) {
		return ctr.processCumulativeAggregateFuncRange(idx, ap, proc, outputStart, outputEnd)
	}

	n := ctr.bat.RowCount()
	for j := outputStart; j < outputEnd; j++ {
		if err := checkCanceled(proc, j-outputStart); err != nil {
			return nil, err
		}

		partitionStart, partitionEnd := 0, n
		if ctr.ps != nil {
			partitionStart, partitionEnd = buildPartitionInterval(ctr.ps, j, n)
		}

		left, right, err := ctr.buildInterval(
			j, partitionStart, partitionEnd, frame)
		if err != nil {
			return nil, err
		}
		if right < partitionStart || left > partitionEnd || left >= right {
			continue
		}
		left = max(left, partitionStart)
		right = min(right, partitionEnd)

		group := j - outputStart
		for k := left; k < right; k++ {
			if err = checkCanceled(proc, k-left); err != nil {
				return nil, err
			}
			if err = ctr.batAggs[idx].Fill(group, k, ctr.aggVecs[idx].Vec); err != nil {
				return nil, err
			}
		}
	}

	vecs, err := ctr.batAggs[idx].Flush()
	if err != nil {
		return nil, err
	}
	// groupCount is bounded by DefaultBatchSize (the aggregate chunk size),
	// so this is normally the zero-copy one-vector path. Keep the merge for
	// defensive compatibility with aggregate implementations that split early.
	vec, err := aggexec.MergeSplitResult(vecs, proc.Mp())
	if err != nil {
		return nil, err
	}
	// Aggregate state initializes its physical capacity as NULL. Keep only
	// logical-row nulls so downstream HasNull checks do not see an unused tail.
	nulls.RemoveRange(vec.GetNulls(), uint64(vec.Length()), math.MaxUint64)
	return vec, nil
}

// cumulativeRowsFrame reports whether every frame in the materialized batch
// starts at its partition boundary and ends at the current row. A finite
// PRECEDING bound is equivalent to UNBOUNDED PRECEDING when it covers the
// largest runtime partition.
func cumulativeRowsFrame(frame *plan.FrameClause, partitions []int64, rowCount int) bool {
	if frame == nil || frame.Type != plan.FrameClause_ROWS ||
		frame.Start == nil || frame.End == nil ||
		frame.Start.Type != plan.FrameBound_PRECEDING ||
		frame.End.Type != plan.FrameBound_CURRENT_ROW || frame.End.UnBounded {
		return false
	}
	if frame.Start.UnBounded {
		return true
	}
	if frame.Start.Val == nil || frame.Start.Val.GetLit() == nil {
		return false
	}
	bound, ok := frame.Start.Val.GetLit().Value.(*plan.Literal_U64Val)
	if !ok {
		return false
	}

	maxPartitionRows, ok := largestPartitionSize(partitions, rowCount)
	if !ok {
		return false
	}
	if maxPartitionRows <= 1 {
		return true
	}
	return bound.U64Val >= uint64(maxPartitionRows-1)
}

func largestPartitionSize(partitions []int64, rowCount int) (int, bool) {
	if rowCount < 0 {
		return 0, false
	}
	if len(partitions) == 0 {
		return rowCount, true
	}
	if partitions[0] != 0 {
		return 0, false
	}

	maxRows := 0
	for i, start := range partitions {
		end := int64(rowCount)
		if i+1 < len(partitions) {
			end = partitions[i+1]
		}
		if start < 0 || end <= start || end > int64(rowCount) {
			return 0, false
		}
		maxRows = max(maxRows, int(end-start))
	}
	return maxRows, true
}

func partitionEnd(partitions []int64, partition int, rowCount int) int {
	if len(partitions) == 0 || partition+1 >= len(partitions) {
		return rowCount
	}
	return int(partitions[partition+1])
}

// processCumulativeAggregateFuncRange advances one aggregate state per input
// row and snapshots it into the corresponding output group. This changes
// fixed-size cumulative aggregates such as SUM from O(partitionRows^2) Fill
// calls to O(partitionRows), while retaining state across output chunks.
func (ctr *container) processCumulativeAggregateFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
) (_ *vector.Vector, retErr error) {
	if outputStart != ctr.runningNextRow {
		ctr.freeRunningAgg()
		return nil, moerr.NewInternalErrorNoCtx("cumulative window output is not sequential")
	}
	defer func() {
		if retErr != nil {
			ctr.freeRunningAgg()
		}
	}()

	if ctr.runningAgg == nil {
		ctr.runningAgg, retErr = ctr.newAggregateExecutor(idx, ap, proc, 1)
		if retErr != nil {
			return nil, retErr
		}
	}

	n := ctr.bat.RowCount()
	currentPartitionEnd := partitionEnd(ctr.ps, ctr.runningPartition, n)
	for j := outputStart; j < outputEnd; j++ {
		if err := checkCanceled(proc, j-outputStart); err != nil {
			return nil, err
		}
		if j == currentPartitionEnd {
			ctr.runningAgg.Free()
			ctr.runningAgg, retErr = ctr.newAggregateExecutor(idx, ap, proc, 1)
			if retErr != nil {
				return nil, retErr
			}
			ctr.runningPartition++
			currentPartitionEnd = partitionEnd(ctr.ps, ctr.runningPartition, n)
		}
		if err := ctr.runningAgg.Fill(0, j, ctr.aggVecs[idx].Vec); err != nil {
			return nil, err
		}
		if err := ctr.batAggs[idx].Merge(ctr.runningAgg, j-outputStart, 0); err != nil {
			return nil, err
		}
		ctr.runningNextRow = j + 1
	}

	vecs, err := ctr.batAggs[idx].Flush()
	if err != nil {
		return nil, err
	}
	vec, err := aggexec.MergeSplitResult(vecs, proc.Mp())
	if err != nil {
		return nil, err
	}
	nulls.RemoveRange(vec.GetNulls(), uint64(vec.Length()), math.MaxUint64)
	if outputEnd == n {
		ctr.freeRunningAgg()
	}
	return vec, nil
}

func (ctr *container) processOrderFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
) (*vector.Vector, error) {
	n := ctr.bat.RowCount()
	funcName := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.Name

	if funcName == "percent_rank" || funcName == "cume_dist" {
		values := make([]float64, outputEnd-outputStart)
		peerIndex, peerStart, peerEnd := peerInterval(ctr.os, outputStart, n)
		for j := outputStart; j < outputEnd; j++ {
			if err := checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			for j >= peerEnd {
				peerIndex++
				peerStart = peerEnd
				peerEnd = n
				if peerIndex+1 < len(ctr.os) {
					peerEnd = int(ctr.os[peerIndex+1])
				}
			}
			if funcName == "percent_rank" {
				if n > 1 {
					values[j-outputStart] = float64(peerStart) / float64(n-1)
				}
			} else {
				values[j-outputStart] = float64(peerEnd) / float64(n)
			}
		}
		vec := vector.NewVec(types.T_float64.ToType())
		if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		return vec, nil
	}
	values := make([]int64, outputEnd-outputStart)
	switch funcName {
	case "row_number":
		for j := outputStart; j < outputEnd; j++ {
			if err := checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			values[j-outputStart] = int64(j + 1)
		}
	case "ntile":
		bucketCount, err := ctr.ntileBucketCount(idx)
		if err != nil {
			return nil, err
		}
		for j := outputStart; j < outputEnd; j++ {
			if err := checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			values[j-outputStart] = ntileBucket(int64(j), int64(n), bucketCount)
		}
	case "rank", "dense_rank":
		peerIndex, peerStart, peerEnd := peerInterval(ctr.os, outputStart, n)
		for j := outputStart; j < outputEnd; j++ {
			if err := checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			for j >= peerEnd {
				peerIndex++
				peerStart = peerEnd
				peerEnd = n
				if peerIndex+1 < len(ctr.os) {
					peerEnd = int(ctr.os[peerIndex+1])
				}
			}
			if funcName == "rank" {
				values[j-outputStart] = int64(peerStart + 1)
			} else {
				values[j-outputStart] = int64(peerIndex + 1)
			}
		}
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported order window function: %s", funcName)
	}
	vec := vector.NewVec(types.T_int64.ToType())
	if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	return vec, nil
}

func peerInterval(boundaries []int64, row int, rowCount int) (index, start, end int) {
	if len(boundaries) == 0 {
		return 0, 0, rowCount
	}
	lo, hi := 0, len(boundaries)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if boundaries[mid] <= int64(row) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	index = max(0, lo-1)
	start = int(boundaries[index])
	end = rowCount
	if index+1 < len(boundaries) {
		end = int(boundaries[index+1])
	}
	return index, start, end
}

func (ctr *container) ntileBucketCount(idx int) (int64, error) {
	if idx >= len(ctr.aggVecs) || len(ctr.aggVecs[idx].Vec) == 0 {
		return 1, nil
	}
	vec := ctr.aggVecs[idx].Vec[0]
	if vec.Length() == 0 || vec.IsNull(0) {
		return 1, nil
	}
	bucketCount, ok := getInt64FromVec(vec, 0)
	if !ok {
		return 0, moerr.NewInternalErrorNoCtx("ntile bucket count must be integer type")
	}
	if bucketCount <= 0 {
		return 0, moerr.NewInternalErrorNoCtx("ntile bucket count must be positive")
	}
	return bucketCount, nil
}

func ntileBucket(row, rowCount, bucketCount int64) int64 {
	regularSize := rowCount / bucketCount
	largerBuckets := rowCount % bucketCount
	largerSize := regularSize + 1
	largerRows := largerBuckets * largerSize
	if row < largerRows {
		return row/largerSize + 1
	}
	// When there are more buckets than rows, every emitted row belongs to
	// one of the larger buckets and the division below is unreachable.
	return largerBuckets + (row-largerRows)/regularSize + 1
}

// processValueFunc handles WIN_VALUE functions (lag/lead/first_value/last_value/nth_value)
// by directly computing results via index lookup, avoiding O(n²) frame materialization.
func (ctr *container) processValueFunc(idx int, ap *Window, proc *process.Process) (result *vector.Vector, err error) {
	return ctr.processValueFuncRange(idx, ap, proc, 0, ctr.bat.RowCount())
}

func (ctr *container) processValueFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
) (result *vector.Vector, err error) {
	n := ctr.bat.RowCount()
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	funcName := w.Name

	// aggVecs already evaluated by caller (eval case in Call)
	srcVec := ctr.aggVecs[idx].Vec[0] // the expression column
	retType := types.NewWithCharset(
		types.T(w.WindowFunc.Typ.Id), w.WindowFunc.Typ.Width, w.WindowFunc.Typ.Scale,
		uint8(w.WindowFunc.Typ.Charset),
	)
	localResult := vector.NewVec(retType)
	defer func() {
		if err != nil && localResult != nil {
			localResult.Free(proc.Mp())
			result = nil
		}
	}()

	switch funcName {
	case "lag":
		var offsetVec *vector.Vector
		constOffset, constOK := int64(1), true
		if len(ctr.aggVecs[idx].Vec) >= 2 {
			offsetVec = ctr.aggVecs[idx].Vec[1]
			if offsetVec.IsConst() {
				constOffset, constOK = getInt64FromVec(offsetVec, 0)
			}
		}
		var defaultVec *vector.Vector
		if len(ctr.aggVecs[idx].Vec) >= 3 {
			defaultVec = ctr.aggVecs[idx].Vec[2]
		}
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			offset, ok := constOffset, constOK
			if offsetVec != nil && !offsetVec.IsConst() {
				offset, ok = getInt64FromVec(offsetVec, j)
			}
			if !ok || offset < 0 {
				if err := appendDefaultOrNull(localResult, defaultVec, j, proc.Mp()); err != nil {
					return nil, err
				}
				continue
			}
			start, _ := 0, n
			if ctr.ps != nil {
				start, _ = buildPartitionInterval(ctr.ps, j, n)
			}
			if offset > int64(j-start) {
				if err := appendDefaultOrNull(localResult, defaultVec, j, proc.Mp()); err != nil {
					return nil, err
				}
			} else {
				srcRow := j - int(offset)
				if err := localResult.UnionOne(srcVec, int64(srcRow), proc.Mp()); err != nil {
					return nil, err
				}
			}
		}

	case "lead":
		var offsetVec *vector.Vector
		constOffset, constOK := int64(1), true
		if len(ctr.aggVecs[idx].Vec) >= 2 {
			offsetVec = ctr.aggVecs[idx].Vec[1]
			if offsetVec.IsConst() {
				constOffset, constOK = getInt64FromVec(offsetVec, 0)
			}
		}
		var defaultVec *vector.Vector
		if len(ctr.aggVecs[idx].Vec) >= 3 {
			defaultVec = ctr.aggVecs[idx].Vec[2]
		}
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			offset, ok := constOffset, constOK
			if offsetVec != nil && !offsetVec.IsConst() {
				offset, ok = getInt64FromVec(offsetVec, j)
			}
			if !ok || offset < 0 {
				if err := appendDefaultOrNull(localResult, defaultVec, j, proc.Mp()); err != nil {
					return nil, err
				}
				continue
			}
			_, end := 0, n
			if ctr.ps != nil {
				_, end = buildPartitionInterval(ctr.ps, j, n)
			}
			if offset >= int64(end-j) {
				if err := appendDefaultOrNull(localResult, defaultVec, j, proc.Mp()); err != nil {
					return nil, err
				}
			} else {
				srcRow := j + int(offset)
				if err := localResult.UnionOne(srcVec, int64(srcRow), proc.Mp()); err != nil {
					return nil, err
				}
			}
		}

	case "first_value":
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			start, end := 0, n
			if ctr.ps != nil {
				start, end = buildPartitionInterval(ctr.ps, j, n)
			}
			left, right, err := ctr.buildInterval(j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if left >= right {
				if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
					return nil, err
				}
			} else {
				if err := localResult.UnionOne(srcVec, int64(left), proc.Mp()); err != nil {
					return nil, err
				}
			}
		}

	case "last_value":
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			start, end := 0, n
			if ctr.ps != nil {
				start, end = buildPartitionInterval(ctr.ps, j, n)
			}
			left, right, err := ctr.buildInterval(j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if left >= right {
				if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
					return nil, err
				}
			} else {
				if err := localResult.UnionOne(srcVec, int64(right-1), proc.Mp()); err != nil {
					return nil, err
				}
			}
		}

	case "nth_value":
		// nth_value(expr, n): n is the second argument, must be >= 1
		var nthVec *vector.Vector
		constNth, constOK := int64(1), true
		if len(ctr.aggVecs[idx].Vec) >= 2 {
			nthVec = ctr.aggVecs[idx].Vec[1]
			if nthVec.IsConst() {
				constNth, constOK = getInt64FromVec(nthVec, 0)
			}
		}
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			nthVal, ok := constNth, constOK
			if nthVec != nil && !nthVec.IsConst() {
				nthVal, ok = getInt64FromVec(nthVec, j)
			}
			if !ok || nthVal < 1 {
				if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
					return nil, err
				}
				continue
			}
			start, end := 0, n
			if ctr.ps != nil {
				start, end = buildPartitionInterval(ctr.ps, j, n)
			}
			left, right, err := ctr.buildInterval(j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if left >= right || nthVal > int64(right-left) {
				if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
					return nil, err
				}
			} else {
				targetRow := left + int(nthVal) - 1
				if err := localResult.UnionOne(srcVec, int64(targetRow), proc.Mp()); err != nil {
					return nil, err
				}
			}
		}

	default:
		err = moerr.NewInternalErrorNoCtxf("unsupported value window function: %s", funcName)
		return nil, err
	}

	return localResult, nil
}

// getInt64FromVec extracts an int64 value from a vector at the given row.
// Returns (value, false) if the value is NULL, out of range, or the type is unsupported.
func getInt64FromVec(vec *vector.Vector, row int) (int64, bool) {
	if vec.Length() == 0 || vec.IsNull(uint64(row)) {
		return 0, false
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		return int64(vector.MustFixedColNoTypeCheck[int8](vec)[row]), true
	case types.T_int16:
		return int64(vector.MustFixedColNoTypeCheck[int16](vec)[row]), true
	case types.T_int32:
		return int64(vector.MustFixedColNoTypeCheck[int32](vec)[row]), true
	case types.T_int64:
		return vector.MustFixedColNoTypeCheck[int64](vec)[row], true
	case types.T_uint8:
		return int64(vector.MustFixedColNoTypeCheck[uint8](vec)[row]), true
	case types.T_uint16:
		return int64(vector.MustFixedColNoTypeCheck[uint16](vec)[row]), true
	case types.T_uint32:
		return int64(vector.MustFixedColNoTypeCheck[uint32](vec)[row]), true
	case types.T_uint64:
		v := vector.MustFixedColNoTypeCheck[uint64](vec)[row]
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

// appendDefaultOrNull appends the default value (if provided) or NULL to the result vector.
func appendDefaultOrNull(result *vector.Vector, defaultVec *vector.Vector, rowIdx int, mp *mpool.MPool) error {
	if defaultVec == nil {
		return vector.AppendAny(result, nil, true, mp)
	}
	// Default value vector: use row 0 for const, or rowIdx for non-const
	srcRow := int64(0)
	if !defaultVec.IsConst() {
		srcRow = int64(rowIdx)
	}
	if defaultVec.IsNull(uint64(srcRow)) {
		return vector.AppendAny(result, nil, true, mp)
	}
	return result.UnionOne(defaultVec, srcRow, mp)
}

func (ctr *container) buildInterval(rowIdx, start, end int, frame *plan.FrameClause) (int, int, error) {
	// FrameClause_ROWS
	if frame.Type == plan.FrameClause_ROWS {
		start, end = ctr.buildRowsInterval(rowIdx, start, end, frame)
		return start, end, nil
	}

	if len(ctr.orderVecs) == 0 {
		return start, end, nil
	}

	// FrameClause_Range
	return ctr.buildRangeInterval(rowIdx, start, end, frame)
}

func (ctr *container) buildRowsInterval(rowIdx int, start, end int, frame *plan.FrameClause) (int, int) {
	partitionStart, partitionEnd := start, end
	switch frame.Start.Type {
	case plan.FrameBound_CURRENT_ROW:
		start = rowIdx
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			pre := frame.Start.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if pre >= uint64(rowIdx-partitionStart) {
				start = partitionStart
			} else {
				start = rowIdx - int(pre)
			}
		}
	case plan.FrameBound_FOLLOWING:
		fol := frame.Start.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
		if fol >= uint64(partitionEnd-rowIdx) {
			start = partitionEnd
		} else {
			start = rowIdx + int(fol)
		}
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		end = rowIdx + 1
	case plan.FrameBound_PRECEDING:
		pre := frame.End.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
		if pre >= uint64(rowIdx-partitionStart+1) {
			end = partitionStart
		} else {
			end = rowIdx - int(pre) + 1
		}
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			fol := frame.End.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if fol >= uint64(partitionEnd-rowIdx) {
				end = partitionEnd
			} else {
				end = rowIdx + int(fol) + 1
			}
		}
	}
	return start, end
}

func (ctr *container) buildRangeInterval(rowIdx int, start, end int, frame *plan.FrameClause) (int, int, error) {
	var err error
	var desc bool
	if len(ctr.desc) > 0 {
		desc = ctr.desc[len(ctr.desc)-1]
	}
	switch frame.Start.Type {
	case plan.FrameBound_CURRENT_ROW:
		if len(ctr.os) > 0 || end-start <= 1 {
			start, _ = buildPeerInterval(ctr.os, rowIdx, start, end)
		} else {
			start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_FOLLOWING:
		start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, true, desc)
		if err != nil {
			return start, end, err
		}
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		if len(ctr.os) > 0 || end-start <= 1 {
			_, end = buildPeerInterval(ctr.os, rowIdx, start, end)
		} else {
			end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_PRECEDING:
		end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, true, desc)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	}
	return start, end, nil
}

// buildPeerInterval returns the peer group containing rowIdx. orderBoundaries
// contains the first row of every peer group in the sorted window input.
func buildPeerInterval(orderBoundaries []int64, rowIdx, start, end int) (int, int) {
	low, high := 0, len(orderBoundaries)
	for low < high {
		mid := low + (high-low)/2
		if orderBoundaries[mid] <= int64(rowIdx) {
			low = mid + 1
		} else {
			high = mid
		}
	}

	peerStart, peerEnd := start, end
	if low > 0 && int(orderBoundaries[low-1]) > peerStart {
		peerStart = int(orderBoundaries[low-1])
	}
	if low < len(orderBoundaries) && int(orderBoundaries[low]) < peerEnd {
		peerEnd = int(orderBoundaries[low])
	}
	return peerStart, peerEnd
}

func buildPartitionInterval(ps []int64, j int, l int) (int, int) {
	left, right := 0, 0
	for i, p := range ps {
		if p > int64(j) {
			right = int(p)
			if i == 0 {
				left = 0
			} else {
				left = int(ps[i-1])
			}
			break
		}
	}
	if right == 0 {
		return int(ps[len(ps)-1]), l
	}
	return left, right
}

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) (err error) {
	input := []*batch.Batch{bat}

	for i := range ctr.aggVecs {
		for j := range ctr.aggVecs[i].Executor {
			vec, err := ctr.aggVecs[i].Executor[j].Eval(proc, input, nil)
			if err != nil {
				return err
			}
			if ctr.aggVecs[i].Vec[j] != nil {
				ctr.aggVecs[i].Vec[j].CleanOnlyData()
				if err = ctr.aggVecs[i].Vec[j].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
					return err
				}
				if !ctr.aggVecs[i].Vec[j].HasPrepareParamKind() {
					ctr.aggVecs[i].Vec[j].SetPrepareParamKind(vec.GetPrepareParamKind())
				}
			} else {
				ctr.aggVecs[i].Vec[j], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func makeArgFs(window *Window) {
	window.ctr.desc = make([]bool, len(window.Fs))
	window.ctr.nullsLast = make([]bool, len(window.Fs))
	for i, f := range window.Fs {
		window.ctr.desc[i] = f.Flag&plan.OrderBySpec_DESC != 0
		if f.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
			window.ctr.nullsLast[i] = false
		} else if f.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			window.ctr.nullsLast[i] = true
		} else {
			window.ctr.nullsLast[i] = window.ctr.desc[i]
		}
	}
}

func makeOrderBy(expr *plan.Expr) []*plan.OrderBySpec {
	w := expr.Expr.(*plan.Expr_W).W
	if len(w.PartitionBy) == 0 && len(w.OrderBy) == 0 {
		return nil
	}
	return w.OrderBy
}

func (ctr *container) evalOrderVector(bat *batch.Batch, proc *process.Process) (err error) {
	input := []*batch.Batch{bat}

	for i := range ctr.orderVecs {
		for j := range ctr.orderVecs[i].Executor {
			vec, err := ctr.orderVecs[i].Executor[j].Eval(proc, input, nil)
			if err != nil {
				return err
			}

			if ctr.orderVecs[i].Vec[j] != nil {
				ctr.orderVecs[i].Vec[j].CleanOnlyData()
				if err = ctr.orderVecs[i].Vec[j].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
					return err
				}
			} else {
				ctr.orderVecs[i].Vec[j], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *container) processOrder(idx int, ap *Window, bat *batch.Batch, proc *process.Process) (bool, error) {
	makeArgFs(ap)

	if err := ctr.evalOrderVector(bat, proc); err != nil {
		return false, err
	}
	if bat.RowCount() < 2 {
		return false, nil
	}

	ovec := ctr.orderVecs[0].Vec[0]

	rowCount := bat.RowCount()
	// if ctr.sels == nil {
	//	ctr.sels = make([]int64, rowCount)
	// }
	ctr.sels = make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		if err := checkCanceled(proc, i); err != nil {
			return false, err
		}
		ctr.sels[i] = int64(i)
	}

	// skip sort for const vector
	if !ovec.IsConst() {
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		nullCnt := ovec.GetNulls().Count()
		if nullCnt < ovec.Length() {
			sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.sels, ovec)
		}
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
	}

	ps := make([]int64, 0, 16)
	ds := make([]bool, len(ctr.sels))

	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	n := len(w.PartitionBy)

	i, j := 1, len(ctr.orderVecs)
	for ; i < j; i++ {
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		desc := ctr.desc[i]
		nullsLast := ctr.nullsLast[i]
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		vec := ctr.orderVecs[i].Vec[0]
		// skip sort for const vector
		if !vec.IsConst() {
			nullCnt := vec.GetNulls().Count()
			if nullCnt < vec.Length() {
				for i, j := 0, len(ps); i < j; i++ {
					if err := checkCanceled(proc, i); err != nil {
						return false, err
					}
					if i == j-1 {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:], vec)
					} else {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:ps[i+1]], vec)
					}
				}
			}
		}
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		ovec = vec
		if n == i {
			ctr.ps = make([]int64, len(ps))
			copy(ctr.ps, ps)
		}
	}

	if n == i {
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		ctr.ps = make([]int64, len(ps))
		copy(ctr.ps, ps)
	} else if n == 0 {
		ctr.ps = nil
	}

	if len(ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.OrderBy) > 0 {
		ctr.os = partition.Partition(ctr.sels, ds, ps, ovec)
	} else {
		ctr.os = nil
	}

	if err := bat.Shuffle(ctr.sels, proc.Mp()); err != nil {
		panic(err)
	}

	// shuffle agg vector
	for k := idx; k < len(ctr.aggVecs); k++ {
		for v := range ctr.aggVecs[k].Vec {
			if ctr.aggVecs[k].Vec[v] != nil && !ctr.aggVecs[k].Vec[v].IsConst() {
				if err := ctr.aggVecs[k].Vec[v].Shuffle(ctr.sels, proc.Mp()); err != nil {
					panic(err)
				}
			}
		}
	}

	t := len(ctr.orderVecs) - 1
	if len(ctr.orderVecs[t].Vec) > 0 {
		if err := ctr.orderVecs[t].Vec[0].Shuffle(ctr.sels, proc.Mp()); err != nil {
			panic(err)
		}
	}

	ctr.ps = nil

	return false, nil
}

func searchLeft(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, plus bool, desc bool) (int, error) {
	if vec.GetNulls().Contains(uint64(rowIdx)) {
		// NULL order-key rows are peers; find the start of the NULL peer group
		left := rowIdx
		for left > start && vec.GetNulls().Contains(uint64(left-1)) {
			left--
		}
		return left, nil
	}

	// Confine the binary search to the non-NULL data range within [start, end).
	// When NULLs sort last, the raw-value array is not monotonically sorted
	// (e.g. [1,2,4,0,0]), so binary search must operate on the non-NULL subrange only.
	for start < end && vec.GetNulls().Contains(uint64(start)) {
		start++
	}
	for end > start && vec.GetNulls().Contains(uint64(end-1)) {
		end--
	}

	// For DESC, swap the arithmetic direction.
	if desc {
		plus = !plus
	}

	var left int
	switch vec.GetType().Oid {
	case types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		cmpl := genericGreater[uint64]
		if desc {
			cmpl = genericLess[uint64]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint64], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], cmpl)
			}
		}
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		cmpl := genericGreater[int8]
		if desc {
			cmpl = genericLess[int8]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int8], cmpl)
		} else {
			c := int8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I8Val).I8Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int8], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int8], cmpl)
			}
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		cmpl := genericGreater[int16]
		if desc {
			cmpl = genericLess[int16]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int16], cmpl)
		} else {
			c := int16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I16Val).I16Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int16], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int16], cmpl)
			}
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		cmpl := genericGreater[int32]
		if desc {
			cmpl = genericLess[int32]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int32], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I32Val).I32Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int32], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int32], cmpl)
			}
		}
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		cmpl := genericGreater[int64]
		if desc {
			cmpl = genericLess[int64]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int64], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int64], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int64], cmpl)
			}
		}
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		cmpl := genericGreater[uint8]
		if desc {
			cmpl = genericLess[uint8]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint8], cmpl)
		} else {
			c := uint8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U8Val).U8Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint8], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint8], cmpl)
			}
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		cmpl := genericGreater[uint16]
		if desc {
			cmpl = genericLess[uint16]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint16], cmpl)
		} else {
			c := uint16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U16Val).U16Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint16], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint16], cmpl)
			}
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		cmpl := genericGreater[uint32]
		if desc {
			cmpl = genericLess[uint32]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint32], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U32Val).U32Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint32], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint32], cmpl)
			}
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		cmpl := genericGreater[uint64]
		if desc {
			cmpl = genericLess[uint64]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint64], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], cmpl)
			}
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		cmpl := genericGreater[float32]
		if desc {
			cmpl = genericLess[float32]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float32], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float32], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float32], cmpl)
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		cmpl := genericGreater[float64]
		if desc {
			cmpl = genericLess[float64]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float64], cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float64], cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float64], cmpl)
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		cmpl := decimal64Greater
		if desc {
			cmpl = decimal64Less
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal64Equal, cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal64Val).Decimal64Val.A
			if plus {
				fol, err := col[rowIdx].Add64(types.Decimal64(c))
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal64Equal, cmpl)
			} else {
				fol, err := col[rowIdx].Sub64(types.Decimal64(c))
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal64Equal, cmpl)
			}
		}
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		cmpl := decimal128Greater
		if desc {
			cmpl = decimal128Less
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal128Equal, cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal128Val).Decimal128Val
			if plus {
				fol, err := col[rowIdx].Add128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal128Equal, cmpl)
			} else {
				fol, err := col[rowIdx].Sub128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal128Equal, cmpl)
			}
		}
	case types.T_date:
		col := vector.MustFixedColNoTypeCheck[types.Date](vec)
		cmpl := genericGreater[types.Date]
		if desc {
			cmpl = genericLess[types.Date]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Date], cmpl)
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doDateAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			} else {
				fol, err := doDateSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			}
		}
	case types.T_datetime:
		col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		cmpl := genericGreater[types.Datetime]
		if desc {
			cmpl = genericLess[types.Datetime]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Datetime], cmpl)
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doDatetimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			} else {
				fol, err := doDatetimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			}
		}
	case types.T_time:
		col := vector.MustFixedColNoTypeCheck[types.Time](vec)
		cmpl := genericGreater[types.Time]
		if desc {
			cmpl = genericLess[types.Time]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Time], cmpl)
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doTimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			} else {
				fol, err := doTimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			}
		}
	case types.T_timestamp:
		col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		cmpl := genericGreater[types.Timestamp]
		if desc {
			cmpl = genericLess[types.Timestamp]
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Timestamp], cmpl)
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doTimestampAdd(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			} else {
				fol, err := doTimestampSub(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			}
		}
	default:
		return left, moerr.NewInternalErrorNoCtxf("unsupported type %v for RANGE frame in window function", vec.GetType().Oid)
	}
	return left, nil
}

func doDateSub(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(-diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeSub(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(-diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeSub(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampSub(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func searchRight(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, sub bool, desc bool) (int, error) {
	if vec.GetNulls().Contains(uint64(rowIdx)) {
		// NULL order-key rows are peers; find the end of the NULL peer group (exclusive)
		right := rowIdx + 1
		for right < end && vec.GetNulls().Contains(uint64(right)) {
			right++
		}
		return right, nil
	}

	// Confine the binary search to the non-NULL data range within [start, end).
	// When NULLs sort last, the raw-value array is not monotonically sorted,
	// so binary search must operate on the non-NULL subrange only.
	for start < end && vec.GetNulls().Contains(uint64(start)) {
		start++
	}
	for end > start && vec.GetNulls().Contains(uint64(end-1)) {
		end--
	}

	// For DESC, swap the arithmetic direction.
	if desc {
		sub = !sub
	}

	var right int
	switch vec.GetType().Oid {
	case types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		cmpl := genericGreater[uint64]
		if desc {
			cmpl = genericLess[uint64]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], cmpl)
			}
		}
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		cmpl := genericGreater[int8]
		if desc {
			cmpl = genericLess[int8]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int8])
		} else {
			c := int8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I8Val).I8Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int8], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int8], cmpl)
			}
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		cmpl := genericGreater[int16]
		if desc {
			cmpl = genericLess[int16]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int16])
		} else {
			c := int16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I16Val).I16Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int16], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int16], cmpl)
			}
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		cmpl := genericGreater[int32]
		if desc {
			cmpl = genericLess[int32]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I32Val).I32Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int32], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int32], cmpl)
			}
		}
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		cmpl := genericGreater[int64]
		if desc {
			cmpl = genericLess[int64]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int64], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int64], cmpl)
			}
		}
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		cmpl := genericGreater[uint8]
		if desc {
			cmpl = genericLess[uint8]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint8])
		} else {
			c := uint8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U8Val).U8Val)
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint8], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint8], cmpl)
			}
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		cmpl := genericGreater[uint16]
		if desc {
			cmpl = genericLess[uint16]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint16])
		} else {
			c := uint16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U16Val).U16Val)
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint16], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint16], cmpl)
			}
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		cmpl := genericGreater[uint32]
		if desc {
			cmpl = genericLess[uint32]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U32Val).U32Val
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint32], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint32], cmpl)
			}
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		cmpl := genericGreater[uint64]
		if desc {
			cmpl = genericLess[uint64]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], cmpl)
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], cmpl)
			}
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		cmpl := genericGreater[float32]
		if desc {
			cmpl = genericLess[float32]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[float32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float32], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float32], cmpl)
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		cmpl := genericGreater[float64]
		if desc {
			cmpl = genericLess[float64]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[float64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float64], cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float64], cmpl)
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		cmpl := decimal64Greater
		if desc {
			cmpl = decimal64Less
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], decimal64Equal)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal64Val).Decimal64Val.A
			if sub {
				fol, err := col[rowIdx].Sub64(types.Decimal64(c))
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal64Equal, cmpl)
			} else {
				fol, err := col[rowIdx].Add64(types.Decimal64(c))
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal64Equal, cmpl)
			}
		}
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		cmpl := decimal128Greater
		if desc {
			cmpl = decimal128Less
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], decimal128Equal)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal128Val).Decimal128Val
			if sub {
				fol, err := col[rowIdx].Sub128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal128Equal, cmpl)
			} else {
				fol, err := col[rowIdx].Add128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal128Equal, cmpl)
			}
		}
	case types.T_date:
		col := vector.MustFixedColNoTypeCheck[types.Date](vec)
		cmpl := genericGreater[types.Date]
		if desc {
			cmpl = genericLess[types.Date]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Date])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doDateSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			} else {
				fol, err := doDateAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			}
		}
	case types.T_datetime:
		col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		cmpl := genericGreater[types.Datetime]
		if desc {
			cmpl = genericLess[types.Datetime]
		}
		i := start
		for ; i < end; i++ {
			if !vec.GetNulls().Contains(uint64(i)) {
				break
			}
		}
		for j := start; j < i; j++ {
			col[j] = col[i]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Datetime])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doDatetimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			} else {
				fol, err := doDatetimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			}
		}
	case types.T_time:
		col := vector.MustFixedColNoTypeCheck[types.Time](vec)
		cmpl := genericGreater[types.Time]
		if desc {
			cmpl = genericLess[types.Time]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Time])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doTimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			} else {
				fol, err := doTimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			}
		}
	case types.T_timestamp:
		col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		cmpl := genericGreater[types.Timestamp]
		if desc {
			cmpl = genericLess[types.Timestamp]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Timestamp])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doTimestampSub(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			} else {
				fol, err := doTimestampAdd(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			}
		}
	default:
		return right, moerr.NewInternalErrorNoCtxf("unsupported type %v for RANGE frame in window function", vec.GetType().Oid)
	}
	// genericSearchRight returns high in [start-1, end-1]. When all values > target,
	// high = start-1, so right+1 = start (correct exclusive upper bound).
	return right + 1, nil
}

func doDateAdd(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeAdd(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func genericSearchLeft[T any](low, high int, nums []T, target T, equal func(a, b T) bool, greater func(a, b T) bool) int {
	for low <= high {
		mid := low + (high-low)/2
		if equal(nums[mid], target) {
			high = mid - 1
		} else if greater(nums[mid], target) {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return low
}

func genericSearchRight[T any](low, high int, nums []T, target T, equal func(a, b T) bool, greater func(a, b T) bool) int {
	for low <= high {
		mid := low + (high-low)/2
		if equal(nums[mid], target) {
			low = mid + 1
		} else if greater(nums[mid], target) {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return high
}

func genericSearchEqualRight[T any](low, high int, nums []T, target T, equal func(a, b T) bool) int {
	i := low + 1
	for ; i <= high; i++ {
		if !equal(nums[i], target) {
			break
		}
	}
	return i - 1
}

func genericEqual[T types.OrderedT](a, b T) bool {
	return a == b
}

func genericGreater[T types.OrderedT](a, b T) bool {
	return a > b
}

func genericLess[T types.OrderedT](a, b T) bool {
	return a < b
}

func decimal64Equal(a, b types.Decimal64) bool {
	return a.Compare(b) == 0
}

func decimal64Greater(a, b types.Decimal64) bool {
	return a.Compare(b) == 1
}

func decimal64Less(a, b types.Decimal64) bool {
	return a.Compare(b) == -1
}

func decimal128Equal(a, b types.Decimal128) bool {
	return a.Compare(b) == 0
}

func decimal128Greater(a, b types.Decimal128) bool {
	return a.Compare(b) == 1
}

func decimal128Less(a, b types.Decimal128) bool {
	return a.Compare(b) == -1
}
