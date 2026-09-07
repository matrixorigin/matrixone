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
	"context"
	"math"
	"strconv"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
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
		runtimeFrames[i], err = materializeWindowFrame(proc, expr.GetW().Frame)
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
	if len(w.PartitionBy) == 0 || window.PartitionTopN {
		ctr.status = receiveAll
	}

	ctr.runtimeFrames = runtimeFrames
	return nil
}

func materializeWindowFrame(proc *process.Process, planned *plan.FrameClause) (*plan.FrameClause, error) {
	if planned == nil {
		return nil, nil
	}

	runtimeFrame := &plan.FrameClause{Type: planned.Type}
	var err error
	if planned.Type == plan.FrameClause_ROWS || planned.Type == plan.FrameClause_RANGE {
		runtimeFrame.Start, err = materializeWindowBound(proc, planned.Start, planned.Type)
		if err != nil {
			return nil, err
		}
		runtimeFrame.End, err = materializeWindowBound(proc, planned.End, planned.Type)
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

func materializeWindowBound(
	proc *process.Process,
	planned *plan.FrameBound,
	frameType plan.FrameClause_FrameType,
) (*plan.FrameBound, error) {
	runtimeBound := cloneFrameBound(planned)
	if planned == nil || planned.Val == nil || planned.Val.GetLit() != nil {
		return runtimeBound, nil
	}
	// Temporal RANGE bounds are normalized interval expression lists at bind
	// time. They are already immutable constants, not deferred parameters.
	if frameType == plan.FrameClause_RANGE && planned.Val.GetList() != nil {
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
	if frameType == plan.FrameClause_ROWS && vec.GetType().Oid != types.T_uint64 {
		return nil, moerr.NewInvalidInputf(
			proc.Ctx,
			"window frame bound must evaluate to uint64, got %s",
			vec.GetType().String(),
		)
	}
	if frameType == plan.FrameClause_RANGE {
		if err = validateRangeFrameBound(proc.Ctx, vec); err != nil {
			return nil, err
		}
	}

	runtimeBound.Val = &plan.Expr{
		Typ:  planned.Val.Typ,
		Expr: &plan.Expr_Lit{Lit: rule.GetConstantValue(vec, false, 0)},
	}
	return runtimeBound, nil
}

func validateRangeFrameBound(ctx context.Context, vec *vector.Vector) error {
	valid := true
	switch vec.GetType().Oid {
	case types.T_bit, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8:
		valid = vector.MustFixedColWithTypeCheck[int8](vec)[0] >= 0
	case types.T_int16:
		valid = vector.MustFixedColWithTypeCheck[int16](vec)[0] >= 0
	case types.T_int32:
		valid = vector.MustFixedColWithTypeCheck[int32](vec)[0] >= 0
	case types.T_int64:
		valid = vector.MustFixedColWithTypeCheck[int64](vec)[0] >= 0
	case types.T_float32:
		value := vector.MustFixedColWithTypeCheck[float32](vec)[0]
		valid = value >= 0 && !math.IsInf(float64(value), 0) && !math.IsNaN(float64(value))
	case types.T_float64:
		value := vector.MustFixedColWithTypeCheck[float64](vec)[0]
		valid = value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
	case types.T_decimal64:
		valid = !vector.MustFixedColWithTypeCheck[types.Decimal64](vec)[0].Sign()
	case types.T_decimal128:
		valid = !vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0].Sign()
	default:
		return moerr.NewInvalidInputf(
			ctx,
			"window RANGE frame bound must be numeric, got %s",
			vec.GetType().String(),
		)
	}
	if !valid {
		return moerr.NewInvalidInput(ctx, "window RANGE frame bound must be a finite non-negative numeric value")
	}
	return nil
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
			if err = ctr.validateLagLeadOffsets(0, window, proc); err != nil {
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
			if window.PartitionTopN {
				window.Fs = makePartitionTopNOrderBy(w)
			} else {
				window.Fs = makeOrderBy(w)
			}
			if window.Fs != nil {
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
	if (boundedSlidingRowsFrame(frame) || boundedSlidingRangeFrame(frame, ctr.orderVecs)) &&
		aggexec.MergePreservesSource(ctr.batAggs[idx]) &&
		aggexec.SupportsWindowSliding(ctr.batAggs[idx]) {
		return ctr.processSlidingAggregateFuncRange(idx, ap, proc, outputStart, outputEnd, frame)
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

		left, right, selection, err := ctr.buildIntervalRows(
			proc, j, partitionStart, partitionEnd, frame)
		if err != nil {
			return nil, err
		}
		if selection == nil && (right < partitionStart || left > partitionEnd || left >= right) {
			continue
		}
		left = max(left, partitionStart)
		right = min(right, partitionEnd)

		group := j - outputStart
		if selection == nil {
			for k := left; k < right; k++ {
				if err = checkCanceled(proc, k-left); err != nil {
					return nil, err
				}
				if err = ctr.batAggs[idx].Fill(group, k, ctr.aggVecs[idx].Vec); err != nil {
					return nil, err
				}
			}
		} else {
			iteration := 0
			for _, span := range selection.spans {
				for frameRow := span.start; frameRow < span.end; frameRow++ {
					if err = checkCanceled(proc, iteration); err != nil {
						return nil, err
					}
					iteration++
					if err = ctr.batAggs[idx].Fill(group, frameRow, ctr.aggVecs[idx].Vec); err != nil {
						return nil, err
					}
				}
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

// boundedSlidingRowsFrame recognizes the finite ROWS shape whose left and
// right edges each advance monotonically by one row. Aggregates that expose an
// exact inverse can evaluate it with one add and one remove per output row.
func boundedSlidingRowsFrame(frame *plan.FrameClause) bool {
	if frame == nil || frame.Type != plan.FrameClause_ROWS ||
		frame.Start == nil || frame.End == nil ||
		frame.Start.Type != plan.FrameBound_PRECEDING || frame.Start.UnBounded ||
		frame.End.Type != plan.FrameBound_CURRENT_ROW || frame.End.UnBounded ||
		frame.Start.Val == nil || frame.Start.Val.GetLit() == nil {
		return false
	}
	_, ok := frame.Start.Val.GetLit().Value.(*plan.Literal_U64Val)
	return ok
}

// boundedSlidingRangeFrame recognizes a contiguous, finite RANGE frame whose
// boundaries move monotonically and which always contains the current peer
// group. Floating-point order keys stay on the ordinary evaluator because NaN
// ordering is not suitable for inverse aggregation. TIMESTAMP stays there too:
// session-civil frames can be disjoint across a daylight-saving-time fold.
func boundedSlidingRangeFrame(frame *plan.FrameClause, orderVecs []colexec.ExprEvalVector) bool {
	if frame == nil || frame.Type != plan.FrameClause_RANGE ||
		frame.Start == nil || frame.End == nil ||
		len(orderVecs) != 1 || len(orderVecs[0].Vec) != 1 || orderVecs[0].Vec[0] == nil ||
		!finiteRangeStart(frame.Start) || !finiteRangeEnd(frame.End) {
		return false
	}

	switch orderVecs[0].Vec[0].GetType().Oid {
	case types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime, types.T_time:
		return true
	default:
		return false
	}
}

func finiteRangeStart(bound *plan.FrameBound) bool {
	switch bound.Type {
	case plan.FrameBound_CURRENT_ROW:
		return !bound.UnBounded
	case plan.FrameBound_PRECEDING:
		return !bound.UnBounded && bound.Val != nil
	default:
		return false
	}
}

func finiteRangeEnd(bound *plan.FrameBound) bool {
	switch bound.Type {
	case plan.FrameBound_CURRENT_ROW:
		return !bound.UnBounded
	case plan.FrameBound_FOLLOWING:
		return !bound.UnBounded && bound.Val != nil
	default:
		return false
	}
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

// cumulativePartitionUsesRunning compares the prefix Fill calls saved by a
// running aggregate with the AggBatchSize physical state chunk initialized for
// that partition. Small partitions stay on the direct evaluator; large
// partitions use one running state. Evaluating this per partition avoids both
// allocation churn on high-cardinality inputs and quadratic work in mixed
// inputs that contain a few large partitions.
func cumulativePartitionUsesRunning(start, end int) bool {
	rows := end - start
	if rows <= 1 {
		return false
	}
	savedFills := uint64(rows) * uint64(rows-1) / 2
	return savedFills > uint64(aggexec.AggBatchSize)
}

// processCumulativeAggregateFuncRange evaluates small partitions directly and
// advances one retained aggregate state for each large partition. The running
// state changes large cumulative aggregates from O(partitionRows^2) Fill calls
// to O(partitionRows), while remaining valid across output chunks.
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

	n := ctr.bat.RowCount()
	currentPartitionStart := 0
	if len(ctr.ps) > 0 {
		currentPartitionStart = int(ctr.ps[ctr.runningPartition])
	}
	currentPartitionEnd := partitionEnd(ctr.ps, ctr.runningPartition, n)
	for j := outputStart; j < outputEnd; j++ {
		if err := checkCanceled(proc, j-outputStart); err != nil {
			return nil, err
		}
		if j == currentPartitionEnd {
			if ctr.runningAgg != nil {
				ctr.runningAgg.Free()
				ctr.runningAgg = nil
			}
			ctr.runningPartition++
			currentPartitionStart = j
			currentPartitionEnd = partitionEnd(ctr.ps, ctr.runningPartition, n)
		}

		group := j - outputStart
		if cumulativePartitionUsesRunning(currentPartitionStart, currentPartitionEnd) {
			if ctr.runningAgg == nil {
				ctr.runningAgg, retErr = ctr.newAggregateExecutor(idx, ap, proc, 1)
				if retErr != nil {
					return nil, retErr
				}
			}
			if err := ctr.runningAgg.Fill(0, j, ctr.aggVecs[idx].Vec); err != nil {
				return nil, err
			}
			if err := ctr.batAggs[idx].Merge(ctr.runningAgg, group, 0); err != nil {
				return nil, err
			}
		} else {
			for k := currentPartitionStart; k <= j; k++ {
				if err := checkCanceled(proc, k-currentPartitionStart); err != nil {
					return nil, err
				}
				if err := ctr.batAggs[idx].Fill(group, k, ctr.aggVecs[idx].Vec); err != nil {
					return nil, err
				}
			}
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

// processSlidingAggregateFuncRange retains one aggregate for a bounded ROWS or
// RANGE frame. Each boundary change adds the new right edge and removes the
// expired left edge, reducing bounded SUM/AVG evaluation from O(N*W) to O(N).
// RANGE peers share boundaries, so their binary searches and state changes are
// performed once per peer group rather than once per output row.
func (ctr *container) processSlidingAggregateFuncRange(
	idx int,
	ap *Window,
	proc *process.Process,
	outputStart int,
	outputEnd int,
	frame *plan.FrameClause,
) (_ *vector.Vector, retErr error) {
	if outputStart != ctr.runningNextRow {
		ctr.freeRunningAgg()
		return nil, moerr.NewInternalErrorNoCtx("sliding window output is not sequential")
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
		if !aggexec.SupportsWindowSliding(ctr.runningAgg) {
			return nil, moerr.NewInternalErrorNoCtx("running aggregate does not support sliding windows")
		}
	}

	n := ctr.bat.RowCount()
	partitionStart := 0
	if len(ctr.ps) > 0 {
		partitionStart = int(ctr.ps[ctr.runningPartition])
	}
	currentPartitionEnd := partitionEnd(ctr.ps, ctr.runningPartition, n)
	edgeWork := 0
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
			partitionStart = j
			currentPartitionEnd = partitionEnd(ctr.ps, ctr.runningPartition, n)
			ctr.runningLeft = partitionStart
			ctr.runningRight = partitionStart
			ctr.runningPeerEnd = partitionStart
		}

		if frame.Type != plan.FrameClause_RANGE || j >= ctr.runningPeerEnd {
			left, right, err := ctr.buildInterval(proc, j, partitionStart, currentPartitionEnd, frame)
			if err != nil {
				return nil, err
			}
			left = max(left, partitionStart)
			right = min(right, currentPartitionEnd)
			if left < ctr.runningLeft || right < ctr.runningRight || left >= right {
				return nil, moerr.NewInternalErrorNoCtx("invalid sliding window interval")
			}

			for row := ctr.runningLeft; row < left; row++ {
				if err = checkCanceled(proc, edgeWork); err != nil {
					return nil, err
				}
				edgeWork++
				if err = aggexec.RemoveWindowRow(ctr.runningAgg, row, ctr.aggVecs[idx].Vec); err != nil {
					return nil, err
				}
			}
			for row := ctr.runningRight; row < right; row++ {
				if err = checkCanceled(proc, edgeWork); err != nil {
					return nil, err
				}
				edgeWork++
				if err = aggexec.AddWindowRow(ctr.runningAgg, row, ctr.aggVecs[idx].Vec); err != nil {
					return nil, err
				}
			}
			ctr.runningLeft = left
			ctr.runningRight = right
			if frame.Type == plan.FrameClause_RANGE {
				_, ctr.runningPeerEnd = buildPeerInterval(ctr.os, j, partitionStart, currentPartitionEnd)
				if ctr.runningPeerEnd <= j {
					return nil, moerr.NewInternalErrorNoCtx("invalid sliding window peer interval")
				}
			}
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
	switch funcName {
	case "row_number":
		values := make([]uint64, outputEnd-outputStart)
		for j := outputStart; j < outputEnd; j++ {
			if err := checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			partitionStart := 0
			if ctr.ps != nil {
				partitionStart, _ = buildPartitionInterval(ctr.ps, j, n)
			}
			values[j-outputStart] = uint64(j - partitionStart + 1)
		}
		vec := vector.NewVec(types.T_uint64.ToType())
		if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		return vec, nil
	case "ntile":
		values := make([]int64, outputEnd-outputStart)
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
		vec := vector.NewVec(types.T_int64.ToType())
		if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		return vec, nil
	case "rank", "dense_rank":
		values := make([]uint64, outputEnd-outputStart)
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
				values[j-outputStart] = uint64(peerStart + 1)
			} else {
				values[j-outputStart] = uint64(peerIndex + 1)
			}
		}
		vec := vector.NewVec(types.T_uint64.ToType())
		if err := vector.AppendFixedList(vec, values, nil, proc.Mp()); err != nil {
			vec.Free(proc.Mp())
			return nil, err
		}
		return vec, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported order window function: %s", funcName)
	}
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
	if vec.Length() == 0 {
		return 1, nil
	}
	if vec.IsNull(0) {
		return 0, moerr.NewInvalidInputNoCtx("ntile bucket count cannot be NULL")
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

// validateLagLeadOffsets checks the complete evaluated offset vector before
// the first output chunk is emitted. Literal offsets are rejected by the
// binder; this pass covers prepared parameters and row-dependent expressions.
func (ctr *container) validateLagLeadOffsets(idx int, ap *Window, proc *process.Process) error {
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	if (w.Name != "lag" && w.Name != "lead") ||
		idx >= len(ctr.aggVecs) || len(ctr.aggVecs[idx].Vec) < 2 {
		return nil
	}

	offsetVec := ctr.aggVecs[idx].Vec[1]
	rows := offsetVec.Length()
	if offsetVec.IsConst() && rows > 1 {
		rows = 1
	}
	for row := 0; row < rows; row++ {
		if err := checkCanceled(proc, row); err != nil {
			return err
		}
		if _, err := getLagLeadOffsetFromVec(proc.Ctx, w.Name, offsetVec, row); err != nil {
			return err
		}
	}
	return nil
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
		constOffset := int64(1)
		if len(ctr.aggVecs[idx].Vec) >= 2 {
			offsetVec = ctr.aggVecs[idx].Vec[1]
			if offsetVec.IsConst() {
				constOffset, err = getLagLeadOffsetFromVec(proc.Ctx, funcName, offsetVec, 0)
				if err != nil {
					return nil, err
				}
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
			offset := constOffset
			if offsetVec != nil && !offsetVec.IsConst() {
				offset, err = getLagLeadOffsetFromVec(proc.Ctx, funcName, offsetVec, j)
				if err != nil {
					return nil, err
				}
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
		constOffset := int64(1)
		if len(ctr.aggVecs[idx].Vec) >= 2 {
			offsetVec = ctr.aggVecs[idx].Vec[1]
			if offsetVec.IsConst() {
				constOffset, err = getLagLeadOffsetFromVec(proc.Ctx, funcName, offsetVec, 0)
				if err != nil {
					return nil, err
				}
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
			offset := constOffset
			if offsetVec != nil && !offsetVec.IsConst() {
				offset, err = getLagLeadOffsetFromVec(proc.Ctx, funcName, offsetVec, j)
				if err != nil {
					return nil, err
				}
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
			left, right, selection, err := ctr.buildIntervalRows(proc, j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if selection != nil {
				if len(selection.spans) == 0 {
					if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
						return nil, err
					}
				} else if err := localResult.UnionOne(srcVec, int64(selection.spans[0].start), proc.Mp()); err != nil {
					return nil, err
				}
				continue
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
			left, right, selection, err := ctr.buildIntervalRows(proc, j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if selection != nil {
				if len(selection.spans) == 0 {
					if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
						return nil, err
					}
				} else if err := localResult.UnionOne(srcVec, int64(selection.spans[len(selection.spans)-1].end-1), proc.Mp()); err != nil {
					return nil, err
				}
				continue
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
				constNth, constOK, err = getNthValueOffsetFromVec(proc.Ctx, nthVec, 0)
				if err != nil {
					return nil, err
				}
			}
		}
		for j := outputStart; j < outputEnd; j++ {
			if err = checkCanceled(proc, j-outputStart); err != nil {
				return nil, err
			}
			nthVal, ok := constNth, constOK
			if nthVec != nil && !nthVec.IsConst() {
				nthVal, ok, err = getNthValueOffsetFromVec(proc.Ctx, nthVec, j)
				if err != nil {
					return nil, err
				}
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
			left, right, selection, err := ctr.buildIntervalRows(proc, j, start, end, ctr.frameAt(idx, w.Frame))
			if err != nil {
				return nil, err
			}
			if left < start {
				left = start
			}
			if right > end {
				right = end
			}
			if selection != nil {
				frameRow, ok := selection.nth(int(nthVal))
				if !ok {
					if err := vector.AppendAny(localResult, nil, true, proc.Mp()); err != nil {
						return nil, err
					}
				} else if err := localResult.UnionOne(srcVec, int64(frameRow), proc.Mp()); err != nil {
					return nil, err
				}
				continue
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

func getNthValueOffsetFromVec(ctx context.Context, vec *vector.Vector, row int) (int64, bool, error) {
	if !types.T(vec.GetType().Oid).IsMySQLString() {
		value, ok := getInt64FromVec(vec, row)
		return value, ok, nil
	}

	if vec.IsConst() {
		row = 0
	}
	if vec.Length() == 0 || vec.IsNull(uint64(row)) ||
		!vec.HasPrepareParamKind() || vec.GetPrepareParamKindAt(row) != vector.PrepareParamInteger {
		return 0, false, moerr.NewWrongArguments(ctx, "nth_value")
	}
	value, err := strconv.ParseUint(vec.GetStringAt(row), 10, 63)
	if err != nil || value == 0 {
		return 0, false, moerr.NewWrongArguments(ctx, "nth_value")
	}
	return int64(value), true, nil
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

func getLagLeadOffsetFromVec(ctx context.Context, name string, vec *vector.Vector, row int) (int64, error) {
	if vec.IsConst() {
		row = 0
	}
	offset, ok := getInt64FromVec(vec, row)
	if !ok || offset < 0 {
		return 0, moerr.NewWrongArguments(ctx, name)
	}
	return offset, nil
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
	return result.UnionOne(defaultVec, srcRow, mp)
}

func (ctr *container) buildInterval(proc *process.Process, rowIdx, start, end int, frame *plan.FrameClause) (int, int, error) {
	// FrameClause_ROWS
	if frame.Type == plan.FrameClause_ROWS {
		start, end = ctr.buildRowsInterval(rowIdx, start, end, frame)
		return start, end, nil
	}

	if len(ctr.orderVecs) == 0 {
		return start, end, nil
	}

	// FrameClause_Range
	return ctr.buildRangeIntervalWithLocation(windowSessionLocation(proc), rowIdx, start, end, frame)
}

// buildIntervalRows retains the fast contiguous interval representation for
// ordinary RANGE frames. A TIMESTAMP frame whose session-civil bounds cross a
// fall-back fold instead returns its qualifying spans in window order: local
// civil membership can then be split into multiple instant-sorted spans.
func (ctr *container) buildIntervalRows(
	proc *process.Process,
	rowIdx, start, end int,
	frame *plan.FrameClause,
) (left, right int, selection *timestampRangeSelection, err error) {
	left, right, err = ctr.buildInterval(proc, rowIdx, start, end, frame)
	if err != nil || frame.Type != plan.FrameClause_RANGE || len(ctr.orderVecs) != 1 {
		return left, right, nil, err
	}

	vec := ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0]
	loc := windowSessionLocation(proc)
	selection, err = ctr.timestampRangeSelection(proc, loc, rowIdx, start, end, vec, frame)
	return left, right, selection, err
}

func (ctr *container) timestampRangeSelection(
	proc *process.Process,
	loc *time.Location,
	rowIdx, start, end int,
	vec *vector.Vector,
	frame *plan.FrameClause,
) (*timestampRangeSelection, error) {
	// A const vector stores one physical value for all logical rows. It cannot
	// cross a timezone transition, so the ordinary RANGE path already has the
	// complete peer interval and fold indexing must not address it by logical
	// row position.
	if vec.GetType().Oid != types.T_timestamp || vec.IsConst() || vec.GetNulls().Contains(uint64(rowIdx)) {
		return nil, nil
	}
	index, err := ctr.timestampCivilOrderIndex(proc, loc, start, end, vec, ctr.rangeDescending())
	if err != nil || !index.hasFold {
		return nil, err
	}

	desc := ctr.rangeDescending()
	startBoundary, hasStart, err := timestampRangeCivilBoundary(loc, vec, rowIdx, frame.Start, desc)
	if err != nil {
		return nil, err
	}
	endBoundary, hasEnd, err := timestampRangeCivilBoundary(loc, vec, rowIdx, frame.End, desc)
	if err != nil {
		return nil, err
	}

	// RANGE bounds are expressed in ORDER BY direction. Convert finite bounds
	// into the natural civil-time order used for membership; an unbounded side
	// remains open. This matters at a fall-back fold, where (for example)
	// UNBOUNDED PRECEDING ... CURRENT ROW is not necessarily an instant prefix.
	var low, high types.Datetime
	hasLow, hasHigh := false, false
	if hasStart && hasEnd {
		low, high = startBoundary, endBoundary
		if low > high {
			low, high = high, low
		}
		hasLow, hasHigh = true, true
	} else if hasStart {
		if desc {
			high, hasHigh = startBoundary, true
		} else {
			low, hasLow = startBoundary, true
		}
	} else if hasEnd {
		if desc {
			low, hasLow = endBoundary, true
		} else {
			high, hasHigh = endBoundary, true
		}
	}

	if !hasLow && !hasHigh {
		return nil, nil
	}

	selection := &timestampRangeSelection{}
	// The civil spans deliberately omit NULL order keys. A NULL peer can still
	// belong to a frame with an unbounded side: it is included only when that
	// side reaches the NULL end of the already sorted window order.
	nullsLast := ctr.rangeNullsLast()
	if frame.Start.UnBounded && !nullsLast && index.nullPrefixEnd > start {
		selection.spans = append(selection.spans, timestampCivilOrderSpan{start: start, end: index.nullPrefixEnd})
	}
	for _, span := range index.spans {
		left, right := timestampCivilSpanBounds(vec, loc, span, desc, low, hasLow, high, hasHigh)
		if left < right {
			selection.spans = append(selection.spans, timestampCivilOrderSpan{start: left, end: right})
		}
	}
	if frame.End.UnBounded && nullsLast && index.nullSuffixStart < end {
		selection.spans = append(selection.spans, timestampCivilOrderSpan{start: index.nullSuffixStart, end: end})
	}
	return selection, nil
}

func (ctr *container) rangeDescending() bool {
	return len(ctr.desc) > 0 && ctr.desc[len(ctr.desc)-1]
}

func (ctr *container) rangeNullsLast() bool {
	return len(ctr.nullsLast) > 0 && ctr.nullsLast[len(ctr.nullsLast)-1]
}

func (ctr *container) timestampCivilOrderIndex(
	proc *process.Process,
	loc *time.Location,
	start, end int,
	vec *vector.Vector,
	desc bool,
) (*timestampCivilOrderIndex, error) {
	key := timestampCivilOrderKey{vec: vec, loc: loc, start: start, end: end, desc: desc}
	if index, ok := ctr.timestampCivilOrder[key]; ok {
		return index, nil
	}
	if ctr.timestampCivilOrder == nil {
		ctr.timestampCivilOrder = make(map[timestampCivilOrderKey]*timestampCivilOrderIndex)
	}

	index := &timestampCivilOrderIndex{nullPrefixEnd: start, nullSuffixStart: end}
	col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
	// A sparse vector can cross a fall-back transition without its sampled
	// civil values reversing (01:00 EDT followed by 01:30 EST, for example).
	// Find transitions from the instant range itself rather than inferring them
	// solely from adjacent samples.
	var firstTimestamp, lastTimestamp types.Timestamp
	haveFirst, haveLast := false, false
	for i := start; i < end; i++ {
		if err := checkCanceled(proc, i-start); err != nil {
			return nil, err
		}
		if vec.GetNulls().Contains(uint64(i)) {
			continue
		}
		index.nullPrefixEnd = i
		if col[i] == types.ZeroTimestamp {
			continue
		}
		firstTimestamp, haveFirst = col[i], true
		break
	}
	for i := end - 1; i >= start; i-- {
		if err := checkCanceled(proc, end-1-i); err != nil {
			return nil, err
		}
		if vec.GetNulls().Contains(uint64(i)) {
			continue
		}
		index.nullSuffixStart = i + 1
		if col[i] == types.ZeroTimestamp {
			continue
		}
		lastTimestamp, haveLast = col[i], true
		break
	}
	foldTransitions, err := timestampCivilFoldTransitions(proc, loc, firstTimestamp, lastTimestamp, haveFirst && haveLast)
	if err != nil {
		return nil, err
	}
	index.hasFold = len(foldTransitions) != 0

	var previous types.Datetime
	var previousTimestamp types.Timestamp
	var previousOffset types.Datetime
	havePrevious := false
	spanStart := -1
	transition := 0
	if desc {
		transition = len(foldTransitions) - 1
	}
	for i := start; i < end; i++ {
		if err := checkCanceled(proc, i-start); err != nil {
			return nil, err
		}
		if vec.GetNulls().Contains(uint64(i)) {
			if spanStart >= 0 {
				index.spans = append(index.spans, timestampCivilOrderSpan{start: spanStart, end: i})
				spanStart = -1
			}
			havePrevious = false
			continue
		}
		civil := col[i].ToDatetime(loc)
		if spanStart < 0 {
			spanStart = i
		}
		// Keep a sampled offset proof alongside the ZoneBounds-derived
		// transition list. ZoneBounds handles sparse and recurring transitions,
		// while an actual offset decrease between adjacent materialized rows is
		// an independent, no-extra-scan confirmation that this input crosses a
		// fold. In particular, it must not be lost when the civil samples happen
		// to increase or compare equal on opposite sides of the repeated hour.
		offset := civil - types.Datetime(col[i])
		crossedFold := false
		if havePrevious {
			if !desc {
				for transition < len(foldTransitions) && previousTimestamp < foldTransitions[transition] && col[i] >= foldTransitions[transition] {
					crossedFold = true
					transition++
				}
			} else {
				for transition >= 0 && previousTimestamp >= foldTransitions[transition] && col[i] < foldTransitions[transition] {
					crossedFold = true
					transition--
				}
			}
			if (!desc && offset < previousOffset) || (desc && offset > previousOffset) {
				crossedFold = true
				index.hasFold = true
			}
		}
		civilReversed := havePrevious && ((!desc && civil < previous) || (desc && civil > previous))
		if civilReversed {
			index.hasFold = true
		}
		if havePrevious && (crossedFold || civilReversed) {
			index.spans = append(index.spans, timestampCivilOrderSpan{start: spanStart, end: i})
			spanStart = i
		}
		previous, previousTimestamp, previousOffset, havePrevious = civil, col[i], offset, true
	}
	if spanStart >= 0 {
		index.spans = append(index.spans, timestampCivilOrderSpan{start: spanStart, end: end})
	}
	ctr.timestampCivilOrder[key] = index
	return index, nil
}

// timestampCivilFoldTransitions returns every UTC-offset decrease in the
// instant range. ZoneBounds follows a location's recurring rules, so this
// remains correct for sparse inputs and for ranges beyond its explicit zone
// transition table.
func timestampCivilFoldTransitions(
	proc *process.Process,
	loc *time.Location,
	minTimestamp, maxTimestamp types.Timestamp,
	haveTimestamp bool,
) ([]types.Timestamp, error) {
	if !haveTimestamp || loc == nil || minTimestamp == types.ZeroTimestamp || maxTimestamp == types.ZeroTimestamp {
		return nil, nil
	}
	if minTimestamp > maxTimestamp {
		minTimestamp, maxTimestamp = maxTimestamp, minTimestamp
	}
	probe := time.UnixMicro(int64(minTimestamp) - int64(types.UnixToTimestamp(0))).In(loc)
	maxInstant := time.UnixMicro(int64(maxTimestamp) - int64(types.UnixToTimestamp(0)))
	var transitions []types.Timestamp
	for i := 0; ; i++ {
		if err := checkCanceled(proc, i); err != nil {
			return nil, err
		}
		_, next := probe.ZoneBounds()
		if next.IsZero() || next.After(maxInstant) {
			break
		}
		_, beforeOffset := time.UnixMicro(next.UnixMicro() - 1).In(loc).Zone()
		_, afterOffset := next.In(loc).Zone()
		if afterOffset < beforeOffset {
			transitions = append(transitions, types.UnixMicroToTimestamp(next.UnixMicro()))
		}
		if !next.After(probe) {
			break
		}
		probe = next.In(loc)
	}
	return transitions, nil
}

func (selection *timestampRangeSelection) nth(n int) (int, bool) {
	for _, span := range selection.spans {
		if n <= span.end-span.start {
			return span.start + n - 1, true
		}
		n -= span.end - span.start
	}
	return 0, false
}

func timestampCivilSpanBounds(
	vec *vector.Vector,
	loc *time.Location,
	span timestampCivilOrderSpan,
	desc bool,
	low types.Datetime,
	hasLow bool,
	high types.Datetime,
	hasHigh bool,
) (int, int) {
	col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
	left, right := span.start, span.end
	if !desc {
		if hasLow {
			left = timestampCivilLowerBound(col, loc, span.start, span.end, low)
		}
		if hasHigh {
			right = timestampCivilUpperBound(col, loc, left, span.end, high)
		}
		return left, right
	}
	if hasHigh {
		left = timestampCivilDescendingLowerBound(col, loc, span.start, span.end, high)
	}
	if hasLow {
		right = timestampCivilDescendingUpperBound(col, loc, left, span.end, low)
	}
	return left, right
}

func timestampCivilLowerBound(col []types.Timestamp, loc *time.Location, left, right int, target types.Datetime) int {
	for left < right {
		mid := left + (right-left)/2
		if col[mid].ToDatetime(loc) < target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func timestampCivilUpperBound(col []types.Timestamp, loc *time.Location, left, right int, target types.Datetime) int {
	for left < right {
		mid := left + (right-left)/2
		if col[mid].ToDatetime(loc) <= target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func timestampCivilDescendingLowerBound(col []types.Timestamp, loc *time.Location, left, right int, target types.Datetime) int {
	for left < right {
		mid := left + (right-left)/2
		if col[mid].ToDatetime(loc) > target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func timestampCivilDescendingUpperBound(col []types.Timestamp, loc *time.Location, left, right int, target types.Datetime) int {
	for left < right {
		mid := left + (right-left)/2
		if col[mid].ToDatetime(loc) >= target {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func timestampRangeCivilBoundary(
	loc *time.Location,
	vec *vector.Vector,
	rowIdx int,
	bound *plan.FrameBound,
	desc bool,
) (types.Datetime, bool, error) {
	if bound.UnBounded {
		return 0, false, nil
	}
	current := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[rowIdx].ToDatetime(loc)
	if bound.Type == plan.FrameBound_CURRENT_ROW {
		return current, true, nil
	}

	diff := bound.Val.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
	unit := bound.Val.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
	add := bound.Type == plan.FrameBound_FOLLOWING
	if desc {
		add = !add
	}
	if add {
		result, err := doDatetimeAdd(current, diff, unit)
		return result, true, err
	}
	result, err := doDatetimeSub(current, diff, unit)
	return result, true, err
}

func windowSessionLocation(proc *process.Process) *time.Location {
	if proc != nil {
		if sessionInfo := proc.GetSessionInfo(); sessionInfo != nil && sessionInfo.TimeZone != nil {
			return sessionInfo.TimeZone
		}
	}
	return time.Local
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
	return ctr.buildRangeIntervalWithLocation(time.Local, rowIdx, start, end, frame)
}

func (ctr *container) buildRangeIntervalWithLocation(loc *time.Location, rowIdx int, start, end int, frame *plan.FrameClause) (int, int, error) {
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
			start, err = searchLeftWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			start, err = searchLeftWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_FOLLOWING:
		start, err = searchLeftWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, true, desc)
		if err != nil {
			return start, end, err
		}
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		if len(ctr.os) > 0 || end-start <= 1 {
			_, end = buildPeerInterval(ctr.os, rowIdx, start, end)
		} else {
			end, err = searchRightWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false, desc)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_PRECEDING:
		end, err = searchRightWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, true, desc)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			end, err = searchRightWithLocation(loc, start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, false, desc)
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

func makePartitionTopNOrderBy(expr *plan.Expr) []*plan.OrderBySpec {
	w := expr.Expr.(*plan.Expr_W).W
	orderBy := make([]*plan.OrderBySpec, 0, len(w.PartitionBy)+len(w.OrderBy))
	for _, partitionExpr := range w.PartitionBy {
		orderBy = append(orderBy, &plan.OrderBySpec{
			Expr: partitionExpr,
			Flag: plan.OrderBySpec_INTERNAL,
		})
	}
	return append(orderBy, w.OrderBy...)
}

func (ctr *container) evalOrderVector(bat *batch.Batch, proc *process.Process) (err error) {
	// Eval reuses ctr.orderVecs' backing vectors by replacing their data below.
	// Fold detection is derived from that data, so entries keyed by a vector
	// pointer must never survive into the next materialized input batch.
	ctr.timestampCivilOrder = nil
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
	ctr.ps = nil

	if err := ctr.evalOrderVector(bat, proc); err != nil {
		return false, err
	}
	if bat.RowCount() < 2 {
		return false, nil
	}

	ovec := ctr.orderVecs[0].Vec[0]
	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	partitionKeyCount := 0
	if ap.PartitionTopN {
		// PartitionTopN coalesces input partitions, so its order-vector prefix
		// contains identity partition keys followed by SQL ORDER BY keys.
		partitionKeyCount = len(w.PartitionBy)
	}

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
			if partitionKeyCount > 0 {
				sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.sels, ovec)
			} else {
				sort.SortForSQLOrder(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.sels, ovec)
			}
		}
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
	}

	ps := make([]int64, 0, 16)
	ds := make([]bool, len(ctr.sels))

	i, j := 1, len(ctr.orderVecs)
	for ; i < j; i++ {
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		desc := ctr.desc[i]
		nullsLast := ctr.nullsLast[i]
		if i <= partitionKeyCount {
			ps = partition.Partition(ctr.sels, ds, ps, ovec)
		} else {
			ps = partition.PartitionForOrder(ctr.sels, ds, ps, ovec)
		}
		vec := ctr.orderVecs[i].Vec[0]
		// skip sort for const vector
		if !vec.IsConst() {
			nullCnt := vec.GetNulls().Count()
			if nullCnt < vec.Length() {
				for group, groupCount := 0, len(ps); group < groupCount; group++ {
					if err := checkCanceled(proc, group); err != nil {
						return false, err
					}
					start := ps[group]
					end := int64(len(ctr.sels))
					if group < groupCount-1 {
						end = ps[group+1]
					}
					if i < partitionKeyCount {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[start:end], vec)
					} else {
						sort.SortForSQLOrder(desc, nullsLast, nullCnt > 0, ctr.sels[start:end], vec)
					}
				}
			}
		}
		if err := checkCanceled(proc, 0); err != nil {
			return false, err
		}
		ovec = vec
		if i == partitionKeyCount {
			ctr.ps = append(ctr.ps, ps...)
		}
	}

	if ap.PartitionTopN && partitionKeyCount == i {
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		ctr.ps = append(ctr.ps, ps...)
	}

	if len(w.OrderBy) > 0 {
		ctr.os = partition.PartitionForOrder(ctr.sels, ds, ps, ovec)
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

	return false, nil
}

func searchLeft(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, plus bool, desc bool) (int, error) {
	return searchLeftWithLocation(time.Local, start, end, rowIdx, vec, expr, plus, desc)
}

func searchLeftWithLocation(loc *time.Location, start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, plus bool, desc bool) (int, error) {
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
	// A const vector stores one physical value, while the search bounds are
	// logical rows. Evaluate the one physical row and project its boundary back
	// to this logical interval instead of indexing the scalar column by mid.
	if vec.IsConst() && end-start > 1 {
		boundary, err := searchLeftWithLocation(loc, 0, 1, 0, vec, expr, plus, desc)
		if err != nil || boundary == 0 {
			return start, err
		}
		return end, nil
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
			bound, aboveDomain, ok := uint64RangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[uint64], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[int8], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[int16], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[int32], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[int64], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[uint8], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[uint16], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[uint32], cmpl)
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
			bound, aboveDomain, ok := uint64RangeBound(col[rowIdx], c, !plus)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			left = genericSearchLeft(start, end-1, col, bound, genericEqual[uint64], cmpl)
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		cmpl := float32OrderAscGreater
		if desc {
			cmpl = float32OrderDescGreater
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], float32OrderEqual, cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, float32OrderEqual, cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, float32OrderEqual, cmpl)
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		cmpl := float64OrderAscGreater
		if desc {
			cmpl = float64OrderDescGreater
		}
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], float64OrderEqual, cmpl)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, float64OrderEqual, cmpl)
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, float64OrderEqual, cmpl)
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			} else {
				fol, err := doDateSub(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			} else {
				fol, err := doDatetimeSub(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			} else {
				fol, err := doTimeSub(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
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
				fol, err := doTimestampAdd(loc, col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			} else {
				fol, err := doTimestampSub(loc, col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start.ToDatetime(), diff, types.IntervalType(unit), true) {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start.ToDatetime(), diff, true, types.DateType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("date", "")
		}
		return dt.ToDate(), nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		t, ok := checkedTimeMicrosecondInterval(start, diff, true)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("time", "")
		}
		return t, nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start, diff, types.IntervalType(unit), true) {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start, diff, true, types.DateTimeType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
		}
		return dt, nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start.ToDatetime(loc), diff, types.IntervalType(unit), true) {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start.ToDatetime(loc), diff, true, types.DateTimeType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
		}
		return timestampRangeBoundary(dt, loc), nil
	}
	dt, success := start.ToDatetime(loc).AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return timestampRangeBoundary(dt, loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

// timestampRangeBoundary resolves a session-civil frame boundary back to a
// timestamp instant. Go maps nonexistent civil times in a DST gap to one side
// of the transition; clamp that result to the transition instant so RANGE uses
// the first valid local time after the gap, matching MySQL semantics.
func timestampRangeBoundary(civil types.Datetime, loc *time.Location) types.Timestamp {
	boundary := civil.ToTimestamp(loc)
	roundTrip := boundary.ToDatetime(loc)
	if roundTrip == civil {
		return boundary
	}

	instant := time.UnixMicro(
		int64(boundary) - int64(types.UnixToTimestamp(0))).In(loc)
	transitionStart, transitionEnd := instant.ZoneBounds()
	if roundTrip < civil && !transitionEnd.IsZero() {
		return types.UnixMicroToTimestamp(transitionEnd.UnixMicro())
	}
	if roundTrip > civil && !transitionStart.IsZero() {
		return types.UnixMicroToTimestamp(transitionStart.UnixMicro())
	}
	return boundary
}

func searchRight(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, sub bool, desc bool) (int, error) {
	return searchRightWithLocation(time.Local, start, end, rowIdx, vec, expr, sub, desc)
}

func searchRightWithLocation(loc *time.Location, start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, sub bool, desc bool) (int, error) {
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
	// See searchLeftWithLocation: resolve scalar storage against one physical
	// row, then preserve the result's logical interval boundary.
	if vec.IsConst() && end-start > 1 {
		boundary, err := searchRightWithLocation(loc, 0, 1, 0, vec, expr, sub, desc)
		if err != nil || boundary == 0 {
			return start, err
		}
		return end, nil
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
			bound, aboveDomain, ok := uint64RangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[uint64], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[int8], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[int16], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[int32], cmpl)
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
			bound, aboveDomain, ok := signedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[int64], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[uint8], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[uint16], cmpl)
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
			bound, aboveDomain, ok := unsignedRangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[uint32], cmpl)
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
			bound, aboveDomain, ok := uint64RangeBound(col[rowIdx], c, sub)
			if !ok {
				return outOfDomainRangeBoundary(start, end, aboveDomain, desc), nil
			}
			right = genericSearchRight(start, end-1, col, bound, genericEqual[uint64], cmpl)
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		cmpl := float32OrderAscGreater
		if desc {
			cmpl = float32OrderDescGreater
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], float32OrderEqual)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, float32OrderEqual, cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, float32OrderEqual, cmpl)
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		cmpl := float64OrderAscGreater
		if desc {
			cmpl = float64OrderDescGreater
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], float64OrderEqual)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, float64OrderEqual, cmpl)
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, float64OrderEqual, cmpl)
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Date], cmpl)
			} else {
				fol, err := doDateAdd(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Datetime], cmpl)
			} else {
				fol, err := doDatetimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
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
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Time], cmpl)
			} else {
				fol, err := doTimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
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
				fol, err := doTimestampSub(loc, col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, false, diff, desc), nil
					}
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Timestamp], cmpl)
			} else {
				fol, err := doTimestampAdd(loc, col[rowIdx], diff, unit)
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
						return temporalRangeIntervalOverflowBoundary(start, end, true, diff, desc), nil
					}
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

type unsignedRangeInteger interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// unsignedRangeBound computes a finite RANGE search key without allowing
// unsigned arithmetic to wrap into the opposite end of the type domain.
// aboveDomain distinguishes addition overflow from subtraction underflow.
func unsignedRangeBound[T unsignedRangeInteger](value, offset T, subtract bool) (bound T, aboveDomain bool, ok bool) {
	if subtract {
		if value < offset {
			return 0, false, false
		}
		return value - offset, false, true
	}
	if value > ^T(0)-offset {
		return 0, true, false
	}
	return value + offset, false, true
}

func uint64RangeBound(value, offset uint64, subtract bool) (bound uint64, aboveDomain bool, ok bool) {
	return unsignedRangeBound(value, offset, subtract)
}

type signedRangeInteger interface {
	~int8 | ~int16 | ~int32 | ~int64
}

// signedRangeBound computes a finite RANGE search key without allowing signed
// arithmetic to wrap into the opposite end of the type domain. aboveDomain
// distinguishes overflow above the maximum from underflow below the minimum.
func signedRangeBound[T signedRangeInteger](value, offset T, subtract bool) (bound T, aboveDomain bool, ok bool) {
	if subtract {
		bound = value - offset
		if (offset > 0 && bound > value) || (offset < 0 && bound < value) {
			return 0, offset < 0, false
		}
		return bound, false, true
	}

	bound = value + offset
	if (offset > 0 && bound < value) || (offset < 0 && bound > value) {
		return 0, offset > 0, false
	}
	return bound, false, true
}

// outOfDomainRangeBoundary maps a conceptual search key outside a numeric type
// domain to its insertion boundary in the current SQL sort direction.
func outOfDomainRangeBoundary(start, end int, aboveDomain, desc bool) int {
	if aboveDomain != desc {
		return end
	}
	return start
}

// temporalRangeOverflowBoundary maps a temporal search key outside the type
// domain to the insertion point it would have if that key were representable.
// A key above the domain sorts after every ASC value and before every DESC
// value; a key below the domain does the opposite.
func temporalRangeOverflowBoundary(start, end int, aboveDomain, desc bool) int {
	if aboveDomain != desc {
		return end
	}
	return start
}

// temporalRangeIntervalOverflowBoundary derives the side of a temporal-domain
// overflow from the effective signed arithmetic. A negative interval reverses
// the operation, so add/sub alone cannot identify the insertion point.
func temporalRangeIntervalOverflowBoundary(start, end int, add bool, diff int64, desc bool) int {
	aboveDomain := add
	if diff < 0 {
		aboveDomain = !aboveDomain
	}
	return temporalRangeOverflowBoundary(start, end, aboveDomain, desc)
}

// checkedMicrosecondArithmetic performs signed microsecond arithmetic without
// allowing an int64 wrap to masquerade as an in-domain temporal value.
func checkedMicrosecondArithmetic(start, diff int64, subtract bool) (int64, bool) {
	if subtract {
		if (diff > 0 && start < math.MinInt64+diff) || (diff < 0 && start > math.MaxInt64+diff) {
			return 0, false
		}
		return start - diff, true
	}
	if (diff > 0 && start > math.MaxInt64-diff) || (diff < 0 && start < math.MinInt64-diff) {
		return 0, false
	}
	return start + diff, true
}

// temporalRangeIntervalConversionOK prevents fixed-duration units from
// wrapping while AddInterval converts them to microseconds. The magnitude
// check accepts values that fit in an int64, but their later conversion can
// still overflow before AddInterval validates the temporal domain.
func temporalRangeIntervalConversionOK(diff int64, unit types.IntervalType) bool {
	var multiplier int64
	switch unit {
	case types.Second:
		multiplier = types.MicroSecsPerSec
	case types.Minute:
		multiplier = types.MicroSecsPerSec * types.SecsPerMinute
	case types.Hour:
		multiplier = types.MicroSecsPerSec * types.SecsPerHour
	case types.Day:
		multiplier = types.MicroSecsPerSec * types.SecsPerDay
	case types.Week:
		multiplier = types.MicroSecsPerSec * types.SecsPerWeek
	default:
		return true
	}
	return diff <= math.MaxInt64/multiplier && diff >= math.MinInt64/multiplier
}

// temporalRangeCalendarIntervalInDomain evaluates calendar arithmetic in the
// wide representation used by AddDateTime before that function narrows the
// result year to int32. A wrapped year is a valid-looking but incorrect RANGE
// search key, so calendar bounds outside the temporal domain must follow the
// same out-of-domain path as fixed-duration bounds.
func temporalRangeCalendarIntervalInDomain(start types.Datetime, diff int64, unit types.IntervalType, subtract bool) bool {
	if subtract {
		if diff == math.MinInt64 {
			return false
		}
		diff = -diff
	}

	year, month, _, _ := start.ToDate().Calendar(true)
	boundaryYear := int64(year)
	boundaryMonth := int64(month)
	var yearDelta, monthDelta int64
	switch unit {
	case types.Month, types.Year_Month:
		yearDelta = diff / 12
		monthDelta = diff % 12
	case types.Quarter:
		if diff > math.MaxInt64/3 || diff < math.MinInt64/3 {
			return false
		}
		months := diff * 3
		yearDelta = months / 12
		monthDelta = months % 12
	case types.Year:
		yearDelta = diff
	default:
		return true
	}
	if (yearDelta > 0 && boundaryYear > math.MaxInt64-yearDelta) ||
		(yearDelta < 0 && boundaryYear < math.MinInt64-yearDelta) {
		return false
	}
	boundaryYear += yearDelta
	boundaryMonth += monthDelta

	if boundaryMonth <= 0 {
		boundaryYear--
	} else if boundaryMonth > 12 {
		boundaryYear++
	}
	return boundaryYear >= int64(types.MinDatetimeYear) && boundaryYear <= int64(types.MaxDatetimeYear)
}

// checkedDatetimeMicrosecondInterval validates both the signed arithmetic and
// the resulting DATE/DATETIME domain. Datetime.AddInterval intentionally
// fast-paths MICROSECOND without a calendar validation, so RANGE bounds must
// validate it before using the result as a binary-search key.
func checkedDatetimeMicrosecondInterval(start types.Datetime, diff int64, subtract bool, timeType types.TimeType) (types.Datetime, bool) {
	result, ok := checkedMicrosecondArithmetic(int64(start), diff, subtract)
	if !ok {
		return 0, false
	}
	dt := types.Datetime(result)
	year, month, day, _ := dt.ToDate().Calendar(true)
	if timeType == types.DateType {
		return dt, types.ValidDate(year, month, day)
	}
	return dt, types.ValidDatetime(year, month, day)
}

func checkedTimeMicrosecondInterval(start types.Time, diff int64, subtract bool) (types.Time, bool) {
	result, ok := checkedMicrosecondArithmetic(int64(start), diff, subtract)
	if !ok {
		return 0, false
	}
	return types.Time(result).AddInterval(0, types.MicroSecond)
}

func doDateAdd(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start.ToDatetime(), diff, types.IntervalType(unit), false) {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start.ToDatetime(), diff, false, types.DateType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("date", "")
		}
		return dt.ToDate(), nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		t, ok := checkedTimeMicrosecondInterval(start, diff, false)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("time", "")
		}
		return t, nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start, diff, types.IntervalType(unit), false) {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start, diff, false, types.DateTimeType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
		}
		return dt, nil
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
	if !temporalRangeIntervalConversionOK(diff, types.IntervalType(unit)) {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
	if !temporalRangeCalendarIntervalInDomain(start.ToDatetime(loc), diff, types.IntervalType(unit), false) {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
	if types.IntervalType(unit) == types.MicroSecond {
		dt, ok := checkedDatetimeMicrosecondInterval(start.ToDatetime(loc), diff, false, types.DateTimeType)
		if !ok {
			return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
		}
		return timestampRangeBoundary(dt, loc), nil
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return timestampRangeBoundary(dt, loc), nil
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

// The RANGE binary searches operate on vectors sorted with the SQL ORDER BY
// relation. Native float comparisons do not provide the peer/equality and
// boundary behavior required for NaNs, so keep the search predicates aligned
// with the sort relation for both directions.
func float32OrderEqual(a, b float32) bool {
	return types.Float32OrderAscCompare(a, b) == 0
}

func float32OrderAscGreater(a, b float32) bool {
	return types.Float32OrderAscCompare(a, b) > 0
}

func float32OrderDescGreater(a, b float32) bool {
	return types.Float32OrderDescCompare(a, b) > 0
}

func float64OrderEqual(a, b float64) bool {
	return types.Float64OrderAscCompare(a, b) == 0
}

func float64OrderAscGreater(a, b float64) bool {
	return types.Float64OrderAscCompare(a, b) > 0
}

func float64OrderDescGreater(a, b float64) bool {
	return types.Float64OrderDescCompare(a, b) > 0
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
