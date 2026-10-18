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

package loopjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (ctr *container) unmatchedRowBytes(
	ap *LoopJoin,
	inBat *batch.Batch,
	probeRow int,
) int {
	size := 0
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			size += vectorLogicalRowBytes(inBat.Vecs[rp.Pos], probeRow)
		} else {
			// NULL right columns and MARK booleans still occupy one fixed-width
			// slot in Batch.Size().
			size += ctr.resBat.Vecs[i].GetType().TypeSize()
		}
	}
	return size
}

func (ctr *container) appendJoinedRow(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	buildRow int,
) error {
	for k, rp := range ap.ResultCols {
		switch rp.Rel {
		case 0:
			if err := ctr.resBat.Vecs[k].UnionOne(
				inBat.Vecs[rp.Pos], int64(probeRow), proc.Mp()); err != nil {
				return err
			}
		case 1:
			if err := ctr.resBat.Vecs[k].UnionOne(
				buildBat.Vecs[rp.Pos], int64(buildRow), proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) admittedJoinedRows(
	ap *LoopJoin,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	start int,
	currentRows int,
) int {
	usedBytes := ctr.resBat.Size()
	count := 0
	for row := start; row < buildBat.RowCount(); row++ {
		rowBytes := ctr.joinedRowBytes(ap, inBat, probeRow, buildBat, row)
		if !ctr.canAppendRow(currentRows+count, usedBytes, rowBytes) {
			break
		}
		usedBytes += rowBytes
		count++
	}
	return count
}

func (ctr *container) appendJoinedRange(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	start int,
	count int,
) error {
	for k, rp := range ap.ResultCols {
		switch rp.Rel {
		case 0:
			if err := ctr.resBat.Vecs[k].UnionMulti(
				inBat.Vecs[rp.Pos], int64(probeRow), count, proc.Mp()); err != nil {
				return err
			}
		case 1:
			if err := ctr.resBat.Vecs[k].UnionBatch(
				buildBat.Vecs[rp.Pos], int64(start), count, nil, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ctr *container) appendUnmatchedRow(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
) error {
	for k, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := ctr.resBat.Vecs[k].UnionOne(
				inBat.Vecs[rp.Pos], int64(probeRow), proc.Mp()); err != nil {
				return err
			}
		} else if err := ctr.resBat.Vecs[k].UnionNull(proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) appendMarkRow(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
	value bool,
	isNull bool,
) error {
	for k, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := ctr.resBat.Vecs[k].UnionOne(
				inBat.Vecs[rp.Pos], int64(probeRow), proc.Mp()); err != nil {
				return err
			}
		} else if err := vector.AppendFixed(
			ctr.resBat.Vecs[k], value, isNull, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) evalConditionWindow(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	start int,
) (vec *vector.Vector, window *batch.Batch, end int, err error) {
	if ap.PairCondition != nil {
		vec, err = ap.PairCondition(proc, inBat, probeRow, buildBat, start)
		return vec, nil, start + 1, err
	}
	end = min(start+conditionWindowRowLimit(buildBat), buildBat.RowCount())
	conditionInput := buildBat
	if start != 0 || end != buildBat.RowCount() {
		window, err = buildBat.WindowWithAllocation(
			start, end, proc.Mp(), ap.conditionAllocation)
		if err != nil {
			return nil, nil, 0, err
		}
		conditionInput = window
	}
	owned := window != nil
	defer func() {
		if owned {
			window.Clean(proc.Mp())
			window = nil
		}
	}()
	if err = colexec.SetJoinBatchValues(
		ctr.joinBat, inBat, int64(probeRow), end-start, ctr.cfs); err != nil {
		return nil, nil, 0, err
	}
	vec, err = ctr.expr.Eval(
		proc, []*batch.Batch{ctr.joinBat, conditionInput}, nil)
	if err != nil {
		return nil, nil, 0, err
	}
	if !vec.IsConst() && vec.Length() != end-start {
		return nil, nil, 0, moerr.NewInternalErrorNoCtx(
			"loop join: condition result length mismatch")
	}
	owned = false
	return vec, window, end, nil
}

func conditionWindowRowLimit(buildBat *batch.Batch) int {
	// Eight eighth-bytes account for the boolean result byte. A Window borrows
	// vector data/area, but each source null/grouping bitmap that is present may
	// need one additional bit per row.
	eighthBytesPerRow := 8
	for _, vec := range buildBat.Vecs {
		if vec.HasNull() {
			eighthBytesPerRow++
		}
		if vec.HasGrouping() {
			eighthBytesPerRow++
		}
	}
	byBytes := loopJoinConditionWindowByteLimit * 8 / eighthBytesPerRow
	return max(1, min(loopJoinConditionMaxWindowRows, byBytes))
}

func (ctr *container) conditionResult(
	ap *LoopJoin,
	proc *process.Process,
	inBat *batch.Batch,
	probeRow int,
	buildBat *batch.Batch,
	buildBatch int,
	start int,
) (*vector.Vector, int, error) {
	if ctr.condVec != nil {
		if ctr.condProbeIdx != probeRow || ctr.condBatIdx != buildBatch ||
			start < ctr.condStart || start >= ctr.condEnd {
			return nil, 0, moerr.NewInternalErrorNoCtx(
				"loop join: stale condition resume state")
		}
		return ctr.condVec, ctr.condEnd, nil
	}
	vec, window, end, err := ctr.evalConditionWindow(
		ap, proc, inBat, probeRow, buildBat, start)
	if err != nil {
		return nil, 0, err
	}
	if vec == nil {
		return nil, 0, nil
	}
	ctr.condVec = vec
	ctr.condProbeIdx = probeRow
	ctr.condBatIdx = buildBatch
	ctr.condStart = start
	ctr.condEnd = end
	ctr.condWindow = window
	ctr.condWindowMP = proc.Mp()
	return vec, end, nil
}

func conditionMatchAt(vec *vector.Vector, row int) (match, isNull bool) {
	rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
	idx := uint64(row)
	if vec.IsConst() {
		idx = 0
	}
	value, null := rs.GetValue(idx)
	return !null && value, null
}

// scanProbeMatches preserves both the scan cursor and its summary across a
// predicate suspension. SINGLE validates the second match before publishing.
func (ctr *container) scanProbeMatches(
	ap *LoopJoin, proc *process.Process, inBat *batch.Batch, probeRow int,
	mpbat []*batch.Batch, stopAfter int,
) (matchCount int, firstBatch int, firstRow int, hasNull bool, err error) {
	if ctr.probeScanStarted && ctr.probeScanIdx != probeRow {
		return 0, -1, -1, false, moerr.NewInternalErrorNoCtx("loop join: stale bounded probe scan state")
	}
	if !ctr.probeScanStarted {
		ctr.probeScanStarted = true
		ctr.probeScanIdx = probeRow
		ctr.probeScanFirstBatch, ctr.probeScanFirstRow = -1, -1
	}
	matchCount, firstBatch, firstRow, hasNull = ctr.probeScanMatches, ctr.probeScanFirstBatch, ctr.probeScanFirstRow, ctr.probeScanHasNull
	if ctr.probeScanValid {
		return
	}
	defer func() {
		if err == nil {
			ctr.probeScanMatches, ctr.probeScanFirstBatch, ctr.probeScanFirstRow, ctr.probeScanHasNull = matchCount, firstBatch, firstRow, hasNull
		}
	}()
	for idx := ctr.probeScanBatch; idx < len(mpbat); idx++ {
		bat := mpbat[idx]
		start := ctr.probeScanRow
		for start < bat.RowCount() {
			end := bat.RowCount()
			var vec *vector.Vector
			if ctr.expr != nil || ap.PairCondition != nil {
				vec, end, err = ctr.conditionResult(ap, proc, inBat, probeRow, bat, idx, start)
				if err != nil {
					return
				}
				if vec == nil {
					ctr.conditionPending = true
					ctr.probeScanBatch, ctr.probeScanRow = idx, start
					return
				}
			}
			for row := start; row < end; row++ {
				match, null := true, false
				if vec != nil {
					match, null = conditionMatchAt(vec, row-ctr.condStart)
				}
				hasNull = hasNull || null
				if !match {
					continue
				}
				matchCount++
				if firstBatch < 0 {
					firstBatch, firstRow = idx, row
				}
				if stopAfter > 0 && matchCount >= stopAfter {
					ctr.clearConditionResult()
					ctr.probeScanValid = true
					return
				}
			}
			ctr.clearConditionResult()
			start = end
		}
		ctr.probeScanBatch, ctr.probeScanRow = idx+1, 0
	}
	ctr.probeScanValid = true
	return
}

func (ctr *container) yieldProbe(
	result *vm.CallResult,
	rows, probeRow, buildBatch, buildRow int,
	matched bool,
) {
	ctr.probeIdx = probeRow
	ctr.batIdx = buildBatch
	ctr.batRowIdx = buildRow
	ctr.probeMatched = matched
	ctr.resBat.SetRowCount(rows)
	result.Batch = ctr.resBat
}

func (ctr *container) probe(
	ap *LoopJoin,
	proc *process.Process,
	result *vm.CallResult,
) (err error) {
	defer func() {
		if err != nil {
			ctr.clearProbeResume()
		}
	}()

	inbat := ctr.inBat
	mpbat := ctr.mp.GetBatches()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(inbat, proc.Mp())
	}

	rowCountIncrease := 0
	if ctr.conditionPending {
		rowCountIncrease = ctr.resBat.RowCount()
		ctr.conditionPending = false
	}
	for i := ctr.probeIdx; i < inbat.RowCount(); i++ {
		if ctr.resultBatchFull(rowCountIncrease) {
			ctr.yieldProbe(result, rowCountIncrease, i, 0, 0, false)
			return nil
		}

		switch ap.JoinType {
		case plan.Node_MARK:
			matches, _, _, hasNull, scanErr := ctr.scanProbeMatches(
				ap, proc, inbat, i, mpbat, 1)
			if scanErr != nil {
				return scanErr
			}
			if ctr.conditionPending {
				ctr.suspendCondition(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			rowBytes := ctr.unmatchedRowBytes(ap, inbat, i)
			if !ctr.canAppendRow(rowCountIncrease, ctr.resBat.Size(), rowBytes) {
				ctr.yieldProbe(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			if err = ctr.appendMarkRow(
				ap, proc, inbat, i, matches > 0, matches == 0 && hasNull); err != nil {
				return err
			}
			rowCountIncrease++

		case plan.Node_SEMI, plan.Node_ANTI:
			matches, _, _, _, scanErr := ctr.scanProbeMatches(
				ap, proc, inbat, i, mpbat, 1)
			if scanErr != nil {
				return scanErr
			}
			if ctr.conditionPending {
				ctr.suspendCondition(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			emit := ap.JoinType == plan.Node_SEMI && matches > 0 ||
				ap.JoinType == plan.Node_ANTI && matches == 0
			if !emit {
				break
			}
			rowBytes := ctr.unmatchedRowBytes(ap, inbat, i)
			if !ctr.canAppendRow(rowCountIncrease, ctr.resBat.Size(), rowBytes) {
				ctr.yieldProbe(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			if err = ctr.appendUnmatchedRow(ap, proc, inbat, i); err != nil {
				return err
			}
			rowCountIncrease++

		case plan.Node_SINGLE:
			matches, firstBatch, firstRow, _, scanErr := ctr.scanProbeMatches(
				ap, proc, inbat, i, mpbat, 2)
			if scanErr != nil {
				return scanErr
			}
			if ctr.conditionPending {
				ctr.suspendCondition(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			if matches > 1 {
				return moerr.NewErrSubqueryNo1Row(proc.Ctx)
			}
			rowBytes := ctr.unmatchedRowBytes(ap, inbat, i)
			if matches == 1 {
				rowBytes = ctr.joinedRowBytes(
					ap, inbat, i, mpbat[firstBatch], firstRow)
			}
			if !ctr.canAppendRow(rowCountIncrease, ctr.resBat.Size(), rowBytes) {
				ctr.yieldProbe(result, rowCountIncrease, i, 0, 0, false)
				return nil
			}
			if matches == 1 {
				err = ctr.appendJoinedRow(
					ap, proc, inbat, i, mpbat[firstBatch], firstRow)
			} else {
				err = ctr.appendUnmatchedRow(ap, proc, inbat, i)
			}
			if err != nil {
				return err
			}
			rowCountIncrease++

		case plan.Node_INNER, plan.Node_LEFT, plan.Node_OUTER:
			matched := ctr.probeMatched
			for idx := ctr.batIdx; idx < len(mpbat); idx++ {
				bat := mpbat[idx]
				start := 0
				if idx == ctr.batIdx {
					start = ctr.batRowIdx
				}
				if ctr.resultBatchFull(rowCountIncrease) {
					ctr.yieldProbe(
						result, rowCountIncrease, i, idx, start, matched)
					return nil
				}

				if ctr.expr != nil || ap.PairCondition != nil {
					for row := start; row < bat.RowCount(); {
						if ctr.resultBatchFull(rowCountIncrease) {
							ctr.yieldProbe(
								result, rowCountIncrease, i, idx, row, matched)
							return nil
						}
						vec, windowEnd, evalErr := ctr.conditionResult(
							ap, proc, inbat, i, bat, idx, row)
						if evalErr != nil {
							return evalErr
						}
						if vec == nil {
							ctr.suspendCondition(result, rowCountIncrease, i, idx, row, matched)
							return nil
						}
						windowStart := ctr.condStart
						for ; row < windowEnd; row++ {
							match, _ := conditionMatchAt(vec, row-windowStart)
							if !match {
								continue
							}
							rowBytes := ctr.joinedRowBytes(
								ap, inbat, i, bat, row)
							if !ctr.canAppendRow(
								rowCountIncrease, ctr.resBat.Size(), rowBytes) {
								ctr.yieldProbe(
									result, rowCountIncrease, i, idx, row, matched)
								return nil
							}
							if err = ctr.appendJoinedRow(
								ap, proc, inbat, i, bat, row); err != nil {
								return err
							}
							matched = true
							rowCountIncrease++
							if ap.JoinType == plan.Node_OUTER {
								ctr.rightRowsMatched.Add(
									ctr.rightBatchOffset[idx] + uint64(row))
							}
						}
						ctr.clearConditionResult()
					}
				} else {
					admitted := ctr.admittedJoinedRows(
						ap, inbat, i, bat, start, rowCountIncrease)
					if admitted == 0 && start < bat.RowCount() {
						ctr.yieldProbe(
							result, rowCountIncrease, i, idx, start, matched)
						return nil
					}
					if admitted > 0 {
						if err = ctr.appendJoinedRange(
							ap, proc, inbat, i, bat, start, admitted); err != nil {
							return err
						}
						matched = true
						rowCountIncrease += admitted
						if ap.JoinType == plan.Node_OUTER {
							base := ctr.rightBatchOffset[idx] + uint64(start)
							ctr.rightRowsMatched.AddRange(
								base, base+uint64(admitted))
						}
					}
					if start+admitted < bat.RowCount() {
						ctr.yieldProbe(
							result, rowCountIncrease, i, idx,
							start+admitted, matched)
						return nil
					}
				}
				ctr.batIdx = idx + 1
				ctr.batRowIdx = 0
				ctr.probeMatched = matched
			}

			if !matched &&
				(ap.JoinType == plan.Node_LEFT || ap.JoinType == plan.Node_OUTER) {
				rowBytes := ctr.unmatchedRowBytes(ap, inbat, i)
				if !ctr.canAppendRow(
					rowCountIncrease, ctr.resBat.Size(), rowBytes) {
					ctr.yieldProbe(
						result, rowCountIncrease, i, len(mpbat), 0, false)
					return nil
				}
				if err = ctr.appendUnmatchedRow(ap, proc, inbat, i); err != nil {
					return err
				}
				rowCountIncrease++
			}
		}

		ctr.probeIdx = i + 1
		ctr.batIdx = 0
		ctr.batRowIdx = 0
		ctr.clearProbeResume()
	}

	ctr.inBat = nil
	ctr.probeIdx = 0
	ctr.resBat.SetRowCount(rowCountIncrease)
	result.Batch = ctr.resBat
	return nil
}

// suspendCondition keeps provisional rows private while the candidate ON
// expression runs. No matched bit is changed before that expression completes.
func (ctr *container) suspendCondition(result *vm.CallResult, rows, probe, build, row int, matched bool) {
	ctr.yieldProbe(result, rows, probe, build, row, matched)
	ctr.conditionPending = true
	result.Batch = nil
	result.Status = vm.ExecYield
}
