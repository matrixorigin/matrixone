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

package hashjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// callAsofBuildLeft is the bounded-memory physical path for a small logical
// left input. HashBuild materializes the logical left rows. This operator then
// scans the logical right input once, retains one projected right candidate per
// left row, and emits only after the right input reaches EOF.
func (hashJoin *HashJoin) callAsofBuildLeft(proc *process.Process) (vm.CallResult, error) {
	analyzer := hashJoin.OpAnalyzer
	ctr := &hashJoin.ctr
	result := vm.NewCallResult()

	for {
		switch ctr.state {
		case Build:
			if err := hashJoin.build(analyzer, proc); err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			if ctr.mp == nil && ctr.spillEngine == nil {
				// The preserved logical left side is empty. No ASOF variant can
				// produce a row, so the huge logical right side need not be read.
				ctr.state = End
				continue
			}
			if ctr.mp != nil {
				if err := ctr.initAsofBuildLeftState(hashJoin, proc); err != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
			}
			ctr.state = Probe

		case Probe:
			input, err := hashJoin.getInputBatch(proc, analyzer)
			if err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			if input.Batch == nil {
				if ctr.mp == nil {
					// A spilled engine can exhaust without loading a bucket when no
					// logical-left row can contribute output.
					ctr.state = End
					continue
				}
				if !ctr.asofBuildLeftInitialized {
					if err := ctr.initAsofBuildLeftState(hashJoin, proc); err != nil {
						return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
					}
				}
				ctr.state = Finalize
				continue
			}
			if input.Batch.Last() {
				return input, nil
			}
			if input.Batch.IsEmpty() {
				continue
			}
			if ctr.mp == nil {
				return result, moerr.NewInternalErrorNoCtx(
					"ASOF build-left probe received a batch without a build map")
			}
			if !ctr.asofBuildLeftInitialized {
				if err := ctr.initAsofBuildLeftState(hashJoin, proc); err != nil {
					return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
				}
			}
			ctr.leftBat = input.Batch // physical probe = logical right
			if err := ctr.probeAsofBuildLeft(hashJoin, proc); err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			ctr.leftBat = nil
			if err := ctr.compactAsofBuildLeftCandidates(hashJoin, proc); err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			// This is a blocking operator: keep scanning until right EOF rather
			// than exposing a candidate that a later right row can replace.
			continue

		case Finalize:
			if err := ctr.finalizeAsofBuildLeft(hashJoin, proc, &result); err != nil {
				return result, hashbuild.TerminalBudgetError(proc.Ctx, err)
			}
			if result.Batch != nil {
				return result, nil
			}

			ctr.cleanAsofBuildLeftState(proc)
			if ctr.spillEngine != nil &&
				(ctr.spillEngine.HasMoreBuckets() || ctr.spillEngine.IsProbing()) {
				ctr.cleanHashMap()
				ctr.state = Probe
				continue
			}
			ctr.state = End

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) initAsofBuildLeftState(hashJoin *HashJoin, proc *process.Process) error {
	if ctr.asofBuildLeftInitialized {
		return nil
	}
	if ctr.rightRowCnt < 0 || int64(int(ctr.rightRowCnt)) != ctr.rightRowCnt {
		return moerr.NewInternalErrorNoCtx("ASOF logical-left row count exceeds addressable memory")
	}
	rowCount := int(ctr.rightRowCnt)
	bestTimes, err := mpool.MakeSliceAccounted[int64](
		rowCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return err
	}
	matched, err := mpool.MakeSliceAccounted[uint8](
		rowCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		mpool.FreeSlice(proc.Mp(), bestTimes)
		return err
	}
	batchRows, err := mpool.MakeSliceAccounted[int32](
		rowCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		mpool.FreeSlice(proc.Mp(), bestTimes)
		mpool.FreeSlice(proc.Mp(), matched)
		return err
	}
	for i := range batchRows {
		batchRows[i] = -1
	}

	var bestRight *batch.Batch
	for _, result := range hashJoin.ResultCols {
		if result.Rel == 1 {
			bestRight = batch.NewOffHeapWithSize(len(hashJoin.ResultCols))
			break
		}
	}
	if bestRight != nil {
		for output, result := range hashJoin.ResultCols {
			if result.Rel == 1 {
				bestRight.Vecs[output] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[result.Pos])
			}
		}
		if err = bestRight.SetAllocationAccount(hashJoin.asofCandidateAllocation); err != nil {
			bestRight.Clean(proc.Mp())
			mpool.FreeSlice(proc.Mp(), bestTimes)
			mpool.FreeSlice(proc.Mp(), matched)
			mpool.FreeSlice(proc.Mp(), batchRows)
			return err
		}
		for _, vec := range bestRight.Vecs {
			if vec == nil {
				continue
			}
			if err = vec.PreExtend(rowCount, proc.Mp()); err != nil {
				bestRight.Clean(proc.Mp())
				mpool.FreeSlice(proc.Mp(), bestTimes)
				mpool.FreeSlice(proc.Mp(), matched)
				mpool.FreeSlice(proc.Mp(), batchRows)
				return err
			}
			if err = vec.PreExtendNulls(rowCount, proc.Mp()); err != nil {
				bestRight.Clean(proc.Mp())
				mpool.FreeSlice(proc.Mp(), bestTimes)
				mpool.FreeSlice(proc.Mp(), matched)
				mpool.FreeSlice(proc.Mp(), batchRows)
				return err
			}
			vec.SetLength(rowCount)
			vec.GetNulls().AddRange(0, uint64(rowCount))
		}
		bestRight.SetRowCount(rowCount)
	}

	ctr.asofBuildLeftBestTimes = bestTimes
	ctr.asofBuildLeftMatched = matched
	ctr.asofBuildLeftBatchRows = batchRows
	ctr.asofBuildLeftBestRight = bestRight
	ctr.asofBuildLeftFinalRow = 0
	ctr.asofBuildLeftInitialized = true
	return nil
}

func (ctr *container) probeAsofBuildLeft(hashJoin *HashJoin, proc *process.Process) error {
	for i := range ctr.asofBuildLeftBatchRows {
		ctr.asofBuildLeftBatchRows[i] = -1
	}
	if err := ctr.evalJoinCondition(ctr.leftBat, proc); err != nil {
		return err
	}
	if ctr.itr == nil {
		ctr.itr = ctr.mp.NewIterator()
	}

	rowCount := ctr.leftBat.RowCount()
	for offset := 0; offset < rowCount; offset += hashmap.UnitLimit {
		count := min(rowCount-offset, hashmap.UnitLimit)
		values, zValues, err := ctr.itr.Find(offset, count, ctr.eqCondVecs)
		if err != nil {
			return err
		}
		for i, value := range values {
			if zValues[i] == 0 || value == 0 {
				continue
			}
			rightRow := offset + i
			rightTime, valid := asofTemporalValue(
				ctr.leftBat.Vecs[hashJoin.AsofRightCol], int64(rightRow))
			if !valid {
				continue
			}

			if ctr.probeHashOnPK {
				if err := ctr.updateAsofBuildLeftCandidate(
					hashJoin, proc, int32(value-1), rightRow, rightTime); err != nil {
					return err
				}
				continue
			}
			for _, leftRow := range ctr.mp.GetSels(value - 1) {
				if err := ctr.updateAsofBuildLeftCandidate(
					hashJoin, proc, leftRow, rightRow, rightTime); err != nil {
					return err
				}
			}
		}
	}
	for destinationRow, sourceRow := range ctr.asofBuildLeftBatchRows {
		if sourceRow < 0 {
			continue
		}
		if err := ctr.copyAsofBuildLeftRightRow(
			hashJoin, proc, destinationRow, ctr.leftBat, int(sourceRow)); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) updateAsofBuildLeftCandidate(
	hashJoin *HashJoin,
	proc *process.Process,
	leftOrdinal int32,
	rightRow int,
	rightTime int64,
) error {
	if leftOrdinal < 0 || int(leftOrdinal) >= len(ctr.asofBuildLeftMatched) {
		return moerr.NewInternalErrorNoCtx("ASOF logical-left row ordinal is out of range")
	}
	if int64(rightRow) > int64(^uint32(0)>>1) {
		return moerr.NewInternalErrorNoCtx("ASOF physical-right batch row is out of range")
	}
	leftBatch := int(leftOrdinal) / colexec.DefaultBatchSize
	leftRow := int(leftOrdinal) % colexec.DefaultBatchSize
	if leftBatch < 0 || leftBatch >= len(ctr.rightBats) ||
		leftRow >= ctr.rightBats[leftBatch].RowCount() {
		return moerr.NewInternalErrorNoCtx("ASOF logical-left build row is out of range")
	}
	leftTime, valid := asofTemporalValue(
		ctr.rightBats[leftBatch].Vecs[ctr.asofLeftCol], int64(leftRow))
	if !valid || !asofPredecessorEligible(rightTime, leftTime, ctr.asofStrict) {
		return nil
	}
	ordinal := int(leftOrdinal)
	if ctr.asofBuildLeftMatched[ordinal] != 0 &&
		rightTime <= ctr.asofBuildLeftBestTimes[ordinal] {
		return nil
	}
	qualified, err := ctr.evalAsofBuildLeftCondition(
		proc, ctr.rightBats[leftBatch], int64(leftRow), ctr.leftBat, int64(rightRow))
	if err != nil || !qualified {
		return err
	}
	ctr.asofBuildLeftBestTimes[ordinal] = rightTime
	ctr.asofBuildLeftMatched[ordinal] = 1
	ctr.asofBuildLeftBatchRows[ordinal] = int32(rightRow)
	return nil
}

func (ctr *container) evalAsofBuildLeftCondition(
	proc *process.Process,
	leftBatch *batch.Batch,
	leftRow int64,
	rightBatch *batch.Batch,
	rightRow int64,
) (bool, error) {
	if ctr.joinBats[0] == nil {
		ctr.joinBats[0], ctr.cfs1 = colexec.NewJoinBatch(leftBatch, proc.Mp())
	}
	if ctr.joinBats[1] == nil {
		ctr.joinBats[1], ctr.cfs2 = colexec.NewJoinBatch(rightBatch, proc.Mp())
	}
	if err := colexec.SetJoinBatchValues(ctr.joinBats[0], leftBatch, leftRow, 1, ctr.cfs1); err != nil {
		return false, err
	}
	if err := colexec.SetJoinBatchValues(ctr.joinBats[1], rightBatch, rightRow, 1, ctr.cfs2); err != nil {
		return false, err
	}
	vec, err := ctr.nonEqCondExec.Eval(proc, ctr.joinBats, nil)
	if err != nil {
		return false, err
	}
	return !vec.IsConstNull() && !vec.GetNulls().Contains(0) &&
		vector.MustFixedColWithTypeCheck[bool](vec)[0], nil
}

func asofRetainedAreaBytes(vec *vector.Vector, row int) int64 {
	if vec == nil || vec.IsNull(uint64(row)) || !vec.GetType().IsVarlen() {
		return 0
	}
	length := len(vec.GetBytesAt(row))
	if length <= types.VarlenaInlineSize {
		return 0
	}
	return int64(length)
}

func (ctr *container) copyAsofBuildLeftRightRow(
	hashJoin *HashJoin,
	proc *process.Process,
	destinationRow int,
	source *batch.Batch,
	sourceRow int,
) error {
	if ctr.asofBuildLeftBestRight == nil {
		return nil
	}
	for output, destination := range ctr.asofBuildLeftBestRight.Vecs {
		if destination == nil {
			continue
		}
		resultPos := hashJoin.ResultCols[output]
		if resultPos.Rel != 1 || resultPos.Pos < 0 || int(resultPos.Pos) >= len(source.Vecs) {
			return moerr.NewInternalErrorNoCtx("ASOF projected right column is out of range")
		}
		sourceVec := source.Vecs[resultPos.Pos]
		oldBytes := asofRetainedAreaBytes(destination, destinationRow)
		newBytes := asofRetainedAreaBytes(sourceVec, sourceRow)
		if err := destination.Copy(sourceVec, int64(destinationRow), int64(sourceRow), proc.Mp()); err != nil {
			return err
		}
		ctr.asofBuildLeftDeadBytes += oldBytes
		ctr.asofBuildLeftLiveBytes += newBytes - oldBytes
	}
	return nil
}

func (ctr *container) compactAsofBuildLeftCandidates(hashJoin *HashJoin, proc *process.Process) error {
	if ctr.asofBuildLeftBestRight == nil ||
		ctr.asofBuildLeftDeadBytes <= ctr.asofBuildLeftLiveBytes {
		return nil
	}
	clones := make([]*vector.Vector, len(ctr.asofBuildLeftBestRight.Vecs))
	for i, old := range ctr.asofBuildLeftBestRight.Vecs {
		if old == nil {
			continue
		}
		clone, err := old.CloneToFlatCompactWithAllocation(proc.Mp(), hashJoin.asofCandidateAllocation)
		if err != nil {
			for _, created := range clones {
				if created != nil {
					created.Free(proc.Mp())
				}
			}
			return err
		}
		clones[i] = clone
	}
	for i, old := range ctr.asofBuildLeftBestRight.Vecs {
		if old != nil {
			old.Free(proc.Mp())
		}
		ctr.asofBuildLeftBestRight.Vecs[i] = clones[i]
	}
	ctr.asofBuildLeftDeadBytes = 0
	return nil
}

func (ctr *container) finalizeAsofBuildLeft(
	hashJoin *HashJoin,
	proc *process.Process,
	result *vm.CallResult,
) error {
	if err := hashJoin.resetResultBat(); err != nil {
		return err
	}
	outputRows := 0
	for ctr.asofBuildLeftFinalRow < int64(len(ctr.asofBuildLeftMatched)) &&
		outputRows < colexec.DefaultBatchSize {
		ordinal := ctr.asofBuildLeftFinalRow
		ctr.asofBuildLeftFinalRow++
		matched := ctr.asofBuildLeftMatched[ordinal] != 0
		if !matched && hashJoin.JoinType == plan.Node_ASOF {
			continue
		}
		leftBatch := ordinal / colexec.DefaultBatchSize
		leftRow := ordinal % colexec.DefaultBatchSize
		if leftBatch < 0 || leftBatch >= int64(len(ctr.rightBats)) ||
			leftRow >= int64(ctr.rightBats[leftBatch].RowCount()) {
			return moerr.NewInternalErrorNoCtx("ASOF logical-left finalize row is out of range")
		}
		for output, resultPos := range hashJoin.ResultCols {
			switch resultPos.Rel {
			case 0:
				if err := ctr.resBat.Vecs[output].UnionOne(
					ctr.rightBats[leftBatch].Vecs[resultPos.Pos], leftRow, proc.Mp()); err != nil {
					return err
				}
			case 1:
				if matched {
					if err := ctr.resBat.Vecs[output].UnionOne(
						ctr.asofBuildLeftBestRight.Vecs[output], ordinal, proc.Mp()); err != nil {
						return err
					}
				} else if err := ctr.resBat.Vecs[output].UnionNull(proc.Mp()); err != nil {
					return err
				}
			}
		}
		outputRows++
	}
	if outputRows == 0 {
		result.Batch = nil
		return nil
	}
	ctr.resBat.SetRowCount(outputRows)
	result.Batch = ctr.resBat
	return nil
}
