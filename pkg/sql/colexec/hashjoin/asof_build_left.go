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
	"slices"

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

// Actual equality groups up to this size use the lower-constant direct path.
// Larger groups use range updates, so this is a performance crossover rather
// than a planner estimate or correctness bound.
const asofBuildLeftDirectGroupLimit = 64

// callAsofBuildLeft is the bounded-memory physical path for a memory-cheaper
// logical left input. HashBuild materializes the logical left rows. This
// operator scans the logical right once, retains bounded shared candidate
// payloads, and emits only after the right input reaches EOF.
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

func (ctr *container) initAsofBuildLeftState(hashJoin *HashJoin, proc *process.Process) (err error) {
	if ctr.asofBuildLeftInitialized {
		return nil
	}
	if ctr.rightRowCnt < 0 || ctr.rightRowCnt > int64(^uint32(0)>>1) ||
		int64(int(ctr.rightRowCnt)) != ctr.rightRowCnt {
		return moerr.NewInternalErrorNoCtx("ASOF logical-left row count exceeds addressable memory")
	}
	rowCount := int(ctr.rightRowCnt)
	groups, order, leafPos, indexed, slotCount, err :=
		ctr.initAsofBuildLeftRangeIndex(hashJoin, proc, rowCount)
	if err != nil {
		return err
	}
	var bestTimes []int64
	var bestSequences []uint64
	var matched []uint8
	var nodePayload []int32
	var payloadRefs []int32
	var payloadLive []uint8
	var freePayloadSlots []int32
	var batchRows []int32
	var touchedSlots []int32
	var bestRight *batch.Batch
	committed := false
	defer func() {
		if committed {
			return
		}
		if bestRight != nil {
			bestRight.Clean(proc.Mp())
		}
		mpool.FreeSlice(proc.Mp(), bestTimes)
		mpool.FreeSlice(proc.Mp(), bestSequences)
		mpool.FreeSlice(proc.Mp(), matched)
		mpool.FreeSlice(proc.Mp(), nodePayload)
		mpool.FreeSlice(proc.Mp(), payloadRefs)
		mpool.FreeSlice(proc.Mp(), payloadLive)
		mpool.FreeSlice(proc.Mp(), freePayloadSlots)
		mpool.FreeSlice(proc.Mp(), batchRows)
		mpool.FreeSlice(proc.Mp(), touchedSlots)
		mpool.FreeSlice(proc.Mp(), groups)
		mpool.FreeSlice(proc.Mp(), order)
		mpool.FreeSlice(proc.Mp(), leafPos)
	}()

	bestTimes, err = mpool.MakeSliceAccounted[int64](
		slotCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return err
	}
	if indexed {
		bestSequences, err = mpool.MakeSliceAccounted[uint64](
			slotCount, proc.Mp(), hashJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
		)
		if err != nil {
			return err
		}
		nodePayload, err = mpool.MakeSliceAccounted[int32](
			slotCount, proc.Mp(), hashJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
		)
		if err != nil {
			return err
		}
		for i := range nodePayload {
			nodePayload[i] = -1
		}
		payloadRefs, err = mpool.MakeSliceAccounted[int32](
			slotCount, proc.Mp(), hashJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
		)
		if err != nil {
			return err
		}
		payloadLive, err = mpool.MakeSliceAccounted[uint8](
			slotCount, proc.Mp(), hashJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
		)
		if err != nil {
			return err
		}
		freePayloadSlots, err = mpool.MakeSliceAccounted[int32](
			slotCount, proc.Mp(), hashJoin.allocationAccount,
			mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
		)
		if err != nil {
			return err
		}
	}
	matched, err = mpool.MakeSliceAccounted[uint8](
		slotCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return err
	}
	batchRows, err = mpool.MakeSliceAccounted[int32](
		slotCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return err
	}
	for i := range batchRows {
		batchRows[i] = -1
	}
	touchedSlots, err = mpool.MakeSliceAccounted[int32](
		slotCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return err
	}

	bestRight = batch.NewOffHeapWithSize(len(hashJoin.RightTypes))
	for column := range hashJoin.RightTypes {
		bestRight.Vecs[column] = vector.NewOffHeapVecWithType(hashJoin.RightTypes[column])
	}
	if err = bestRight.SetAllocationAccount(hashJoin.asofCandidateAllocation); err != nil {
		return err
	}
	for _, vec := range bestRight.Vecs {
		if err = vec.PreExtend(slotCount, proc.Mp()); err != nil {
			return err
		}
		if err = vec.PreExtendNulls(slotCount, proc.Mp()); err != nil {
			return err
		}
		vec.SetLength(slotCount)
		vec.GetNulls().AddRange(0, uint64(slotCount))
	}
	bestRight.SetRowCount(slotCount)

	ctr.asofBuildLeftBestTimes = bestTimes
	ctr.asofBuildLeftBestSequences = bestSequences
	ctr.asofBuildLeftMatched = matched
	ctr.asofBuildLeftNodePayload = nodePayload
	ctr.asofBuildLeftPayloadRefs = payloadRefs
	ctr.asofBuildLeftPayloadLive = payloadLive
	ctr.asofBuildLeftFreePayloadSlots = freePayloadSlots
	ctr.asofBuildLeftFreePayloadCount = 0
	ctr.asofBuildLeftNextPayload = 0
	ctr.asofBuildLeftBatchRows = batchRows
	ctr.asofBuildLeftTouchedSlots = touchedSlots
	ctr.asofBuildLeftTouchedCount = 0
	ctr.asofBuildLeftGroups = groups
	ctr.asofBuildLeftOrder = order
	ctr.asofBuildLeftLeafPos = leafPos
	ctr.asofBuildLeftIndexed = indexed
	ctr.asofBuildLeftProbeSequence = 0
	ctr.asofBuildLeftBestRight = bestRight
	ctr.asofBuildLeftFinalRow = 0
	ctr.asofBuildLeftInitialized = true
	committed = true
	return nil
}

func (ctr *container) initAsofBuildLeftRangeIndex(
	hashJoin *HashJoin,
	proc *process.Process,
	rowCount int,
) (groups []asofBuildLeftGroup, order, leafPos []int32, indexed bool, slotCount int, err error) {
	if ctr.mp == nil || ctr.mp.HashOnUnique() {
		return nil, nil, nil, false, rowCount, nil
	}
	groupCount64 := ctr.mp.GetGroupCount()
	if groupCount64 > uint64(^uint32(0)>>1) || uint64(int(groupCount64)) != groupCount64 {
		return nil, nil, nil, false, 0,
			moerr.NewInternalErrorNoCtx("ASOF equality-group count exceeds addressable memory")
	}
	groupCount := int(groupCount64)
	maxGroupRows := 0
	for group := range groupCount {
		maxGroupRows = max(maxGroupRows, len(ctr.mp.GetSels(uint64(group))))
	}
	if maxGroupRows <= asofBuildLeftDirectGroupLimit {
		return nil, nil, nil, false, rowCount, nil
	}

	groups, err = mpool.MakeSliceAccounted[asofBuildLeftGroup](
		groupCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return nil, nil, nil, false, 0, err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		mpool.FreeSlice(proc.Mp(), groups)
		mpool.FreeSlice(proc.Mp(), order)
		mpool.FreeSlice(proc.Mp(), leafPos)
		groups, order, leafPos = nil, nil, nil
	}()

	validRows := 0
	for group := range groupCount {
		start := validRows
		for _, ordinal := range ctr.mp.GetSels(uint64(group)) {
			if ordinal < 0 || int(ordinal) >= rowCount {
				return nil, nil, nil, false, 0,
					moerr.NewInternalErrorNoCtx("ASOF logical-left group row is out of range")
			}
			if _, valid := ctr.asofBuildLeftTemporalValue(ordinal); valid {
				validRows++
			}
		}
		if validRows > int(^uint32(0)>>1) {
			return nil, nil, nil, false, 0,
				moerr.NewInternalErrorNoCtx("ASOF temporal index exceeds addressable memory")
		}
		groups[group] = asofBuildLeftGroup{
			start: int32(start), length: int32(validRows - start),
		}
	}
	if validRows > int(^uint32(0)>>2) {
		return nil, nil, nil, false, 0,
			moerr.NewInternalErrorNoCtx("ASOF temporal range tree exceeds addressable memory")
	}
	order, err = mpool.MakeSliceAccounted[int32](
		validRows, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return nil, nil, nil, false, 0, err
	}
	leafPos, err = mpool.MakeSliceAccounted[int32](
		rowCount, proc.Mp(), hashJoin.allocationAccount,
		mpool.AllocationOwnerHashBuild, hashJoinAllocationSiteAsofCandidateState,
	)
	if err != nil {
		return nil, nil, nil, false, 0, err
	}
	for i := range leafPos {
		leafPos[i] = -1
	}
	for group := range groupCount {
		metadata := groups[group]
		next := int(metadata.start)
		end := next + int(metadata.length)
		for _, ordinal := range ctr.mp.GetSels(uint64(group)) {
			if _, valid := ctr.asofBuildLeftTemporalValue(ordinal); valid {
				order[next] = ordinal
				next++
			}
		}
		if next != end {
			return nil, nil, nil, false, 0,
				moerr.NewInternalErrorNoCtx("ASOF temporal group changed during index construction")
		}
		slices.SortFunc(order[metadata.start:int32(end)], func(left, right int32) int {
			leftTime, _ := ctr.asofBuildLeftTemporalValue(left)
			rightTime, _ := ctr.asofBuildLeftTemporalValue(right)
			if leftTime < rightTime {
				return -1
			}
			if leftTime > rightTime {
				return 1
			}
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		})
		for position := int(metadata.start); position < end; position++ {
			leafPos[order[position]] = int32(position)
		}
	}
	committed = true
	return groups, order, leafPos, true, 2 * validRows, nil
}

func (ctr *container) asofBuildLeftTemporalValue(ordinal int32) (int64, bool) {
	if ordinal < 0 {
		return 0, false
	}
	leftBatch := int(ordinal) / colexec.DefaultBatchSize
	leftRow := int(ordinal) % colexec.DefaultBatchSize
	if leftBatch < 0 || leftBatch >= len(ctr.rightBats) ||
		leftRow >= ctr.rightBats[leftBatch].RowCount() {
		return 0, false
	}
	return asofTemporalValue(
		ctr.rightBats[leftBatch].Vecs[ctr.asofLeftCol], int64(leftRow))
}

func (ctr *container) probeAsofBuildLeft(hashJoin *HashJoin, proc *process.Process) error {
	if ctr.asofBuildLeftTouchedCount != 0 {
		return moerr.NewInternalErrorNoCtx("ASOF candidate batch state was not drained")
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
			if ctr.asofBuildLeftProbeSequence == ^uint64(0) {
				return moerr.NewInternalErrorNoCtx("ASOF physical-right row sequence overflow")
			}
			ctr.asofBuildLeftProbeSequence++

			if ctr.asofBuildLeftIndexed {
				if _, err := ctr.updateAsofBuildLeftRange(
					uint64(value-1), rightRow, rightTime,
					ctr.asofBuildLeftProbeSequence); err != nil {
					return err
				}
				continue
			}
			if ctr.probeHashOnPK {
				if err := ctr.updateAsofBuildLeftCandidate(
					int32(value-1), rightRow, rightTime,
					ctr.asofBuildLeftProbeSequence); err != nil {
					return err
				}
				continue
			}
			for _, leftRow := range ctr.mp.GetSels(value - 1) {
				if err := ctr.updateAsofBuildLeftCandidate(
					leftRow, rightRow, rightTime,
					ctr.asofBuildLeftProbeSequence); err != nil {
					return err
				}
			}
		}
	}
	for i := 0; i < ctr.asofBuildLeftTouchedCount; i++ {
		destinationRow := int(ctr.asofBuildLeftTouchedSlots[i])
		if destinationRow < 0 || destinationRow >= len(ctr.asofBuildLeftBatchRows) {
			return moerr.NewInternalErrorNoCtx("ASOF touched candidate slot is out of range")
		}
		sourceRow := ctr.asofBuildLeftBatchRows[destinationRow]
		if sourceRow < 0 {
			return moerr.NewInternalErrorNoCtx("ASOF touched candidate slot has no source row")
		}
		if ctr.asofBuildLeftIndexed && ctr.asofBuildLeftPayloadRefs[destinationRow] == 0 {
			ctr.asofBuildLeftBatchRows[destinationRow] = -1
			continue
		}
		if err := ctr.copyAsofBuildLeftRightRow(
			proc, destinationRow, ctr.leftBat, int(sourceRow)); err != nil {
			return err
		}
		ctr.asofBuildLeftBatchRows[destinationRow] = -1
	}
	ctr.asofBuildLeftTouchedCount = 0
	return nil
}

func (ctr *container) updateAsofBuildLeftCandidate(
	leftOrdinal int32,
	rightRow int,
	rightTime int64,
	sequence uint64,
) error {
	if leftOrdinal < 0 || int(leftOrdinal) >= len(ctr.asofBuildLeftMatched) {
		return moerr.NewInternalErrorNoCtx("ASOF logical-left row ordinal is out of range")
	}
	if rightRow < 0 || int64(rightRow) > int64(^uint32(0)>>1) {
		return moerr.NewInternalErrorNoCtx("ASOF physical-right batch row is out of range")
	}
	leftTime, valid := ctr.asofBuildLeftTemporalValue(leftOrdinal)
	if !valid {
		leftBatch := int(leftOrdinal) / colexec.DefaultBatchSize
		leftRow := int(leftOrdinal) % colexec.DefaultBatchSize
		if leftBatch < 0 || leftBatch >= len(ctr.rightBats) ||
			leftRow >= ctr.rightBats[leftBatch].RowCount() {
			return moerr.NewInternalErrorNoCtx("ASOF logical-left build row is out of range")
		}
		return nil
	}
	if !asofPredecessorEligible(rightTime, leftTime, ctr.asofStrict) {
		return nil
	}
	return ctr.recordAsofBuildLeftCandidate(
		int(leftOrdinal), rightRow, rightTime, sequence)
}

func (ctr *container) recordAsofBuildLeftCandidate(
	slot int,
	rightRow int,
	rightTime int64,
	sequence uint64,
) error {
	if ctr.asofBuildLeftIndexed {
		return moerr.NewInternalErrorNoCtx("ASOF direct candidate update used an indexed generation")
	}
	if slot < 0 || slot >= len(ctr.asofBuildLeftMatched) {
		return moerr.NewInternalErrorNoCtx("ASOF candidate slot is out of range")
	}
	if rightRow < 0 || int64(rightRow) > int64(^uint32(0)>>1) {
		return moerr.NewInternalErrorNoCtx("ASOF physical-right batch row is out of range")
	}
	if !ctr.asofBuildLeftCandidateWins(slot, rightTime, sequence) {
		return nil
	}
	ctr.asofBuildLeftBestTimes[slot] = rightTime
	ctr.asofBuildLeftMatched[slot] = 1
	return ctr.markAsofBuildLeftPayload(slot, rightRow)
}

func (ctr *container) asofBuildLeftCandidateWins(slot int, rightTime int64, sequence uint64) bool {
	if ctr.asofBuildLeftMatched[slot] == 0 {
		return true
	}
	if rightTime != ctr.asofBuildLeftBestTimes[slot] {
		return rightTime > ctr.asofBuildLeftBestTimes[slot]
	}
	return ctr.asofBuildLeftIndexed && sequence < ctr.asofBuildLeftBestSequences[slot]
}

func (ctr *container) markAsofBuildLeftPayload(payloadSlot, rightRow int) error {
	if payloadSlot < 0 || payloadSlot >= len(ctr.asofBuildLeftBatchRows) {
		return moerr.NewInternalErrorNoCtx("ASOF retained payload slot is out of range")
	}
	if rightRow < 0 || int64(rightRow) > int64(^uint32(0)>>1) {
		return moerr.NewInternalErrorNoCtx("ASOF physical-right batch row is out of range")
	}
	if ctr.asofBuildLeftBatchRows[payloadSlot] < 0 {
		if ctr.asofBuildLeftTouchedCount >= len(ctr.asofBuildLeftTouchedSlots) {
			return moerr.NewInternalErrorNoCtx("ASOF touched candidate slots exceed their bound")
		}
		ctr.asofBuildLeftTouchedSlots[ctr.asofBuildLeftTouchedCount] = int32(payloadSlot)
		ctr.asofBuildLeftTouchedCount++
	}
	ctr.asofBuildLeftBatchRows[payloadSlot] = int32(rightRow)
	return nil
}

func (ctr *container) allocateAsofBuildLeftPayload(rightRow int) (int, error) {
	payloadSlot := -1
	if ctr.asofBuildLeftFreePayloadCount > 0 {
		ctr.asofBuildLeftFreePayloadCount--
		payloadSlot = int(ctr.asofBuildLeftFreePayloadSlots[ctr.asofBuildLeftFreePayloadCount])
	} else {
		if ctr.asofBuildLeftNextPayload >= len(ctr.asofBuildLeftPayloadRefs) {
			return 0, moerr.NewInternalErrorNoCtx("ASOF retained payload slots exceed their bound")
		}
		payloadSlot = ctr.asofBuildLeftNextPayload
		ctr.asofBuildLeftNextPayload++
	}
	if payloadSlot < 0 || payloadSlot >= len(ctr.asofBuildLeftPayloadRefs) ||
		ctr.asofBuildLeftPayloadRefs[payloadSlot] != 0 ||
		ctr.asofBuildLeftPayloadLive[payloadSlot] != 0 {
		return 0, moerr.NewInternalErrorNoCtx("ASOF retained payload slot was reused while active")
	}
	if err := ctr.markAsofBuildLeftPayload(payloadSlot, rightRow); err != nil {
		return 0, err
	}
	return payloadSlot, nil
}

func (ctr *container) releaseAsofBuildLeftPayload(payloadSlot int) error {
	if payloadSlot < 0 || payloadSlot >= len(ctr.asofBuildLeftPayloadRefs) ||
		ctr.asofBuildLeftPayloadRefs[payloadSlot] <= 0 {
		return moerr.NewInternalErrorNoCtx("ASOF retained payload reference is invalid")
	}
	ctr.asofBuildLeftPayloadRefs[payloadSlot]--
	if ctr.asofBuildLeftPayloadRefs[payloadSlot] != 0 {
		return nil
	}
	if ctr.asofBuildLeftPayloadLive[payloadSlot] != 0 {
		for _, vec := range ctr.asofBuildLeftBestRight.Vecs {
			oldBytes := asofRetainedAreaBytes(vec, payloadSlot)
			ctr.asofBuildLeftLiveBytes -= oldBytes
			ctr.asofBuildLeftDeadBytes += oldBytes
			vec.GetNulls().Add(uint64(payloadSlot))
		}
		ctr.asofBuildLeftPayloadLive[payloadSlot] = 0
	}
	if ctr.asofBuildLeftFreePayloadCount >= len(ctr.asofBuildLeftFreePayloadSlots) {
		return moerr.NewInternalErrorNoCtx("ASOF free payload slots exceed their bound")
	}
	ctr.asofBuildLeftFreePayloadSlots[ctr.asofBuildLeftFreePayloadCount] = int32(payloadSlot)
	ctr.asofBuildLeftFreePayloadCount++
	return nil
}

func (ctr *container) recordAsofBuildLeftTreeCandidate(
	node int,
	rightRow int,
	rightTime int64,
	sequence uint64,
	sharedPayload *int,
) error {
	if node <= 0 || node >= len(ctr.asofBuildLeftMatched) {
		return moerr.NewInternalErrorNoCtx("ASOF range-tree candidate node is out of range")
	}
	if !ctr.asofBuildLeftCandidateWins(node, rightTime, sequence) {
		return nil
	}
	if *sharedPayload < 0 {
		payloadSlot, err := ctr.allocateAsofBuildLeftPayload(rightRow)
		if err != nil {
			return err
		}
		*sharedPayload = payloadSlot
	}
	oldPayload := int(ctr.asofBuildLeftNodePayload[node])
	if oldPayload >= 0 {
		if oldPayload == *sharedPayload {
			return moerr.NewInternalErrorNoCtx("ASOF range-tree node retained its replacement payload")
		}
		if err := ctr.releaseAsofBuildLeftPayload(oldPayload); err != nil {
			return err
		}
	}
	if ctr.asofBuildLeftPayloadRefs[*sharedPayload] == int32(^uint32(0)>>1) {
		return moerr.NewInternalErrorNoCtx("ASOF retained payload reference count overflow")
	}
	ctr.asofBuildLeftPayloadRefs[*sharedPayload]++
	ctr.asofBuildLeftNodePayload[node] = int32(*sharedPayload)
	ctr.asofBuildLeftBestTimes[node] = rightTime
	ctr.asofBuildLeftBestSequences[node] = sequence
	ctr.asofBuildLeftMatched[node] = 1
	return nil
}

// updateAsofBuildLeftRange records one logical-right row on O(log L) tree
// nodes covering every temporal-eligible logical-left row in this equality
// group. The returned visit count is a deterministic work oracle for tests.
func (ctr *container) updateAsofBuildLeftRange(
	groupKey uint64,
	rightRow int,
	rightTime int64,
	sequence uint64,
) (int, error) {
	if groupKey >= uint64(len(ctr.asofBuildLeftGroups)) {
		return 0, moerr.NewInternalErrorNoCtx("ASOF equality-group id is out of range")
	}
	group := ctr.asofBuildLeftGroups[groupKey]
	start := int(group.start)
	length := int(group.length)
	if start < 0 || length < 0 || start > len(ctr.asofBuildLeftOrder)-length {
		return 0, moerr.NewInternalErrorNoCtx("ASOF temporal group range is out of bounds")
	}
	if length == 0 {
		return 0, nil
	}
	low, high := 0, length
	for low < high {
		mid := low + (high-low)/2
		ordinal := ctr.asofBuildLeftOrder[start+mid]
		leftTime, valid := ctr.asofBuildLeftTemporalValue(ordinal)
		if !valid {
			return 0, moerr.NewInternalErrorNoCtx("ASOF temporal range index contains a NULL row")
		}
		beforeRange := leftTime < rightTime || ctr.asofStrict && leftTime == rightTime
		if beforeRange {
			low = mid + 1
		} else {
			high = mid
		}
	}
	left := start + low
	right := start + length
	if left >= right {
		return 0, nil
	}
	treeBase := len(ctr.asofBuildLeftOrder)
	left += treeBase
	right += treeBase
	visits := 0
	sharedPayload := -1
	for left < right {
		if left&1 != 0 {
			if err := ctr.recordAsofBuildLeftTreeCandidate(
				left, rightRow, rightTime, sequence, &sharedPayload); err != nil {
				return visits, err
			}
			visits++
			left++
		}
		if right&1 != 0 {
			right--
			if err := ctr.recordAsofBuildLeftTreeCandidate(
				right, rightRow, rightTime, sequence, &sharedPayload); err != nil {
				return visits, err
			}
			visits++
		}
		left /= 2
		right /= 2
	}
	return visits, nil
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
	proc *process.Process,
	destinationRow int,
	source *batch.Batch,
	sourceRow int,
) error {
	if ctr.asofBuildLeftBestRight == nil {
		return nil
	}
	if destinationRow < 0 || destinationRow >= ctr.asofBuildLeftBestRight.RowCount() ||
		source == nil || sourceRow < 0 || sourceRow >= source.RowCount() {
		return moerr.NewInternalErrorNoCtx("ASOF retained payload copy row is out of range")
	}
	if ctr.asofBuildLeftIndexed && ctr.asofBuildLeftPayloadLive[destinationRow] != 0 {
		return moerr.NewInternalErrorNoCtx("ASOF retained payload overwrite is still live")
	}
	newLiveBytes := int64(0)
	for column, destination := range ctr.asofBuildLeftBestRight.Vecs {
		if column >= len(source.Vecs) || source.Vecs[column] == nil {
			return moerr.NewInternalErrorNoCtx("ASOF retained right column is out of range")
		}
		sourceVec := source.Vecs[column]
		oldBytes := asofRetainedAreaBytes(destination, destinationRow)
		newBytes := asofRetainedAreaBytes(sourceVec, sourceRow)
		if err := destination.Copy(sourceVec, int64(destinationRow), int64(sourceRow), proc.Mp()); err != nil {
			return err
		}
		if ctr.asofBuildLeftIndexed {
			newLiveBytes += newBytes
		} else {
			ctr.asofBuildLeftDeadBytes += oldBytes
			ctr.asofBuildLeftLiveBytes += newBytes - oldBytes
		}
	}
	if ctr.asofBuildLeftIndexed {
		ctr.asofBuildLeftLiveBytes += newLiveBytes
		ctr.asofBuildLeftPayloadLive[destinationRow] = 1
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
	for ctr.asofBuildLeftFinalRow < ctr.rightRowCnt &&
		outputRows < colexec.DefaultBatchSize {
		ordinal := ctr.asofBuildLeftFinalRow
		ctr.asofBuildLeftFinalRow++
		leftBatch := ordinal / colexec.DefaultBatchSize
		leftRow := ordinal % colexec.DefaultBatchSize
		if leftBatch < 0 || leftBatch >= int64(len(ctr.rightBats)) ||
			leftRow >= int64(ctr.rightBats[leftBatch].RowCount()) {
			return moerr.NewInternalErrorNoCtx("ASOF logical-left finalize row is out of range")
		}
		candidateSlot, matched, err := ctr.asofBuildLeftCandidateSlot(ordinal)
		if err != nil {
			return err
		}
		if matched {
			qualified, evalErr := ctr.evalAsofBuildLeftCondition(
				proc,
				ctr.rightBats[leftBatch], leftRow,
				ctr.asofBuildLeftBestRight, candidateSlot,
			)
			if evalErr != nil {
				return evalErr
			}
			matched = qualified
		}
		if !matched && hashJoin.JoinType == plan.Node_ASOF {
			continue
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
					if resultPos.Pos < 0 || int(resultPos.Pos) >= len(ctr.asofBuildLeftBestRight.Vecs) {
						return moerr.NewInternalErrorNoCtx("ASOF finalized right column is out of range")
					}
					if err := ctr.resBat.Vecs[output].UnionOne(
						ctr.asofBuildLeftBestRight.Vecs[resultPos.Pos], candidateSlot, proc.Mp()); err != nil {
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

func (ctr *container) asofBuildLeftCandidateSlot(ordinal int64) (int64, bool, error) {
	if ordinal < 0 || ordinal >= ctr.rightRowCnt {
		return 0, false, moerr.NewInternalErrorNoCtx("ASOF logical-left candidate row is out of range")
	}
	if !ctr.asofBuildLeftIndexed {
		return ordinal, ctr.asofBuildLeftMatched[ordinal] != 0, nil
	}
	if ordinal >= int64(len(ctr.asofBuildLeftLeafPos)) {
		return 0, false, moerr.NewInternalErrorNoCtx("ASOF logical-left leaf row is out of range")
	}
	position := ctr.asofBuildLeftLeafPos[ordinal]
	if position < 0 {
		return 0, false, nil
	}
	node := int(position) + len(ctr.asofBuildLeftOrder)
	bestNode := -1
	for node > 0 {
		if node >= len(ctr.asofBuildLeftMatched) {
			return 0, false, moerr.NewInternalErrorNoCtx("ASOF range-tree node is out of range")
		}
		if ctr.asofBuildLeftMatched[node] != 0 &&
			(bestNode < 0 ||
				ctr.asofBuildLeftBestTimes[node] > ctr.asofBuildLeftBestTimes[bestNode] ||
				ctr.asofBuildLeftBestTimes[node] == ctr.asofBuildLeftBestTimes[bestNode] &&
					ctr.asofBuildLeftBestSequences[node] < ctr.asofBuildLeftBestSequences[bestNode]) {
			bestNode = node
		}
		node /= 2
	}
	if bestNode < 0 {
		return 0, false, nil
	}
	payloadSlot := int(ctr.asofBuildLeftNodePayload[bestNode])
	if payloadSlot < 0 || payloadSlot >= len(ctr.asofBuildLeftPayloadRefs) ||
		ctr.asofBuildLeftPayloadRefs[payloadSlot] <= 0 ||
		ctr.asofBuildLeftPayloadLive[payloadSlot] == 0 {
		return 0, false, moerr.NewInternalErrorNoCtx("ASOF range-tree payload is not retained")
	}
	return int64(payloadSlot), true, nil
}
