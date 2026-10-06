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

package order

import (
	"bytes"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/ordersites"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "order"

func orderPhaseError(phase string, err error, retained, incoming *batch.Batch) error {
	if err == nil {
		return nil
	}
	retainedBytes, retainedRows := 0, 0
	if retained != nil {
		retainedBytes, retainedRows = retained.Size(), retained.RowCount()
	}
	incomingBytes, incomingRows := 0, 0
	if incoming != nil {
		incomingBytes, incomingRows = incoming.Size(), incoming.RowCount()
	}
	return fmt.Errorf(
		"order phase=%s retained-bytes=%d retained-rows=%d incoming-bytes=%d incoming-rows=%d: %w",
		phase, retainedBytes, retainedRows, incomingBytes, incomingRows, err,
	)
}

func addOrderBytes(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}

func multiplyOrderBytes(value, factor uint64) uint64 {
	if value != 0 && factor > math.MaxUint64/value {
		return math.MaxUint64
	}
	return value * factor
}

func (ctr *container) orderScratchBytes() uint64 {
	if ctr == nil {
		return 0
	}
	bytes := multiplyOrderBytes(uint64(cap(ctr.resultOrderList)), 8)
	bytes = addOrderBytes(bytes,
		multiplyOrderBytes(uint64(cap(ctr.sortScratch.Partitions)), 8))
	bytes = addOrderBytes(bytes, uint64(cap(ctr.sortScratch.Diffs)))
	return bytes
}

func (ctr *container) retainedOrderBytes() uint64 {
	bytes := ctr.orderScratchBytes()
	if ctr != nil && ctr.batWaitForSort != nil {
		bytes = addOrderBytes(bytes, uint64(max(0, ctr.batWaitForSort.Allocated())))
	}
	return bytes
}

// recoveryCapacityTarget bounds the allocations required after the input has
// been retained but before the sorted run can be returned. Shuffle replaces
// the logical vector payload and may temporarily duplicate accounted bitmap
// and row-metadata storage. Sort selectors and multi-key scratch can each grow
// to at most twice their requested size. Allocated bytes, rather than logical
// Size, cover spare vector and sidecar capacity in the projected run.
func (ctr *container) recoveryCapacityTarget(incoming *batch.Batch) uint64 {
	if ctr == nil || incoming == nil {
		return 0
	}
	currentData := uint64(0)
	data := uint64(max(0, incoming.Allocated()))
	rows := uint64(max(0, incoming.RowCount()))
	if ctr.batWaitForSort != nil {
		currentData = uint64(max(0, ctr.batWaitForSort.Allocated()))
		data = addOrderBytes(data, currentData)
		rows = addOrderBytes(rows,
			uint64(max(0, ctr.batWaitForSort.RowCount())))
	}
	requiredScratch := multiplyOrderBytes(rows, 8)
	if len(ctr.sortVectors) > 1 {
		requiredScratch = addOrderBytes(
			requiredScratch, multiplyOrderBytes(rows, 9))
	}
	// Scratch survives between emitted runs. It is already borrowed from this
	// controller, so a small next run must reserve its data in addition to that
	// live scratch rather than mistaking the old floor for unused headroom.
	existingScratch := ctr.orderScratchBytes()
	scratchPeak := multiplyOrderBytes(requiredScratch, 2)
	if existingScratch > scratchPeak {
		scratchPeak = existingScratch
	}
	appendCapacity := uint64(0)
	// Append rollback checkpoints are retained for the attempt and are needed
	// before any vector can consume the incoming rows. Include their rounded
	// physical capacity in the pre-admitted floor as well.
	if len(incoming.Vecs) > 0 {
		_, checkpointBytes, err := vector.AppendCheckpointScratch(
			nil, len(incoming.Vecs))
		if err != nil {
			return math.MaxUint64
		}
		checkpointCapacity := int64(0)
		if ctr.appendScratch != nil {
			checkpointCapacity = int64(ctr.appendScratch.Cap())
		}
		if int64(checkpointBytes) > checkpointCapacity {
			if grown, ok := mpool.GrowCapacity(
				checkpointCapacity, int64(checkpointBytes)); ok {
				checkpointCapacity = grown
			} else {
				return math.MaxUint64
			}
		}
		appendCapacity = uint64(checkpointCapacity)
	}
	modeledCurrent := addOrderBytes(currentData, existingScratch)
	if ctr.appendScratch != nil {
		modeledCurrent = addOrderBytes(
			modeledCurrent, uint64(ctr.appendScratch.Cap()))
	}
	unmodeledBorrowed := uint64(0)
	if ctr.recoveryCapacity != nil {
		_, borrowed := ctr.recoveryCapacity.Snapshot()
		if borrowed > modeledCurrent {
			unmodeledBorrowed = borrowed - modeledCurrent
		}
	}
	target := addOrderBytes(
		unmodeledBorrowed,
		// Append growth can retain up to twice the combined input capacity;
		// shuffling one vector then overlaps one further payload-sized copy.
		// Match the three-copy peak used by shouldFlushBeforeAppend so the
		// recovery controller never has to grow after input is retained.
		multiplyOrderBytes(data, 3),
	)
	target = addOrderBytes(target, scratchPeak)
	return addOrderBytes(target, appendCapacity)
}

func (ctr *container) ensureRecoveryCapacity(incoming *batch.Batch) error {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return nil
	}
	target := ctr.recoveryCapacityTarget(incoming)
	if target <= ctr.recoveryCapacityFloor {
		return nil
	}
	if err := ctr.recoveryCapacity.EnsureCapacity(target); err != nil {
		return err
	}
	ctr.recoveryCapacityFloor = target
	return nil
}

// shouldFlushBeforeAppend asks the shared query/CN controller for this live
// sorter's work-conserving share. The estimate includes append growth, the
// shuffle copy, and row-index scratch needed to turn the retained rows into a
// sorted run. The fixed 64 MiB run size remains the normal throughput policy;
// this path only shortens a run when live memory is scarce.
func (ctr *container) shouldFlushBeforeAppend(incoming *batch.Batch) (bool, error) {
	if ctr == nil || ctr.growthParticipant == nil || ctr.batWaitForSort == nil ||
		incoming == nil {
		return false, nil
	}
	current := ctr.retainedOrderBytes()
	limit, err := ctr.growthParticipant.RetainedLimit(current)
	if err != nil {
		return false, err
	}
	currentData := uint64(max(0, ctr.batWaitForSort.Allocated()))
	incomingData := uint64(max(0, incoming.Allocated()))
	combinedData := addOrderBytes(currentData, incomingData)
	rows := uint64(max(0,
		ctr.batWaitForSort.RowCount()+incoming.RowCount()))
	requiredScratch := multiplyOrderBytes(rows, 8)
	if len(ctr.sortVectors) > 1 {
		requiredScratch = addOrderBytes(requiredScratch,
			multiplyOrderBytes(rows, 9))
	}
	additionalScratch := uint64(0)
	if existing := ctr.orderScratchBytes(); requiredScratch > existing {
		additionalScratch = requiredScratch - existing
	}
	// Appending can grow one vector to twice its previous capacity while the
	// old allocation is still live. Shuffling the completed run then overlaps
	// another replacement vector with those retained capacities. Three times
	// the combined allocated bytes is the bounded one-vector-at-a-time peak;
	// Batch.Size is intentionally not used because it omits spare capacity.
	peak := addOrderBytes(ctr.orderScratchBytes(),
		multiplyOrderBytes(combinedData, 3))
	peak = addOrderBytes(peak, additionalScratch)
	return peak > limit, nil
}

func (ctr *container) shouldSortCurrentRun() (bool, error) {
	if ctr == nil || ctr.growthParticipant == nil || ctr.batWaitForSort == nil {
		return false, nil
	}
	current := ctr.retainedOrderBytes()
	limit, err := ctr.growthParticipant.RetainedLimit(current)
	if err != nil {
		return false, err
	}
	data := uint64(max(0, ctr.batWaitForSort.Allocated()))
	rows := uint64(max(0, ctr.batWaitForSort.RowCount()))
	requiredScratch := multiplyOrderBytes(rows, 8)
	if len(ctr.sortVectors) > 1 {
		requiredScratch = addOrderBytes(requiredScratch,
			multiplyOrderBytes(rows, 9))
	}
	additionalScratch := uint64(0)
	if existing := ctr.orderScratchBytes(); requiredScratch > existing {
		additionalScratch = requiredScratch - existing
	}
	// Shuffle replaces vectors one at a time, so at most one further retained
	// payload overlaps the already allocated batch.
	peak := addOrderBytes(current, data)
	peak = addOrderBytes(peak, additionalScratch)
	return peak > limit, nil
}

func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (enoughToSend bool, err error) {
	if len(bat.ExtraBuf) != 0 {
		return false, moerr.NewInternalError(proc.Ctx,
			"order build should not have extra buffers")
	}
	s1, s2 := 0, bat.Size()
	if ctr.batWaitForSort != nil {
		s1 = ctr.batWaitForSort.Size()
	}
	all := s1 + s2

	if ctr.batWaitForSort == nil && ctr.retainedAllocation != nil {
		ctr.batWaitForSort, err = proc.NewBatchFromSrcWithAllocation(
			bat,
			0,
			ctr.retainedAllocation,
		)
		if err != nil {
			return false, err
		}
		// NewBatchFromSrcWithAllocation creates an empty destination, so copy
		// the shuffle routing metadata explicitly before appending the rows.
		// The legacy Dup path preserves the same field through CloneTo.
		ctr.batWaitForSort.ShuffleIDX = bat.ShuffleIDX
	}
	if ctr.batWaitForSort == nil {
		ctr.batWaitForSort, err = bat.Dup(proc.Mp())
	} else if ctr.allocationAccount == nil {
		ctr.batWaitForSort, err = ctr.batWaitForSort.AppendWithCopy(
			proc.Ctx, proc.Mp(), bat)
	} else {
		err = ctr.appendBatchAccounted(proc, bat)
	}
	if err != nil {
		return false, err
	}
	return all >= maxBatchSizeToSort, nil
}

func (ctr *container) appendBatchAccounted(
	proc *process.Process,
	bat *batch.Batch,
) error {
	if ctr == nil || ctr.batWaitForSort == nil || bat == nil ||
		len(ctr.batWaitForSort.Vecs) != len(bat.Vecs) {
		return mpool.ErrAllocationAccountInvalid
	}
	if len(bat.Vecs) == 0 {
		ctr.batWaitForSort.AddRowCount(bat.RowCount())
		return nil
	}
	checkpoints, err := ctr.appendCheckpoints(len(ctr.batWaitForSort.Vecs), proc)
	if err != nil {
		return err
	}
	for i := range ctr.batWaitForSort.Vecs {
		checkpoints[i] = ctr.batWaitForSort.Vecs[i].MakeAppendCheckpoint()
	}
	for i := range ctr.batWaitForSort.Vecs {
		if ctr.recoveryCapacityActive {
			err = ctr.batWaitForSort.Vecs[i].UnionBatchWithAllocationAccount(
				bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.Mp(),
				ctr.recoveryAllocation,
			)
		} else {
			err = ctr.batWaitForSort.Vecs[i].UnionBatch(
				bat.Vecs[i], 0, bat.Vecs[i].Length(), nil, proc.Mp())
		}
		if err != nil {
			for j := range ctr.batWaitForSort.Vecs {
				ctr.batWaitForSort.Vecs[j].RollbackAppend(
					checkpoints[j], bat.Vecs[j].Length())
			}
			return err
		}
		ctr.batWaitForSort.Vecs[i].SetSorted(false)
	}
	ctr.batWaitForSort.AddRowCount(bat.RowCount())
	return nil
}

func (ctr *container) sortAndSend(proc *process.Process, result *vm.CallResult) (err error) {
	if ctr.batWaitForSort != nil {
		for i := range ctr.sortExprExecutor {
			ctr.sortVectors[i], err = ctr.sortExprExecutor[i].Eval(proc, []*batch.Batch{ctr.batWaitForSort}, nil)
			if err != nil {
				return err
			}
		}

		rowCount := ctr.batWaitForSort.RowCount()
		ctr.resultOrderList, ctr.resultOrderMP, err = growOrderSliceWithCapacityClass(
			ctr.resultOrderList,
			ctr.resultOrderMP,
			rowCount,
			proc,
			ctr.allocationAccount,
			ordersites.OrderSelections,
			ctr.recoveryCapacityClass,
		)
		if err != nil {
			return err
		}
		if len(ctr.sortVectors) > 1 && ctr.allocationAccount != nil {
			ctr.sortScratch.Partitions, ctr.sortPartitionsMP, err = growOrderSliceWithCapacityClass(
				ctr.sortScratch.Partitions,
				ctr.sortPartitionsMP,
				rowCount,
				proc,
				ctr.allocationAccount,
				ordersites.OrderSortPartitions,
				ctr.recoveryCapacityClass,
			)
			if err != nil {
				return err
			}
			ctr.sortScratch.Partitions = ctr.sortScratch.Partitions[:0]
			ctr.sortScratch.Diffs, ctr.sortDiffsMP, err = growOrderSliceWithCapacityClass(
				ctr.sortScratch.Diffs,
				ctr.sortDiffsMP,
				rowCount,
				proc,
				ctr.allocationAccount,
				ordersites.OrderSortDiffs,
				ctr.recoveryCapacityClass,
			)
			if err != nil {
				return err
			}
		}

		for i := range ctr.resultOrderList {
			ctr.resultOrderList[i] = int64(i)
		}

		if ctr.allocationAccount == nil {
			sort.SortByVectors(
				ctr.resultOrderList, ctr.sortVectors, ctr.desc, ctr.nullsLast)
		} else {
			sort.SortByVectorsWithScratch(
				ctr.resultOrderList,
				ctr.sortVectors,
				ctr.desc,
				ctr.nullsLast,
				&ctr.sortScratch,
			)
		}

		if ctr.finalizationAllocation == nil {
			err = ctr.batWaitForSort.Shuffle(ctr.resultOrderList, proc.Mp())
		} else {
			err = ctr.batWaitForSort.ShuffleWithAllocationAccount(
				ctr.resultOrderList,
				proc.Mp(),
				ctr.finalizationAllocation,
			)
		}
		if err != nil {
			return err
		}
	}
	ctr.rbat = ctr.batWaitForSort
	result.Batch = ctr.rbat
	ctr.batWaitForSort = nil
	// Peak overlap is no longer possible once the run is finalized. Keep only
	// the exact bytes still backing the returned run and reusable scratch;
	// downstream operators must not be starved by an idle worst-case floor.
	if err = ctr.trimRecoveryCapacity(); err != nil {
		return err
	}
	return nil
}

func (order *Order) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	ap := order
	buf.WriteString(": τ([")
	for i, f := range ap.OrderBySpec {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func (order *Order) OpType() vm.OpType {
	return vm.Order
}

func (order *Order) Prepare(proc *process.Process) (err error) {
	defer func() {
		err = orderTerminalCapacityError(proc.Ctx, err)
	}()
	if order.OpAnalyzer == nil {
		order.OpAnalyzer = process.NewAnalyzer(order.GetIdx(), order.IsFirst, order.IsLast, "order")
	} else {
		order.OpAnalyzer.Reset()
	}

	ctr := &order.ctr
	registeredGrowth := false
	activatedRecovery := false
	if ctr.allocationAccount != nil && ctr.growthParticipant == nil {
		budget, budgetErr := proc.GetExecutionResourceBudget()
		if budgetErr != nil {
			return budgetErr
		}
		ctr.growthParticipant, err = budget.RegisterMemoryGrowthParticipant()
		if err != nil {
			return err
		}
		registeredGrowth = true
		if err = ctr.installRecoveryCapacity(budget); err != nil {
			ctr.releaseGrowthParticipant()
			return err
		}
		activatedRecovery = true
	}
	defer func() {
		if err != nil {
			if registeredGrowth {
				ctr.releaseGrowthParticipant()
			}
			if activatedRecovery {
				_ = ctr.releaseRecoveryCapacity()
			}
		}
	}()
	if len(ctr.desc) == 0 {
		ctr.desc = make([]bool, len(order.OrderBySpec))
		ctr.nullsLast = make([]bool, len(order.OrderBySpec))
		ctr.sortVectors = make([]*vector.Vector, len(order.OrderBySpec))
		for i, f := range order.OrderBySpec {
			ctr.desc[i] = f.Flag&pbplan.OrderBySpec_DESC != 0
			if f.Flag&pbplan.OrderBySpec_NULLS_FIRST != 0 {
				order.ctr.nullsLast[i] = false
			} else if f.Flag&pbplan.OrderBySpec_NULLS_LAST != 0 {
				order.ctr.nullsLast[i] = true
			} else {
				order.ctr.nullsLast[i] = order.ctr.desc[i]
			}
		}

		planExprs := make([]*pbplan.Expr, len(order.OrderBySpec))
		for i := range order.OrderBySpec {
			planExprs[i] = order.OrderBySpec[i].Expr
		}
		ctr.sortExprExecutor, err =
			colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(
				proc,
				planExprs,
				ctr.expressionAllocation,
			)
		if err != nil {
			ctr.releaseExpressionExecutors()
			ctr.desc = nil
			ctr.nullsLast = nil
			return err
		}
	}

	return nil
}

func (order *Order) Call(proc *process.Process) (
	result vm.CallResult,
	err error,
) {
	analyzer := order.OpAnalyzer

	ctr := &order.ctr
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
		if err = ctr.trimRecoveryCapacity(); err != nil {
			return vm.CancelResult, orderTerminalCapacityError(proc.Ctx, err)
		}
	}

	if ctr.state == vm.Build {
		for {
			input := vm.NewCallResult()
			if ctr.pendingInput != nil {
				input.Batch = ctr.pendingInput
				ctr.pendingInput = nil
			} else {
				input, err = vm.ChildrenCall(order.GetChildren(0), proc, analyzer)
				if err != nil {
					// Preserve the producing operator's attribution. Wrapping every
					// child capacity error as Order made unrelated failures appear to
					// originate here and hid Order's own phase diagnostics.
					return vm.CancelResult, err
				}
			}
			if input.Batch == nil {
				if err, canceled := vm.CancelCheck(proc); canceled {
					return vm.CancelResult, err
				}
				ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			flushBeforeAppend, err := ctr.shouldFlushBeforeAppend(input.Batch)
			if err != nil {
				return vm.CancelResult, orderTerminalCapacityError(proc.Ctx, err)
			}
			if flushBeforeAppend {
				result = vm.NewCallResult()
				if err = ctr.sortAndSend(proc, &result); err != nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
						orderPhaseError("sort-before-append", err,
							ctr.batWaitForSort, input.Batch))
				}
				// Do not overlap a returned run with a copy of the next child batch.
				// Pull execution guarantees the child batch remains valid until its
				// next Call, which cannot occur while pendingInput is non-nil.
				ctr.pendingInput = input.Batch
				return result, nil
			}
			if err = ctr.ensureRecoveryCapacity(input.Batch); err != nil {
				if ctr.batWaitForSort == nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
						orderPhaseError("reserve-first-run", err, nil, input.Batch))
				}
				result = vm.NewCallResult()
				if sortErr := ctr.sortAndSend(proc, &result); sortErr != nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
						orderPhaseError("sort-after-reserve-reject", sortErr,
							ctr.batWaitForSort, input.Batch))
				}
				ctr.pendingInput = input.Batch
				return result, nil
			}
			// Reserving the finalization floor reduces the ordinary live share.
			// Recheck before retaining the next batch so append cannot consume the
			// progress capacity which was just set aside.
			flushBeforeAppend, err = ctr.shouldFlushBeforeAppend(input.Batch)
			if err != nil {
				return vm.CancelResult, orderTerminalCapacityError(proc.Ctx, err)
			}
			if flushBeforeAppend && ctr.batWaitForSort != nil {
				result = vm.NewCallResult()
				if err = ctr.sortAndSend(proc, &result); err != nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
						orderPhaseError("sort-after-reserve", err,
							ctr.batWaitForSort, input.Batch))
				}
				ctr.pendingInput = input.Batch
				return result, nil
			}

			enoughToSend, err := ctr.appendBatch(proc, input.Batch)
			if err != nil {
				return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
					orderPhaseError("append", err,
						ctr.batWaitForSort, input.Batch))
			}
			if !enoughToSend {
				enoughToSend, err = ctr.shouldSortCurrentRun()
				if err != nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx, err)
				}
			}

			if enoughToSend {
				err := ctr.sortAndSend(proc, &input)
				if err != nil {
					return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
						orderPhaseError("sort-run", err, ctr.batWaitForSort, nil))
				}
				return input, nil
			}
		}
	}

	result = vm.NewCallResult()
	if ctr.state == vm.Eval {
		err := ctr.sortAndSend(proc, &result)
		if err != nil {
			return vm.CancelResult, orderTerminalCapacityError(proc.Ctx,
				orderPhaseError("sort-final", err, ctr.batWaitForSort, nil))
		}
		ctr.state = vm.End
		ctr.releaseGrowthParticipant()
		return result, nil
	}

	if ctr.state == vm.End {
		// The parent has consumed the final returned batch; all Order-owned
		// scratch and its recovery floor can now leave the shared query budget.
		// Keeping that idle floor until pipeline Reset can starve a downstream
		// terminal operator which only needs a few bytes to publish the result.
		ctr.releaseAttempt()
		return vm.CancelResult, nil
	}

	panic("bug")
}
