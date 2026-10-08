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
	"context"
	"errors"
	"math"
	"slices"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/ordersites"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Order)

var _ interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
} = new(Order)

const maxBatchSizeToSort = 64 * mpool.MB

type Order struct {
	ctr container

	OrderBySpec []*plan.OrderBySpec

	vm.OperatorBase
}

func (order *Order) GetOperatorBase() *vm.OperatorBase {
	return &order.OperatorBase
}

func init() {
	reuse.CreatePool[Order](
		func() *Order {
			return &Order{}
		},
		func(a *Order) {
			*a = Order{}
		},
		reuse.DefaultOptions[Order]().
			WithEnableChecker(),
	)
}

func (order Order) TypeName() string {
	return opName
}

func NewArgument() *Order {
	return reuse.Alloc[Order](nil)
}

func (order *Order) Release() {
	if order != nil {
		reuse.Free[Order](order, nil)
	}
}

type container struct {
	state          vm.CtrState
	batWaitForSort *batch.Batch
	rbat           *batch.Batch

	desc      []bool // ds[i] == true: the attrs[i] are in descending order
	nullsLast []bool

	sortExprExecutor       []colexec.ExpressionExecutor
	sortVectors            []*vector.Vector
	resultOrderList        []int64
	resultOrderMP          *mpool.MPool
	sortScratch            mosort.ByVectorsScratch
	sortPartitionsMP       *mpool.MPool
	sortDiffsMP            *mpool.MPool
	appendScratch          *mpool.AccountedBuffer
	growthParticipant      *process.ExecutionMemoryGrowthParticipant
	recoveryCapacity       *process.ExecutionRecoveryCapacity
	recoveryCapacityClass  mpool.AllocationCapacityClass
	recoveryCapacityActive bool
	recoveryCapacityFloor  uint64
	finalizationAllocation *vector.AllocationAccountSelection
	// pendingInput is borrowed from the child. When memory pressure closes the
	// current run, keep the just-received batch here instead of copying it while
	// that run is still live. The child is not called again until pendingInput
	// has been consumed, so the borrowed batch remains valid.
	pendingInput *batch.Batch

	allocationAccount    *mpool.AllocationAccount
	retainedAllocation   *vector.AllocationAccountSelection
	recoveryAllocation   *vector.AllocationAccountSelection
	expressionAllocation *vector.AllocationAccountSelection
}

func (order *Order) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &order.ctr
	if ctr.batWaitForSort != nil {
		if ctr.batWaitForSort.HasAllocationAccount() ||
			ctr.batWaitForSort.RowCount() > colexec.DefaultBatchSize {
			// A partially accumulated sort batch can survive when an upstream
			// pipeline is stopped before sortAndSend transfers it to rbat. Its
			// allocation account belongs to the completed execution generation.
			ctr.batWaitForSort.Clean(proc.Mp())
			ctr.batWaitForSort = nil
		} else {
			ctr.batWaitForSort.CleanOnlyData()
		}
	}
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
		ctr.rbat = nil
	}
	ctr.state = vm.Build
	if ctr.allocationAccount != nil {
		// Account-owned backing cannot cross a statement-attempt generation.
		ctr.releaseAttempt()
		ctr.desc = nil
		ctr.nullsLast = nil
		return
	}
	for _, executor := range ctr.sortExprExecutor {
		if executor != nil {
			executor.ResetForNextQuery()
		}
	}
	// Preserve the legacy unaccounted reset path: expression executors and
	// plan-width metadata remain reusable, while row-scaled selectors do not.
	ctr.resultOrderList = nil
}

func (order *Order) Free(proc *process.Process, _ bool, err error) {
	order.cleanBatch(proc)
	order.ctr.releaseAttempt()
	order.ctr.desc = nil
	order.ctr.nullsLast = nil
	order.ctr.state = vm.Build
}

func (order *Order) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if order == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return order.ctr.setAllocationAccount(account)
}

func (order *Order) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if order == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return order.ctr.clearAllocationAccount(account)
}

func (ctr *container) setAllocationAccount(account *mpool.AllocationAccount) error {
	if ctr == nil || account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.allocationAccount != nil {
		if ctr.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.batWaitForSort != nil || ctr.rbat != nil || ctr.appendScratch != nil ||
		ctr.pendingInput != nil ||
		len(ctr.sortExprExecutor) != 0 || cap(ctr.resultOrderList) != 0 ||
		ctr.resultOrderMP != nil || cap(ctr.sortScratch.Partitions) != 0 ||
		ctr.sortPartitionsMP != nil || cap(ctr.sortScratch.Diffs) != 0 ||
		ctr.sortDiffsMP != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	retained, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerOrder,
		ordersites.OrderRetainedData,
		ordersites.OrderRetainedArea,
		ordersites.OrderRetainedNulls,
		ordersites.OrderRetainedGrouping,
	)
	if err != nil {
		return err
	}
	expression, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerOrder,
		ordersites.OrderExpressionData,
		ordersites.OrderExpressionArea,
		ordersites.OrderExpressionNulls,
		ordersites.OrderExpressionGrouping,
	)
	if err != nil {
		return err
	}
	recoveryCapacity := process.NewExecutionRecoveryCapacitySlot()
	recoveryClass, err := account.RegisterCapacityController(recoveryCapacity)
	if err != nil {
		return err
	}
	rollbackRecovery := func(cause error) error {
		_ = account.UnregisterCapacityController(recoveryClass, recoveryCapacity)
		return cause
	}
	recovery, err := vector.NewAllocationAccountSelectionWithCapacityClass(
		account,
		mpool.AllocationOwnerOrder,
		ordersites.OrderRetainedData,
		ordersites.OrderRetainedArea,
		ordersites.OrderRetainedNulls,
		ordersites.OrderRetainedGrouping,
		recoveryClass,
	)
	if err != nil {
		return rollbackRecovery(err)
	}
	ctr.allocationAccount = account
	ctr.retainedAllocation = retained
	ctr.recoveryAllocation = recovery
	ctr.expressionAllocation = expression
	ctr.recoveryCapacity = recoveryCapacity
	ctr.recoveryCapacityClass = recoveryClass
	// The floor is admitted before a child batch is retained. Charge both the
	// retained run and its replacement shuffle to that same floor; reserving
	// progress capacity while allocating the run from the ordinary class can
	// otherwise strand an entirely unused floor at a saturated query cap.
	ctr.finalizationAllocation = recovery
	return nil
}

func (ctr *container) clearAllocationAccount(account *mpool.AllocationAccount) error {
	if ctr == nil || ctr.allocationAccount == nil {
		return nil
	}
	if ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.batWaitForSort != nil || ctr.rbat != nil || ctr.appendScratch != nil ||
		ctr.pendingInput != nil ||
		len(ctr.sortExprExecutor) != 0 || cap(ctr.resultOrderList) != 0 ||
		ctr.resultOrderMP != nil || cap(ctr.sortScratch.Partitions) != 0 ||
		ctr.sortPartitionsMP != nil || cap(ctr.sortScratch.Diffs) != 0 ||
		ctr.sortDiffsMP != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if err := ctr.clearRecoveryCapacity(account); err != nil {
		return err
	}
	ctr.allocationAccount = nil
	ctr.retainedAllocation = nil
	ctr.recoveryAllocation = nil
	ctr.expressionAllocation = nil
	return nil
}

func (ctr *container) installRecoveryCapacity(
	budget *process.ExecutionResourceGeneration,
) error {
	if ctr == nil || ctr.allocationAccount == nil || budget == nil ||
		ctr.recoveryCapacity == nil ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault ||
		ctr.finalizationAllocation == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.recoveryCapacityActive {
		return nil
	}
	if err := ctr.recoveryCapacity.Activate(budget); err != nil {
		return err
	}
	ctr.recoveryCapacityActive = true
	return nil
}

func (ctr *container) releaseRecoveryCapacity() error {
	if ctr == nil || ctr.recoveryCapacity == nil || !ctr.recoveryCapacityActive {
		return nil
	}
	if err := ctr.recoveryCapacity.Close(); err != nil {
		return err
	}
	ctr.recoveryCapacityActive = false
	ctr.recoveryCapacityFloor = 0
	return nil
}

func (ctr *container) trimRecoveryCapacity() error {
	if ctr == nil || ctr.recoveryCapacity == nil || !ctr.recoveryCapacityActive {
		return nil
	}
	capacity, err := ctr.recoveryCapacity.TrimUnusedCapacity()
	if err != nil {
		return err
	}
	ctr.recoveryCapacityFloor = capacity
	return nil
}

func (ctr *container) clearRecoveryCapacity(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return nil
	}
	if account == nil || account != ctr.allocationAccount ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault {
		return mpool.ErrAllocationAccountInvariant
	}
	if err := ctr.releaseRecoveryCapacity(); err != nil {
		return err
	}
	capacity := ctr.recoveryCapacity
	class := ctr.recoveryCapacityClass
	if err := account.UnregisterCapacityController(class, capacity); err != nil {
		return err
	}
	ctr.recoveryCapacity = nil
	ctr.recoveryCapacityClass = mpool.AllocationCapacityClassDefault
	ctr.recoveryAllocation = nil
	ctr.finalizationAllocation = nil
	return nil
}

func growOrderSlice[T any](
	values []T,
	owner *mpool.MPool,
	required int,
	proc *process.Process,
	account *mpool.AllocationAccount,
	site mpool.AllocationSite,
) ([]T, *mpool.MPool, error) {
	return growOrderSliceWithCapacityClass(
		values, owner, required, proc, account, site,
		mpool.AllocationCapacityClassDefault,
	)
}

func growOrderSliceWithCapacityClass[T any](
	values []T,
	owner *mpool.MPool,
	required int,
	proc *process.Process,
	account *mpool.AllocationAccount,
	site mpool.AllocationSite,
	capacityClass mpool.AllocationCapacityClass,
) ([]T, *mpool.MPool, error) {
	if required < 0 || proc == nil ||
		(account == nil && owner != nil) ||
		(account != nil && (cap(values) == 0) != (owner == nil)) {
		return nil, owner, mpool.ErrAllocationAccountInvalid
	}
	if required <= cap(values) {
		return values[:required], owner, nil
	}
	if account == nil {
		values = slices.Grow(values, required-len(values))
		return values[:required], nil, nil
	}
	var value T
	elementSize := uint64(unsafe.Sizeof(value))
	if elementSize == 0 || uint64(required) > uint64(math.MaxInt64)/elementSize {
		return nil, owner, mpool.ErrAllocationAllocatorLimit
	}
	oldBytes := int64(uint64(cap(values)) * elementSize)
	requiredBytes := int64(uint64(required) * elementSize)
	nextBytes, ok := mpool.GrowCapacity(oldBytes, requiredBytes)
	if !ok || nextBytes < requiredBytes {
		return nil, owner, mpool.ErrAllocationAllocatorLimit
	}
	capacity := int((uint64(nextBytes) + elementSize - 1) / elementSize)
	next, err := mpool.MakeSliceAccountedWithCapacityClass[T](
		capacity,
		proc.Mp(),
		account,
		mpool.AllocationOwnerOrder,
		site,
		capacityClass,
	)
	if err != nil {
		return nil, owner, err
	}
	copy(next, values)
	if cap(values) != 0 {
		mpool.FreeSlice(owner, values)
	}
	return next[:required], proc.Mp(), nil
}

func (ctr *container) appendCheckpoints(
	columns int,
	proc *process.Process,
) ([]vector.AppendCheckpoint, error) {
	if ctr == nil || ctr.allocationAccount == nil || proc == nil || columns <= 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if ctr.appendScratch == nil {
		var err error
		capacityClass := mpool.AllocationCapacityClassDefault
		if ctr.recoveryCapacityActive {
			capacityClass = ctr.recoveryCapacityClass
		}
		ctr.appendScratch, err = mpool.NewAccountedBufferWithCapacityClass(
			proc.Mp(),
			ctr.allocationAccount,
			mpool.AllocationOwnerOrder,
			ordersites.OrderAppendCheckpoints,
			capacityClass,
		)
		if err != nil {
			return nil, err
		}
	}
	_, required, err := vector.AppendCheckpointScratch(nil, columns)
	if err != nil {
		return nil, err
	}
	if err = ctr.appendScratch.Resize(required); err != nil {
		return nil, err
	}
	checkpoints, _, err := vector.AppendCheckpointScratch(
		ctr.appendScratch.Bytes(), columns)
	return checkpoints, err
}

func (ctr *container) releaseSlices() {
	if ctr == nil {
		return
	}
	if ctr.allocationAccount != nil {
		if cap(ctr.resultOrderList) != 0 {
			mpool.FreeSlice(ctr.resultOrderMP, ctr.resultOrderList)
		}
		if cap(ctr.sortScratch.Partitions) != 0 {
			mpool.FreeSlice(ctr.sortPartitionsMP, ctr.sortScratch.Partitions)
		}
		if cap(ctr.sortScratch.Diffs) != 0 {
			mpool.FreeSlice(ctr.sortDiffsMP, ctr.sortScratch.Diffs)
		}
	}
	ctr.resultOrderList = nil
	ctr.resultOrderMP = nil
	ctr.sortScratch = mosort.ByVectorsScratch{}
	ctr.sortPartitionsMP = nil
	ctr.sortDiffsMP = nil
}

func (ctr *container) releaseAttempt() {
	ctr.releaseGrowthParticipant()
	// pendingInput is child-owned. Dropping the reference is sufficient; the
	// normal child Reset/Free path remains its sole cleanup owner.
	ctr.pendingInput = nil
	ctr.releaseSlices()
	if ctr.appendScratch != nil {
		ctr.appendScratch.Free()
		ctr.appendScratch = nil
	}
	ctr.releaseExpressionExecutors()
	_ = ctr.releaseRecoveryCapacity()
}

func (ctr *container) releaseGrowthParticipant() {
	if ctr == nil || ctr.growthParticipant == nil {
		return
	}
	ctr.growthParticipant.Release()
	ctr.growthParticipant = nil
}

func (ctr *container) releaseExpressionExecutors() {
	if ctr == nil {
		return
	}
	for i := range ctr.sortExprExecutor {
		if ctr.sortExprExecutor[i] != nil {
			ctr.sortExprExecutor[i].Free()
			ctr.sortExprExecutor[i] = nil
		}
	}
	ctr.sortExprExecutor = nil
	ctr.sortVectors = nil
}

func orderTerminalCapacityError(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, mpool.ErrAllocationAccountInvariant) ||
		errors.Is(err, mpool.ErrAllocationAccountInvalid) ||
		errors.Is(err, mpool.ErrAllocationAccountMismatch) ||
		errors.Is(err, mpool.ErrAllocationAccountLive) ||
		errors.Is(err, mpool.ErrAllocationAccountSealed) ||
		errors.Is(err, process.ErrExecutionResourceClosed) ||
		errors.Is(err, process.ErrExecutionResourceInvalid) ||
		errors.Is(err, process.ErrExecutionMemoryCeilingMissing) {
		return err
	}
	if mpool.AllocationFailureReasonOf(err) == mpool.AllocationFailureCapacity &&
		!mpool.IsMPoolCapacityFailure(err) {
		return moerr.NewResourceExhaustedf(
			ctx,
			"order memory capacity exceeded: %v; reduce sort width or query concurrency, or increase processLimitationSize",
			err,
		)
	}
	return err
}

func (order *Order) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (order *Order) cleanBatch(proc *process.Process) {
	//big memory, just clean
	ctr := &order.ctr
	if ctr.batWaitForSort != nil {
		ctr.batWaitForSort.Clean(proc.Mp())
		ctr.batWaitForSort = nil
	}
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
		ctr.rbat = nil
	}
}
