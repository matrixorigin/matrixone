// Copyright 2021 - 2024 Matrix Origin
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

package mergetop

import (
	"context"
	"errors"
	"math"
	"slices"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/topsites"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeTop)

var _ interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
} = new(MergeTop)

type container struct {
	n     int // result vector number
	state vm.CtrState
	sels  []int64
	poses []int32           // sorted list of attributes
	cmps  []compare.Compare // compare structure used to do sort work

	limit         uint64
	limitExecutor colexec.ExpressionExecutor

	bat *batch.Batch // bat stores the final result of merge-top
	// bufMP is the operator's own pool: retained rows, stream output and the
	// accounted views all come from it.
	bufMP *mpool.MPool

	executorsForOrderList []colexec.ExpressionExecutor

	allocationAccount *mpool.AllocationAccount
	// Accounted views of allocationViewPool, built on first use because the
	// account is bound before the operator knows its process pool.
	allocationViewPool   *mpool.MPool
	retainedAllocation   *mpool.MPool
	expressionAllocation *mpool.MPool
	appendScratch        *mpool.AccountedBuffer

	// stream is non-nil when MergeTop is the leaf of an ordered merge scope.
	// The legacy child mode is retained for decoded plans produced by older CNs.
	stream *streamContainer
}

type MergeTop struct {
	Limit          *plan.Expr          // Limit store the number of mergeTop-operator
	ctr            container           // ctr stores the attributes needn't do Serialization work
	Fs             []*plan.OrderBySpec // Fs store the order information
	OrderedStreams bool                // consume one sorted stream per merge receiver

	vm.OperatorBase
}

func (mergeTop *MergeTop) GetOperatorBase() *vm.OperatorBase {
	return &mergeTop.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *MergeTop {
			return &MergeTop{}
		},
		func(a *MergeTop) {
			*a = MergeTop{}
		},
		reuse.DefaultOptions[MergeTop]().
			WithEnableChecker(),
	)
}

func (mergeTop MergeTop) TypeName() string {
	return opName
}

func NewArgument() *MergeTop {
	return reuse.Alloc[MergeTop](nil)
}

func (mergeTop *MergeTop) WithLimit(limit *plan.Expr) *MergeTop {
	mergeTop.Limit = limit
	return mergeTop
}

func (mergeTop *MergeTop) WithFs(fs []*plan.OrderBySpec) *MergeTop {
	mergeTop.Fs = fs
	return mergeTop
}

func (mergeTop *MergeTop) WithOrderedStreams() *MergeTop {
	mergeTop.OrderedStreams = true
	return mergeTop
}

func (mergeTop *MergeTop) Release() {
	if mergeTop != nil {
		reuse.Free(mergeTop, nil)
	}
}

func (mergeTop *MergeTop) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeTop.ctr.resetStream(proc, pipelineFailed, err)
	mergeTop.ctr.reset(proc)
}

func (mergeTop *MergeTop) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeTop.ctr.freeStream(proc)
	mergeTop.ctr.free(proc)
}

func (mergeTop *MergeTop) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeTop == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeTop.ctr.setAllocationAccount(account)
}

func (mergeTop *MergeTop) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeTop == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeTop.ctr.clearAllocationAccount(account)
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
	if ctr.bat != nil || ctr.limitExecutor != nil || ctr.appendScratch != nil || ctr.stream != nil ||
		len(ctr.executorsForOrderList) != 0 || cap(ctr.sels) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	ctr.allocationAccount = account
	ctr.allocationViewPool = nil
	ctr.retainedAllocation = nil
	ctr.expressionAllocation = nil
	return nil
}

// allocationViews builds the accounted views of mp the bound account charges
// through. It is a no-op when no account is bound or the views already view mp.
func (ctr *container) allocationViews(mp *mpool.MPool) error {
	if ctr.allocationAccount == nil || mp == nil ||
		(ctr.allocationViewPool == mp && ctr.retainedAllocation != nil) {
		return nil
	}
	retained, err := mp.WithAccount(
		ctr.allocationAccount,
		mpool.AllocationOwnerTop,
		topsites.MergeTopRetainedData,
		mpool.AllocationCapacityClassDefault,
	)
	if err != nil {
		return err
	}
	expression, err := mp.WithAccount(
		ctr.allocationAccount,
		mpool.AllocationOwnerTop,
		topsites.MergeTopExpressionData,
		mpool.AllocationCapacityClassDefault,
	)
	if err != nil {
		return err
	}
	ctr.allocationViewPool = mp
	ctr.retainedAllocation = retained
	ctr.expressionAllocation = expression
	return nil
}

// ownPool returns the merge-top pool vec belongs to: proc's pool or one of
// the operator's accounted views. Any other pool is returned proc's pool so
// freeing reports the mismatch.
func (ctr *container) ownPool(proc *process.Process, vec *vector.Vector) *mpool.MPool {
	switch mp := vec.MPool(); mp {
	case nil:
		return nil
	case proc.Mp(), ctr.bufMP, ctr.retainedAllocation, ctr.expressionAllocation:
		return mp
	}
	return proc.Mp()
}

// cleanOwned frees a batch the operator owns, each vector with its own pool.
func (ctr *container) cleanOwned(proc *process.Process, bat *batch.Batch) {
	if bat == nil {
		return
	}
	seen := make(map[*vector.Vector]struct{}, len(bat.Vecs))
	for i, vec := range bat.Vecs {
		if vec == nil {
			continue
		}
		if _, ok := seen[vec]; !ok {
			seen[vec] = struct{}{}
			vec.Free(ctr.ownPool(proc, vec))
		}
		bat.Vecs[i] = nil
	}
	bat.Clean(proc.Mp())
}

// ensureBufMP creates the operator's own pool.
func (ctr *container) ensureBufMP(proc *process.Process) (err error) {
	if ctr.bufMP == nil {
		ctr.bufMP, err = colexec.NewOperatorMPool(proc, "mergetop")
	}
	return err
}

// retainedMP returns the pool retained merge-top state allocates from: the
// retained view of the operator's own pool, or the own pool when no account
// is bound.
func (ctr *container) retainedMP(proc *process.Process) (*mpool.MPool, error) {
	if err := ctr.ensureBufMP(proc); err != nil {
		return nil, err
	}
	if ctr.allocationAccount == nil {
		return ctr.bufMP, nil
	}
	if err := ctr.allocationViews(ctr.bufMP); err != nil {
		return nil, err
	}
	return ctr.retainedAllocation, nil
}

// copyCompareRow replaces dst row dstRow of cmp's retained vector with src
// row srcRow, allocating any growth through mp.
func copyCompareRow(
	cmp compare.Compare,
	src *vector.Vector,
	srcRow, dstRow int64,
	mp *mpool.MPool,
	proc *process.Process,
) error {
	if !mp.IsView() {
		return cmp.Copy(1, 0, srcRow, dstRow, proc)
	}
	return cmp.Vector().Copy(src, dstRow, srcRow, mp)
}

func (ctr *container) clearAllocationAccount(account *mpool.AllocationAccount) error {
	if ctr == nil || ctr.allocationAccount == nil {
		return nil
	}
	if ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.bat != nil || ctr.limitExecutor != nil || ctr.appendScratch != nil || ctr.stream != nil ||
		len(ctr.executorsForOrderList) != 0 || cap(ctr.sels) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	ctr.allocationAccount = nil
	ctr.allocationViewPool = nil
	ctr.retainedAllocation = nil
	ctr.expressionAllocation = nil
	return nil
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
		ctr.appendScratch, err = mpool.NewAccountedBuffer(
			proc.Mp(),
			ctr.allocationAccount,
			mpool.AllocationOwnerTop,
			topsites.MergeTopAppendCheckpoints,
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

func growMergeTopSelections(
	values []int64,
	required int,
	proc *process.Process,
	account *mpool.AllocationAccount,
) ([]int64, error) {
	if required < 0 || proc == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if required <= cap(values) {
		return values[:required], nil
	}
	if account == nil {
		values = slices.Grow(values, required-len(values))
		return values[:required], nil
	}
	elementSize := uint64(unsafe.Sizeof(int64(0)))
	if uint64(required) > uint64(math.MaxInt64)/elementSize {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	oldBytes := int64(uint64(cap(values)) * elementSize)
	requiredBytes := int64(uint64(required) * elementSize)
	nextBytes, ok := mpool.GrowCapacity(oldBytes, requiredBytes)
	if !ok || nextBytes < requiredBytes {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	capacity := int((uint64(nextBytes) + elementSize - 1) / elementSize)
	next, err := mpool.MakeSliceAccounted[int64](
		capacity,
		proc.Mp(),
		account,
		mpool.AllocationOwnerTop,
		topsites.MergeTopSelections,
	)
	if err != nil {
		return nil, err
	}
	copy(next, values)
	if cap(values) != 0 {
		mpool.FreeSlice(proc.Mp(), values)
	}
	return next[:required], nil
}

func mergeTopTerminalCapacityError(ctx context.Context, err error) error {
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
	reason := mpool.AllocationFailureReasonOf(err)
	if (reason == mpool.AllocationFailureCapacity ||
		reason == mpool.AllocationFailureAllocatorLimit) &&
		!mpool.IsMPoolCapacityFailure(err) {
		return moerr.NewResourceExhaustedf(
			ctx,
			"merge top memory capacity exceeded; reduce LIMIT or query concurrency, or increase processLimitationSize",
		)
	}
	return err
}

func (ctr *container) reset(proc *process.Process) {
	ctr.n = 0
	ctr.state = vm.Build
	if ctr.allocationAccount != nil && cap(ctr.sels) != 0 {
		mpool.FreeSlice(proc.Mp(), ctr.sels)
	}
	ctr.sels = nil
	ctr.poses = nil
	ctr.cmps = nil

	ctr.limit = 0
	if ctr.limitExecutor != nil {
		ctr.limitExecutor.Free()
		ctr.limitExecutor = nil
	}

	if ctr.bat != nil {
		ctr.cleanOwned(proc, ctr.bat)
		ctr.bat = nil
	}
	if ctr.appendScratch != nil {
		ctr.appendScratch.Free()
		ctr.appendScratch = nil
	}

	for i, executor := range ctr.executorsForOrderList {
		if executor != nil {
			executor.Free()
			ctr.executorsForOrderList[i] = nil
		}
	}
	ctr.executorsForOrderList = nil
}

func (ctr *container) free(proc *process.Process) {
	ctr.state = vm.Build
	if ctr.allocationAccount != nil && cap(ctr.sels) != 0 {
		mpool.FreeSlice(proc.Mp(), ctr.sels)
	}
	ctr.sels = nil
	ctr.poses = nil
	ctr.cmps = nil
	ctr.limit = 0
	if ctr.bat != nil {
		ctr.cleanOwned(proc, ctr.bat)
		ctr.bat = nil
	}
	if ctr.appendScratch != nil {
		ctr.appendScratch.Free()
		ctr.appendScratch = nil
	}

	for i := range ctr.executorsForOrderList {
		if ctr.executorsForOrderList[i] == nil {
			continue
		}
		ctr.executorsForOrderList[i].Free()
	}
	ctr.executorsForOrderList = nil

	if ctr.limitExecutor != nil {
		ctr.limitExecutor.Free()
	}
	ctr.limitExecutor = nil
	// Views of the deleted pool are rebuilt on the next pool.
	ctr.allocationViewPool = nil
	colexec.FreeOperatorMPool(&ctr.bufMP)
}

func (ctr *container) compare(vi, vj int, i, j int64) int {
	for _, pos := range ctr.poses {
		if r := ctr.cmps[pos].Compare(vi, vj, i, j); r != 0 {
			return r
		}
	}
	return 0
}

func (ctr *container) Len() int {
	return len(ctr.sels)
}

func (ctr *container) Less(i, j int) bool {
	return ctr.compare(0, 0, ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}
