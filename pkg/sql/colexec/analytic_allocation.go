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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"unsafe"
)

// AnalyticAllocation keeps expression, buffered-vector and aggregate payloads
// in the same statement account across Window and TimeWin native generations.
type AnalyticAllocation struct {
	Account   *mpool.AllocationAccount
	Vector    *vector.AllocationAccountSelection
	Aggregate *aggexec.AllocationAccount
	budget    *process.ExecutionResourceGeneration
	metadata  uint64
}

func (a *AnalyticAllocation) Set(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if a.Account != nil && a.Account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if a.Account == account {
		return nil
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerWindow, 1, 2, 3, 4)
	if err != nil {
		return err
	}
	aggregate, err := aggexec.NewAllocationAccount(account, mpool.AllocationOwnerWindow, aggexec.AllocationAccountSites{
		VectorData: 5, VectorArea: 6, VectorNulls: 7, VectorGrouping: 8, ArgumentCount: 9, ArgumentArena: 10})
	if err != nil {
		return err
	}
	a.Account, a.Vector, a.Aggregate = account, selection, aggregate
	return nil
}
func (a *AnalyticAllocation) Clear(account *mpool.AllocationAccount) error {
	if a.Account == nil {
		return nil
	}
	if a.Account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if a.metadata != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	*a = AnalyticAllocation{}
	return nil
}

// ReserveMetadata accounts for Go-owned directories and scratch that cannot
// carry a vector allocation handle. Replacement storage is admitted while its
// old backing remains live; callers drop references before releasing it.
func (a *AnalyticAllocation) ReserveMetadata(proc *process.Process, size uint64) error {
	if a.Account == nil || size == 0 {
		return nil
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return err
	}
	if a.budget != nil && a.budget != budget {
		return mpool.ErrAllocationAccountMismatch
	}
	if size > math.MaxUint64-a.metadata {
		return mpool.ErrAllocationAccountInvariant
	}
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return err
	}
	a.budget, a.metadata = budget, a.metadata+size
	return nil
}
func (a *AnalyticAllocation) ReleaseMetadata(size uint64) {
	if a.Account == nil || size == 0 {
		return
	}
	if size > a.metadata {
		panic(mpool.ErrAllocationAccountInvariant)
	}
	a.budget.ReleaseAllocationCapacity(size)
	a.metadata -= size
}
func (a *AnalyticAllocation) FreeMetadata() {
	if a.budget != nil {
		a.budget.ReleaseAllocationCapacity(a.metadata)
	}
	a.budget, a.metadata = nil, 0
}

// GrowAnalyticSlice keeps pointer-bearing directories on the Go heap while
// charging their actual capacity, including the copy peak on expansion.
func GrowAnalyticSlice[T any](old []T, length int, proc *process.Process, a *AnalyticAllocation) ([]T, error) {
	if length < 0 {
		return old, mpool.ErrAllocationAccountInvariant
	}
	if length <= cap(old) {
		next := old[:length]
		if length > len(old) {
			clear(next[len(old):])
		}
		return next, nil
	}
	capacity := length
	if cap(old) <= math.MaxInt/2 {
		capacity = max(capacity, 2*cap(old))
	}
	var item T
	width := uint64(unsafe.Sizeof(item))
	if width != 0 && uint64(capacity) > math.MaxUint64/width {
		return old, mpool.ErrAllocationAccountInvariant
	}
	if err := a.ReserveMetadata(proc, uint64(capacity)*width); err != nil {
		return old, err
	}
	next := make([]T, length, capacity)
	copy(next, old)
	a.ReleaseMetadata(uint64(cap(old)) * width)
	return next, nil
}
func (a *AnalyticAllocation) NewVector(typ types.Type) (*vector.Vector, error) {
	if a.Vector == nil {
		return vector.NewVec(typ), nil
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, a.Vector)
}
func (a *AnalyticAllocation) BindAggregate(exec aggexec.AggFuncExec) error {
	if a.Aggregate == nil {
		return nil
	}
	owner, ok := exec.(aggexec.AllocationAccountOwner)
	if !ok {
		return mpool.ErrAllocationAccountInvariant
	}
	return owner.SetAllocationAccount(a.Aggregate)
}
