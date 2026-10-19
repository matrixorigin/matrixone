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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"unsafe"
)

// SetOperationAllocation is shared by the four membership kernels. It owns
// only allocation provenance and fixed output headers; each kernel retains
// its authoritative hash map, multiplicities and output batch.
type SetOperationAllocation struct {
	account  *mpool.AllocationAccount
	hash     *hashtable.AllocationAccountSelection
	iterator *hashmap.IteratorAllocation
	vectors  *vector.AllocationAccountSelection
	budget   *process.ExecutionResourceGeneration
	metadata uint64
	selected [hashmap.UnitLimit]uint8
	zeroes   [hashmap.UnitLimit]uint8
}

func (a *SetOperationAllocation) SetAllocationAccount(account *mpool.AllocationAccount) (err error) {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if a.account != nil && a.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if a.metadata != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	hash, err := hashtable.NewAllocationAccountSelection(account, mpool.AllocationOwnerSet, 1, 2)
	if err != nil {
		return err
	}
	iterator, err := hashmap.NewIteratorAllocation(account, mpool.AllocationOwnerSet, 3)
	if err != nil {
		return err
	}
	vectors, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSet, 4, 5, 6, 7)
	if err != nil {
		return err
	}
	a.account, a.hash, a.iterator, a.vectors = account, hash, iterator, vectors
	return nil
}

func (a *SetOperationAllocation) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if a.account == nil {
		return nil
	}
	if a.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if a.metadata != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	*a = SetOperationAllocation{}
	return nil
}

func (a *SetOperationAllocation) NewHashMap(proc *process.Process) (*hashmap.StrHashMap, error) {
	return hashmap.NewStrHashMapWithAllocations(true, proc.Mp(), a.hash, a.iterator)
}

func (a *SetOperationAllocation) Scratch() ([]uint8, []uint8) { return a.selected[:], a.zeroes[:] }

func (a *SetOperationAllocation) ExpressionAllocation() *vector.AllocationAccountSelection {
	return a.vectors
}

func (a *SetOperationAllocation) NewBatch(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if a.account != nil {
		budget, err := proc.GetExecutionResourceBudget()
		if err != nil {
			return nil, err
		}
		size := uint64(unsafe.Sizeof(batch.Batch{})) + uint64(len(input.Vecs))*uint64(unsafe.Sizeof(vector.Vector{})+unsafe.Sizeof((*vector.Vector)(nil)))
		if err = budget.AcquireAllocationCapacity(size); err != nil {
			return nil, err
		}
		a.budget, a.metadata = budget, a.metadata+size
	}
	b := batch.NewWithSize(len(input.Vecs))
	for i, v := range input.Vecs {
		var err error
		if a.vectors == nil {
			b.Vecs[i] = vector.NewVec(*v.GetType())
		} else {
			b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(*v.GetType(), a.vectors)
		}
		if err != nil {
			b.Clean(proc.Mp())
			return nil, err
		}
	}
	return b, nil
}

func (a *SetOperationAllocation) FreeMetadata() {
	if a.budget != nil {
		a.budget.ReleaseAllocationCapacity(a.metadata)
	}
	a.budget, a.metadata = nil, 0
}

// GrowSetOperationSlice admits the replacement while the old allocation is
// still live, so the growth peak is charged before copying or publication.
func GrowSetOperationSlice[T any](old []T, length int, mp *mpool.MPool, a *SetOperationAllocation) ([]T, error) {
	if length <= len(old) {
		return old, nil
	}
	if length <= cap(old) {
		next := old[:length]
		clear(next[len(old):])
		return next, nil
	}
	capacity := length
	if cap(old) <= math.MaxInt/2 {
		capacity = max(length, 2*cap(old))
	}
	var next []T
	var err error
	if a.account == nil {
		next, err = mpool.MakeSlice[T](capacity, mp, true)
	} else {
		next, err = mpool.MakeSliceAccounted[T](capacity, mp, a.account, mpool.AllocationOwnerSet, 8)
	}
	if err != nil {
		return old, err
	}
	copy(next, old)
	clear(next[len(old):])
	if cap(old) > 0 {
		mpool.FreeSlice(mp, old)
	}
	return next[:length], nil
}
