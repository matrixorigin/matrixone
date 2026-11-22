// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package spillutil

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Spill allocation sites occupy a dedicated range within each operator owner
// supplied to NewSpillAllocationAccount.
const (
	SpillAllocationSiteDecodedData mpool.AllocationSite = iota + 32
	SpillAllocationSiteDecodedArea
	SpillAllocationSiteSelectedData
	SpillAllocationSiteSelectedArea
	SpillAllocationSiteHashValues
	SpillAllocationSiteRowIDs
	SpillAllocationSiteMarshalBuffer
	SpillAllocationSiteCoalesceBuffer
	_ // reserved: was SpillAllocationSiteDecodedNulls
	_ // reserved: was SpillAllocationSiteDecodedGrouping
	_ // reserved: was SpillAllocationSiteSelectedNulls
	_ // reserved: was SpillAllocationSiteSelectedGrouping
)

// Sites 44-59 belong to hashbuild runtime-filter and dedup allocations.
const SpillAllocationSiteReadBuffer mpool.AllocationSite = 60

// SpillAllocationAccount is the allocation provenance for one spill
// engine. Decoded (spill-read) and selected (scatter) vectors allocate
// through accounted views of the pool the engine runs on; the views are built
// on first use for that pool.
type SpillAllocationAccount struct {
	account       *mpool.AllocationAccount
	owner         mpool.AllocationOwner
	capacityClass mpool.AllocationCapacityClass

	viewPool *mpool.MPool
	decoded  *mpool.MPool
	selected *mpool.MPool
}

func NewSpillAllocationAccount(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
) (*SpillAllocationAccount, error) {
	return NewSpillAllocationAccountWithCapacityClass(
		account, owner, mpool.AllocationCapacityClassDefault)
}

// NewSpillAllocationAccountWithCapacityClass keeps spill allocation
// provenance unchanged while routing its physical growth through one
// execution-local capacity controller.
func NewSpillAllocationAccountWithCapacityClass(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	capacityClass mpool.AllocationCapacityClass,
) (*SpillAllocationAccount, error) {
	a := &SpillAllocationAccount{
		account:       account,
		owner:         owner,
		capacityClass: capacityClass,
	}
	if err := a.validate(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *SpillAllocationAccount) validate() error {
	if a == nil || a.account == nil || a.account.Handle() == 0 ||
		a.owner < mpool.AllocationOwnerMin ||
		a.owner > mpool.AllocationOwnerCatalogMax {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func (a *SpillAllocationAccount) views(mp *mpool.MPool) error {
	if err := a.validate(); err != nil {
		return err
	}
	if mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if a.viewPool == mp && a.decoded != nil && a.selected != nil {
		return nil
	}
	decoded, err := mp.WithAccount(
		a.account, a.owner, SpillAllocationSiteDecodedData, a.capacityClass)
	if err != nil {
		return err
	}
	selected, err := mp.WithAccount(
		a.account, a.owner, SpillAllocationSiteSelectedData, a.capacityClass)
	if err != nil {
		return err
	}
	a.viewPool, a.decoded, a.selected = mp, decoded, selected
	return nil
}

// DecodedMP returns the accounted view of mp that spill-read (decoded)
// batches allocate through.
func (a *SpillAllocationAccount) DecodedMP(mp *mpool.MPool) (*mpool.MPool, error) {
	if err := a.views(mp); err != nil {
		return nil, err
	}
	return a.decoded, nil
}

// SelectedMP returns the accounted view of mp that scatter-selected batches
// allocate through.
func (a *SpillAllocationAccount) SelectedMP(mp *mpool.MPool) (*mpool.MPool, error) {
	if err := a.views(mp); err != nil {
		return nil, err
	}
	return a.selected, nil
}

// newSpillBatch returns an empty off-heap batch; callers allocate its data
// through DecodedMP or SelectedMP.
func newSpillBatch(size int) *batch.Batch {
	return batch.NewOffHeapWithSize(size)
}

// GrowAccountedSlice grows an off-heap typed slice under the spill engine's
// allocation owner. Growth admits the replacement while the old backing is
// still live, then releases the old backing after the copy.
func GrowAccountedSlice[T any](
	values []T,
	length int,
	mp *mpool.MPool,
	allocation *SpillAllocationAccount,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < 0 || mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if length <= cap(values) {
		return values[:length], nil
	}
	if err := allocation.validate(); err != nil {
		return nil, err
	}
	newCapacity := cap(values)
	if newCapacity == 0 {
		newCapacity = 1
	}
	for newCapacity < length {
		if newCapacity > math.MaxInt/2 {
			newCapacity = length
			break
		}
		newCapacity *= 2
	}
	next, err := mpool.MakeSliceAccountedWithCapacityClass[T](
		newCapacity,
		mp,
		allocation.account,
		allocation.owner,
		site,
		allocation.capacityClass,
	)
	if err != nil {
		return nil, err
	}
	copy(next, values)
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
	return next[:length], nil
}

// FreeAccountedSlice releases a slice returned by GrowAccountedSlice.
func FreeAccountedSlice[T any](
	values []T,
	mp *mpool.MPool,
) {
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
}

func (a *SpillAllocationAccount) newBuffer(
	mp *mpool.MPool,
	site mpool.AllocationSite,
) (*mpool.AccountedBuffer, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	return mpool.NewAccountedBufferWithCapacityClass(
		mp,
		a.account,
		a.owner,
		site,
		a.capacityClass,
	)
}
