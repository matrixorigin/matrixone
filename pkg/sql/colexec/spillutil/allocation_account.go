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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	SpillAllocationSiteDecodedNulls
	SpillAllocationSiteDecodedGrouping
	SpillAllocationSiteSelectedNulls
	SpillAllocationSiteSelectedGrouping
)

// Sites 44-59 belong to hashbuild runtime-filter and dedup allocations.
const SpillAllocationSiteReadBuffer mpool.AllocationSite = 60

// SpillAllocationAccount is the allocation provenance for one spill
// engine.
type SpillAllocationAccount struct {
	account *mpool.AllocationAccount
	owner   mpool.AllocationOwner

	decoded  *vector.AllocationAccountSelection
	selected *vector.AllocationAccountSelection
}

func NewSpillAllocationAccount(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
) (*SpillAllocationAccount, error) {
	decoded, err := vector.NewAllocationAccountSelection(
		account,
		owner,
		SpillAllocationSiteDecodedData,
		SpillAllocationSiteDecodedArea,
		SpillAllocationSiteDecodedNulls,
		SpillAllocationSiteDecodedGrouping,
	)
	if err != nil {
		return nil, err
	}
	selected, err := vector.NewAllocationAccountSelection(
		account,
		owner,
		SpillAllocationSiteSelectedData,
		SpillAllocationSiteSelectedArea,
		SpillAllocationSiteSelectedNulls,
		SpillAllocationSiteSelectedGrouping,
	)
	if err != nil {
		return nil, err
	}
	return &SpillAllocationAccount{
		account:  account,
		owner:    owner,
		decoded:  decoded,
		selected: selected,
	}, nil
}

func (a *SpillAllocationAccount) validate() error {
	if a == nil || a.account == nil || a.account.Handle() == 0 ||
		a.owner < mpool.AllocationOwnerMin ||
		a.owner > mpool.AllocationOwnerCatalogMax ||
		a.decoded == nil || a.selected == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func newSpillBatch(
	size int,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if selection == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	bat := batch.NewOffHeapWithSize(size)
	if err := bat.SetAllocationAccount(selection); err != nil {
		bat.Clean(nil)
		return nil, err
	}
	return bat, nil
}

// ConfigureDecodedBatch assigns the spill engine's decoded-data provenance to
// an empty off-heap destination before its first physical allocation.
func (a *SpillAllocationAccount) ConfigureDecodedBatch(bat *batch.Batch) error {
	if err := a.validate(); err != nil {
		return err
	}
	if bat == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return bat.SetAllocationAccount(a.decoded)
}

func newSpillVector(
	typ types.Type,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
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
	next, err := mpool.MakeSliceAccounted[T](
		newCapacity,
		mp,
		allocation.account,
		allocation.owner,
		site,
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
	return mpool.NewAccountedBuffer(
		mp,
		a.account,
		a.owner,
		site,
	)
}
