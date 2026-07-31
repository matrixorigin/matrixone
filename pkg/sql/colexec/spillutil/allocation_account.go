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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
)

// Spill allocation sites use a range disjoint from colexec expression sites
// when both subsystems share one logical owner.
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

// SpillAllocationAccount is the allocation provenance for one spill
// engine.
type SpillAllocationAccount struct {
	account *mpool.AllocationAccount
	owner   mpool.AllocationOwner

	decoded    *vector.AllocationAccountSelection
	selected   *vector.AllocationAccountSelection
	expression *colexec.ExpressionAllocationAccount
}

func NewSpillAllocationAccount(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
) (*SpillAllocationAccount, error) {
	decoded, err := vector.NewAllocationAccountSelectionWithBitmaps(
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
	selected, err := vector.NewAllocationAccountSelectionWithBitmaps(
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
	expression, err := colexec.NewExpressionAllocationAccount(account, owner)
	if err != nil {
		return nil, err
	}
	return &SpillAllocationAccount{
		account:    account,
		owner:      owner,
		decoded:    decoded,
		selected:   selected,
		expression: expression,
	}, nil
}

func (a *SpillAllocationAccount) validate() error {
	if a == nil || a.account == nil || a.account.Handle() == 0 ||
		a.owner < mpool.AllocationOwnerMin ||
		a.owner > mpool.AllocationOwnerMax ||
		a.decoded == nil || a.selected == nil || a.expression == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func newSpillBatch(
	size int,
	selection *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	bat := batch.NewOffHeapWithSize(size)
	if selection != nil {
		if err := bat.SetAllocationAccount(selection); err != nil {
			bat.Clean(nil)
			return nil, err
		}
	}
	return bat, nil
}

func newSpillVector(
	typ types.Type,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewOffHeapVecWithType(typ), nil
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
}

func growSpillSlice[T any](
	values []T,
	length int,
	mp *mpool.MPool,
	allocation *SpillAllocationAccount,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if length <= cap(values) {
		return values[:length], nil
	}
	if allocation == nil {
		return make([]T, length), nil
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

func freeSpillSlice[T any](
	values []T,
	mp *mpool.MPool,
	allocation *SpillAllocationAccount,
) {
	if allocation != nil && cap(values) > 0 {
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
