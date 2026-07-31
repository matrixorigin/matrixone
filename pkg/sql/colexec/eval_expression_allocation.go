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

package colexec

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Expression allocation sites are stable diagnostics within the owner chosen
// by the caller. The API is dormant: legacy expression constructors do not
// create or select an account.
const (
	ExpressionAllocationSiteConstantData mpool.AllocationSite = iota + 1
	ExpressionAllocationSiteConstantArea
	ExpressionAllocationSiteResultData
	ExpressionAllocationSiteResultArea
	ExpressionAllocationSiteScratchData
	ExpressionAllocationSiteScratchArea
	ExpressionAllocationSiteSelection
	ExpressionAllocationSiteSelectedRows
	ExpressionAllocationSiteConstantNulls
	ExpressionAllocationSiteConstantGrouping
	ExpressionAllocationSiteResultNulls
	ExpressionAllocationSiteResultGrouping
	ExpressionAllocationSiteScratchNulls
	ExpressionAllocationSiteScratchGrouping
	ExpressionAllocationSiteParameterConversion
)

// ExpressionAllocationAccount is the immutable allocation provenance shared
// by one expression tree. Vector selections remain separate so diagnostics
// distinguish constants, results, and selected-row scratch.
type ExpressionAllocationAccount struct {
	account *mpool.AllocationAccount
	owner   mpool.AllocationOwner

	constant  *vector.AllocationAccountSelection
	result    *vector.AllocationAccountSelection
	scratch   *vector.AllocationAccountSelection
	parameter *vector.FunctionParameterAllocation
}

func NewExpressionAllocationAccount(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
) (*ExpressionAllocationAccount, error) {
	constant, err := vector.NewAllocationAccountSelectionWithBitmaps(
		account,
		owner,
		ExpressionAllocationSiteConstantData,
		ExpressionAllocationSiteConstantArea,
		ExpressionAllocationSiteConstantNulls,
		ExpressionAllocationSiteConstantGrouping,
	)
	if err != nil {
		return nil, err
	}
	result, err := vector.NewAllocationAccountSelectionWithBitmaps(
		account,
		owner,
		ExpressionAllocationSiteResultData,
		ExpressionAllocationSiteResultArea,
		ExpressionAllocationSiteResultNulls,
		ExpressionAllocationSiteResultGrouping,
	)
	if err != nil {
		return nil, err
	}
	scratch, err := vector.NewAllocationAccountSelectionWithBitmaps(
		account,
		owner,
		ExpressionAllocationSiteScratchData,
		ExpressionAllocationSiteScratchArea,
		ExpressionAllocationSiteScratchNulls,
		ExpressionAllocationSiteScratchGrouping,
	)
	if err != nil {
		return nil, err
	}
	parameter, err := vector.NewFunctionParameterAllocation(
		account,
		owner,
		ExpressionAllocationSiteParameterConversion,
	)
	if err != nil {
		return nil, err
	}
	return &ExpressionAllocationAccount{
		account:   account,
		owner:     owner,
		constant:  constant,
		result:    result,
		scratch:   scratch,
		parameter: parameter,
	}, nil
}

func (a *ExpressionAllocationAccount) validate() error {
	if a == nil || a.account == nil || a.account.Handle() == 0 ||
		a.owner < mpool.AllocationOwnerMin ||
		a.owner > mpool.AllocationOwnerMax ||
		a.constant == nil || a.result == nil || a.scratch == nil ||
		a.parameter == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func newExpressionVector(
	typ types.Type,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewOffHeapVecWithType(typ), nil
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
}

func newExpressionConstNull(
	typ types.Type,
	length int,
	selection *vector.AllocationAccountSelection,
	mp *mpool.MPool,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstNull(typ, length, mp), nil
	}
	return vector.NewConstNullWithAllocation(typ, length, selection)
}

func newExpressionConstFixed[T any](
	typ types.Type,
	value T,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstFixed(typ, value, length, mp)
	}
	return vector.NewConstFixedWithAllocation(
		typ,
		value,
		length,
		mp,
		selection,
	)
}

func newExpressionConstBytes(
	typ types.Type,
	value []byte,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstBytes(typ, value, length, mp)
	}
	return vector.NewConstBytesWithAllocation(
		typ,
		value,
		length,
		mp,
		selection,
	)
}

func newExpressionConstArray[T types.ArrayElement](
	typ types.Type,
	value []T,
	length int,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	if selection == nil {
		return vector.NewConstArray(typ, value, length, mp)
	}
	return vector.NewConstArrayWithAllocation(
		typ,
		value,
		length,
		mp,
		selection,
	)
}

func ensureExpressionSlice[T any](
	values []T,
	length int,
	mp *mpool.MPool,
	allocation *ExpressionAllocationAccount,
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

func freeExpressionSlice[T any](
	values []T,
	mp *mpool.MPool,
	allocation *ExpressionAllocationAccount,
) {
	if allocation != nil && cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
}
