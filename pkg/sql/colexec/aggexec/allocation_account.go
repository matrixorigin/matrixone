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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// AllocationAccountSites assigns physical aggregate storage to sites in the
// caller's owner namespace. Sites are explicit because aggexec is shared by
// several operators; aggexec must not silently attribute their memory to one
// operator family.
type AllocationAccountSites struct {
	VectorData     mpool.AllocationSite
	VectorArea     mpool.AllocationSite
	VectorNulls    mpool.AllocationSite
	VectorGrouping mpool.AllocationSite
	ArgumentCount  mpool.AllocationSite
	ArgumentArena  mpool.AllocationSite
}

// AllocationAccount is an immutable physical-allocation selection shared by
// all state owned by one aggregate executor. The underlying MPool allocation
// metadata remains the sole release owner.
type AllocationAccount struct {
	account       *mpool.AllocationAccount
	owner         mpool.AllocationOwner
	sites         AllocationAccountSites
	capacityClass mpool.AllocationCapacityClass

	vectors *vector.AllocationAccountSelection
}

func NewAllocationAccount(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	sites AllocationAccountSites,
) (*AllocationAccount, error) {
	return NewAllocationAccountWithCapacityClass(
		account,
		owner,
		sites,
		mpool.AllocationCapacityClassDefault,
	)
}

// NewAllocationAccountWithCapacityClass selects an execution-local recovery
// controller without changing the aggregate's physical owner or sites.
func NewAllocationAccountWithCapacityClass(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	sites AllocationAccountSites,
	capacityClass mpool.AllocationCapacityClass,
) (*AllocationAccount, error) {
	vectors, err := vector.NewAllocationAccountSelectionWithCapacityClass(
		account,
		owner,
		sites.VectorData,
		sites.VectorArea,
		sites.VectorNulls,
		sites.VectorGrouping,
		capacityClass,
	)
	if err != nil {
		return nil, err
	}
	if sites.ArgumentCount < mpool.AllocationSiteMin ||
		sites.ArgumentArena < mpool.AllocationSiteMin ||
		sites.ArgumentCount == sites.ArgumentArena {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return &AllocationAccount{
		account:       account,
		owner:         owner,
		sites:         sites,
		capacityClass: capacityClass,
		vectors:       vectors,
	}, nil
}

func (a *AllocationAccount) sameGeneration(other *AllocationAccount) bool {
	return a == other || a != nil && other != nil &&
		a.account == other.account && a.owner == other.owner &&
		a.sites == other.sites && a.capacityClass == other.capacityClass
}

func (a *AllocationAccount) newVector(typ types.Type) (*vector.Vector, error) {
	if a == nil {
		return vector.NewOffHeapVecWithType(typ), nil
	}
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, a.vectors)
}

func (a *AllocationAccount) vectorSelection() *vector.AllocationAccountSelection {
	if a == nil {
		return nil
	}
	return a.vectors
}

func (a *AllocationAccount) makeArgumentCounts(
	mp *mpool.MPool,
	length int,
) ([]uint32, error) {
	if a == nil {
		return mpool.MakeSlice[uint32](length, mp, true)
	}
	return mpool.MakeSliceAccountedWithCapacityClass[uint32](
		length,
		mp,
		a.account,
		a.owner,
		a.sites.ArgumentCount,
		a.capacityClass,
	)
}

func (a *AllocationAccount) allocArgumentArena(
	mp *mpool.MPool,
	size int,
) ([]byte, error) {
	if a == nil {
		return mp.Alloc(size, true)
	}
	return mp.AllocAccountedWithCapacityClass(
		size,
		a.account,
		a.owner,
		a.sites.ArgumentArena,
		a.capacityClass,
	)
}

func (a *AllocationAccount) newArgumentBuffer(
	mp *mpool.MPool,
) (*mpool.AccountedBuffer, error) {
	if a == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return mpool.NewAccountedBufferWithCapacityClass(
		mp,
		a.account,
		a.owner,
		a.sites.ArgumentArena,
		a.capacityClass,
	)
}

func makeAccountedScratch[T any](
	a *AllocationAccount, mp *mpool.MPool, length int,
) ([]T, error) {
	if a == nil {
		return mpool.MakeSlice[T](length, mp, true)
	}
	return mpool.MakeSliceAccountedWithCapacityClass[T](
		length,
		mp,
		a.account,
		a.owner,
		a.sites.ArgumentArena,
		a.capacityClass,
	)
}
