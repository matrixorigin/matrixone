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
	"sync"
)

// AllocationAccountSites assigns physical aggregate storage to sites in the
// caller's owner namespace. Sites are explicit because aggexec is shared by
// several operators; aggexec must not silently attribute their memory to one
// operator family.
type AllocationAccountSites struct {
	VectorData    mpool.AllocationSite
	VectorArea    mpool.AllocationSite
	ArgumentCount mpool.AllocationSite
	ArgumentArena mpool.AllocationSite
}

// AllocationAccount is an immutable physical-allocation descriptor shared by
// all state owned by one aggregate executor. An executor that binds it builds
// an accounted MPool view (see VectorMP) and allocates its state vectors
// through that view. The underlying MPool allocation metadata remains the sole
// release owner.
type AllocationAccount struct {
	account       *mpool.AllocationAccount
	owner         mpool.AllocationOwner
	sites         AllocationAccountSites
	capacityClass mpool.AllocationCapacityClass

	// viewMu guards the cached vector view. Every state vector of the
	// aggregates sharing this descriptor is created with and operated
	// through the same view pointer (#33019: a vector call's pool must be
	// exactly the vector's pool).
	viewMu   sync.Mutex
	viewBase *mpool.MPool
	view     *mpool.MPool
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
	if account == nil || account.Handle() == 0 ||
		owner < mpool.AllocationOwnerMin ||
		owner > mpool.AllocationOwnerCatalogMax ||
		sites.VectorData < mpool.AllocationSiteMin ||
		sites.VectorArea < mpool.AllocationSiteMin {
		return nil, mpool.ErrAllocationAccountInvalid
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
	}, nil
}

func (a *AllocationAccount) sameGeneration(other *AllocationAccount) bool {
	return a == other || a != nil && other != nil &&
		a.account == other.account && a.owner == other.owner &&
		a.sites == other.sites && a.capacityClass == other.capacityClass
}

// VectorMP returns an accounted view of mp that charges fresh vector
// allocations to the aggregate's account at its VectorData site. A nil
// descriptor returns mp unchanged.
func (a *AllocationAccount) VectorMP(mp *mpool.MPool) (*mpool.MPool, error) {
	if a == nil {
		return mp, nil
	}
	if mp.IsView() {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	a.viewMu.Lock()
	defer a.viewMu.Unlock()
	if a.view != nil && a.viewBase == mp {
		return a.view, nil
	}
	view, err := mp.WithAccount(a.account, a.owner, a.sites.VectorData, a.capacityClass)
	if err != nil {
		return nil, err
	}
	a.viewBase, a.view = mp, view
	return view, nil
}

// OwnsVectorMP reports whether mp is the cached vector view of this
// descriptor, so an operator can free aggregate result vectors with it.
func (a *AllocationAccount) OwnsVectorMP(mp *mpool.MPool) bool {
	if a == nil || mp == nil {
		return false
	}
	a.viewMu.Lock()
	defer a.viewMu.Unlock()
	return a.view == mp
}

// newVector creates an empty state vector of mp. The owning executor grows it
// through its accounted view of mp.
func (a *AllocationAccount) newVector(typ types.Type, mp *mpool.MPool) (*vector.Vector, error) {
	return vector.NewVec(typ, mp), nil
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

// reserveCapacity admits n bytes allocated outside the MPool (native GPU host staging)
// before they are allocated; a nil account admits without a reservation.
func (a *AllocationAccount) reserveCapacity(n uint64) (*mpool.CapacityReservation, error) {
	if a == nil {
		return nil, nil
	}
	return a.account.ReserveCapacityWithClass(n, a.capacityClass, a.owner, a.sites.ArgumentArena)
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
