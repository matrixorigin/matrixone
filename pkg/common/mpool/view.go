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

package mpool

// allocationView is how an accounted view charges fresh off-heap
// allocations: to an allocation account, or through a transient physical
// admission. Growth and release of an existing allocation follow that
// allocation's own metadata, whichever MPool value performs them.
type allocationView struct {
	request allocationAccountRequest
	admit   PhysicalMemoryAdmission
}

func (v *allocationView) alloc(mp *MPool, detailk string, sz int64) ([]byte, error) {
	if v.admit != nil {
		return mp.allocAdmittedWithDetailK(detailk, sz, v.admit)
	}
	result, err := mp.allocAccounted(detailk, sz, v.request)
	if err != nil {
		return nil, allocationAccountSiteError(v.request, err)
	}
	return result, nil
}

// WithAccount returns a view of mp whose fresh allocations are charged to
// account under owner/site/class. The view shares mp's state: it frees and
// grows mp's allocations and is never deleted on its own. Every allocation
// through the view is off-heap and charged, whatever off-heap mode the caller
// requests: accounting is decided here, not by the caller.
func (mp *MPool) WithAccount(
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
	class AllocationCapacityClass,
) (*MPool, error) {
	if mp == nil {
		return nil, ErrAllocationAccountInvalid
	}
	request := allocationAccountRequest{
		account:       account,
		owner:         owner,
		site:          site,
		capacityClass: class,
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	return &MPool{poolCore: mp.poolCore, view: &allocationView{request: request}}, nil
}

// WithAdmission returns a view of mp whose fresh off-heap allocations are
// admitted through admit instead of an account.
func (mp *MPool) WithAdmission(admit PhysicalMemoryAdmission) (*MPool, error) {
	if mp == nil || admit == nil {
		return nil, ErrAllocationAccountInvalid
	}
	return &MPool{poolCore: mp.poolCore, view: &allocationView{admit: admit}}, nil
}

// Base returns the pool mp is a view of, or mp itself.
func (mp *MPool) Base() *MPool {
	if mp == nil || mp.view == nil {
		return mp
	}
	if base, ok := globalPools.Load(mp.id); ok {
		return base.(*MPool)
	}
	return &MPool{poolCore: mp.poolCore}
}

// IsView reports whether mp is an accounted view.
func (mp *MPool) IsView() bool { return mp != nil && mp.view != nil }

// ViewAccount returns the account a view charges, or nil.
func (mp *MPool) ViewAccount() *AllocationAccount {
	if mp == nil || mp.view == nil {
		return nil
	}
	return mp.view.request.account
}

// SameView reports whether two MPool values charge fresh allocations the
// same way: the same pool and the same account, owner, site and class (or
// both unaccounted).
func SameView(a, b *MPool) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.poolCore != b.poolCore {
		return false
	}
	if a.view == nil || b.view == nil {
		return a.view == nil && b.view == nil
	}
	if a.view.admit != nil || b.view.admit != nil {
		return a.view == b.view
	}
	return a.view.request == b.view.request
}

// NewAccountedBuffer creates owned auxiliary bytes charged the way mp charges
// fresh off-heap allocations. The caller must Free the buffer or transfer it
// to an owner that releases it through its MPool.
func (mp *MPool) NewAccountedBuffer() (*AccountedBuffer, error) {
	if mp == nil || mp.view == nil {
		return nil, ErrAllocationAccountInvalid
	}
	if mp.view.admit != nil {
		return NewAdmittedBuffer(mp, mp.view.admit)
	}
	r := mp.view.request
	return NewAccountedBufferWithCapacityClass(mp, r.account, r.owner, r.site, r.capacityClass)
}

// ReservePinnedCapacity charges a non-MPool backing retained by a reader to
// mp's account. The caller owns the returned lease and releases it when the
// retained data no longer pins the backing.
func (mp *MPool) ReservePinnedCapacity(capacity uint64) (*CapacityLease, error) {
	if mp == nil || mp.view == nil {
		return nil, ErrAllocationAccountInvalid
	}
	if mp.view.admit != nil {
		return NewAdmittedCapacityLease(capacity, mp.view.admit)
	}
	r := mp.view.request
	reservation, err := r.account.ReserveCapacityWithClass(capacity, r.capacityClass, r.owner, r.site)
	if err != nil {
		return nil, err
	}
	lease, err := reservation.Commit(capacity)
	if err != nil {
		reservation.Abort()
		return nil, err
	}
	return lease, nil
}

// ViewOwner and ViewCapacityClass describe an accounted view's charge.
func (mp *MPool) ViewOwner() AllocationOwner {
	if mp == nil || mp.view == nil {
		return 0
	}
	return mp.view.request.owner
}

func (mp *MPool) ViewCapacityClass() AllocationCapacityClass {
	if mp == nil || mp.view == nil {
		return AllocationCapacityClassDefault
	}
	return mp.view.request.capacityClass
}

// IsAdmissionView reports whether mp is a transient-admission view.
func (mp *MPool) IsAdmissionView() bool {
	return mp != nil && mp.view != nil && mp.view.admit != nil
}
