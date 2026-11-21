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

// WithAccount returns a view of mp whose fresh off-heap allocations are
// charged to account under owner/site/class. The view shares mp's state: it
// frees and grows mp's allocations and is never deleted on its own. On-heap
// allocations through the view stay unaccounted.
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
