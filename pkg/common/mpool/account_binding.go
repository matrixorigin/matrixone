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

// AccountBinding is an operator's binding of an allocation account to one
// owner/site/class. Compile binds the account before the operator knows its
// process pool, so the accounted MPool view is built lazily on the pool the
// operator allocates from and cached for that pool.
type AccountBinding struct {
	account *AllocationAccount
	owner   AllocationOwner
	site    AllocationSite
	class   AllocationCapacityClass
	base    *MPool
	view    *MPool
}

// NewAccountBinding binds account under owner/site with the default capacity
// class.
func NewAccountBinding(
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
) (*AccountBinding, error) {
	return NewAccountBindingWithCapacityClass(
		account, owner, site, AllocationCapacityClassDefault)
}

// NewAccountBindingWithCapacityClass binds account under owner/site/class.
func NewAccountBindingWithCapacityClass(
	account *AllocationAccount,
	owner AllocationOwner,
	site AllocationSite,
	class AllocationCapacityClass,
) (*AccountBinding, error) {
	if account == nil || owner == 0 || site == 0 {
		return nil, ErrAllocationAccountInvalid
	}
	return &AccountBinding{
		account: account,
		owner:   owner,
		site:    site,
		class:   class,
	}, nil
}

// Account returns the bound account, or nil.
func (v *AccountBinding) Account() *AllocationAccount {
	if v == nil {
		return nil
	}
	return v.account
}

// MP returns the accounted view of mp. A nil binding returns mp unchanged.
func (v *AccountBinding) MP(mp *MPool) (*MPool, error) {
	if v == nil {
		return mp, nil
	}
	if v.view != nil && v.base == mp {
		return v.view, nil
	}
	view, err := mp.WithAccount(v.account, v.owner, v.site, v.class)
	if err != nil {
		return nil, err
	}
	v.base, v.view = mp, view
	return view, nil
}
