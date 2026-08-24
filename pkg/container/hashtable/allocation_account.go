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

package hashtable

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

func HashMapBlockDescriptorBytes() uint64 {
	return uint64(unsafe.Sizeof([]Int64HashMapCell(nil)))
}

// AllocationAccountSelection is the immutable provenance for one hash table's
// cell blocks and outer descriptor storage. The outer []slice-header backing
// is itself data-scaled ownership and therefore uses a distinct physical site.
type AllocationAccountSelection struct {
	account        *mpool.AllocationAccount
	owner          mpool.AllocationOwner
	cellSite       mpool.AllocationSite
	descriptorSite mpool.AllocationSite
}

func NewAllocationAccountSelection(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	cellSite mpool.AllocationSite,
	descriptorSite mpool.AllocationSite,
) (*AllocationAccountSelection, error) {
	selection := &AllocationAccountSelection{
		account:        account,
		owner:          owner,
		cellSite:       cellSite,
		descriptorSite: descriptorSite,
	}
	if err := selection.validate(); err != nil {
		return nil, err
	}
	return selection, nil
}

func (s *AllocationAccountSelection) validate() error {
	if s == nil || s.account == nil || s.account.Handle() == 0 ||
		s.owner < mpool.AllocationOwnerMin ||
		s.owner > mpool.AllocationOwnerCatalogMax ||
		s.cellSite < mpool.AllocationSiteMin ||
		s.descriptorSite < mpool.AllocationSiteMin ||
		s.cellSite == s.descriptorSite {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func makeHashTableCellSlice[T any](
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
	site mpool.AllocationSite,
) ([]T, error) {
	if selection == nil {
		return mpool.MakeSlice[T](length, mp, true)
	}
	if err := selection.validate(); err != nil {
		return nil, err
	}
	return mpool.MakeSliceAccounted[T](
		length,
		mp,
		selection.account,
		selection.owner,
		site,
	)
}

func makeHashTableDescriptorSlice[T any](
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
	site mpool.AllocationSite,
) ([]T, error) {
	if selection == nil {
		return make([]T, length), nil
	}
	return makeHashTableCellSlice[T](length, mp, selection, site)
}

func freeHashTableCellSlice[T any](mp *mpool.MPool, values []T) {
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
}

func freeHashTableDescriptorSlice[T any](
	mp *mpool.MPool,
	values []T,
	selection *AllocationAccountSelection,
) {
	if selection != nil {
		freeHashTableCellSlice(mp, values)
	}
}
