// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// AllocationAccountSelection is an immutable choice for the first owned
// off-heap data and area allocations of a Vector. The physical MPool
// allocation metadata remains the sole owner of the resulting charge.
//
// A selection may be shared by all vectors owned by one Batch. Views do not
// copy it: they share storage and therefore must not create a second charge.
type AllocationAccountSelection struct {
	account  *mpool.AllocationAccount
	owner    mpool.AllocationOwner
	dataSite mpool.AllocationSite
	areaSite mpool.AllocationSite
}

func NewAllocationAccountSelection(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	dataSite mpool.AllocationSite,
	areaSite mpool.AllocationSite,
) (*AllocationAccountSelection, error) {
	selection := &AllocationAccountSelection{
		account:  account,
		owner:    owner,
		dataSite: dataSite,
		areaSite: areaSite,
	}
	if err := selection.validate(); err != nil {
		return nil, err
	}
	return selection, nil
}

func (s *AllocationAccountSelection) validate() error {
	if s == nil || s.account == nil || s.account.Handle() == 0 ||
		s.owner < mpool.AllocationOwnerMin ||
		s.owner > mpool.AllocationOwnerMax ||
		s.dataSite < mpool.AllocationSiteMin ||
		s.areaSite < mpool.AllocationSiteMin {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

// AllocationAccountSelection returns the immutable selection used by this
// vector's future owned allocations. It is nil for legacy vectors and views.
func (v *Vector) AllocationAccountSelection() *AllocationAccountSelection {
	if v == nil {
		return nil
	}
	return v.allocationAccount
}

// CanSetAllocationAccount reports whether selection can be installed without
// converting or relabeling an existing physical allocation.
func (v *Vector) CanSetAllocationAccount(
	selection *AllocationAccountSelection,
) error {
	if v == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if selection != nil {
		if err := selection.validate(); err != nil {
			return err
		}
		if !v.offHeap {
			return fmt.Errorf(
				"%w: allocation-accounted vector must be off-heap",
				mpool.ErrAllocationAccountInvalid,
			)
		}
	}
	if v.allocationAccount == selection {
		return nil
	}
	if v.hasBackingStorage() {
		return fmt.Errorf(
			"%w: vector already has backing storage",
			mpool.ErrAllocationAccountInvalid,
		)
	}
	return nil
}

func (v *Vector) hasBackingStorage() bool {
	return cap(v.data) != 0 || cap(v.area) != 0
}

// SetAllocationAccount selects the account used by future owned data and area
// allocations. It is intentionally explicit and is legal only before the
// first backing allocation. Reset retains the selection; Free clears it.
func (v *Vector) SetAllocationAccount(
	selection *AllocationAccountSelection,
) error {
	if err := v.CanSetAllocationAccount(selection); err != nil {
		return err
	}
	v.allocationAccount = selection
	return nil
}

func (v *Vector) allocData(mp *mpool.MPool, size int) ([]byte, error) {
	return v.allocOwned(mp, size, v.offHeap, true)
}

func (v *Vector) allocArea(mp *mpool.MPool, size int) ([]byte, error) {
	return v.allocOwned(mp, size, v.offHeap, false)
}

func (v *Vector) allocOwned(
	mp *mpool.MPool,
	size int,
	offHeap bool,
	data bool,
) ([]byte, error) {
	if mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"vector allocation does not have a mpool",
		)
	}
	if v.allocationAccount == nil {
		return mp.Alloc(size, offHeap)
	}
	if !offHeap {
		return nil, fmt.Errorf(
			"%w: accounted allocation must be off-heap",
			mpool.ErrAllocationAccountInvalid,
		)
	}
	site := v.allocationAccount.areaSite
	if data {
		site = v.allocationAccount.dataSite
	}
	return mp.AllocAccounted(
		size,
		v.allocationAccount.account,
		v.allocationAccount.owner,
		site,
	)
}

func (v *Vector) growData(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.data, size, true)
}

func (v *Vector) growArea(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.area, size, false)
}

func (v *Vector) growOwned(
	mp *mpool.MPool,
	old []byte,
	size int,
	data bool,
) ([]byte, error) {
	if size <= cap(old) {
		return old[:size], nil
	}
	if mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"vector growth does not have a mpool",
		)
	}
	if cap(old) != 0 || v.allocationAccount == nil {
		return mp.Grow(old, size, v.offHeap)
	}

	capacity, ok := mpool.GrowCapacity(0, int64(size))
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf(
			"invalid mpool grow capacity, old %d, required %d",
			cap(old),
			size,
		)
	}
	buf, err := v.allocOwned(mp, int(capacity), true, data)
	if err != nil {
		return nil, err
	}
	return buf[:size], nil
}

func (v *Vector) growArea2(
	mp *mpool.MPool,
	src []byte,
	size int,
) ([]byte, error) {
	oldLen := len(v.area)
	if size < oldLen+len(src) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"mpool grow2 actually shrinks, %d+%d, %d",
			oldLen,
			len(src),
			size,
		)
	}
	grown, err := v.growArea(mp, size)
	if err != nil {
		return nil, err
	}
	copy(grown[oldLen:oldLen+len(src)], src)
	return grown, nil
}

func (v *Vector) readSizeBytes(
	r io.Reader,
	mp *mpool.MPool,
	data bool,
) (int32, []byte, error) {
	size, err := types.ReadInt32(r)
	if err != nil {
		return 0, nil, err
	}
	var old []byte
	if data {
		old = v.data
	} else {
		old = v.area
	}
	if size == 0 {
		if old != nil {
			old = old[:0]
		}
		if data {
			v.data = old
		} else {
			v.area = old
		}
		return 0, old, nil
	}
	if size < 0 {
		return size, nil, moerr.NewInvalidInputNoCtx(
			"negative vector buffer size",
		)
	}
	var buf []byte
	if data {
		buf, err = v.growData(mp, int(size))
	} else {
		buf, err = v.growArea(mp, int(size))
	}
	if err != nil {
		return 0, nil, err
	}
	// Grow may already have freed the old allocation. Publish its replacement
	// before reading so a short reader still leaves one reachable cleanup owner.
	if data {
		v.data = buf
	} else {
		v.area = buf
	}
	if _, err = io.ReadFull(r, buf); err != nil {
		return size, buf, err
	}
	return size, buf, nil
}
