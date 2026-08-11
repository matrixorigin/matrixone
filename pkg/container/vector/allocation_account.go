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
	"errors"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func allocationAccountInvalid(message string) error {
	return errors.Join(
		mpool.ErrAllocationAccountInvalid,
		moerr.NewInternalErrorNoCtx(message),
	)
}

// AllocationAccountSelection is an immutable choice for the first owned
// off-heap allocations of a Vector. The physical MPool allocation metadata
// remains the sole owner of the resulting charge.
//
// A selection may be shared by all vectors owned by one Batch. Views do not
// copy it: they share storage and therefore must not create a second charge.
type AllocationAccountSelection struct {
	account       *mpool.AllocationAccount
	owner         mpool.AllocationOwner
	dataSite      mpool.AllocationSite
	areaSite      mpool.AllocationSite
	nullsSite     mpool.AllocationSite
	groupingSite  mpool.AllocationSite
	capacityClass mpool.AllocationCapacityClass
}

// AllocationAccountSelectionsEqual reports whether two immutable selections
// describe the same physical allocation provenance. Separately constructed
// selections are interchangeable only when they charge the same account,
// owner, and allocation sites.
func AllocationAccountSelectionsEqual(
	left, right *AllocationAccountSelection,
) bool {
	if left == right {
		return true
	}
	return left != nil && right != nil &&
		left.account == right.account &&
		left.owner == right.owner &&
		left.dataSite == right.dataSite &&
		left.areaSite == right.areaSite &&
		left.nullsSite == right.nullsSite &&
		left.groupingSite == right.groupingSite &&
		left.capacityClass == right.capacityClass
}

func NewAllocationAccountSelection(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	dataSite mpool.AllocationSite,
	areaSite mpool.AllocationSite,
	nullsSite mpool.AllocationSite,
	groupingSite mpool.AllocationSite,
) (*AllocationAccountSelection, error) {
	return NewAllocationAccountSelectionWithCapacityClass(
		account,
		owner,
		dataSite,
		areaSite,
		nullsSite,
		groupingSite,
		mpool.AllocationCapacityClassDefault,
	)
}

// NewAllocationAccountSelectionWithCapacityClass applies one execution-local
// capacity class to every physical vector backing allocation.
func NewAllocationAccountSelectionWithCapacityClass(
	account *mpool.AllocationAccount,
	owner mpool.AllocationOwner,
	dataSite mpool.AllocationSite,
	areaSite mpool.AllocationSite,
	nullsSite mpool.AllocationSite,
	groupingSite mpool.AllocationSite,
	capacityClass mpool.AllocationCapacityClass,
) (*AllocationAccountSelection, error) {
	selection := &AllocationAccountSelection{
		account:       account,
		owner:         owner,
		dataSite:      dataSite,
		areaSite:      areaSite,
		nullsSite:     nullsSite,
		groupingSite:  groupingSite,
		capacityClass: capacityClass,
	}
	if err := selection.validate(); err != nil {
		return nil, err
	}
	return selection, nil
}

// NewOffHeapVecWithTypeAndAllocation constructs an empty owning Vector whose
// future allocations use selection.
func NewOffHeapVecWithTypeAndAllocation(
	typ types.Type,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	vec := NewOffHeapVecWithType(typ)
	if err := vec.SetAllocationAccount(selection); err != nil {
		vec.Free(nil)
		return nil, err
	}
	return vec, nil
}

// NewConstNullWithAllocation constructs a constant NULL Vector with explicit
// allocation provenance for any future owned backing.
func NewConstNullWithAllocation(
	typ types.Type,
	length int,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	vec.class = CONSTANT
	vec.length = length
	return vec, nil
}

// NewConstFixedWithAllocation constructs an off-heap constant fixed-width
// Vector and charges its physical backing to selection.
func NewConstFixedWithAllocation[T any](
	typ types.Type,
	value T,
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	vec.class = CONSTANT
	if length > 0 {
		if err = SetConstFixed(vec, value, length, mp); err != nil {
			vec.Free(mp)
			return nil, err
		}
	}
	return vec, nil
}

// NewConstBytesWithAllocation constructs an off-heap constant varlen Vector
// and charges its backing independently through selection.
func NewConstBytesWithAllocation(
	typ types.Type,
	value []byte,
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	vec.class = CONSTANT
	if length > 0 {
		if err = SetConstBytes(vec, value, length, mp); err != nil {
			vec.Free(mp)
			return nil, err
		}
	}
	return vec, nil
}

// NewConstArrayWithAllocation constructs an off-heap constant array Vector
// and charges its backing independently through selection.
func NewConstArrayWithAllocation[T types.ArrayElement](
	typ types.Type,
	value []T,
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	vec.class = CONSTANT
	if length > 0 {
		if err = SetConstArray(vec, value, length, mp); err != nil {
			vec.Free(mp)
			return nil, err
		}
	}
	return vec, nil
}

func (s *AllocationAccountSelection) validate() error {
	if s == nil || s.account == nil || s.account.Handle() == 0 ||
		s.owner < mpool.AllocationOwnerMin ||
		s.owner > mpool.AllocationOwnerCatalogMax ||
		s.dataSite < mpool.AllocationSiteMin ||
		s.areaSite < mpool.AllocationSiteMin ||
		s.nullsSite < mpool.AllocationSiteMin ||
		s.groupingSite < mpool.AllocationSiteMin {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

// AllocationAccountSelection returns the immutable selection used by this
// vector's future owned allocations. It is nil for unaccounted vectors and
// views outside the retained HashBuild domain.
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
			return allocationAccountInvalid(
				"allocation-accounted vector must be off-heap",
			)
		}
	}
	if AllocationAccountSelectionsEqual(v.allocationAccount, selection) {
		return nil
	}
	if v.hasBackingStorage() {
		return allocationAccountInvalid("vector already has backing storage")
	}
	return nil
}

func (v *Vector) hasBackingStorage() bool {
	return cap(v.data) != 0 ||
		cap(v.area) != 0 ||
		cap(v.prepareParamKinds) != 0 ||
		(v.binaryStringRows != nil &&
			(v.binaryStringRows.Size() != 0 ||
				v.binaryStringRows.ExternalStorageCapacity() != 0)) ||
		v.nsp.GetBitmap().Size() != 0 ||
		v.gsp.GetBitmap().Size() != 0 ||
		v.nsp.GetBitmap().ExternalStorageCapacity() != 0 ||
		v.gsp.GetBitmap().ExternalStorageCapacity() != 0
}

// hasOwnedBackingStorage reports storage that UnmarshalBinary cannot replace
// without losing an MPool-owned allocation. Data and area marked cantFree are
// borrowed aliases; ordinary bitmap backing is Go-owned and remains GC-visible
// after replacement. Accounted bitmap storage is explicit external storage.
func (v *Vector) hasOwnedBackingStorage() bool {
	return cap(v.data) != 0 && !v.cantFreeData ||
		cap(v.area) != 0 && !v.cantFreeArea ||
		cap(v.prepareParamKinds) != 0 ||
		(v.binaryStringRows != nil && v.binaryStringRows.ExternalStorageCapacity() != 0) ||
		v.nsp.GetBitmap().ExternalStorageCapacity() != 0 ||
		v.gsp.GetBitmap().ExternalStorageCapacity() != 0
}

// SetAllocationAccount selects the account used by future owned allocations.
// It is intentionally explicit and is legal only before the first backing
// allocation. Reset retains the selection; Free clears it.
func (v *Vector) SetAllocationAccount(
	selection *AllocationAccountSelection,
) error {
	if err := v.CanSetAllocationAccount(selection); err != nil {
		return err
	}
	if AllocationAccountSelectionsEqual(v.allocationAccount, selection) {
		return nil
	}
	if v.allocationAccount != nil && selection == nil {
		v.nsp.GetBitmap().ReleaseExternalStorage()
		v.gsp.GetBitmap().ReleaseExternalStorage()
		if v.binaryStringRows != nil {
			v.binaryStringRows.ReleaseExternalStorage()
		}
	}
	v.allocationAccount = selection
	if selection != nil {
		v.nsp.GetBitmap().InstallExternalStorage(nil)
		v.gsp.GetBitmap().InstallExternalStorage(nil)
		if v.binaryStringRows != nil {
			v.binaryStringRows.InstallExternalStorage(nil)
		}
	}
	return nil
}

func (v *Vector) ensureBitmapCapacity(rows int, mp *mpool.MPool) error {
	if v.allocationAccount == nil {
		return nil
	}
	if rows < 0 || rows > math.MaxInt-64 || mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	logicalRows := rows
	// Nulls.AddRange currently expands through end+1 even though end is
	// exclusive. Keep one admitted sentinel bit so raw bitmap mutation cannot
	// escape to a Go allocation at the vector's logical row boundary.
	if rows > 0 {
		rows++
	}
	requiredWords := (rows + 63) / 64
	if requiredWords <= v.nsp.GetBitmap().ExternalStorageCapacity() &&
		requiredWords <= v.gsp.GetBitmap().ExternalStorageCapacity() &&
		(v.binaryStringRows == nil || requiredWords <= v.binaryStringRows.ExternalStorageCapacity()) {
		return nil
	}
	nulls, err := v.allocateBitmapGrowth(
		v.nsp.GetBitmap(),
		rows,
		mp,
		v.allocationAccount.nullsSite,
	)
	if err != nil {
		return err
	}
	grouping, err := v.allocateBitmapGrowth(
		v.gsp.GetBitmap(),
		rows,
		mp,
		v.allocationAccount.groupingSite,
	)
	if err != nil {
		mpool.FreeSlice(mp, nulls)
		return err
	}
	if cap(nulls) > 0 {
		previous := v.nsp.GetBitmap().InstallExternalStorage(nulls)
		mpool.FreeSlice(mp, previous)
	}
	if cap(grouping) > 0 {
		previous := v.gsp.GetBitmap().InstallExternalStorage(grouping)
		mpool.FreeSlice(mp, previous)
	}
	if v.binaryStringRows != nil {
		if err := v.ensureBinaryStringCapacity(logicalRows, mp); err != nil {
			return err
		}
	}
	return nil
}

func (v *Vector) ensureBinaryStringCapacity(rows int, mp *mpool.MPool) error {
	if rows < v.Capacity() {
		rows = v.Capacity()
	}
	if v.allocationAccount != nil {
		// InplaceSort has an infallible public API and reorders all row
		// metadata together. Reserve its NULL and grouping sidecars while an
		// MPool is available, before publishing binary row metadata.
		if err := v.ensureNullCapacity(rows, mp); err != nil {
			return err
		}
		if err := v.ensureGroupingCapacity(rows, mp); err != nil {
			return err
		}
	}
	if v.binaryStringRows == nil {
		v.binaryStringRows = &bitmap.Bitmap{}
		if v.allocationAccount != nil {
			v.binaryStringRows.InstallExternalStorage(nil)
		}
	}
	if v.allocationAccount == nil {
		return nil
	}
	return v.ensureSingleBitmapCapacity(
		v.binaryStringRows,
		rows,
		mp,
		v.allocationAccount.nullsSite,
	)
}

func (v *Vector) ensureNullCapacity(rows int, mp *mpool.MPool) error {
	if v.allocationAccount == nil {
		return nil
	}
	return v.ensureSingleBitmapCapacity(
		v.nsp.GetBitmap(),
		rows,
		mp,
		v.allocationAccount.nullsSite,
	)
}

func (v *Vector) ensureGroupingCapacity(rows int, mp *mpool.MPool) error {
	if v.allocationAccount == nil {
		return nil
	}
	return v.ensureSingleBitmapCapacity(
		v.gsp.GetBitmap(),
		rows,
		mp,
		v.allocationAccount.groupingSite,
	)
}

func (v *Vector) ensureSingleBitmapCapacity(
	value *bitmap.Bitmap,
	rows int,
	mp *mpool.MPool,
	site mpool.AllocationSite,
) error {
	if rows < 0 || rows > math.MaxInt-64 || mp == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if rows > 0 {
		rows++
	}
	storage, err := v.allocateBitmapGrowth(value, rows, mp, site)
	if err != nil {
		return err
	}
	if cap(storage) > 0 {
		previous := value.InstallExternalStorage(storage)
		mpool.FreeSlice(mp, previous)
	}
	return nil
}

func (v *Vector) allocateBitmapGrowth(
	value *bitmap.Bitmap,
	rows int,
	mp *mpool.MPool,
	site mpool.AllocationSite,
) ([]uint64, error) {
	requiredWords := (rows + 63) / 64
	if requiredWords <= value.ExternalStorageCapacity() {
		return nil, nil
	}
	requiredBytes := int64(requiredWords) * 8
	oldBytes := int64(value.ExternalStorageCapacity()) * 8
	newBytes, ok := mpool.GrowCapacity(oldBytes, requiredBytes)
	if !ok || newBytes > int64(math.MaxInt) || newBytes%8 != 0 {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	next, err := mpool.MakeSliceAccountedWithCapacityClass[uint64](
		int(newBytes/8),
		mp,
		v.allocationAccount.account,
		v.allocationAccount.owner,
		site,
		v.allocationAccount.capacityClass,
	)
	if err != nil {
		return nil, err
	}
	clear(next)
	return next, nil
}

func (v *Vector) freeBitmapStorage(mp *mpool.MPool) {
	for _, value := range []*bitmap.Bitmap{
		v.nsp.GetBitmap(),
		v.gsp.GetBitmap(),
		v.binaryStringRows,
	} {
		if value == nil {
			continue
		}
		storage := value.ReleaseExternalStorage()
		if cap(storage) > 0 {
			mpool.FreeSlice(mp, storage)
		}
	}
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
		return nil, allocationAccountInvalid(
			"accounted allocation must be off-heap",
		)
	}
	site := v.allocationAccount.areaSite
	if data {
		site = v.allocationAccount.dataSite
	}
	return mp.AllocAccountedWithCapacityClass(
		size,
		v.allocationAccount.account,
		v.allocationAccount.owner,
		site,
		v.allocationAccount.capacityClass,
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
	if err := validateStreamingReadSize(r, int64(size)); err != nil {
		return size, nil, err
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

func validateStreamingReadSize(r io.Reader, size int64) error {
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative vector buffer size")
	}
	var remaining int64 = -1
	switch reader := r.(type) {
	case *io.LimitedReader:
		remaining = reader.N
	case interface{ Len() int }:
		remaining = int64(reader.Len())
	}
	if remaining >= 0 && size > remaining {
		return io.ErrUnexpectedEOF
	}
	return nil
}
