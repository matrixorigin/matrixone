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
// off-heap allocations of a Vector. It normally identifies an allocation
// account; JSON readers can instead use a transient admission. Physical MPool
// metadata remains the sole owner of each resulting allocation charge.
//
// A selection may be shared by all vectors owned by one Batch. Views do not
// copy it: they share storage and therefore must not create a second charge.
type AllocationAccountSelection struct {
	account       *mpool.AllocationAccount
	transient     JSONReaderMemoryAdmission
	owner         mpool.AllocationOwner
	dataSite      mpool.AllocationSite
	areaSite      mpool.AllocationSite
	nullsSite     mpool.AllocationSite
	groupingSite  mpool.AllocationSite
	capacityClass mpool.AllocationCapacityClass
}

// UsesAccount checks account identity without changing the immutable owner/site
// provenance, including selections supplied by a native execution factory.
func (s *AllocationAccountSelection) UsesAccount(account *mpool.AllocationAccount) bool {
	return s != nil && s.account == account
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
	if left != nil && left.transient != nil || right != nil && right.transient != nil {
		return false
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

func allocationAccountSelectionsShareProvenance(
	left, right *AllocationAccountSelection,
) bool {
	if left != nil && left == right {
		return true
	}
	if left != nil && left.transient != nil || right != nil && right.transient != nil {
		return false
	}
	return left != nil && right != nil &&
		left.account == right.account &&
		left.owner == right.owner &&
		left.dataSite == right.dataSite &&
		left.areaSite == right.areaSite &&
		left.nullsSite == right.nullsSite &&
		left.groupingSite == right.groupingSite
}

// ShuffleWithAllocationAccount is Shuffle with a capacity class selected for
// the replacement storage. The alternate selection must preserve the vector's
// account, owner, and sites: this changes admission only, never allocation
// provenance. Existing and replacement buffers keep their own physical leases,
// so restoring the ordinary selection after the call cannot relabel memory.
func (v *Vector) ShuffleWithAllocationAccount(
	sels []int64,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) error {
	if v == nil ||
		!allocationAccountSelectionsShareProvenance(v.allocationAccount, selection) {
		return mpool.ErrAllocationAccountMismatch
	}
	if err := selection.validate(); err != nil {
		return err
	}
	ordinary := v.allocationAccount
	v.allocationAccount = selection
	defer func() {
		v.allocationAccount = ordinary
	}()
	return v.Shuffle(sels, mp)
}

// UnionBatchWithAllocationAccount is UnionBatch with an alternate capacity
// class for allocations made by this copy only. The destination keeps its
// ordinary selection so a downstream Dup or append cannot accidentally spend
// the producing operator's private recovery floor.
func (v *Vector) UnionBatchWithAllocationAccount(
	source *Vector,
	offset int64,
	count int,
	flags []uint8,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) error {
	if v == nil ||
		!allocationAccountSelectionsShareProvenance(v.allocationAccount, selection) {
		return mpool.ErrAllocationAccountMismatch
	}
	if err := selection.validate(); err != nil {
		return err
	}
	ordinary := v.allocationAccount
	v.allocationAccount = selection
	defer func() {
		v.allocationAccount = ordinary
	}()
	return v.UnionBatch(source, offset, count, flags, mp)
}

// NewAccountedBuffer creates owned auxiliary bytes under this destination's
// immutable provenance and capacity class. The caller must Free the buffer or
// transfer it to an owner that releases it through its MPool.
func (s *AllocationAccountSelection) NewAccountedBuffer(mp *mpool.MPool) (*mpool.AccountedBuffer, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if s.transient != nil {
		return mpool.NewAdmittedBuffer(mp, s.admitPhysical)
	}
	return mpool.NewAccountedBufferWithCapacityClass(mp, s.account, s.owner, s.dataSite, s.capacityClass)
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

// NewRollupConstWithAllocation constructs a grouping-set sentinel Vector and
// charges its grouping bitmap to selection. The logical value has no data
// payload; every row is identified by the grouping bitmap instead.
func NewRollupConstWithAllocation(
	typ types.Type,
	length int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	if length < 0 || mp == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	vec.class = CONSTANT
	vec.length = length
	if length > 0 {
		if err = vec.ensureGroupingCapacity(length, mp); err != nil {
			vec.Free(mp)
			return nil, err
		}
		vec.gsp.AddRange(0, uint64(length))
	}
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
	if s != nil && s.transient != nil && s.account == nil {
		return nil
	}
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

// ReservePinnedCapacity accounts for a non-MPool backing retained by a reader.
// The caller owns the returned lease and releases it when the final retained
// view or result no longer pins that physical allocation.
func (s *AllocationAccountSelection) ReservePinnedCapacity(capacity uint64) (*mpool.CapacityLease, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if s.transient != nil {
		return mpool.NewAdmittedCapacityLease(capacity, mpool.PhysicalMemoryAdmission(s.transient))
	}
	reservation, err := s.account.ReserveCapacityWithClass(capacity, s.capacityClass, s.owner, s.areaSite)
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
	return v.IsDictionary() || cap(v.data) != 0 ||
		cap(v.area) != 0 ||
		cap(v.prepareParamKinds) != 0 ||
		cap(v.stringSources) != 0 ||
		(v.binaryStringRows != nil && v.binaryStringRows.Size() != 0) ||
		(v.textStringRows != nil && v.textStringRows.Size() != 0) ||
		v.nsp.GetBitmap().Size() != 0 ||
		v.gsp.GetBitmap().Size() != 0
}

// hasOwnedBackingStorage reports storage that UnmarshalBinary cannot replace
// without losing an MPool-owned allocation. Data and area marked cantFree are
// borrowed aliases; bitmap backing is always Go-owned and remains GC-visible
// after replacement.
func (v *Vector) hasOwnedBackingStorage() bool {
	return v.IsDictionary() || v.dataLease != nil || v.areaLease != nil ||
		cap(v.data) != 0 && !v.cantFreeData ||
		cap(v.area) != 0 && !v.cantFreeArea ||
		cap(v.prepareParamKinds) != 0 ||
		cap(v.stringSources) != 0
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
	v.allocationAccount = selection
	return nil
}

// Bitmaps always use Go-owned memory. When a vector grows, it reserves room in
// the bitmaps it uses for all of its rows; a bitmap also grows itself on write.

func validBitmapRows(rows int) error {
	if rows < 0 || rows > math.MaxInt-63 {
		return moerr.NewInvalidInputNoCtxf("invalid bitmap row count %d", rows)
	}
	return nil
}

func (v *Vector) ensureBitmapCapacity(rows int, mp *mpool.MPool) error {
	if err := v.ensureNullCapacity(rows, mp); err != nil {
		return err
	}
	if err := v.ensureGroupingCapacity(rows, mp); err != nil {
		return err
	}
	if v.binaryStringRows != nil {
		return v.ensureBinaryStringCapacity(rows, mp)
	}
	return nil
}

func (v *Vector) ensureBinaryStringCapacity(rows int, mp *mpool.MPool) error {
	// Constant payload and provenance have one physical row even when their
	// logical broadcast length or a reused payload buffer has larger capacity.
	if !v.IsConst() && rows < v.Capacity() {
		rows = v.Capacity()
	}
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	if v.binaryStringRows == nil {
		v.binaryStringRows = &bitmap.Bitmap{}
	}
	if v.textStringRows == nil {
		v.textStringRows = &bitmap.Bitmap{}
	}
	// Callers size these with InitWithSize or TryExpandWithSize before writing.
	return nil
}

func (v *Vector) ensureNullCapacity(rows int, _ *mpool.MPool) error {
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	v.nsp.GetBitmap().Reserve(rows)
	return nil
}

func (v *Vector) ensureGroupingCapacity(rows int, _ *mpool.MPool) error {
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	v.gsp.GetBitmap().Reserve(rows)
	return nil
}

func (v *Vector) freeBitmapStorage() {
	if v.nsp.HasBorrowedValidity() {
		// Release the source view without materializing it.
		v.nsp.Reset()
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
	if v.allocationAccount.transient != nil {
		return mp.AllocAdmitted(size, v.allocationAccount.admitPhysical)
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
	if v.HasBorrowedBacking() {
		if err := v.MaterializeOwned(mp); err != nil {
			return nil, err
		}
		if data {
			old = v.data
		} else {
			old = v.area
		}
	}
	if size <= cap(old) {
		return old[:size], nil
	}
	if mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"vector growth does not have a mpool",
		)
	}
	capacity, ok := mpool.GrowCapacity(int64(cap(old)), int64(size))
	if !ok {
		return nil, mpool.ErrAllocationAllocatorLimit
	}
	if cap(old) != 0 || v.allocationAccount == nil {
		return mp.Grow(old, size, v.offHeap)
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
