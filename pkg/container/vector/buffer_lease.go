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
	"math"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// BackingKind describes the release authority for one physical Vector backing.
// The zero value deliberately preserves the existing MPool-owned behavior.
type BackingKind uint8

const (
	OwnedMPoolUnique BackingKind = iota
	BorrowedLease
	LegacyAlias
)

// BackingPart selects one independently-owned Vector backing.
type BackingPart uint8

const (
	BackingData BackingPart = iota
	BackingArea
)

// BufferLease is a ref-counted immutable byte backing. Retain must fail after
// the refcount reaches zero; every successful Retain requires one Release.
// Bytes is valid only while the caller owns a live reference.
type BufferLease = bufferlease.BufferLease

// RefCountedBufferLease is the default lease implementation used by external
// decoders and FileService adapters. NewRefCountedBufferLease returns one
// initial owner reference.
type RefCountedBufferLease = bufferlease.RefCounted

func NewRefCountedBufferLease(
	data []byte,
	accountedBytes int64,
	releaseOne func(),
) (*RefCountedBufferLease, error) {
	return bufferlease.NewRefCounted(data, accountedBytes, releaseOne)
}

func (v *Vector) DataBackingKind() BackingKind {
	if v == nil {
		return OwnedMPoolUnique
	}
	if v.dataLease != nil {
		return BorrowedLease
	}
	if v.cantFreeData {
		return LegacyAlias
	}
	return OwnedMPoolUnique
}

func (v *Vector) AreaBackingKind() BackingKind {
	if v == nil {
		return OwnedMPoolUnique
	}
	if v.areaLease != nil {
		return BorrowedLease
	}
	if v.cantFreeArea {
		return LegacyAlias
	}
	return OwnedMPoolUnique
}

func (v *Vector) HasBorrowedBacking() bool {
	return v != nil && (v.dataLease != nil || v.areaLease != nil || v.nsp.HasBorrowedValidity())
}

func (v *Vector) BorrowedAccountedBytes() int64 {
	if v == nil {
		return 0
	}
	var bytes int64
	if v.dataLease != nil {
		bytes += v.dataLease.AccountedBytes()
	}
	if v.areaLease != nil && !sameBufferLease(v.areaLease, v.dataLease) {
		bytes += v.areaLease.AccountedBytes()
	}
	bytes += v.nsp.BorrowedAccountedBytes()
	return bytes
}

// sameBufferLease only de-duplicates implementations whose dynamic values are
// comparable. BufferLease is a public interface and can legally be
// implemented by slice-backed value types; comparing those interfaces
// directly would panic. When identity cannot be proven, accounting both
// references is the safe upper bound.
func sameBufferLease(left, right BufferLease) bool {
	if left == nil || right == nil {
		return false
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	return leftValue.Type() == rightValue.Type() &&
		leftValue.Comparable() && leftValue.Interface() == rightValue.Interface()
}

func (v *Vector) CanDetach(part BackingPart) bool {
	if v == nil {
		return false
	}
	switch part {
	case BackingData:
		return v.dataLease == nil && !v.cantFreeData
	case BackingArea:
		return v.areaLease == nil && !v.cantFreeArea
	default:
		return false
	}
}

// InstallBorrowedData atomically installs a retained read-only data view. The
// Vector must not already own data capacity; callers must Free or materialize
// the previous generation first.
func (v *Vector) InstallBorrowedData(data []byte, lease BufferLease) error {
	if v == nil || lease == nil || cap(v.data) != 0 || v.dataLease != nil {
		return moerr.NewInternalErrorNoCtx("cannot install borrowed vector data")
	}
	if !lease.Retain() {
		return moerr.NewInternalErrorNoCtx("buffer lease is already released")
	}
	v.data = data
	v.dataLease = lease
	v.cantFreeData = true
	return nil
}

// InstallBorrowedArea is the independently-owned area counterpart of
// InstallBorrowedData.
func (v *Vector) InstallBorrowedArea(area []byte, lease BufferLease) error {
	if v == nil || lease == nil || cap(v.area) != 0 || v.areaLease != nil {
		return moerr.NewInternalErrorNoCtx("cannot install borrowed vector area")
	}
	if !lease.Retain() {
		return moerr.NewInternalErrorNoCtx("buffer lease is already released")
	}
	v.area = area
	v.areaLease = lease
	v.cantFreeArea = true
	return nil
}

// PrepareBorrowedValidity reserves the owned COW destination before a
// borrowed Arrow validity view is published. Nulls' legacy mutation APIs
// cannot return allocation errors, so admission must happen here while the
// bridge can still fail transactionally. The bitmap remains logically empty
// until the validity view is materialized.
func (v *Vector) PrepareBorrowedValidity(rows int, mp *mpool.MPool) error {
	if v == nil || mp == nil || rows < 0 || rows > math.MaxInt-63 ||
		v.nsp.HasBorrowedValidity() || v.nsp.Len() != 0 {
		return moerr.NewInvalidInputNoCtx("invalid borrowed validity reservation")
	}
	requiredWords := (rows + 63) / 64
	bitmap := v.nsp.GetBitmap()
	if requiredWords > bitmap.ExternalStorageCapacity() {
		var (
			storage []uint64
			err     error
		)
		if v.allocationAccount == nil {
			storage, err = mpool.MakeSlice[uint64](requiredWords, mp, v.offHeap)
		} else {
			storage, err = v.allocateBitmapGrowth(
				bitmap, rows, mp, v.allocationAccount.nullsSite,
			)
		}
		if err != nil {
			return err
		}
		if cap(storage) > 0 {
			previous := bitmap.InstallExternalStorage(storage)
			mpool.FreeSlice(mp, previous)
		}
	}
	bitmap.Reset()
	return nil
}

// NewBorrowedFixedVector constructs a fixed-width immutable Vector. The
// constructor retains lease; the caller continues to own its incoming ref.
func NewBorrowedFixedVector(
	typ types.Type,
	rows int,
	data []byte,
	lease BufferLease,
) (*Vector, error) {
	return NewBorrowedFixedVectorWithAllocation(typ, rows, data, lease, nil)
}

// NewBorrowedFixedVectorWithAllocation installs selection before borrowed
// backing, so any later COW or bitmap materialization remains in the same
// statement account.
func NewBorrowedFixedVectorWithAllocation(
	typ types.Type,
	rows int,
	data []byte,
	lease BufferLease,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	if rows < 0 || typ.IsVarlen() || typ.TypeSize() <= 0 ||
		uint64(rows) > uint64(^uint(0)>>1)/uint64(typ.TypeSize()) ||
		len(data) != rows*typ.TypeSize() {
		return nil, moerr.NewInvalidInputNoCtx("invalid borrowed fixed vector layout")
	}
	vec, err := NewOffHeapVecWithTypeAndAllocation(typ, selection)
	if err != nil {
		return nil, err
	}
	if err := vec.InstallBorrowedData(data, lease); err != nil {
		vec.Free(nil)
		return nil, err
	}
	vec.length = rows
	return vec, nil
}

func (v *Vector) releaseBorrowedData() {
	if v == nil || v.dataLease == nil {
		return
	}
	lease := v.dataLease
	v.dataLease = nil
	v.data = nil
	v.cantFreeData = false
	lease.Release()
}

func (v *Vector) releaseBorrowedArea() {
	if v == nil || v.areaLease == nil {
		return
	}
	lease := v.areaLease
	v.areaLease = nil
	v.area = nil
	v.cantFreeArea = false
	lease.Release()
}

func (v *Vector) releaseBorrowedBacking() {
	if v == nil {
		return
	}
	v.releaseBorrowedArea()
	v.releaseBorrowedData()
}

// MaterializeOwned performs copy-on-write transactionally. Allocation failure
// leaves the borrowed Vector unchanged and readable.
func (v *Vector) MaterializeOwned(mp *mpool.MPool) error {
	if v == nil || !v.HasBorrowedBacking() {
		return nil
	}
	if mp == nil {
		return moerr.NewInternalErrorNoCtx("borrowed vector materialization does not have a mpool")
	}
	owned, err := v.dup(mp, true, true, v.allocationAccount)
	if err != nil {
		return err
	}
	old := *v
	*v = *owned
	old.Free(mp)
	return nil
}

// RetainedReadonlyView returns an explicitly retained full-row view. Legacy
// aliases are excluded because they do not carry a lifetime owner.
func (v *Vector) RetainedReadonlyView() (*Vector, error) {
	if v == nil || v.DataBackingKind() == LegacyAlias || v.AreaBackingKind() == LegacyAlias {
		return nil, moerr.NewInternalErrorNoCtx("vector backing has no retainable owner")
	}
	if !v.HasBorrowedBacking() {
		return nil, moerr.NewInternalErrorNoCtx("owned vector has no shared backing lease")
	}
	return v.WindowByLogicalRows(0, v.length)
}

// RetainedReadonlyViewWithMP creates an asynchronously safe full-row snapshot.
// Borrowed backings are retained; unique-owned backings are copied so the
// source may be recycled immediately. The result remains immutable while any
// retained backing is present and must be materialized before mutation.
func (v *Vector) RetainedReadonlyViewWithMP(mp *mpool.MPool) (*Vector, error) {
	if v == nil {
		return nil, moerr.NewInvalidInputNoCtx("retained vector snapshot requires a vector and mpool")
	}
	return v.RetainedReadonlyWindowWithMP(0, v.length, mp)
}

// RetainedReadonlyWindowWithMP is the row-range form of
// RetainedReadonlyViewWithMP. It preserves leased payload backings while
// copying source-owned descriptors and other unique-owned slices.
func (v *Vector) RetainedReadonlyWindowWithMP(start, end int, mp *mpool.MPool) (*Vector, error) {
	if v == nil || mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("retained vector snapshot requires a vector and mpool")
	}
	if v.DataBackingKind() == LegacyAlias || v.AreaBackingKind() == LegacyAlias {
		return nil, moerr.NewInternalErrorNoCtx("vector backing has no retainable owner")
	}
	if !v.HasBorrowedBacking() {
		return nil, moerr.NewInternalErrorNoCtx("owned vector has no shared backing lease")
	}

	var (
		view *Vector
		err  error
	)
	if v.allocationAccount != nil {
		view, err = v.WindowByLogicalRowsWithAllocation(
			start, end, mp, v.allocationAccount,
		)
	} else {
		view, err = v.window(start, end, mp, nil, true)
	}
	if err != nil {
		return nil, err
	}
	view.offHeap = true

	// Window can safely alias only a leased source backing. Copy every
	// source-owned part before the source becomes reusable.
	if v.dataLease == nil && len(view.data) > 0 {
		owned, allocErr := view.allocOwned(mp, len(view.data), true, true)
		if allocErr != nil {
			view.Free(mp)
			return nil, allocErr
		}
		copy(owned, view.data)
		view.data = owned
		view.cantFreeData = false
	}
	if v.areaLease == nil && len(view.area) > 0 {
		owned, allocErr := view.allocOwned(mp, len(view.area), true, false)
		if allocErr != nil {
			view.Free(mp)
			return nil, allocErr
		}
		copy(owned, view.area)
		view.area = owned
		view.cantFreeArea = false
	}
	if view.dataLease == nil && len(view.data) == 0 {
		view.cantFreeData = false
	}
	if view.areaLease == nil && len(view.area) == 0 {
		view.cantFreeArea = false
	}
	return view, nil
}
