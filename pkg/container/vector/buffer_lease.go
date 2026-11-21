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
	return v != nil && (v.dataLease != nil || v.areaLease != nil)
}

func (v *Vector) BorrowedAccountedBytes() int64 {
	if v == nil {
		return 0
	}
	bytes := int64(0)
	if v.dataLease != nil {
		bytes += v.dataLease.AccountedBytes()
	}
	if v.areaLease != nil && !sameBufferLease(v.areaLease, v.dataLease) {
		bytes += v.areaLease.AccountedBytes()
	}
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

// NewBorrowedFixedVector constructs a fixed-width immutable Vector. The
// constructor retains lease; the caller continues to own its incoming ref.
func NewBorrowedFixedVector(
	typ types.Type,
	rows int,
	data []byte,
	lease BufferLease,
) (*Vector, error) {
	if rows < 0 || typ.IsVarlen() || typ.TypeSize() <= 0 ||
		uint64(rows) > uint64(^uint(0)>>1)/uint64(typ.TypeSize()) ||
		len(data) != rows*typ.TypeSize() {
		return nil, moerr.NewInvalidInputNoCtx("invalid borrowed fixed vector layout")
	}
	vec := NewOffHeapVecWithType(typ)
	if err := vec.InstallBorrowedData(data, lease); err != nil {
		vec.Free(nil)
		return nil, err
	}
	vec.length = rows
	return vec, nil
}

// PrepareDataForInplaceMutation makes the data backing writable without
// changing values, row metadata, or the independently owned varlen area.
// A failed allocation leaves the existing backing and lease unchanged.
func (v *Vector) PrepareDataForInplaceMutation(mp *mpool.MPool) error {
	if !v.cantFreeData || len(v.data) == 0 {
		return nil
	}
	data, err := v.allocData(mp, len(v.data))
	if err != nil {
		return err
	}
	copy(data, v.data)
	v.releaseBorrowedData()
	v.data = data
	v.cantFreeData = false
	return nil
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

// CopyWithReadonlyArea replaces an empty varlen destination with a private
// materialization of source. Only the newly copied area becomes leased; neither
// source storage nor a previously published destination backing is shared.
// The destination's type, allocation mode and account are preserved. Failure
// leaves both source and destination unchanged.
func (v *Vector) CopyWithReadonlyArea(source *Vector, mp *mpool.MPool) error {
	if v == nil || source == nil || mp == nil || v == source ||
		v.length != 0 || v.IsConst() || !v.typ.IsVarlen() || v.typ.Oid != source.typ.Oid {
		return moerr.NewInvalidInputNoCtx("readonly area copy requires an empty compatible varlen destination")
	}
	private := NewVec(v.typ)
	private.offHeap = v.offHeap
	private.sorted = v.sorted
	private.isBin = v.isBin

	if err := private.UnionBatch(source, 0, source.length, nil, mp); err != nil {
		private.Free(mp)
		return err
	}
	if len(private.area) > 0 {
		buffer := DetachedBuffer{
			data: private.area,
			kind: DetachedAreaBuffer,
		}
		lease, err := NewRefCountedBufferLease(private.area, int64(buffer.Capacity()), func() {
			buffer.Free(mp)
		})
		if err != nil {
			private.Free(mp)
			return err
		}
		// The initial reference moves to this unpublished Vector. The callback
		// captures only the physical allocation, not the Vector or its lease.
		private.areaLease = lease
		private.cantFreeArea = true
	}
	v.Free(mp)
	*v = *private
	return nil
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
	owned, err := v.dup(mp, true, true)
	if err != nil {
		return err
	}
	v.CopyExpressionMetadataTo(owned)
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

	view, err := v.window(start, end, mp, true)
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

// RetainBorrowedBufferInto converts a scoped legacy decoder into an owned
// header with immutable leased payload and private metadata. The full foreign
// capacity is charged before publication. Free/COW releases the cache pin.
func RetainBorrowedBufferInto(dst, source *Vector, lease BufferLease, mp *mpool.MPool) (bool, error) {
	if dst == nil || source == nil || mp == nil || dst == source || lease == nil || dst.HasBorrowedBacking() || dst.Length() != 0 || dst.Allocated() != 0 || source.HasBorrowedBacking() || (source.DataBackingKind() != LegacyAlias && cap(source.data) > 0) || (source.AreaBackingKind() != LegacyAlias && cap(source.area) > 0) {
		return false, nil
	}
	// mp is the destination's allocation view: an accounted view charges the
	// cache pin to the destination's account.
	var pin *mpool.CapacityLease
	if mp.IsView() {
		// A transient policy cannot promote an account-only cache pin.
		// Decline so the loader copies through its admitted owned path.
		if mp.IsAdmissionView() {
			return false, nil
		}
		size := lease.AccountedBytes()
		if size < 0 {
			return false, moerr.NewInvalidInputNoCtx("invalid cache pin size")
		}
		var err error
		pin, err = mp.ReservePinnedCapacity(uint64(size))
		if err != nil {
			return false, err
		}
	}
	if !lease.Retain() {
		pin.Release()
		return false, moerr.NewInvalidInputNoCtx("immutable decoder lease is closed")
	}
	counted, err := NewRefCountedBufferLease(lease.Bytes(), lease.AccountedBytes(), func() { lease.Release(); pin.Release() })
	if err != nil {
		lease.Release()
		pin.Release()
		return false, err
	}
	defer counted.Release()
	temporary := *source
	temporary.dataLease, temporary.areaLease = counted, counted
	// window retains each used backing; the scoped decoder itself owns neither.
	oldType, oldOffHeap := dst.typ, dst.offHeap
	_, err = temporary.windowInto(0, source.length, mp, dst)
	if err != nil {
		dst.Free(mp)
		*dst = Vector{typ: oldType, offHeap: oldOffHeap}
		return false, err
	}
	source.CopyExpressionMetadataTo(dst)
	return true, nil
}
