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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// BackingKind describes the release authority for one physical Vector backing.
// The zero value deliberately preserves the existing MPool-owned behavior.
type BackingKind uint8

const (
	// OwnedMPoolUnique backings were allocated by the Vector and are freed by it.
	OwnedMPoolUnique BackingKind = iota
	// BorrowedAlias backings are aliases the Vector neither owns nor frees: a
	// window, a decoded buffer, or bytes lent by an external owner. Whoever lent
	// the bytes keeps them alive; the Vector copies them before any mutation.
	BorrowedAlias
)

// BackingPart selects one independently-owned Vector backing.
type BackingPart uint8

const (
	BackingData BackingPart = iota
	BackingArea
)

// BufferLease is a ref-counted immutable byte backing. Retain must fail after
// the refcount reaches zero; every successful Retain requires one Release.
// Bytes is valid only while the caller owns a live reference. Vectors never
// hold a BufferLease: the lender that publishes aliasing vectors holds it (see
// bufferlease.Holder and batch.Batch.HoldLease).
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
	if v != nil && v.cantFreeData {
		return BorrowedAlias
	}
	return OwnedMPoolUnique
}

func (v *Vector) AreaBackingKind() BackingKind {
	if v != nil && v.cantFreeArea {
		return BorrowedAlias
	}
	return OwnedMPoolUnique
}

// HasBorrowedBacking reports whether either backing is an alias the Vector
// does not own. Such a Vector must be copied (MaterializeOwned) before it is
// mutated or kept past the lifetime its lender guarantees.
func (v *Vector) HasBorrowedBacking() bool {
	return v != nil && (v.cantFreeData || v.cantFreeArea)
}

// CanDetach reports whether part is owned by the Vector and may therefore be
// moved to another owner.
func (v *Vector) CanDetach(part BackingPart) bool {
	if v == nil {
		return false
	}
	switch part {
	case BackingData:
		return !v.cantFreeData
	case BackingArea:
		return !v.cantFreeArea
	default:
		return false
	}
}

// InstallBorrowedData installs a read-only alias of data. The Vector must not
// own data capacity. The caller (the lender) keeps data alive for every read
// through this Vector and through any window or alias derived from it.
func (v *Vector) InstallBorrowedData(data []byte) error {
	if v == nil || cap(v.data) != 0 && !v.cantFreeData {
		return moerr.NewInternalErrorNoCtx("cannot install borrowed vector data")
	}
	v.data = data
	v.cantFreeData = true
	return nil
}

// InstallBorrowedArea is the independently-owned area counterpart of
// InstallBorrowedData.
func (v *Vector) InstallBorrowedArea(area []byte) error {
	if v == nil || cap(v.area) != 0 && !v.cantFreeArea {
		return moerr.NewInternalErrorNoCtx("cannot install borrowed vector area")
	}
	v.area = area
	v.cantFreeArea = true
	return nil
}

// NewBorrowedFixedVector constructs a fixed-width immutable Vector aliasing
// data. The caller keeps data alive while the Vector is read.
func NewBorrowedFixedVector(
	typ types.Type,
	rows int,
	data []byte,
) (*Vector, error) {
	if rows < 0 || typ.IsVarlen() || typ.TypeSize() <= 0 ||
		uint64(rows) > uint64(^uint(0)>>1)/uint64(typ.TypeSize()) ||
		len(data) != rows*typ.TypeSize() {
		return nil, moerr.NewInvalidInputNoCtx("invalid borrowed fixed vector layout")
	}
	vec := NewOffHeapVecWithType(typ)
	if err := vec.InstallBorrowedData(data); err != nil {
		vec.Free(nil)
		return nil, err
	}
	vec.length = rows
	return vec, nil
}

// InstallBorrowedView replaces an empty destination with a full-row read-only
// alias of source: payload bytes are aliased, row metadata (nulls, grouping) is
// private. dst must not own data or area storage. The caller keeps the source
// bytes alive for every read through dst. On failure dst keeps its type and
// allocation mode and holds no storage.
func (v *Vector) InstallBorrowedView(source *Vector, mp *mpool.MPool) error {
	if v == nil || source == nil || v == source || v.length != 0 || v.hasOwnedBackingStorage() {
		return moerr.NewInternalErrorNoCtx("cannot install a borrowed vector view")
	}
	oldType, oldOffHeap := v.typ, v.offHeap
	if _, err := source.windowInto(0, source.length, mp, v); err != nil {
		v.Free(mp)
		*v = Vector{typ: oldType, offHeap: oldOffHeap}
		return err
	}
	return nil
}

// PrepareDataForInplaceMutation makes the data backing writable without
// changing values, row metadata, or the independently owned varlen area.
// A failed allocation leaves the existing backing unchanged.
func (v *Vector) PrepareDataForInplaceMutation(mp *mpool.MPool) error {
	if !v.cantFreeData {
		return nil
	}
	if len(v.data) == 0 {
		v.data = nil
		v.cantFreeData = false
		return nil
	}
	data, err := v.allocData(mp, len(v.data))
	if err != nil {
		return err
	}
	copy(data, v.data)
	v.data = data
	v.cantFreeData = false
	return nil
}

// dropAliasedData stops referencing an aliased data backing. Owned storage is
// kept for reuse.
func (v *Vector) dropAliasedData() {
	if v != nil && v.cantFreeData {
		v.data = nil
		v.cantFreeData = false
	}
}

// dropAliasedArea is the area counterpart of dropAliasedData.
func (v *Vector) dropAliasedArea() {
	if v != nil && v.cantFreeArea {
		v.area = nil
		v.cantFreeArea = false
	}
}

func (v *Vector) dropAliasedBacking() {
	v.dropAliasedArea()
	v.dropAliasedData()
}

// MaterializeOwned performs copy-on-write transactionally: every aliased
// backing is replaced by a private copy. Allocation failure leaves the
// borrowed Vector unchanged and readable.
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
	old := *v
	*v = *owned
	old.Free(mp)
	return nil
}

// WindowCopyingOwnedParts returns a [start, end) window that does not depend
// on v's own allocations: parts v owns are copied, parts v merely aliases stay
// aliased to the same backing. The caller keeps the lender of those aliased
// parts alive (for example by moving the lender's leases along with the
// window). The result must be materialized before mutation.
func (v *Vector) WindowCopyingOwnedParts(start, end int, mp *mpool.MPool) (*Vector, error) {
	if v == nil || mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("vector window copy requires a vector and mpool")
	}
	view, err := v.window(start, end, mp, true)
	if err != nil {
		return nil, err
	}
	view.offHeap = true
	if !v.cantFreeData && len(view.data) > 0 {
		owned, allocErr := view.allocOwned(mp, len(view.data), true, true)
		if allocErr != nil {
			view.Free(mp)
			return nil, allocErr
		}
		copy(owned, view.data)
		view.data = owned
		view.cantFreeData = false
	}
	if !v.cantFreeArea && len(view.area) > 0 {
		owned, allocErr := view.allocOwned(mp, len(view.area), true, false)
		if allocErr != nil {
			view.Free(mp)
			return nil, allocErr
		}
		copy(owned, view.area)
		view.area = owned
		view.cantFreeArea = false
	}
	if len(view.data) == 0 {
		view.data = nil
		view.cantFreeData = false
	}
	if len(view.area) == 0 {
		view.area = nil
		view.cantFreeArea = false
	}
	return view, nil
}
