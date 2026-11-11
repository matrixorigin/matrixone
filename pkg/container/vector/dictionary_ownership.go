// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"errors"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// RowSelection owns an immutable, validated logical-row mapping. A successful
// Retain requires one Release. Callers cannot modify the incoming slice to
// change a published selection. The physical allocation is charged once and
// remains live until its final owner releases it.
type RowSelection struct {
	lease     BufferLease
	storage   *Vector
	domain    int
	monotonic bool
}

func NewRowSelection(rows []int64, domain int, mp *mpool.MPool, account *AllocationAccountSelection) (*RowSelection, error) {
	if mp == nil || domain < 0 || len(rows) > math.MaxInt/8 {
		return nil, moerr.NewInvalidInputNoCtx("invalid dictionary row-selection size")
	}
	monotonic := true
	for i, row := range rows {
		if row < 0 || uint64(row) >= uint64(domain) {
			return nil, moerr.NewInvalidInputNoCtx("dictionary row-selection index out of bounds")
		}
		if i > 0 && row < rows[i-1] {
			monotonic = false
		}
	}
	storage, err := NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), account)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		if err := AppendFixedList(storage, rows, nil, mp); err != nil {
			storage.Free(mp)
			return nil, err
		}
	}
	lease, err := NewRefCountedBufferLease(storage.data, int64(storage.Allocated()), func() { storage.Free(mp) })
	if err != nil {
		storage.Free(mp)
		return nil, err
	}
	return &RowSelection{lease: lease, storage: storage, domain: domain, monotonic: monotonic}, nil
}

func (s *RowSelection) Retain() bool { return s != nil && s.lease != nil && s.lease.Retain() }

func (s *RowSelection) Release() { s.lease.Release() }

func (s *RowSelection) Length() int { return s.storage.Length() }

func (s *RowSelection) RetainedBytes() int64 { return s.lease.AccountedBytes() }

// DictionaryBase owns one immutable flat/constant snapshot. Its row metadata
// is private; payload ownership may be shared with a reusable source header.
// Freeze once per input vector, then share this handle among its partitions.
type DictionaryBase struct {
	lease BufferLease
	vec   *Vector
}

// This wrapper proves that a promoted MPool backing keeps its original
// statement charge until the physical allocation's final release. A foreign
// decoder/cache lease needs a separate bounded pin admission instead.
type dictionaryPayloadLease struct {
	BufferLease
	account *mpool.AllocationAccount
}

func (b *DictionaryBase) Retain() bool { return b != nil && b.lease != nil && b.lease.Retain() }

func (b *DictionaryBase) Release() { b.lease.Release() }

func (b *DictionaryBase) RetainedBytes() int64 { return b.lease.AccountedBytes() }

// FreezeVector snapshots metadata and promotes uniquely owned payload into
// immutable leases without copying it. The caller must own the source header
// exclusively, including any mutable physical slices it has handed out.
// Ordinary source mutation must use its existing COW APIs. Legacy aliases and
// payload from a different account are copied into the destination account.
// Allocation failure leaves source values and ownership valid.
func FreezeVector(source *Vector, mp *mpool.MPool, account *AllocationAccountSelection) (*DictionaryBase, error) {
	if source == nil || mp == nil || (source.class != FLAT && source.class != CONSTANT) {
		return nil, moerr.NewInvalidInputNoCtx("dictionary base requires a flat or constant source")
	}
	if source.allocationAccount != nil && account == nil {
		return nil, allocationAccountInvalid("accounted dictionary snapshot requires a destination account")
	}
	if account != nil {
		if err := account.validate(); err != nil {
			return nil, err
		}
	}
	copyPayload := source.DataBackingKind() == LegacyAlias || source.AreaBackingKind() == LegacyAlias
	if account != nil && source.allocationAccount != nil && source.allocationAccount.account != account.account {
		copyPayload = true
	}
	if account != nil && source.allocationAccount == nil &&
		((source.dataLease == nil && cap(source.data) > 0) || (source.areaLease == nil && cap(source.area) > 0)) {
		copyPayload = true
	}
	var reservation *mpool.CapacityReservation
	var pinned uint64
	if account != nil && !copyPayload {
		var err error
		pinned, err = dictionaryForeignPinBytes(source, account)
		if err != nil {
			return nil, err
		}
		validityBytes := source.nsp.BorrowedAccountedBytes()
		if validityBytes < 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid dictionary validity pin size")
		}
		upper := pinned + uint64(validityBytes)
		if upper < pinned {
			return nil, moerr.NewInvalidInputNoCtx("dictionary pin size overflows")
		}
		if upper > 0 {
			reservation, err = account.account.ReserveCapacityWithClass(upper, account.capacityClass, account.owner, account.dataSite)
			if errors.Is(err, mpool.ErrAllocationAccountCapacity) {
				copyPayload = true
			} else if err != nil {
				return nil, err
			}
			if reservation != nil {
				defer reservation.Abort()
			}
		}
	}
	var snapshot *Vector
	var err error
	if copyPayload {
		snapshot, err = source.dup(mp, true, true, account)
	} else {
		snapshot, err = source.window(0, source.length, mp, account, true)
	}
	if err != nil {
		return nil, err
	}
	source.CopyExpressionMetadataTo(snapshot)
	if len(snapshot.data) == 0 {
		snapshot.releaseBorrowedData()
		snapshot.data = nil
		snapshot.cantFreeData = false
	}
	if len(snapshot.area) == 0 {
		snapshot.releaseBorrowedArea()
		snapshot.area = nil
		snapshot.cantFreeArea = false
	}
	if !copyPayload {
		if err := promoteDictionaryBacking(source, snapshot, mp, BackingData); err != nil {
			snapshot.Free(mp)
			return nil, err
		}
		if err := promoteDictionaryBacking(source, snapshot, mp, BackingArea); err != nil {
			snapshot.Free(mp)
			return nil, err
		}
	}
	retained := retainedVectorBytes(snapshot) // eagerly owns any borrowed validity metadata
	var pin *mpool.CapacityLease
	if reservation != nil {
		pinned, err = dictionaryForeignPinBytes(snapshot, account)
		if err != nil {
			snapshot.Free(mp)
			return nil, err
		}
		pin, err = reservation.Commit(pinned)
		if err != nil {
			snapshot.Free(mp)
			return nil, err
		}
	}
	lease, err := NewRefCountedBufferLease(nil, retained, func() {
		snapshot.Free(mp)
		pin.Release()
	})
	if err != nil {
		snapshot.Free(mp)
		pin.Release()
		return nil, err
	}
	return &DictionaryBase{lease: lease, vec: snapshot}, nil
}

func dictionaryForeignPinBytes(source *Vector, account *AllocationAccountSelection) (uint64, error) {
	var bytes uint64
	leases := [...]BufferLease{source.dataLease, source.areaLease}
	for i, lease := range leases {
		if lease == nil || (i == 1 && sameBufferLease(leases[0], lease)) {
			continue
		}
		if owned, ok := lease.(*dictionaryPayloadLease); ok && owned.account == account.account {
			continue
		}
		size := lease.AccountedBytes()
		if size < 0 || uint64(size) > math.MaxInt64-bytes {
			return 0, moerr.NewInvalidInputNoCtx("invalid dictionary backing pin size")
		}
		bytes += uint64(size)
	}
	return bytes, nil
}

func retainedVectorBytes(v *Vector) int64 {
	bytes := int64(v.Allocated())
	if v.dataLease != nil {
		bytes -= int64(cap(v.data))
	}
	if v.areaLease != nil {
		bytes -= int64(cap(v.area))
	}
	return bytes + v.BorrowedAccountedBytes()
}

func promoteDictionaryBacking(source, snapshot *Vector, mp *mpool.MPool, part BackingPart) error {
	data, existing, kind := source.data, source.dataLease, DetachedDataBuffer
	used := len(snapshot.data)
	if part == BackingArea {
		data, existing, kind = source.area, source.areaLease, DetachedAreaBuffer
		used = len(snapshot.area)
	}
	if existing != nil || cap(data) == 0 || used == 0 {
		return nil // window already retained an existing lease
	}
	buffer := DetachedBuffer{data: data, selection: source.allocationAccount, kind: kind}
	lease, err := NewRefCountedBufferLease(data, int64(buffer.Capacity()), func() { buffer.Free(mp) })
	if err != nil {
		return err
	}
	// New leases have one guaranteed live reference. Move it into source;
	// retain a second for the unpublished snapshot before either can escape.
	lease.Retain()
	shared := &dictionaryPayloadLease{BufferLease: lease}
	if source.allocationAccount != nil {
		shared.account = source.allocationAccount.account
	}
	if part == BackingData {
		source.dataLease, source.cantFreeData = shared, true
		snapshot.dataLease, snapshot.cantFreeData = shared, true
	} else {
		source.areaLease, source.cantFreeArea = shared, true
		snapshot.areaLease, snapshot.cantFreeArea = shared, true
	}
	return nil
}
