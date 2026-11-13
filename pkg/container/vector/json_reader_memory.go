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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// JSONReaderMemoryAdmission charges reader memory to a statement budget,
// including physical allocations when no AllocationAccount is available.
// A successful reservation returns exactly one release function. The callback
// does not attest that any bytes are valid or immutable.
type JSONReaderMemoryAdmission func(uint64) (func(), error)

// JSONReaderAllocationSelection supplies physical allocation provenance when
// the caller has a transient budget but no statement allocation account.
// It is an allocation policy only; it never confers JSON reader authority.
func JSONReaderAllocationSelection(selection *AllocationAccountSelection, admission JSONReaderMemoryAdmission) *AllocationAccountSelection {
	if selection == nil && admission != nil {
		return &AllocationAccountSelection{transient: admission}
	}
	return selection
}

func (s *AllocationAccountSelection) admitPhysical(size uint64) (func(), error) {
	// MPool records the requested physical capacity exactly. The extra
	// allowance covers the registry entry and opaque release token.
	if size > math.MaxUint64-256 {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader allocation size overflow")
	}
	return ReserveJSONReaderMetadata(nil, s.transient, size+256)
}

func makeSelectedSlice[T any](s *AllocationAccountSelection, n int, mp *mpool.MPool, site mpool.AllocationSite) ([]T, error) {
	if s.transient != nil {
		return mpool.MakeSliceAdmitted[T](n, mp, s.admitPhysical)
	}
	return mpool.MakeSliceAccountedWithCapacityClass[T](n, mp, s.account, s.owner, site, s.capacityClass)
}

// JSONReaderMetadataCapacity conservatively covers Go size-class rounding,
// the admission token and its release closure in addition to object sizes.
// It is intended for fixed-size control objects, not document or row backing.
func JSONReaderMetadataCapacity(objectBytes uintptr) uint64 {
	bytes := uint64(objectBytes)
	if bytes > (math.MaxUint64-256)/2 {
		return math.MaxUint64
	}
	return 2*bytes + 256
}

// JSONReaderContainerMetadataCapacity covers bounded control maps and slices
// associated with a number of input columns or expression nodes. The per-entry
// allowance includes map growth, temporary replacement maps and slice capacity.
func JSONReaderContainerMetadataCapacity(objectBytes uintptr, entries int) (uint64, error) {
	base := JSONReaderMetadataCapacity(objectBytes)
	const perEntry uint64 = 1024
	if entries < 0 || uint64(entries) > (math.MaxUint64-base)/perEntry {
		return 0, moerr.NewInvalidInputNoCtx("JSON metadata capacity overflow")
	}
	return base + uint64(entries)*perEntry, nil
}

// ReserveJSONReaderMetadata uses the selected statement allocation account if
// present; otherwise it delegates to the statement's transient admission. A
// standalone caller with neither has no statement account to charge.
func ReserveJSONReaderMetadata(selection *AllocationAccountSelection, admission JSONReaderMemoryAdmission, capacity uint64) (func(), error) {
	if selection != nil {
		if err := selection.validate(); err != nil {
			return nil, err
		}
		if selection.transient != nil {
			return ReserveJSONReaderMetadata(nil, selection.transient, capacity)
		}
		reservation, err := selection.account.ReserveCapacityWithClass(capacity, selection.capacityClass, selection.owner, selection.dataSite)
		if err != nil {
			return nil, err
		}
		lease, err := reservation.Commit(capacity)
		if err != nil {
			reservation.Abort()
			return nil, err
		}
		return lease.Release, nil
	}
	if admission == nil {
		return nil, nil
	}
	release, err := admission(capacity)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	if release == nil {
		return nil, moerr.NewInternalErrorNoCtx("JSON metadata admission returned no release")
	}
	return release, nil
}

// ReserveJSONReaderBacking admits a Go-backed query allocation before it is
// made. The allowance includes allocator rounding and the release token.
func ReserveJSONReaderBacking(selection *AllocationAccountSelection, admission JSONReaderMemoryAdmission, size uint64) (func(), error) {
	if size > (math.MaxUint64-256)/2 {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader allocation size overflow")
	}
	return ReserveJSONReaderMetadata(selection, admission, 2*size+256)
}

func releaseJSONReaderMetadata(release func()) {
	if release != nil {
		release()
	}
}

func firstJSONReaderMemoryAdmission(admissions []JSONReaderMemoryAdmission) JSONReaderMemoryAdmission {
	if len(admissions) == 0 {
		return nil
	}
	return admissions[0]
}
