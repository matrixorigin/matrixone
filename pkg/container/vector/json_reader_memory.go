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
)

// JSONReaderMemoryAdmission charges non-MPool metadata to a statement budget.
// A successful reservation returns exactly one release function. The callback
// does not attest that any bytes are valid or immutable.
type JSONReaderMemoryAdmission func(uint64) (func(), error)

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
