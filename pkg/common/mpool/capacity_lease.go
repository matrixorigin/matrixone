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

package mpool

import "sync/atomic"

const (
	capacityReservationOpen uint32 = iota
	capacityReservationCommitted
	capacityReservationAborted
)

// CapacityReservation pre-admits an upper bound before a non-MPool backing is
// allocated or pinned. Commit transfers the actual charge to one CapacityLease;
// Abort returns the whole reservation. Both terminal operations are idempotent.
type CapacityReservation struct {
	account       *AllocationAccount
	owner         AllocationOwner
	site          AllocationSite
	capacityClass AllocationCapacityClass
	upperBound    uint64
	state         atomic.Uint32
}

// CapacityLease is the sole release owner for committed non-MPool capacity.
type CapacityLease struct {
	account       *AllocationAccount
	owner         AllocationOwner
	site          AllocationSite
	capacityClass AllocationCapacityClass
	capacity      uint64
	released      atomic.Bool
}

// ReserveCapacity reserves non-MPool capacity against the statement's default
// capacity controller.
func (a *AllocationAccount) ReserveCapacity(
	upperBound uint64,
	owner AllocationOwner,
	site AllocationSite,
) (*CapacityReservation, error) {
	return a.ReserveCapacityWithClass(
		upperBound,
		AllocationCapacityClassDefault,
		owner,
		site,
	)
}

// ReserveCapacityWithClass is the execution-local capacity-class counterpart
// of ReserveCapacity.
func (a *AllocationAccount) ReserveCapacityWithClass(
	upperBound uint64,
	capacityClass AllocationCapacityClass,
	owner AllocationOwner,
	site AllocationSite,
) (*CapacityReservation, error) {
	if a == nil || site < AllocationSiteMin || site > AllocationSiteMax {
		return nil, ErrAllocationAccountInvalid
	}
	if err := a.acquireWithCapacityClass(upperBound, capacityClass, owner); err != nil {
		return nil, err
	}
	return &CapacityReservation{
		account:       a,
		owner:         owner,
		site:          site,
		capacityClass: capacityClass,
		upperBound:    upperBound,
	}, nil
}

func (r *CapacityReservation) UpperBound() uint64 {
	if r == nil {
		return 0
	}
	return r.upperBound
}

// Commit transfers actualCapacity to the returned lease. A failed oversized
// commit leaves the reservation open so its caller can release acquired
// backing and Abort without losing the original admission charge.
func (r *CapacityReservation) Commit(actualCapacity uint64) (*CapacityLease, error) {
	if r == nil || r.account == nil {
		return nil, ErrAllocationAccountInvalid
	}
	if actualCapacity > r.upperBound {
		return nil, newAllocationAccountCapacityError(
			r.upperBound,
			actualCapacity-r.upperBound,
			r.upperBound,
		)
	}
	if !r.state.CompareAndSwap(capacityReservationOpen, capacityReservationCommitted) {
		return nil, ErrAllocationAccountMismatch
	}
	if unused := r.upperBound - actualCapacity; unused > 0 {
		r.account.releaseWithCapacityClass(unused, r.capacityClass, r.owner)
	}
	return &CapacityLease{
		account:       r.account,
		owner:         r.owner,
		site:          r.site,
		capacityClass: r.capacityClass,
		capacity:      actualCapacity,
	}, nil
}

func (r *CapacityReservation) Abort() {
	if r == nil || r.account == nil ||
		!r.state.CompareAndSwap(capacityReservationOpen, capacityReservationAborted) {
		return
	}
	r.account.releaseWithCapacityClass(r.upperBound, r.capacityClass, r.owner)
}

func (l *CapacityLease) Capacity() uint64 {
	if l == nil {
		return 0
	}
	return l.capacity
}

func (l *CapacityLease) Owner() AllocationOwner {
	if l == nil {
		return 0
	}
	return l.owner
}

func (l *CapacityLease) Site() AllocationSite {
	if l == nil {
		return 0
	}
	return l.site
}

func (l *CapacityLease) Release() {
	if l == nil || l.account == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	l.account.releaseWithCapacityClass(l.capacity, l.capacityClass, l.owner)
}
