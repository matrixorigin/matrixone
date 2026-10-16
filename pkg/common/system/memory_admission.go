// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package system

import (
	"math"
	"sync"
)

// MemoryAdmissionBudget is the process-wide physical memory ledger shared by
// execution admission and write-path throttlers. The sampled Used value is the
// already charged domain footprint; reserved is the forward-looking amount
// promised by admission calls but not necessarily visible in the sample yet.
// Using max(Used, reserved) avoids charging an allocation twice after it has
// become visible in memory.current, while still protecting a reservation that
// races the next sample.
type MemoryAdmissionBudget struct {
	mu sync.Mutex

	policy         MemoryDomainReservePolicy
	reserved       uint64
	nonGoHighWater uint64
	last           MemoryAdmissionBudgetSnapshot
}

// MemoryAdmissionBudgetSnapshot is an immutable view of the shared physical
// ledger and its current domain observation.
type MemoryAdmissionBudgetSnapshot struct {
	Domain    MemoryDomainSnapshot
	Reserve   uint64
	Reserved  uint64
	Available uint64
	Measured  bool
}

// NewMemoryAdmissionBudget creates an independent ledger. It is useful for
// deterministic tests and for an embedded service which intentionally owns a
// separate memory domain.
func NewMemoryAdmissionBudget(policy MemoryDomainReservePolicy) *MemoryAdmissionBudget {
	if policy.MinimumBytes == 0 && policy.RuntimeBytes == 0 && policy.ObservedNonGoMultiplier == 0 {
		policy = DefaultMemoryDomainReservePolicy()
	}
	if policy.ObservedNonGoMultiplier == 0 {
		policy.ObservedNonGoMultiplier = 1
	}
	return &MemoryAdmissionBudget{policy: policy}
}

var defaultMemoryAdmissionBudget = NewMemoryAdmissionBudget(DefaultMemoryDomainReservePolicy())

// DefaultMemoryAdmissionBudget returns the process-wide ledger used by the
// production execution and resource-throttling paths.
func DefaultMemoryAdmissionBudget() *MemoryAdmissionBudget {
	return defaultMemoryAdmissionBudget
}

// TryReserve reserves forward-looking memory against the same physical domain
// observed by all production admission gates. A missing first-ever sample is a
// fail-closed result; a stale but measured sample remains usable because the
// memory-domain reader guarantees that failed refreshes retain the last good
// observation.
func (b *MemoryAdmissionBudget) TryReserve(size uint64) (available uint64, ok bool) {
	if b == nil {
		return 0, false
	}
	if size == 0 {
		available, _ = b.Available()
		// A zero-byte request has no physical effect and must not be rejected
		// merely because the first memory-domain sample is unavailable.
		return available, true
	}
	snapshot, _ := CurrentMemoryDomain()
	if !snapshot.Measured || snapshot.Limit == 0 {
		return 0, false
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	available = b.availableLocked(snapshot)
	if size > available {
		b.last = b.snapshotLocked(snapshot, available)
		return available, false
	}
	if b.reserved > math.MaxUint64-size {
		b.last = b.snapshotLocked(snapshot, 0)
		return 0, false
	}
	b.reserved += size
	available -= size
	b.last = b.snapshotLocked(snapshot, available)
	return available, true
}

// Release returns a previously admitted forward-looking reservation. Release
// is intentionally saturating because a failed local CAS may race a cleanup
// path; the component ledger remains authoritative for its own invariant.
func (b *MemoryAdmissionBudget) Release(size uint64) {
	if b == nil || size == 0 {
		return
	}
	b.mu.Lock()
	if size >= b.reserved {
		b.reserved = 0
	} else {
		b.reserved -= size
	}
	if b.last.Measured {
		b.last = b.snapshotLocked(b.last.Domain, b.availableLocked(b.last.Domain))
	}
	b.mu.Unlock()
}

// Snapshot returns the latest shared ledger view. It refreshes the domain only
// through the shared MemoryDomain TTL, so callers do not create an independent
// sampling path.
func (b *MemoryAdmissionBudget) Snapshot() MemoryAdmissionBudgetSnapshot {
	if b == nil {
		return MemoryAdmissionBudgetSnapshot{}
	}
	snapshot, _ := CurrentMemoryDomain()
	b.mu.Lock()
	defer b.mu.Unlock()
	if snapshot.Measured {
		b.last = b.snapshotLocked(snapshot, b.availableLocked(snapshot))
	}
	return b.last
}

// Available reports the remaining forward-looking physical budget.
func (b *MemoryAdmissionBudget) Available() (uint64, bool) {
	snapshot := b.Snapshot()
	return snapshot.Available, snapshot.Measured
}

// Check performs a statement-entry admission check without creating a byte
// reservation. It is intentionally separate from TryReserve: allocation
// ownership remains with the actual MPool/scratch reservation, while callers
// can reject a new statement before starting work when the physical domain has
// no minimum runtime margin left.
func (b *MemoryAdmissionBudget) Check(size uint64) (available uint64, measured, ok bool) {
	available, measured = b.Available()
	if !measured {
		return 0, false, false
	}
	return available, true, available >= size
}

func (b *MemoryAdmissionBudget) Reserved() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reserved
}

func (b *MemoryAdmissionBudget) availableLocked(snapshot MemoryDomainSnapshot) uint64 {
	if !snapshot.Measured || snapshot.Limit == 0 {
		return 0
	}
	if snapshot.NonGo > b.nonGoHighWater {
		b.nonGoHighWater = snapshot.NonGo
	}
	reserveSnapshot := snapshot
	reserveSnapshot.NonGo = b.nonGoHighWater
	reserve := b.policy.Reserve(reserveSnapshot)
	used := snapshot.Used
	if b.reserved > used {
		used = b.reserved
	}
	if reserve >= snapshot.Limit || used >= snapshot.Limit-reserve {
		return 0
	}
	return snapshot.Limit - reserve - used
}

func (b *MemoryAdmissionBudget) snapshotLocked(
	snapshot MemoryDomainSnapshot,
	available uint64,
) MemoryAdmissionBudgetSnapshot {
	reserveSnapshot := snapshot
	reserveSnapshot.NonGo = b.nonGoHighWater
	return MemoryAdmissionBudgetSnapshot{
		Domain:    snapshot,
		Reserve:   b.policy.Reserve(reserveSnapshot),
		Reserved:  b.reserved,
		Available: available,
		Measured:  snapshot.Measured,
	}
}
