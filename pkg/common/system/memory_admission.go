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
// Reservations remain charged until Release because the current allocation
// interfaces do not provide an atomic handoff from a reservation to observed
// residency. A refresh can therefore conservatively charge a reservation twice
// after it becomes visible in memory.current, but it must never allow several
// pending reservations to spend the same sampled headroom.
type MemoryAdmissionBudget struct {
	mu sync.Mutex

	policy         MemoryDomainReservePolicy
	reserved       uint64
	nonGoHighWater uint64
	last           MemoryAdmissionBudgetSnapshot
	// Control work can consume a bounded part of RuntimeBytes, which ordinary
	// allocations cannot spend. These bytes are also included in reserved.
	controlReserved uint64
	controlActive   uint64
	domainProvider  func() (MemoryDomainSnapshot, error)
}

// MemoryAdmissionBudgetSnapshot is an immutable view of the shared physical
// ledger and its current domain observation.
type MemoryAdmissionBudgetSnapshot struct {
	Domain          MemoryDomainSnapshot
	Reserve         uint64
	Reserved        uint64
	Available       uint64
	Measured        bool
	ControlReserved uint64
	ControlActive   uint64
}

// NewMemoryAdmissionBudget creates an independent ledger. It is useful for
// deterministic tests and for an embedded service which intentionally owns a
// separate memory domain.
func NewMemoryAdmissionBudget(policy MemoryDomainReservePolicy) *MemoryAdmissionBudget {
	if policy.MinimumBytes == 0 && policy.RuntimeBytes == 0 &&
		policy.ObservedNonGoMultiplier == 0 && policy.MinimumPercent == 0 &&
		policy.HostObservedNonGoMultiplier == 0 && policy.HostMinimumPercent == 0 {
		policy = DefaultMemoryDomainReservePolicy()
	}
	if policy.ObservedNonGoMultiplier == 0 {
		policy.ObservedNonGoMultiplier = 1
	}
	return &MemoryAdmissionBudget{policy: policy, domainProvider: CurrentMemoryDomain}
}

// NewMemoryAdmissionBudgetWithProvider creates a ledger for an explicitly owned
// memory domain. The provider must report the same boundary for every call.
func NewMemoryAdmissionBudgetWithProvider(policy MemoryDomainReservePolicy, provider func() (MemoryDomainSnapshot, error)) *MemoryAdmissionBudget {
	b := NewMemoryAdmissionBudget(policy)
	if provider != nil {
		b.domainProvider = provider
	}
	return b
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
	snapshot, _ := b.domainProvider()
	if !snapshot.Measured || snapshot.Limit == 0 {
		return 0, false
	}
	return b.tryReserve(snapshot, size)
}

// tryReserve applies one caller-provided immutable sample to the shared
// reservation ledger. Keeping this small seam internal makes the cumulative
// reservation invariant deterministic to test without changing the production
// sampling path.
func (b *MemoryAdmissionBudget) tryReserve(snapshot MemoryDomainSnapshot, size uint64) (available uint64, ok bool) {
	if b == nil || !snapshot.Measured || snapshot.Limit == 0 {
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
	// An ordinary rollback must never release a control allocation's charge.
	b.reserved -= min(size, b.reserved-b.controlReserved)
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
	snapshot, _ := b.domainProvider()
	return b.Observe(snapshot)
}

// Observe publishes a caller-provided domain sample into the shared
// calibration state and returns the resulting reserve/high-water view. The
// execution budget uses this method when it already owns the sample, avoiding
// a second read and ensuring that both gates use the same calibrated reserve.
func (b *MemoryAdmissionBudget) Observe(snapshot MemoryDomainSnapshot) MemoryAdmissionBudgetSnapshot {
	if b == nil {
		return MemoryAdmissionBudgetSnapshot{}
	}
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

func (b *MemoryAdmissionBudget) Reserved() uint64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reserved
}

func (b *MemoryAdmissionBudget) availableLocked(snapshot MemoryDomainSnapshot) uint64 {
	if !snapshot.Measured {
		return 0
	}
	limit := snapshot.AdmissionLimit()
	if limit == 0 {
		return 0
	}
	if snapshot.NonGo > b.nonGoHighWater {
		b.nonGoHighWater = snapshot.NonGo
	}
	reserveSnapshot := snapshot
	reserveSnapshot.NonGo = b.nonGoHighWater
	reserve := b.policy.Reserve(reserveSnapshot)
	return b.availableWithReserveLocked(snapshot, reserve)
}

func (b *MemoryAdmissionBudget) availableWithReserveLocked(snapshot MemoryDomainSnapshot, reserve uint64) uint64 {
	used := snapshot.Used
	// The full non-Go reserve protects future working-set growth. Clean cache
	// already charged to a cgroup must not also consume this entire reserve:
	// build/test caches can otherwise reject even an eight-byte catalog read.
	if snapshot.Source == MemoryDomainSourceHost || snapshot.IsCgroup() {
		used -= min(used, snapshot.Reclaimable)
	}
	available := b.headroomLocked(snapshot, used, reserve)
	if snapshot.IsCgroup() {
		// Cache credit is NOT permission to cross a charged hard/soft boundary.
		// Every pending claim must also fit below raw usage plus the runtime
		// floor. This second gate never assumes a successful kernel reclaim.
		available = min(available, b.headroomLocked(snapshot, snapshot.Used, min(reserve, b.policy.RuntimeBytes)))
	}
	return available
}

// headroomLocked deducts the same cumulative pending claims from one physical
// view. Both the working-set and raw-charge checks run under the ledger mutex.
func (b *MemoryAdmissionBudget) headroomLocked(snapshot MemoryDomainSnapshot, used, reserve uint64) uint64 {
	limit := snapshot.AdmissionLimit()
	if b.reserved > ^uint64(0)-used {
		used = ^uint64(0)
	} else {
		// There is no allocation-complete handoff in this API. Keep every
		// outstanding claim charged, even if a later sample already includes
		// some of the materialized bytes.
		used += b.reserved
	}
	if reserve >= limit || used >= limit-reserve {
		return 0
	}
	return limit - reserve - used
}

func (b *MemoryAdmissionBudget) snapshotLocked(
	snapshot MemoryDomainSnapshot,
	available uint64,
) MemoryAdmissionBudgetSnapshot {
	reserveSnapshot := snapshot
	reserveSnapshot.NonGo = b.nonGoHighWater
	return MemoryAdmissionBudgetSnapshot{
		Domain:          snapshot,
		Reserve:         b.policy.Reserve(reserveSnapshot),
		Reserved:        b.reserved,
		Available:       available,
		Measured:        snapshot.Measured,
		ControlReserved: b.controlReserved,
		ControlActive:   b.controlActive,
	}
}
