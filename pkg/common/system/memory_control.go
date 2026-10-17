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

const (
	memoryControlConcurrency = uint64(4)
	memoryControlTotalBytes  = uint64(64 << 20)
)

// MemoryControlLease belongs to one explicitly classified control statement.
// All fields are protected by budget.mu. Closing the statement does not free
// bytes still owned by physical allocations, nor recycle their concurrency slot.
type MemoryControlLease struct {
	budget   *MemoryAdmissionBudget
	limit    uint64
	capacity uint64
	used     uint64
	closed   bool
	released bool
}

// AcquireControl admits at most four control statements per process. Their
// combined physical allocations can use at most 64 MiB of the existing runtime
// reserve (and never more than that reserve). Ordinary requests retain their
// full reserve. A zero runtime reserve disables this allowance.
func (b *MemoryAdmissionBudget) AcquireControl() (*MemoryControlLease, bool) {
	if b == nil {
		return nil, false
	}
	snapshot, _ := b.domainProvider()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !snapshot.Measured || snapshot.AdmissionLimit() == 0 || b.controlActive >= memoryControlConcurrency {
		return nil, false
	}
	b.availableLocked(snapshot) // Calibrate the same reserve as ordinary work.
	limit := b.controlLimitLocked(snapshot) / memoryControlConcurrency
	if limit == 0 {
		return nil, false
	}
	b.controlActive++
	return &MemoryControlLease{budget: b, limit: limit, capacity: limit * memoryControlConcurrency}, true
}

func (b *MemoryAdmissionBudget) controlLimitLocked(snapshot MemoryDomainSnapshot) uint64 {
	// Preserve at least three quarters of the runtime reserve for the runtime
	// itself, including on small domains with a configured lower floor.
	return min(memoryControlTotalBytes, b.policy.RuntimeBytes/4, snapshot.AdmissionLimit()/100)
}

func (l *MemoryControlLease) Limit() uint64 {
	return l.limit
}

func (l *MemoryControlLease) Capacity() uint64 {
	return l.capacity
}

// TryReserve charges the actual bytes to both the shared physical ledger and
// the per-statement control allowance. No allocation is granted on an exhausted
// domain merely because it is internal or uses the control class.
func (l *MemoryControlLease) TryReserve(size uint64) (uint64, bool) {
	b := l.budget
	snapshot, _ := b.domainProvider()
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed || !snapshot.Measured || snapshot.AdmissionLimit() == 0 {
		return 0, false
	}
	b.availableLocked(snapshot)
	reserveSnapshot := snapshot
	reserveSnapshot.NonGo = b.nonGoHighWater
	reserve := b.policy.Reserve(reserveSnapshot)
	controlLimit := min(b.controlLimitLocked(snapshot), reserve)
	available := b.availableWithReserveLocked(snapshot, reserve-controlLimit)
	available = min(available, l.limit-min(l.used, l.limit), controlLimit-min(b.controlReserved, controlLimit))
	if size > available || size > ^uint64(0)-b.reserved {
		return available, false
	}
	l.used += size
	b.controlReserved += size
	b.reserved += size
	b.last = b.snapshotLocked(snapshot, b.availableLocked(snapshot))
	return available - size, true
}

// Release runs at the allocation's actual free boundary, even after Close.
func (l *MemoryControlLease) Release(size uint64) {
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > l.used || size > b.controlReserved || size > b.reserved {
		panic("control memory reservation release underflow")
	}
	l.used -= size
	b.controlReserved -= size
	b.reserved -= size
	l.releaseSlotLocked()
	if b.last.Measured {
		b.last = b.snapshotLocked(b.last.Domain, b.availableLocked(b.last.Domain))
	}
}

func (l *MemoryControlLease) Close() {
	if l == nil {
		return
	}
	l.budget.mu.Lock()
	defer l.budget.mu.Unlock()
	l.closed = true
	l.releaseSlotLocked()
}

func (l *MemoryControlLease) releaseSlotLocked() {
	if l.closed && l.used == 0 && !l.released {
		l.released = true
		l.budget.controlActive--
	}
}
