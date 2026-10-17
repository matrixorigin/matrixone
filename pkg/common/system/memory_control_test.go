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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryAdmissionHostCacheCreditDoesNotWeakenCgroup(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceHost, MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2} {
		t.Run(string(source), func(t *testing.T) {
			b := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{ObservedNonGoMultiplier: 1})
			s := MemoryDomainSnapshot{Used: 95, Limit: 100, Reclaimable: 50, Source: source, Measured: true}
			want := uint64(5)
			if source == MemoryDomainSourceHost {
				want = 55
			}
			require.Equal(t, want, b.Observe(s).Available)
			_, ok := b.tryReserve(s, want)
			require.True(t, ok)
			require.Zero(t, b.Observe(s).Available)
			_, ok = b.tryReserve(s, 1)
			require.False(t, ok)
			b.Release(want)
			require.Equal(t, want, b.Observe(s).Available)
			s.SoftLimit = 98
			require.Equal(t, want-2, b.Observe(s).Available)
		})
	}
}

func newExhaustedOrdinaryControlBudget() (*MemoryAdmissionBudget, *MemoryDomainSnapshot) {
	s := &MemoryDomainSnapshot{
		Used: (8 << 30) - (512 << 20), Limit: 8 << 30,
		Source: MemoryDomainSourceCgroupV2, Measured: true,
	}
	b := NewMemoryAdmissionBudgetWithProvider(MemoryDomainReservePolicy{
		MinimumBytes: 256 << 20, RuntimeBytes: 256 << 20, ObservedNonGoMultiplier: 1,
	}, func() (MemoryDomainSnapshot, error) { return *s, nil })
	return b, s
}

func TestMemoryControlProgressIsBoundedAndCharged(t *testing.T) {
	b, _ := newExhaustedOrdinaryControlBudget()
	_, ok := b.TryReserve(1)
	require.False(t, ok, "ordinary work cannot spend the runtime reserve")
	var leases []*MemoryControlLease
	for range memoryControlConcurrency {
		lease, ok := b.AcquireControl()
		require.True(t, ok)
		require.Equal(t, uint64(16<<20), lease.Limit())
		_, ok = lease.TryReserve(16 << 20)
		require.True(t, ok)
		leases = append(leases, lease)
	}
	require.Equal(t, uint64(64<<20), b.Reserved())
	require.Equal(t, uint64(64<<20), b.Snapshot().ControlReserved)
	_, ok = b.AcquireControl()
	require.False(t, ok, "concurrency is bounded across all control callers")
	_, ok = leases[0].TryReserve(1)
	require.False(t, ok, "one statement cannot exceed its allowance")
	b.Release(^uint64(0))
	require.Equal(t, uint64(64<<20), b.Reserved(), "ordinary release cannot consume control charges")
	for _, lease := range leases {
		lease.Close()
	}
	_, ok = b.AcquireControl()
	require.False(t, ok, "closing a statement must not recycle live allocation ownership")
	for _, lease := range leases {
		lease.Release(16 << 20)
		lease.Close() // Idempotent after the last physical free.
	}
	require.Zero(t, b.Reserved())
	require.Zero(t, b.Snapshot().ControlActive)
	lease, ok := b.AcquireControl()
	require.True(t, ok)
	lease.Close()
}

func TestMemoryControlCannotBypassDomainExhaustion(t *testing.T) {
	b, s := newExhaustedOrdinaryControlBudget()
	lease, ok := b.AcquireControl()
	require.True(t, ok)
	defer lease.Close()
	s.Used = s.Limit
	s.Reclaimable = s.Limit
	_, ok = lease.TryReserve(1)
	require.False(t, ok, "even control work cannot credit cgroup cache at the hard limit")
	s.Used = (8 << 30) - (512 << 20)
	_, ok = lease.TryReserve(1 << 20)
	require.True(t, ok)
	lease.Release(1 << 20)
	s.SoftLimit = s.Used
	_, ok = lease.TryReserve(1)
	require.False(t, ok, "control work must preserve the domain soft boundary")
}

func TestMemoryControlSlotAdmissionIsConcurrent(t *testing.T) {
	b, _ := newExhaustedOrdinaryControlBudget()
	var wg sync.WaitGroup
	leases := make(chan *MemoryControlLease, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if lease, ok := b.AcquireControl(); ok {
				leases <- lease
			}
		}()
	}
	wg.Wait()
	close(leases)
	require.Len(t, leases, 4)
	for lease := range leases {
		lease.Close()
	}
	require.Zero(t, b.Snapshot().ControlActive)
}

func TestMemoryControlAllowanceScalesDown(t *testing.T) {
	b, _ := newExhaustedOrdinaryControlBudget()
	b.policy.RuntimeBytes = 8 << 20
	lease, ok := b.AcquireControl()
	require.True(t, ok)
	require.Equal(t, uint64(2<<20), lease.Capacity())
	lease.Close()
	b.policy.RuntimeBytes = 0
	_, ok = b.AcquireControl()
	require.False(t, ok)
}

// Exercise the same immutable sample through both public production readers,
// reproducing the cached-host counterexample from the integration review.
func TestHostMemoryReadersAgreeWithReclaimableCache(t *testing.T) {
	const gib = uint64(1 << 30)
	for _, tc := range []struct {
		name                        string
		used, reclaimable, headroom uint64
		allow                       bool
	}{
		{"warm-cache", 14 * gib, 8 * gib, 10 * gib, true},
		{"reclaimed-cache", 6 * gib, 0, 10 * gib, true},
		{"exhausted", 16 * gib, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := memoryDomainState.snapshot.Load()
			sample := &MemoryDomainSnapshot{
				Used: tc.used, Limit: 16 * gib, Reclaimable: tc.reclaimable,
				Source: MemoryDomainSourceHost, Measured: true,
				// Freeze the injected observation for this non-parallel test.
				ObservedAt: time.Now().Add(time.Hour),
			}
			memoryDomainState.snapshot.Store(sample)
			t.Cleanup(func() { memoryDomainState.snapshot.Store(previous) })
			available, measured := MemoryAvailableIncludingCache()
			require.True(t, measured)
			require.Equal(t, tc.headroom, available)
			ledger := NewMemoryAdmissionBudget(DefaultMemoryDomainReservePolicy())
			view := ledger.Snapshot()
			require.Zero(t, view.Reserved)
			_, ok := ledger.TryReserve(1 << 20)
			require.Equal(t, tc.allow, ok)
			if ok {
				require.GreaterOrEqual(t, view.Available, uint64(256<<20))
				ledger.Release(1 << 20)
			} else {
				require.Zero(t, view.Available)
			}
			require.Zero(t, ledger.Reserved())
		})
	}
}
