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
	"testing"

	"github.com/stretchr/testify/require"
)

func ciCgroupBudget(source MemoryDomainSource) (*MemoryAdmissionBudget, *MemoryDomainSnapshot) {
	const gib = uint64(1 << 30)
	s := &MemoryDomainSnapshot{
		Used: 1432 * gib / 100, Limit: 16 * gib, Reclaimable: 7 * gib,
		NonGo: 978 * gib / 1000, Source: source, Measured: true,
	}
	b := NewMemoryAdmissionBudgetWithProvider(MemoryDomainReservePolicy{
		MinimumBytes: 256 << 20, RuntimeBytes: 256 << 20,
		ObservedNonGoMultiplier: 2, MinimumPercent: 5,
	}, func() (MemoryDomainSnapshot, error) { return *s, nil })
	return b, s
}

func TestCgroupWarmCacheRetainsChargedRuntimeBoundary(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2} {
		t.Run(string(source), func(t *testing.T) {
			b, s := ciCgroupBudget(source)
			view := b.Snapshot()
			require.Greater(t, view.Reserve, s.HardHeadroom(), "reproduce CI's reserve larger than free charged space")
			want := s.HardHeadroom() - b.policy.RuntimeBytes
			require.Equal(t, want, view.Available)
			_, ok := b.TryReserve(120)
			require.True(t, ok, "a tiny catalog query must not require the entire non-Go reserve twice")
			_, ok = b.TryReserve(want - 120)
			require.True(t, ok)
			_, ok = b.TryReserve(1)
			require.False(t, ok, "cache credit must not spend raw charged headroom or another pending claim")
			require.Equal(t, want, b.Reserved())
			b.Release(want)
			s.Used = s.Limit
			require.Zero(t, b.Snapshot().Available, "hard-limit exhaustion rejects even with plentiful cache")
		})
	}
}

func TestCgroupWarmCachePreservesFiniteSoftBoundary(t *testing.T) {
	b, s := ciCgroupBudget(MemoryDomainSourceCgroupV2)
	s.SoftLimit = 15 << 30
	for _, hard := range []uint64{16 << 30, math.MaxUint64} {
		s.Limit = hard
		want := s.SoftLimit - s.Used - b.policy.RuntimeBytes
		require.Equal(t, want, b.Snapshot().Available)
		_, ok := b.TryReserve(want)
		require.True(t, ok)
		_, ok = b.TryReserve(1)
		require.False(t, ok)
		b.Release(want)
	}
	s.Used = s.SoftLimit
	_, ok := b.TryReserve(1)
	require.False(t, ok, "an unlimited hard limit must not bypass memory.high")
}

func TestCgroupCacheCreditRequiresObservedCleanCache(t *testing.T) {
	b, s := ciCgroupBudget(MemoryDomainSourceCgroupV2)
	s.Reclaimable = 0
	require.Zero(t, b.Snapshot().Available, "unknown, dirty or unreclaimable pages are not cache credit")
}

func TestControlProgressDoesNotDependOnNonGoReserveDeficit(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceHost, MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2} {
		t.Run(string(source), func(t *testing.T) {
			b, s := ciCgroupBudget(source)
			s.Reclaimable = 0
			_, ok := b.TryReserve(120)
			require.False(t, ok)
			lease, ok := b.AcquireControl()
			require.True(t, ok)
			defer lease.Close()
			_, ok = lease.TryReserve(120)
			require.True(t, ok, "bounded control work needs raw headroom, not the full non-Go reserve")
			require.Equal(t, uint64(120), b.Snapshot().ControlReserved)
			lease.Release(120)
			s.Used = s.AdmissionLimit()
			_, ok = lease.TryReserve(1)
			require.False(t, ok)
		})
	}
}

func TestControlKeepsRuntimeFloorAfterAllSlotsFill(t *testing.T) {
	b, s := ciCgroupBudget(MemoryDomainSourceCgroupV2)
	s.Used = s.Limit - b.policy.RuntimeBytes
	leases := make([]*MemoryControlLease, 0, memoryControlConcurrency)
	for range memoryControlConcurrency {
		lease, ok := b.AcquireControl()
		require.True(t, ok)
		_, ok = lease.TryReserve(lease.Limit())
		require.True(t, ok)
		leases = append(leases, lease)
	}
	require.Equal(t, memoryControlTotalBytes, b.Reserved())
	require.Equal(t, b.policy.RuntimeBytes-memoryControlTotalBytes, s.Limit-s.Used-b.Reserved())
	_, ok := b.TryReserve(1)
	require.False(t, ok)
	for _, lease := range leases {
		lease.Close()
		lease.Release(lease.Limit())
	}
	s.Used = s.Limit - (b.policy.RuntimeBytes - memoryControlTotalBytes)
	lease, ok := b.AcquireControl()
	require.True(t, ok)
	defer lease.Close()
	_, ok = lease.TryReserve(1)
	require.False(t, ok, "even a one-byte control request preserves the retained runtime floor")
}
