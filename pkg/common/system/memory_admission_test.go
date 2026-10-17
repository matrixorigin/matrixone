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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryDomainReservePolicyUsesObservedNonGoHighWater(t *testing.T) {
	policy := MemoryDomainReservePolicy{
		MinimumBytes:            10,
		RuntimeBytes:            5,
		ObservedNonGoMultiplier: 2,
	}
	first := MemoryDomainSnapshot{NonGo: 8}
	require.Equal(t, uint64(21), policy.Reserve(first))
	second := MemoryDomainSnapshot{NonGo: 20}
	require.Equal(t, uint64(45), policy.Reserve(second))
}

func TestMemoryAdmissionBudgetChargesPendingReservationsCumulatively(t *testing.T) {
	budget := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{
		MinimumBytes:            0,
		ObservedNonGoMultiplier: 1,
	})
	snapshot := MemoryDomainSnapshot{Used: 90, Limit: 100, Measured: true}

	available, ok := budget.tryReserve(snapshot, 4)
	require.True(t, ok)
	require.Equal(t, uint64(6), available)
	refreshed := budget.Observe(MemoryDomainSnapshot{Used: 94, Limit: 100, Measured: true})
	require.Equal(t, uint64(2), refreshed.Available,
		"a refresh must retain the pending claim against the new sample")
	available, ok = budget.tryReserve(snapshot, 4)
	require.True(t, ok)
	require.Equal(t, uint64(2), available)
	available, ok = budget.tryReserve(snapshot, 4)
	require.False(t, ok)
	require.Equal(t, uint64(2), available)
	require.Equal(t, uint64(8), budget.Reserved())

	// A release returns the pending claim to the same sampled budget.
	budget.Release(4)
	available, ok = budget.tryReserve(snapshot, 4)
	require.True(t, ok)
	require.Equal(t, uint64(2), available)
}

func TestMemoryAdmissionBudgetConcurrentReservationsShareHeadroom(t *testing.T) {
	budget := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{
		ObservedNonGoMultiplier: 1,
	})
	snapshot := MemoryDomainSnapshot{Used: 90, Limit: 100, Measured: true}
	const workers = 16
	var wg sync.WaitGroup
	var successful int
	var mu sync.Mutex
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, ok := budget.tryReserve(snapshot, 4)
			if ok {
				mu.Lock()
				successful++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 2, successful)
	require.Equal(t, uint64(8), budget.Reserved())
}

func TestMemoryAdmissionBudgetUsesDomainSoftLimit(t *testing.T) {
	budget := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{
		ObservedNonGoMultiplier: 1,
	})
	snapshot := MemoryDomainSnapshot{
		Used: 60, Limit: 100, SoftLimit: 80, Measured: true,
	}
	budget.mu.Lock()
	require.Equal(t, uint64(20), budget.availableLocked(snapshot))
	snapshot.Limit = math.MaxUint64
	require.Equal(t, uint64(20), budget.availableLocked(snapshot),
		"an unlimited hard limit must still honor a finite soft limit")
	budget.mu.Unlock()
}

func TestHostMemorySoftLimitIsConfigurable(t *testing.T) {
	t.Setenv("MO_MEMORY_HOST_SOFT_LIMIT", "8GiB")
	require.Equal(t, uint64(8<<30), hostMemorySoftLimit(10<<30))
	require.Zero(t, hostMemorySoftLimit(4<<30))
}

func TestReservePolicyUsesMoreConservativeHostProfile(t *testing.T) {
	policy := DefaultMemoryDomainReservePolicy()
	cgroup := policy.Reserve(MemoryDomainSnapshot{
		Limit: 10 << 30, Source: MemoryDomainSourceCgroupV2,
	})
	host := policy.Reserve(MemoryDomainSnapshot{
		Limit: 10 << 30, Source: MemoryDomainSourceHost,
	})
	require.Greater(t, host, cgroup)
}

func TestMemoryAdmissionBudgetAllowsZeroByteRequestWithoutMeasurement(t *testing.T) {
	budget := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{})
	_, ok := budget.TryReserve(0)
	require.True(t, ok)
}

func TestMemoryAdmissionBudgetRecoversAfterNonGoPeak(t *testing.T) {
	for _, source := range []MemoryDomainSource{MemoryDomainSourceHost, MemoryDomainSourceCgroupV1, MemoryDomainSourceCgroupV2} {
		t.Run(string(source), func(t *testing.T) {
			const gib = uint64(1 << 30)
			policy := DefaultMemoryDomainReservePolicy()
			sample := MemoryDomainSnapshot{Source: source, Used: gib, Limit: 16 * gib, NonGo: 64 << 20, Measured: true}
			b := NewMemoryAdmissionBudgetWithProvider(policy, func() (MemoryDomainSnapshot, error) { return sample, nil })
			_, ok := b.TryReserve(gib)
			require.True(t, ok)
			sample.Used, sample.NonGo = 12*gib, 8*gib
			peak := b.Snapshot()
			require.Zero(t, peak.Available)
			require.Greater(t, peak.Reserve, sample.Limit)

			sample.Used, sample.NonGo = gib, 64<<20
			for range 10 {
				recovered := b.Snapshot()
				require.Equal(t, policy.Reserve(sample), recovered.Reserve)
				require.Equal(t, gib, recovered.Reserved, "recalibration must not discard a live claim")
				require.Positive(t, recovered.Available)
			}
			_, entry, err := b.EnterStatement(t.Context(), false)
			require.NoError(t, err, "ordinary statement entry must recover too")
			entry.Close()
			_, ok = b.TryReserve(4096)
			require.True(t, ok)
			b.Release(4096)

			// Recovery never permits charged usage/claims to cross a lowered
			// soft boundary, including when cgroup clean-cache credit is large.
			sample.SoftLimit = 2 * gib
			if sample.IsCgroup() {
				sample.Reclaimable = sample.Used
			}
			_, ok = b.TryReserve(1)
			require.False(t, ok)
			require.Equal(t, gib, b.Reserved())
			b.Release(gib)
			require.Zero(t, b.Reserved())
		})
	}
}

func TestMemoryAdmissionBudgetPreservesCalibrationUntilHealthy(t *testing.T) {
	const gib = uint64(1 << 30)
	sample := MemoryDomainSnapshot{Source: MemoryDomainSourceCgroupV2, Used: 12 * gib, Limit: 16 * gib, NonGo: 8 * gib, Measured: true}
	b := NewMemoryAdmissionBudgetWithProvider(DefaultMemoryDomainReservePolicy(), func() (MemoryDomainSnapshot, error) { return sample, nil })
	peak := b.Snapshot()
	// A smaller current non-Go value alone is not evidence that the physical
	// domain has recovered. Keep the peak while the working set is pressured.
	sample.Used, sample.NonGo = 15*gib, 64<<20
	for range 10 {
		require.Equal(t, peak.Reserve, b.Snapshot().Reserve)
		require.Zero(t, b.Snapshot().Available)
	}
	sample.Used = gib
	require.Positive(t, b.Snapshot().Available)

	// Small, still-useful calibration peaks are retained, not reset on every
	// low observation or every Snapshot/Release call.
	sample.NonGo = gib
	smallPeak := b.Snapshot().Reserve
	sample.NonGo = 64 << 20
	require.Equal(t, smallPeak, b.Snapshot().Reserve)
}

func TestMemoryAdmissionRecoveryMarginIncludesLiveClaims(t *testing.T) {
	const gib = uint64(1 << 30)
	sample := MemoryDomainSnapshot{Source: MemoryDomainSourceCgroupV2, Used: gib, Limit: 16 * gib, NonGo: 64 << 20, Measured: true}
	b := NewMemoryAdmissionBudgetWithProvider(DefaultMemoryDomainReservePolicy(), func() (MemoryDomainSnapshot, error) { return sample, nil })
	_, ok := b.TryReserve(13 * gib)
	require.True(t, ok)
	sample.Used, sample.NonGo = 12*gib, 8*gib
	peak := b.Snapshot()
	sample.Used, sample.NonGo = gib, 64<<20
	// The sample alone looks healthy, but the pending claims consume its spare
	// margin. Neither repeated observations nor control/calibration reads may
	// silently discard these commitments or reinterpret them as residency.
	for range 10 {
		s := b.Snapshot()
		require.Equal(t, peak.Reserve, s.Reserve)
		require.Zero(t, s.Available)
		require.Equal(t, 13*gib, s.Reserved)
	}
	b.Release(12 * gib)
	recovered := b.Snapshot()
	require.Positive(t, recovered.Available)
	require.Equal(t, gib, recovered.Reserved)
	b.Release(gib)
	require.Zero(t, b.Reserved())
}

func TestMemoryAdmissionRecoveryRejectsOutOfOrderObservation(t *testing.T) {
	const gib = uint64(1 << 30)
	peak := MemoryDomainSnapshot{Source: MemoryDomainSourceCgroupV2, Used: 12 * gib, Limit: 16 * gib, NonGo: 8 * gib,
		Measured: true, ObservedAt: time.Now()}
	b := NewMemoryAdmissionBudget(DefaultMemoryDomainReservePolicy())
	before := b.Observe(peak)
	stale := peak
	stale.Used, stale.NonGo, stale.ObservedAt = gib, 64<<20, peak.ObservedAt.Add(-time.Second)
	s := b.Observe(stale)
	require.Equal(t, before.Reserve, s.Reserve, "a delayed old reader cannot undo a newer peak")
	require.Zero(t, s.Available)
	stale.ObservedAt = time.Time{}
	require.Zero(t, b.Observe(stale).Available, "an undated observation cannot undo a dated peak")
	stale.ObservedAt = peak.ObservedAt.Add(time.Second)
	require.Positive(t, b.Observe(stale).Available, "a genuinely later healthy observation must recover")
}

func TestDefaultMemoryDomainReservePolicyAcceptsSizeSuffixes(t *testing.T) {
	t.Setenv("MO_MEMORY_NON_GO_RESERVE_BYTES", "2GiB")
	t.Setenv("MO_MEMORY_RUNTIME_RESERVE_BYTES", "128MiB")
	policy := DefaultMemoryDomainReservePolicy()
	require.Equal(t, uint64(2<<30), policy.MinimumBytes)
	require.Equal(t, uint64(128<<20), policy.RuntimeBytes)
}
