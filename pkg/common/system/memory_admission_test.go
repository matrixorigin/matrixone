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

func TestDefaultMemoryDomainReservePolicyAcceptsSizeSuffixes(t *testing.T) {
	t.Setenv("MO_MEMORY_NON_GO_RESERVE_BYTES", "2GiB")
	t.Setenv("MO_MEMORY_RUNTIME_RESERVE_BYTES", "128MiB")
	policy := DefaultMemoryDomainReservePolicy()
	require.Equal(t, uint64(2<<30), policy.MinimumBytes)
	require.Equal(t, uint64(128<<20), policy.RuntimeBytes)
}
