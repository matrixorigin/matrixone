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

func TestMemoryAdmissionBudgetDoesNotDoubleChargeMeasuredAllocations(t *testing.T) {
	budget := NewMemoryAdmissionBudget(MemoryDomainReservePolicy{
		MinimumBytes:            10,
		ObservedNonGoMultiplier: 1,
	})
	snapshot := MemoryDomainSnapshot{Used: 60, Limit: 100, NonGo: 20, Measured: true}
	budget.mu.Lock()
	budget.reserved = 30
	require.Equal(t, uint64(20), budget.availableLocked(snapshot))
	budget.mu.Unlock()

	// The physical sample already includes the outstanding 30-byte claim. The
	// shared ledger therefore keeps 20 bytes, rather than subtracting the claim
	// a second time and incorrectly reporting only 10 bytes.
	view := budget.snapshotLocked(snapshot, 20)
	require.Equal(t, uint64(20), view.Available)
	require.Equal(t, uint64(30), view.Reserved)
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
