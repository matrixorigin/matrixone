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

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapacityReservationCommitAndRelease(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 1024, 1)
	reservation, err := account.ReserveCapacity(800, AllocationOwnerExternal, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(800), account.Snapshot().Used)

	lease, err := reservation.Commit(512)
	require.NoError(t, err)
	require.Equal(t, uint64(512), lease.Capacity())
	require.Equal(t, uint64(512), account.Snapshot().Used)
	lease.Release()
	lease.Release()
	require.Zero(t, account.Snapshot().Used)
	finalizeTestAllocationAccount(t, registry, account)
}

func TestCapacityReservationAbortAndOversizedCommit(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 1024, 1)
	reservation, err := account.ReserveCapacity(128, AllocationOwnerExternal, 2)
	require.NoError(t, err)
	_, err = reservation.Commit(129)
	require.ErrorIs(t, err, ErrAllocationAccountCapacity)
	require.Equal(t, uint64(128), account.Snapshot().Used)
	reservation.Abort()
	reservation.Abort()
	require.Zero(t, account.Snapshot().Used)
	finalizeTestAllocationAccount(t, registry, account)
}

func TestCapacityReservationCommitAbortRace(t *testing.T) {
	for range 100 {
		registry, account := newTestAllocationAccount(t, 16, 1)
		reservation, err := account.ReserveCapacity(16, AllocationOwnerExternal, 3)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		var lease *CapacityLease
		go func() {
			defer wg.Done()
			lease, _ = reservation.Commit(8)
		}()
		go func() {
			defer wg.Done()
			reservation.Abort()
		}()
		wg.Wait()
		if lease != nil {
			lease.Release()
		}
		require.Zero(t, account.Snapshot().Used)
		finalizeTestAllocationAccount(t, registry, account)
	}
}

func TestCapacityReservationRejectsAfterSeal(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 16, 1)
	account.Seal()
	_, err := account.ReserveCapacity(1, AllocationOwnerExternal, 1)
	require.ErrorIs(t, err, ErrAllocationAccountSealed)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}
