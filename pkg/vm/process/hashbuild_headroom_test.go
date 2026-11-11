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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHashBuildHeadroomRetirementAndPromises(t *testing.T) {
	b := MustNewHashBuildBudget(1000, 1000)
	g, err := b.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := g.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(1000, g)
	require.NoError(t, err)
	reserve, err := account.ReserveCapacity(100, mpool.AllocationOwnerTxnWorkspace, 1)
	require.NoError(t, err)
	lease, err := reserve.Commit(100)
	require.NoError(t, err)
	calls := 0
	now := time.Unix(100, 0)
	b.memoryHeadroomProvider = func() (uint64, bool) { calls++; return 200, true }
	b.memoryHeadroomTTL = time.Second
	b.memoryHeadroomNow = func() time.Time { return now }
	b.memoryHeadroomSafety = 10
	require.NoError(t, g.AcquireAllocationCapacity(100))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, g.AcquireAllocationCapacity(91), ErrHashBuildBudgetAdmission)
	lease.Release()
	// Retired pre-sample backing cannot be replaced by a post-sample lease.
	replacement, err := account.ReserveCapacity(80, mpool.AllocationOwnerTxnWorkspace, 1)
	require.NoError(t, err)
	next, err := replacement.Commit(80)
	require.NoError(t, err)
	require.ErrorIs(t, g.AcquireAllocationCapacity(11), ErrHashBuildBudgetAdmission)
	next.Release()
	g.ReleaseAllocationCapacity(100)
	now = now.Add(time.Second)
	require.NoError(t, g.AcquireAllocationCapacity(190))
	require.Equal(t, 2, calls)
	g.ReleaseAllocationCapacity(190)
	require.Zero(t, b.AggregateUsed())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	g.Close()
}
func TestHashBuildHeadroomRecoveryBorrowAndUnavailable(t *testing.T) {
	b := MustNewHashBuildBudget(1000, 1000)
	g, err := b.OpenGeneration(1)
	require.NoError(t, err)
	recovery, err := NewHashBuildRecoveryCapacity(g)
	require.NoError(t, err)
	require.NoError(t, recovery.EnsureCapacity(100))
	b.memoryHeadroomProvider = func() (uint64, bool) { return 0, true }
	b.memoryHeadroomTTL = time.Second
	require.NoError(t, recovery.AcquireAllocationCapacity(100))
	require.Equal(t, uint64(100), g.Used())
	require.ErrorIs(t, recovery.AcquireAllocationCapacity(1), ErrHashBuildBudgetAdmission)
	recovery.ReleaseAllocationCapacity(100)
	require.NoError(t, recovery.Close())
	g.Close()
	b = MustNewHashBuildBudget(1000, 1000)
	g, err = b.OpenGeneration(2)
	require.NoError(t, err)
	b.memoryHeadroomProvider = func() (uint64, bool) { return 0, false }
	require.NoError(t, g.AcquireAllocationCapacity(1000))
	g.ReleaseAllocationCapacity(1000)
	g.Close()
}
