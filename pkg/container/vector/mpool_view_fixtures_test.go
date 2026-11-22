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

package vector

// Shared fixtures for tests that allocate vectors through an accounted MPool
// view. Accounting belongs to mpool; the vector itself carries no account.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const (
	testVectorAllocationOwner    mpool.AllocationOwner = 1
	testVectorDataAllocationSite mpool.AllocationSite  = 1
)

type testVectorAllocationAccount struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
	// class is the capacity class of the views state.view returns.
	class mpool.AllocationCapacityClass
}

// view returns an accounted view of mp that charges the test account.
func (s testVectorAllocationAccount) view(t testing.TB, mp *mpool.MPool) *mpool.MPool {
	t.Helper()
	view, err := mp.WithAccount(
		s.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		s.class,
	)
	require.NoError(t, err)
	return view
}

type rejectNextVectorAllocation struct {
	calls    int
	failAt   int
	rejected bool
	used     uint64
}

func (c *rejectNextVectorAllocation) AcquireAllocationCapacity(size uint64) error {
	c.calls++
	if c.failAt != 0 && c.calls == c.failAt {
		c.rejected = true
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	return nil
}

func (c *rejectNextVectorAllocation) ReleaseAllocationCapacity(size uint64) {
	if c.used < size {
		panic("vector allocation controller release underflow")
	}
	c.used -= size
}

func newTestVectorAllocationAccount(
	t testing.TB,
	limit uint64,
	allocationSlots uint64,
) testVectorAllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, allocationSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	return testVectorAllocationAccount{
		registry: registry,
		account:  account,
	}
}

func finalizeTestVectorAllocationAccount(
	t testing.TB,
	state testVectorAllocationAccount,
) {
	t.Helper()
	snapshot := state.account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	_, err := state.registry.Finalize(state.account)
	require.NoError(t, err)
}

// newAccountedTestVector returns an empty vector for a test that allocates it
// through an accounted view (state.view). The vector itself carries no account.
func newAccountedTestVector(t testing.TB, typ types.Type) *Vector {
	t.Helper()
	return NewOffHeapVecWithType(typ)
}
