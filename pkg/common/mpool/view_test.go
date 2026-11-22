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

package mpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMPoolAccountedView(t *testing.T) {
	registry, account := newTestAllocationAccount(t, 1<<20, 64)
	mp := MustNewZero()

	view, err := mp.WithAccount(account, AllocationOwnerMin, AllocationSiteMin, AllocationCapacityClassDefault)
	require.NoError(t, err)
	require.True(t, view.IsView())
	require.False(t, mp.IsView())
	require.Same(t, mp, view.Base())
	require.Same(t, account, view.ViewAccount())

	// Fresh off-heap allocations through the view are charged to the account.
	data, err := view.Alloc(4096, true)
	require.NoError(t, err)
	used := account.Snapshot().Used
	require.GreaterOrEqual(t, used, uint64(4096))

	// Growth through the base pool keeps the allocation's own account.
	data, err = mp.Grow(data, 8192, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, account.Snapshot().Used, uint64(8192))

	// On-heap allocations through the view are not accounted.
	before := account.Snapshot().Used
	heap, err := view.Alloc(1024, false)
	require.NoError(t, err)
	require.Equal(t, before, account.Snapshot().Used)
	view.Free(heap)

	// Generic helpers route through the view too.
	slice, err := MakeSlice[int64](128, view, true)
	require.NoError(t, err)
	require.Greater(t, account.Snapshot().Used, before)
	FreeSlice(view, slice)

	// Release through the base pool returns the charge.
	mp.Free(data)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())

	// Deleting a view leaves the pool alive.
	DeleteMPool(view)
	again, err := mp.Alloc(16, true)
	require.NoError(t, err)
	mp.Free(again)

	finalizeTestAllocationAccount(t, registry, account)
}

func TestMPoolAccountedViewRejectsInvalidRequest(t *testing.T) {
	mp := MustNewZero()
	_, err := mp.WithAccount(nil, AllocationOwnerMin, AllocationSiteMin, AllocationCapacityClassDefault)
	require.Error(t, err)
	_, err = mp.WithAdmission(nil)
	require.Error(t, err)
	var nilPool *MPool
	_, err = nilPool.WithAccount(nil, AllocationOwnerMin, AllocationSiteMin, AllocationCapacityClassDefault)
	require.Error(t, err)
	require.False(t, nilPool.IsView())
	require.Nil(t, nilPool.ViewAccount())
}

func TestMPoolAdmissionView(t *testing.T) {
	mp := MustNewZero()
	admitted, released := 0, 0
	view, err := mp.WithAdmission(func(size uint64) (func(), error) {
		admitted++
		return func() { released++ }, nil
	})
	require.NoError(t, err)
	data, err := view.Alloc(256, true)
	require.NoError(t, err)
	require.Equal(t, 1, admitted)
	mp.Free(data)
	require.Equal(t, 1, released)
	require.Zero(t, mp.CurrNB())
}
