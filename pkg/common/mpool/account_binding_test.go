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

func TestAccountBinding(t *testing.T) {
	_, err := NewAccountBinding(nil, AllocationOwnerMin, AllocationSiteMin)
	require.Error(t, err)

	registry, account := newTestAllocationAccount(t, 1<<20, 64)
	binding, err := NewAccountBinding(account, AllocationOwnerMin, AllocationSiteMin)
	require.NoError(t, err)
	require.Same(t, account, binding.Account())

	// A nil binding leaves the pool unaccounted.
	mp := MustNewZero()
	var none *AccountBinding
	got, err := none.MP(mp)
	require.NoError(t, err)
	require.Same(t, mp, got)
	require.Nil(t, none.Account())

	// The view is built once per pool and charges the account.
	view, err := binding.MP(mp)
	require.NoError(t, err)
	require.Same(t, account, view.ViewAccount())
	again, err := binding.MP(mp)
	require.NoError(t, err)
	require.Same(t, view, again)
	data, err := view.Alloc(1024, true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, account.Snapshot().Used, uint64(1024))
	mp.Free(data)

	// A different pool gets its own view.
	other := MustNewZero()
	otherView, err := binding.MP(other)
	require.NoError(t, err)
	require.NotSame(t, view, otherView)
	require.Same(t, other, otherView.Base())

	finalizeTestAllocationAccount(t, registry, account)
}
