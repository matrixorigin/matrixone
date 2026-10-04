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

package message

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func testMp() *mpool.MPool {
	return mpool.MustNewZero()
}

func initTestGroupSels(
	t *testing.T,
	sels *GroupSels,
	n int,
	mp *mpool.MPool,
) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	require.NoError(t, sels.InitWithAllocation(n, mp, account, 1, 1))
	t.Cleanup(func() {
		sels.Free(mp)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
}

func TestGroupSels_NilBeforeInit(t *testing.T) {
	var js GroupSels
	require.NoError(t, js.Finalize(3, 3, testMp()))
	require.Nil(t, js.offsets)
	require.Nil(t, js.Get(0))
}

func TestGroupSels_AllUnique(t *testing.T) {
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 3, mp)
	js.Insert(0, 0)
	js.Insert(1, 1)
	js.Insert(2, 2)
	require.NoError(t, js.Finalize(3, 3, mp))
	require.Nil(t, js.offsets)
	require.Nil(t, js.Get(0))
}

func TestGroupSels_Normal0Based(t *testing.T) {
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 4, mp)
	js.Insert(0, 10)
	js.Insert(1, 11)
	js.Insert(0, 12)
	js.Insert(1, 13)
	require.NoError(t, js.Finalize(2, 4, mp))
	require.NotNil(t, js.offsets)
	require.ElementsMatch(t, []int32{10, 12}, js.Get(0))
	require.ElementsMatch(t, []int32{11, 13}, js.Get(1))
	require.Empty(t, js.Get(2))
	js.Free(mp)
}

func TestGroupSels_Dedup1Based(t *testing.T) {
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 3, mp)
	js.Insert(1, 0)
	js.Insert(2, 1)
	js.Insert(1, 2)
	require.NoError(t, js.Finalize(2, 3, mp))
	require.NotNil(t, js.offsets)
	require.ElementsMatch(t, []int32{0, 2}, js.Get(1))
	require.ElementsMatch(t, []int32{1}, js.Get(2))
	require.Empty(t, js.Get(0))
	js.Free(mp)
}

func TestGroupSels_Free(t *testing.T) {
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 2, mp)
	js.Insert(0, 5)
	js.Free(mp)
	require.NoError(t, js.Finalize(1, 1, mp))
	require.Nil(t, js.offsets)
}

func TestGroupSels_AllNulls(t *testing.T) {
	// all rows are null — Init called but Insert never called
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 3, mp)
	// no Insert calls
	require.NoError(t, js.Finalize(0, 3, mp))
	require.Nil(t, js.offsets)
}

func TestGroupSels_NullsSkipped(t *testing.T) {
	// 4 input rows but only 3 inserted (1 null) — groupCount==n but inputRowCount!=groupCount
	// must NOT trigger all-unique path
	mp := testMp()
	var js GroupSels
	initTestGroupSels(t, &js, 4, mp)
	js.Insert(0, 0) // row 0 → group 0
	js.Insert(1, 1) // row 1 → group 1
	// row 2 is null, skipped
	js.Insert(2, 3) // row 3 → group 2
	require.NoError(t, js.Finalize(3, 4, mp))
	require.NotNil(t, js.offsets) // must NOT be nil
	require.ElementsMatch(t, []int32{0}, js.Get(0))
	require.ElementsMatch(t, []int32{1}, js.Get(1))
	require.ElementsMatch(t, []int32{3}, js.Get(2))
	js.Free(mp)
}

func TestGroupSelsAllocationAccountLifecycleAndRollback(t *testing.T) {
	const (
		owner mpool.AllocationOwner = 1
		site  mpool.AllocationSite  = 30
	)
	for _, tc := range []struct {
		name          string
		limit         uint64
		metadataSlots uint64
		wantErr       error
	}{
		{name: "exact", limit: 64, metadataSlots: 3},
		{
			name:          "one byte short",
			limit:         63,
			metadataSlots: 3,
			wantErr:       mpool.ErrAllocationAccountCapacity,
		},
		{
			name:          "one metadata slot short",
			limit:         64,
			metadataSlots: 2,
			wantErr:       mpool.ErrAllocationMetadataSlots,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := mpool.NewAllocationAccountRegistry(1, tc.metadataSlots)
			require.NoError(t, err)
			account, err := registry.Open(tc.limit)
			require.NoError(t, err)
			mp := testMp()
			var sels GroupSels
			require.NoError(t, sels.InitWithAllocation(4, mp, account, owner, site))
			sels.Insert(0, 0)
			sels.Insert(0, 1)
			sels.Insert(1, 2)
			sels.Insert(1, 3)

			err = sels.Finalize(2, 4, mp)
			if tc.wantErr == nil {
				require.NoError(t, err)
				require.Equal(t, uint64(64), account.Snapshot().Peak)
				require.Equal(t, uint64(32), account.Snapshot().Used)
				require.ElementsMatch(t, []int32{0, 1}, sels.Get(0))
				require.ElementsMatch(t, []int32{2, 3}, sels.Get(1))
			} else {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, sels.offsets)
				require.Nil(t, sels.vals)
				require.NotNil(t, sels.tmp)
				require.Equal(t, uint64(32), account.Snapshot().Used)
			}
			sels.Free(mp)
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			snapshot, first, err := registry.CompleteTerminal(account)
			require.NoError(t, err)
			require.True(t, first)
			require.Equal(t, mpool.AllocationAccountTerminalValid, snapshot.State)
		})
	}
}
