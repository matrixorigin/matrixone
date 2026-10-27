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

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCopyWithReadonlyAreaSnapshotAndReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	destination := NewOffHeapVecWithType(types.New(types.T_varchar, 256, 0))
	defer func() {
		source.Free(mp)
		destination.Free(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()
	value := bytes.Repeat([]byte{'a'}, 4096)
	require.NoError(t, AppendBytes(source, value, false, mp))
	require.NoError(t, AppendBytes(source, []byte("inline"), false, mp))
	require.NoError(t, AppendBytes(source, nil, true, mp))
	source.GetGrouping().Add(1)
	require.NoError(t, source.SetPrepareParamKindsWithMP(
		[]PrepareParamKind{PrepareParamFloat, PrepareParamNone, PrepareParamNone}, mp,
	))
	require.NoError(t, source.SetIsBinaryStringAt(0, true, mp))

	require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
	require.Equal(t, types.New(types.T_varchar, 256, 0), *destination.GetType())
	require.False(t, source.HasBorrowedBacking(), "the mutable source must not be promoted")
	require.True(t, destination.HasBorrowedBacking())
	require.NotSame(t, &source.GetArea()[0], &destination.GetArea()[0])
	lease := destination.areaLease
	require.Equal(t, int64(cap(destination.GetArea())), lease.AccountedBytes())
	beforeView := mp.CurrNB()
	readonly, err := destination.RetainedReadonlyViewWithMP(mp)
	require.NoError(t, err)
	defer readonly.Free(mp)
	require.Same(t, &destination.GetArea()[0], &readonly.GetArea()[0])
	require.NotSame(t, &destination.GetData()[0], &readonly.GetData()[0])
	require.Less(t, mp.CurrNB()-beforeView, int64(len(value)), "a view must not allocate another payload")
	require.True(t, readonly.GetNulls().Contains(2))
	require.True(t, readonly.GetGrouping().Contains(1))
	require.Equal(t, PrepareParamFloat, readonly.GetPrepareParamKindAt(0))
	require.True(t, readonly.GetIsBinaryStringAt(0))
	readonly.GetNulls().Del(2)
	readonly.GetGrouping().Del(1)
	require.True(t, destination.GetNulls().Contains(2))
	require.True(t, destination.GetGrouping().Contains(1))

	writable, err := destination.RetainedReadonlyViewWithMP(mp)
	require.NoError(t, err)
	defer writable.Free(mp)
	require.NoError(t, writable.MaterializeOwned(mp))
	require.False(t, writable.HasBorrowedBacking())
	writable.GetBytesAt(0)[0] = 'w'
	require.Equal(t, value, destination.GetBytesAt(0))

	// Reusing the producing Vector must not move the old lease's cleanup
	// authority to the new generation or to the mutable source Vector.
	source.GetBytesAt(0)[0] = 's'
	destination.CleanOnlyData()
	require.NotNil(t, lease.Bytes())
	require.Equal(t, value, readonly.GetBytesAt(0))
	require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
	require.Equal(t, byte('s'), destination.GetBytesAt(0)[0])
	source.Free(mp)
	require.Equal(t, value, readonly.GetBytesAt(0))
	readonly.Free(mp)
	require.Nil(t, lease.Bytes(), "the last retained reference frees the old allocation")
	require.Equal(t, byte('w'), writable.GetBytesAt(0)[0])
	require.Equal(t, byte('s'), destination.GetBytesAt(0)[0])
}

func TestCopyWithReadonlyAreaAllocationModeAndInline(t *testing.T) {
	for _, offHeap := range []bool{false, true} {
		t.Run(strconv.FormatBool(offHeap), func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewVec(types.T_varchar.ToType())
			destination := NewVec(types.T_varchar.ToType())
			destination.SetOffHeap(offHeap)
			defer func() {
				source.Free(mp)
				destination.Free(mp)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			}()
			require.NoError(t, AppendBytes(source, bytes.Repeat([]byte{'a'}, 64), false, mp))
			require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
			require.Equal(t, offHeap, destination.offHeap, "allocation mode belongs to the destination, not the decoded source")
			require.True(t, destination.HasBorrowedBacking())
			destination.CleanOnlyData()
			source.CleanOnlyData()
			require.NoError(t, AppendBytes(source, []byte("inline"), false, mp))
			require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
			require.False(t, destination.HasBorrowedBacking())
			require.Equal(t, "inline", destination.GetStringAt(0))
		})
	}
}

func TestCopyWithReadonlyAreaAllocationFailures(t *testing.T) {
	for _, limit := range []uint64{128, 2048} {
		t.Run(strconv.FormatUint(limit, 10), func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, limit, 32)
			source := NewOffHeapVecWithType(types.T_varchar.ToType())
			destination := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
			defer func() {
				source.Free(mp)
				destination.Free(mp)
				require.Zero(t, mp.CurrNB())
				finalizeTestVectorAllocationAccount(t, state)
				mpool.DeleteMPool(mp)
			}()
			value := bytes.Repeat([]byte{'a'}, 1024)
			require.NoError(t, AppendBytes(source, value, false, mp))
			if limit == 128 {
				require.NoError(t, destination.PreExtendWithArea(1, 32, mp))
				before := state.account.Snapshot().Used
				beforeMP := mp.CurrNB()
				require.ErrorIs(t, destination.CopyWithReadonlyArea(source, mp), mpool.ErrAllocationAccountCapacity)
				require.Zero(t, destination.Length())
				require.False(t, destination.HasBorrowedBacking())
				require.Same(t, state.selection, destination.AllocationAccountSelection())
				require.Equal(t, before, state.account.Snapshot().Used)
				require.Equal(t, beforeMP, mp.CurrNB())
				require.Equal(t, value, source.GetBytesAt(0))
				return
			}

			require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
			require.Same(t, state.selection, destination.AllocationAccountSelection())
			view, err := destination.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			defer view.Free(mp)
			before := state.account.Snapshot().Used
			beforeMP := mp.CurrNB()
			require.ErrorIs(t, view.MaterializeOwned(mp), mpool.ErrAllocationAccountCapacity)
			require.True(t, view.HasBorrowedBacking())
			require.Equal(t, before, state.account.Snapshot().Used)
			require.Equal(t, beforeMP, mp.CurrNB())
			require.Equal(t, value, view.GetBytesAt(0))
			destination.CleanOnlyData()
			require.Equal(t, value, view.GetBytesAt(0))
		})
	}
}

func TestCopyWithReadonlyAreaRejectsInvalidDestination(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	destination := NewOffHeapVecWithType(types.T_varchar.ToType())
	fixed := NewOffHeapVecWithType(types.T_int64.ToType())
	wrong := NewOffHeapVecWithType(types.T_blob.ToType())
	defer func() {
		source.Free(mp)
		destination.Free(mp)
		fixed.Free(mp)
		wrong.Free(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte{'s'}, 64), false, mp))
	require.Error(t, (*Vector)(nil).CopyWithReadonlyArea(source, mp))
	require.Error(t, destination.CopyWithReadonlyArea(nil, mp))
	require.Error(t, destination.CopyWithReadonlyArea(source, nil))
	require.Error(t, destination.CopyWithReadonlyArea(destination, mp))
	require.Error(t, fixed.CopyWithReadonlyArea(source, mp))
	require.Error(t, wrong.CopyWithReadonlyArea(source, mp))
	require.NoError(t, AppendBytes(destination, []byte("already published"), false, mp))
	require.Error(t, destination.CopyWithReadonlyArea(source, mp))
	require.Equal(t, "already published", destination.GetStringAt(0))
	require.False(t, destination.HasBorrowedBacking())
}
