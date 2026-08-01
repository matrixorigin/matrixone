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
	"errors"
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const (
	testVectorAllocationOwner     mpool.AllocationOwner = 1
	testVectorDataAllocationSite  mpool.AllocationSite  = 1
	testVectorAreaAllocationSite  mpool.AllocationSite  = 2
	testVectorNullAllocationSite  mpool.AllocationSite  = 3
	testVectorGroupAllocationSite mpool.AllocationSite  = 4
)

type testVectorAllocationAccount struct {
	registry  *mpool.AllocationAccountRegistry
	account   *mpool.AllocationAccount
	selection *AllocationAccountSelection
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
	selection, err := NewAllocationAccountSelection(
		account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		testVectorAreaAllocationSite,
		testVectorNullAllocationSite,
		testVectorGroupAllocationSite,
	)
	require.NoError(t, err)
	return testVectorAllocationAccount{
		registry:  registry,
		account:   account,
		selection: selection,
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

func newAccountedTestVector(
	t testing.TB,
	typ types.Type,
	selection *AllocationAccountSelection,
) *Vector {
	t.Helper()
	vec := NewOffHeapVecWithType(typ)
	require.NoError(t, vec.SetAllocationAccount(selection))
	return vec
}

func TestVectorAllocationAccountConfiguration(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()

	_, err := NewAllocationAccountSelection(
		nil,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		testVectorAreaAllocationSite,
		testVectorNullAllocationSite,
		testVectorGroupAllocationSite,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	onHeap := NewVec(types.T_int64.ToType())
	require.ErrorIs(
		t,
		onHeap.SetAllocationAccount(state.selection),
		mpool.ErrAllocationAccountInvalid,
	)

	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.Same(t, state.selection, vec.AllocationAccountSelection())
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	require.ErrorIs(
		t,
		vec.SetAllocationAccount(nil),
		mpool.ErrAllocationAccountInvalid,
	)
	require.Panics(t, func() {
		vec.SetOffHeap(false)
	})
	sourceUsed := state.account.Snapshot().Used
	dup, err := vec.Dup(mp)
	require.NoError(t, err)
	require.Same(t, state.selection, dup.AllocationAccountSelection())
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	dup.Free(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	_, err = vec.CloneToFlatCompact(mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	vec.Free(mp)
	require.Nil(t, vec.AllocationAccountSelection())
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountFixedResetReuseAndFree(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)

	require.NoError(t, vec.PreExtend(128, mp))
	initial := state.account.Snapshot()
	require.Equal(t, uint64(cap(vec.data)), initial.Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	for i := 0; i < 64; i++ {
		require.NoError(t, AppendFixed(vec, int64(i), false, mp))
	}
	require.Equal(t, initial.Used, state.account.Snapshot().Used)

	vec.ResetWithSameType()
	require.Equal(t, initial.Used, state.account.Snapshot().Used)
	for i := 0; i < 64; i++ {
		require.NoError(t, AppendFixed(vec, int64(i*2), false, mp))
	}
	require.Equal(t, initial.Used, state.account.Snapshot().Used)

	growAt := vec.Capacity() + 1
	for vec.Length() < growAt {
		require.NoError(t, AppendFixed(vec, int64(vec.Length()), false, mp))
	}
	grown := state.account.Snapshot()
	require.Equal(t, uint64(cap(vec.data)), grown.Used)
	require.Greater(t, grown.Peak, grown.Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	require.NoError(t, vec.Shuffle([]int64{0, 2, 4, 6}, mp))
	shuffled := state.account.Snapshot()
	require.Equal(t, uint64(cap(vec.data)), shuffled.Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	vec.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountVarlenaDataAndArea(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)

	require.NoError(t, vec.PreExtendWithArea(64, 4096, mp))
	initial := state.account.Snapshot()
	require.Equal(t, uint64(cap(vec.data)+cap(vec.area)), initial.Used)
	require.Equal(t, uint64(2), state.registry.LiveAllocationMetadata())

	for i := 0; i < 32; i++ {
		require.NoError(t, AppendBytes(vec, bytes.Repeat([]byte{byte(i)}, 64), false, mp))
	}
	require.Equal(t, initial.Used, state.account.Snapshot().Used)

	vec.ResetWithSameType()
	require.Equal(t, initial.Used, state.account.Snapshot().Used)
	require.NoError(t, AppendBytes(vec, bytes.Repeat([]byte("r"), 128), false, mp))
	require.Equal(t, initial.Used, state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapResetReuseAndFree(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)

	require.NoError(t, vec.PreExtend(32*1024, mp))
	require.NoError(t, vec.PreExtendBitmap(32*1024, mp))
	vec.SetLength(32 * 1024)
	vec.SetAllNulls(32 * 1024)
	vec.GetGrouping().AddRange(0, 32*1024)
	require.True(t, vec.nsp.GetBitmap().HasExternalStorage())
	require.True(t, vec.gsp.GetBitmap().HasExternalStorage())
	require.Equal(t, 32*1024, vec.GetNulls().Count())
	require.Equal(t, 32*1024, vec.GetGrouping().Count())

	initial := state.account.Snapshot()
	expected := cap(vec.data) +
		8*vec.nsp.GetBitmap().ExternalStorageCapacity() +
		8*vec.gsp.GetBitmap().ExternalStorageCapacity()
	require.Equal(t, uint64(expected), initial.Used)
	require.Equal(t, uint64(3), state.registry.LiveAllocationMetadata())

	vec.ResetWithSameType()
	require.True(t, vec.GetNulls().IsEmpty())
	require.True(t, vec.GetGrouping().IsEmpty())
	require.Equal(t, initial.Used, state.account.Snapshot().Used)
	require.NoError(t, vec.PreExtend(32*1024, mp))
	vec.SetNull(32*1024 - 1)
	require.True(t, vec.IsNull(32*1024-1))
	require.Equal(t, initial.Used, state.account.Snapshot().Used)

	vec.ResetWithSameType()
	require.NoError(t, vec.PreExtend(64*1024, mp))
	require.NoError(t, vec.PreExtendBitmap(64*1024, mp))
	grown := state.account.Snapshot()
	require.Greater(t, grown.Used, initial.Used)
	require.Greater(t, grown.Peak, grown.Used)
	require.Equal(t, uint64(3), state.registry.LiveAllocationMetadata())

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapShrinkUsesNoScratch(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(130, mp))
	require.NoError(t, vec.PreExtendBitmap(130, mp))
	for i := range 130 {
		require.NoError(t, AppendFixed(vec, int64(i), false, mp))
	}
	for _, row := range []uint64{0, 2, 63, 64, 129} {
		vec.SetNull(row)
	}
	for _, row := range []uint64{1, 65, 128} {
		vec.GetGrouping().Add(row)
	}
	before := state.account.Snapshot()

	vec.Shrink([]int64{0, 2, 64, 65, 129}, false)
	require.Equal(t, []int64{0, 2, 64, 65, 129}, MustFixedColWithTypeCheck[int64](vec))
	for _, row := range []uint64{0, 1, 2, 4} {
		require.True(t, vec.IsNull(row))
	}
	require.Equal(t, 4, vec.GetNulls().Count())
	require.True(t, vec.GetGrouping().Contains(3))
	require.Equal(t, 1, vec.GetGrouping().Count())
	after := state.account.Snapshot()
	require.Equal(t, before.Used, after.Used)
	require.Equal(t, before.Peak, after.Peak)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapShuffleAccountsScratch(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(130, mp))
	require.NoError(t, vec.PreExtendBitmap(130, mp))
	for i := range 130 {
		require.NoError(t, AppendFixed(vec, int64(i), false, mp))
	}
	for _, row := range []uint64{1, 64, 129} {
		vec.SetNull(row)
	}
	for _, row := range []uint64{2, 63, 128} {
		vec.GetGrouping().Add(row)
	}
	before := state.account.Snapshot()

	require.NoError(t, vec.Shuffle([]int64{129, 1, 64, 1, 2, 128, 63}, mp))
	require.Equal(t, []int64{129, 1, 64, 1, 2, 128, 63}, MustFixedColWithTypeCheck[int64](vec))
	for _, row := range []uint64{0, 1, 2, 3} {
		require.True(t, vec.IsNull(row))
	}
	require.Equal(t, 4, vec.GetNulls().Count())
	for _, row := range []uint64{4, 5, 6} {
		require.True(t, vec.GetGrouping().Contains(row))
	}
	require.Equal(t, 3, vec.GetGrouping().Count())
	after := state.account.Snapshot()
	require.Greater(t, after.Peak, before.Peak)
	require.Equal(t, uint64(3), state.registry.LiveAllocationMetadata())

	var goScratch []byte
	require.NoError(t, vec.ShuffleWithBuf([]int64{6, 5, 4, 3, 2, 1, 0}, mp, &goScratch))
	require.Nil(t, goScratch)
	require.Equal(t, []int64{63, 128, 2, 1, 64, 1, 129}, MustFixedColWithTypeCheck[int64](vec))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapShuffleFailurePreservesVector(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 4)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(130, mp))
	require.NoError(t, vec.PreExtendBitmap(130, mp))
	for i := range 130 {
		require.NoError(t, AppendFixed(vec, int64(i), false, mp))
	}
	vec.SetNull(1)
	vec.GetGrouping().Add(2)
	before := state.account.Snapshot()

	err := vec.Shuffle([]int64{2, 1, 0}, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Equal(t, before.Used, state.account.Snapshot().Used)
	require.Equal(t, uint64(3), state.registry.LiveAllocationMetadata())
	require.Equal(t, 130, vec.Length())
	require.True(t, vec.IsNull(1))
	require.True(t, vec.GetGrouping().Contains(2))
	require.Equal(t, int64(0), MustFixedColWithTypeCheck[int64](vec)[0])
	require.Equal(t, int64(129), MustFixedColWithTypeCheck[int64](vec)[129])

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapGrowthFailurePreservesOwner(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1000, 8)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(64, mp))
	require.NoError(t, vec.PreExtendBitmap(64, mp))
	vec.SetLength(64)
	vec.SetNull(7)
	vec.GetGrouping().Add(9)

	used := state.account.Snapshot().Used
	dataCapacity := cap(vec.data)
	nullCapacity := vec.nsp.GetBitmap().ExternalStorageCapacity()
	groupCapacity := vec.gsp.GetBitmap().ExternalStorageCapacity()
	// The null replacement fits by itself, but admitting the grouping
	// replacement would exceed the account. Neither replacement is published.
	err := vec.PreExtendBitmap(2*1024, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, used, state.account.Snapshot().Used)
	require.Equal(t, dataCapacity, cap(vec.data))
	require.Equal(t, nullCapacity, vec.nsp.GetBitmap().ExternalStorageCapacity())
	require.Equal(t, groupCapacity, vec.gsp.GetBitmap().ExternalStorageCapacity())
	require.True(t, vec.IsNull(7))
	require.True(t, vec.GetGrouping().Contains(9))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapRejectsUnadmittedRawGrowth(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	unaccounted := NewOffHeapVecWithType(types.T_int64.ToType())
	unaccounted.GetNulls().Add(0)
	require.ErrorIs(
		t,
		unaccounted.SetAllocationAccount(state.selection),
		mpool.ErrAllocationAccountInvalid,
	)
	unaccounted.Free(mp)

	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.Panics(t, func() {
		vec.GetNulls().Add(0)
	})
	require.Zero(t, state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapCopyDecode(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 32)
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_int64.ToType())
	for i := 0; i < 128; i++ {
		require.NoError(t, AppendFixed(source, int64(i), i%7 == 0, mp))
	}
	source.GetGrouping().Add(2, 9, 64)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)

	copied := newAccountedTestVector(
		t,
		types.T_int64.ToType(),
		state.selection,
	)
	require.NoError(t, copied.UnmarshalBinaryWithCopy(encoded, mp))
	require.Equal(t, source.Length(), copied.Length())
	require.True(t, copied.GetNulls().IsSame(source.GetNulls()))
	require.True(t, copied.nsp.GetBitmap().HasExternalStorage())
	require.True(t, copied.gsp.GetBitmap().HasExternalStorage())
	copied.Free(mp)

	fromReader := newAccountedTestVector(
		t,
		types.T_int64.ToType(),
		state.selection,
	)
	require.NoError(t, fromReader.UnmarshalWithReader(bytes.NewReader(encoded), mp))
	require.Equal(t, source.Length(), fromReader.Length())
	require.True(t, fromReader.GetNulls().IsSame(source.GetNulls()))
	require.True(t, fromReader.nsp.GetBitmap().HasExternalStorage())
	fromReader.Free(mp)

	duplicate, err := source.DupOffHeapWithAllocation(mp, state.selection)
	require.NoError(t, err)
	require.True(t, duplicate.GetNulls().IsSame(source.GetNulls()))
	require.True(t, duplicate.GetGrouping().IsSame(source.GetGrouping()))
	duplicate.Free(mp)

	window, err := source.CloneWindowWithAllocation(
		1,
		65,
		mp,
		state.selection,
	)
	require.NoError(t, err)
	require.Equal(t, 64, window.Length())
	require.True(t, window.IsNull(6))
	require.True(t, window.IsNull(13))
	window.Free(mp)

	rollup := NewOffHeapVecWithType(types.T_int64.ToType())
	rollup.SetLength(128)
	rollup.GetGrouping().AddRange(0, 128)
	rollup.ToConst()
	rollupCopy, err := rollup.DupOffHeapWithAllocation(mp, state.selection)
	require.NoError(t, err)
	require.True(t, rollupCopy.IsConstNull())
	require.True(t, rollupCopy.GetGrouping().IsSame(rollup.GetGrouping()))
	rollupCopy.Free(mp)
	rollup.Free(mp)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountViewAndDeepCopy(t *testing.T) {
	stateA := newTestVectorAllocationAccount(t, 1<<20, 32)
	stateB := newTestVectorAllocationAccount(t, 1<<20, 32)
	mp := mpool.MustNewZero()

	source := newAccountedTestVector(t, types.T_varchar.ToType(), stateA.selection)
	for _, value := range [][]byte{
		[]byte("first value that is not inline"),
		[]byte("second value that is not inline"),
		[]byte("third value that is not inline"),
	} {
		require.NoError(t, AppendBytes(source, value, false, mp))
	}
	sourceUsed := stateA.account.Snapshot().Used

	view, err := source.Window(1, 3)
	require.NoError(t, err)
	require.Nil(t, view.AllocationAccountSelection())
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)
	view.Free(mp)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)

	dup, err := source.DupOffHeap(mp)
	require.NoError(t, err)
	require.Same(t, stateA.selection, dup.AllocationAccountSelection())
	require.Greater(t, stateA.account.Snapshot().Used, sourceUsed)
	dup.Free(mp)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)

	crossOwner, err := source.DupOffHeapWithAllocation(mp, stateB.selection)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)
	require.NotZero(t, stateB.account.Snapshot().Used)
	crossOwner.Free(mp)
	require.Zero(t, stateB.account.Snapshot().Used)

	window, err := source.CloneWindowWithAllocation(1, 3, mp, stateB.selection)
	require.NoError(t, err)
	require.Equal(t, source.GetBytesAt(1), window.GetBytesAt(0))
	require.Equal(t, source.GetBytesAt(2), window.GetBytesAt(1))
	window.Free(mp)

	compact, err := source.CloneToFlatCompactWithAllocation(mp, stateB.selection)
	require.NoError(t, err)
	require.Equal(t, source.Length(), compact.Length())
	compact.Free(mp)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, stateA)
	finalizeTestVectorAllocationAccount(t, stateB)
}

func TestWindowPreservesGroupingProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		source := NewVec(typ)
		if typ.IsVarlen() {
			for _, value := range []string{"zero", "one", "two", "three"} {
				require.NoError(t, AppendBytes(source, []byte(value), false, mp))
			}
		} else {
			require.NoError(t, AppendFixedList(source, []int64{0, 1, 2, 3}, nil, mp))
		}
		source.GetGrouping().Add(1, 3)

		window, err := source.Window(1, 4)
		require.NoError(t, err)
		require.True(t, window.GetGrouping().Contains(0))
		require.False(t, window.GetGrouping().Contains(1))
		require.True(t, window.GetGrouping().Contains(2))
		window.Free(mp)

		clone, err := source.CloneWindow(1, 4, mp)
		require.NoError(t, err)
		require.True(t, clone.GetGrouping().Contains(0))
		require.False(t, clone.GetGrouping().Contains(1))
		require.True(t, clone.GetGrouping().Contains(2))
		clone.Free(mp)
		source.Free(mp)
	}

	rollup := NewRollupConst(types.T_int64.ToType(), 4, mp)
	window, err := rollup.Window(1, 3)
	require.NoError(t, err)
	require.True(t, window.IsGrouping())
	window.Free(mp)
	clone, err := rollup.CloneWindow(1, 3, mp)
	require.NoError(t, err)
	require.True(t, clone.IsGrouping())
	clone.Free(mp)
	rollup.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedWindowOwnsRangeBitmaps(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(source, []int64{0, 1, 2, 3}, []bool{false, true, false, true}, mp))
	source.GetGrouping().Add(1, 2)

	window, err := source.WindowWithAllocation(1, 4, mp, state.selection)
	require.NoError(t, err)
	require.True(t, window.GetNulls().GetBitmap().HasExternalStorage())
	require.True(t, window.GetGrouping().GetBitmap().HasExternalStorage())
	require.True(t, window.GetGrouping().Contains(0))
	require.True(t, window.GetGrouping().Contains(1))
	require.NotZero(t, state.account.Snapshot().Used)
	window.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestIsGroupingRejectsOutOfRangeBits(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		vec := NewVec(typ)
		vec.SetLength(1)
		vec.GetGrouping().Add(5)
		require.False(t, vec.IsGrouping())
		vec.Free(nil)
	}
}

func TestConstSetPreservesSelectedGrouping(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		source := NewVec(typ)
		if typ.IsVarlen() {
			require.NoError(t, AppendBytesList(source, [][]byte{[]byte("ordinary"), []byte("grouping")}, nil, mp))
		} else {
			require.NoError(t, AppendFixedList(source, []int64{1, 2}, nil, mp))
		}
		source.GetGrouping().Add(1)
		destination := NewVec(typ)
		set := GetConstSetFunction(typ, mp)

		require.NoError(t, set(destination, source, 1, 4))
		require.True(t, destination.IsGrouping())
		require.NoError(t, set(destination, source, 0, 4))
		require.False(t, destination.HasGrouping())

		destination.Free(mp)
		source.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func TestVectorAllocationAccountCopyRollback(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 1)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	require.NoError(
		t,
		AppendBytes(
			source,
			bytes.Repeat([]byte("payload"), 32),
			false,
			mp,
		),
	)

	_, err := source.DupOffHeapWithAllocation(mp, state.selection)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountCrossPoolFreeAndSeal(t *testing.T) {
	t.Run("cross pool free", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 1<<20, 8)
		ownerPool := mpool.MustNewZero()
		freeingPool := mpool.MustNewZero()
		vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		require.NoError(t, vec.PreExtend(128, ownerPool))
		require.NotZero(t, state.account.Snapshot().Used)

		vec.Free(freeingPool)
		require.Zero(t, state.account.Snapshot().Used)
		finalizeTestVectorAllocationAccount(t, state)
	})

	t.Run("sealed before allocation", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 1<<20, 8)
		mp := mpool.MustNewZero()
		vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		state.account.Seal()

		err := AppendFixed(vec, int64(1), false, mp)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
		require.Zero(t, state.account.Snapshot().Used)
		require.Zero(t, state.registry.LiveAllocationMetadata())
		vec.Free(mp)
		_, err = state.registry.Finalize(state.account)
		require.NoError(t, err)
	})
}

func TestVectorAllocationAccountRandomizedAppendAndSelection(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 64)
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(26459))

	fixed := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	var fixedExpected []int64
	for i := 0; i < 2_000; i++ {
		value := rng.Int63()
		fixedExpected = append(fixedExpected, value)
		require.NoError(t, AppendFixed(fixed, value, false, mp))
	}
	require.Equal(t, fixedExpected, MustFixedColNoTypeCheck[int64](fixed))

	varlen := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	var expected [][]byte
	for i := 0; i < 1_000; i++ {
		size := rng.Intn(96)
		value := make([]byte, size)
		_, err := rng.Read(value)
		require.NoError(t, err)
		expected = append(expected, append([]byte(nil), value...))
		require.NoError(t, AppendBytes(varlen, value, false, mp))
	}
	for i := range expected {
		require.True(t, bytes.Equal(expected[i], varlen.GetBytesAt(i)))
	}

	selected := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	sels := []int64{1, 3, 7, 11, 23, 101, 509, 999}
	require.NoError(t, selected.Union(varlen, sels, mp))
	for i, sel := range sels {
		require.True(t, bytes.Equal(expected[sel], selected.GetBytesAt(i)))
	}

	fixed.Free(mp)
	varlen.Free(mp)
	selected.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAccountedUnionPreservesGroupingWithoutNulls(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 64)
	mp := mpool.MustNewZero()

	for _, typ := range []types.Type{
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			source := NewOffHeapVecWithType(typ)
			for i := range 6 {
				if typ.IsVarlen() {
					require.NoError(t, AppendBytes(source, []byte{byte('a' + i)}, false, mp))
				} else {
					require.NoError(t, AppendFixed(source, int32(i), false, mp))
				}
			}
			source.GetGrouping().Add(1, 4)

			tests := []struct {
				name string
				run  func(*Vector) error
				want []bool
			}{
				{
					name: "union",
					run: func(dst *Vector) error {
						return dst.Union(source, []int64{4, 0, 1}, mp)
					},
					want: []bool{true, false, true},
				},
				{
					name: "union int32",
					run: func(dst *Vector) error {
						return dst.UnionInt32(source, []int32{4, 0, 1}, mp)
					},
					want: []bool{true, false, true},
				},
				{
					name: "union batch",
					run: func(dst *Vector) error {
						return dst.UnionBatch(source, 1, 4, nil, mp)
					},
					want: []bool{true, false, false, true},
				},
				{
					name: "union batch flags",
					run: func(dst *Vector) error {
						return dst.UnionBatch(source, 1, 4, []uint8{1, 0, 1, 1}, mp)
					},
					want: []bool{true, false, true},
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					dst := newAccountedTestVector(t, typ, state.selection)
					require.NoError(t, test.run(dst))
					for row, want := range test.want {
						require.Equal(t, want, dst.GetGrouping().Contains(uint64(row)))
					}
					require.True(t, dst.GetGrouping().GetBitmap().HasExternalStorage())
					dst.Free(mp)
				})
			}
			source.Free(mp)
		})
	}
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountDecodeCopyAndReader(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType())
	require.NoError(
		t,
		AppendBytes(source, []byte("decoded payload that is not inline"), false, mp),
	)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)

	copied := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, copied.UnmarshalBinaryWithCopy(encoded, mp))
	require.Equal(t, source.GetBytesAt(0), copied.GetBytesAt(0))
	require.Equal(t, uint64(2), state.registry.LiveAllocationMetadata())
	require.ErrorIs(
		t,
		copied.UnmarshalBinaryWithCopy(encoded, mp),
		mpool.ErrAllocationAccountInvalid,
	)
	require.ErrorIs(
		t,
		copied.UnmarshalBinary(encoded),
		mpool.ErrAllocationAccountInvalid,
	)
	copied.Free(mp)

	aliased := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.ErrorIs(
		t,
		aliased.UnmarshalBinary(encoded),
		mpool.ErrAllocationAccountInvalid,
	)
	aliased.Free(mp)

	readerDecoded := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, readerDecoded.UnmarshalWithReader(bytes.NewReader(encoded), mp))
	require.Equal(t, source.GetBytesAt(0), readerDecoded.GetBytesAt(0))
	readerDecoded.Free(mp)

	short := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	dataHeader := 1 + types.TSize + 4 + 4
	require.Error(
		t,
		short.UnmarshalWithReader(
			bytes.NewReader(encoded[:dataHeader+1]),
			mp,
		),
	)
	require.Zero(t, state.account.Snapshot().Used)
	short.Free(mp)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAccountedReaderRejectsMalformedWire(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixed(source, int64(7), true, mp))
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)

	lengthOffset := 1 + types.TSize
	dataLengthOffset := lengthOffset + 4
	dataLength := int(types.DecodeUint32(encoded[dataLengthOffset : dataLengthOffset+4]))
	areaLengthOffset := dataLengthOffset + 4 + dataLength
	areaLength := int(types.DecodeUint32(encoded[areaLengthOffset : areaLengthOffset+4]))
	nullLengthOffset := areaLengthOffset + 4 + areaLength
	nullOffset := nullLengthOffset + 4

	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{
			name: "invalid class",
			mutate: func(data []byte) {
				data[0] = 0xff
			},
		},
		{
			name: "negative length",
			mutate: func(data []byte) {
				value := uint32(math.MaxUint32)
				copy(data[lengthOffset:lengthOffset+4], types.EncodeUint32(&value))
			},
		},
		{
			name: "mismatched data length",
			mutate: func(data []byte) {
				value := uint32(2)
				copy(data[lengthOffset:lengthOffset+4], types.EncodeUint32(&value))
			},
		},
		{
			name: "oversized data payload",
			mutate: func(data []byte) {
				value := uint32(1 << 30)
				copy(data[dataLengthOffset:dataLengthOffset+4], types.EncodeUint32(&value))
			},
		},
		{
			name: "invalid null bitmap count",
			mutate: func(data []byte) {
				value := int64(2)
				copy(data[nullOffset:nullOffset+8], types.EncodeInt64(&value))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), encoded...)
			test.mutate(data)
			decoded := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalWithReader(bytes.NewReader(data), mp))
			})
			decoded.Free(mp)
			require.Zero(t, state.account.Snapshot().Used)

			copied := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
			require.NotPanics(t, func() {
				require.Error(t, copied.UnmarshalBinaryWithCopy(data, mp))
			})
			copied.Free(mp)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
	for end := range encoded {
		accounted := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		require.NotPanics(t, func() {
			require.Error(t, accounted.UnmarshalBinaryWithCopy(encoded[:end], mp))
		})
		accounted.Free(mp)
		require.Zero(t, state.account.Snapshot().Used)

		unaccounted := NewOffHeapVecWithType(types.T_int64.ToType())
		require.NotPanics(t, func() {
			require.Error(t, unaccounted.UnmarshalBinaryWithCopy(encoded[:end], mp))
		})
		unaccounted.Free(mp)
	}

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func BenchmarkVectorAllocationAccount(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(b, 1<<40, 64)

	b.Run("unaccounted-fixed-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewOffHeapVecWithType(types.T_int64.ToType())
			if err := vec.PreExtend(rows, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(mp)
		}
	})
	b.Run("accounted-fixed-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewOffHeapVecWithType(types.T_int64.ToType())
			if err := vec.SetAllocationAccount(state.selection); err != nil {
				b.Fatal(err)
			}
			if err := vec.PreExtend(rows, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(mp)
		}
	})
	b.Run("unaccounted-varlen-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewOffHeapVecWithType(types.T_varchar.ToType())
			if err := vec.PreExtendWithArea(rows, 1<<20, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(mp)
		}
	})
	b.Run("accounted-varlen-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewOffHeapVecWithType(types.T_varchar.ToType())
			if err := vec.SetAllocationAccount(state.selection); err != nil {
				b.Fatal(err)
			}
			if err := vec.PreExtendWithArea(rows, 1<<20, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(mp)
		}
	})
	b.Run("accounted-fixed-reset-reuse", func(b *testing.B) {
		vec := newAccountedTestVector(b, types.T_int64.ToType(), state.selection)
		if err := vec.PreExtend(rows, mp); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec.ResetWithSameType()
		}
		b.StopTimer()
		vec.Free(mp)
	})
	finalizeTestVectorAllocationAccount(b, state)
}

func TestVectorAllocationAccountErrorsAreTyped(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1, 1)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	err := vec.PreExtend(1, mp)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity))
	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestDetachedBufferPreservesAllocationProvenance(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	source := newAccountedTestVector(
		t,
		types.T_varchar.ToType(),
		state.selection,
	)
	require.NoError(t, AppendBytes(
		source,
		[]byte("detached allocation payload that uses the vector area"),
		false,
		mp,
	))
	used := state.account.Snapshot().Used
	require.Positive(t, used)

	data := DetachVectorData(source)
	area := DetachVectorArea(source)
	require.Positive(t, data.Capacity())
	require.Positive(t, area.Capacity())
	source.Free(mp)
	require.Equal(t, used, state.account.Snapshot().Used)

	destination := newAccountedTestVector(
		t,
		types.T_varchar.ToType(),
		state.selection,
	)
	require.True(t, data.CanAttachTo(destination, DetachedDataBuffer))
	require.False(t, data.CanAttachTo(destination, DetachedAreaBuffer))
	require.NoError(t, data.AttachTo(destination, DetachedDataBuffer))
	require.NoError(t, area.AttachTo(destination, DetachedAreaBuffer))
	require.Zero(t, data.Capacity())
	require.Zero(t, area.Capacity())
	require.Equal(t, used, state.account.Snapshot().Used)

	destination.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)
	data.Free(mp)
	area.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestSetTypeAndFixDataAllocationFailureIsAtomic(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 512, 4)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(
		t,
		types.T_date.ToType(),
		state.selection,
	)
	require.NoError(t, vec.PreExtend(128, mp))
	vec.SetLength(128)
	used := state.account.Snapshot().Used
	require.Equal(t, uint64(512), used)

	err := vec.SetTypeAndFixData(types.T_datetime.ToType(), mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, types.T_date, vec.GetType().Oid)
	require.Equal(t, 128, vec.Length())
	require.Equal(t, used, state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestDetachedUnaccountedBufferAndTypeChange(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(
		source,
		[]byte("unaccounted detached allocation payload"),
		false,
		mp,
	))
	data := DetachVectorData(source)
	area := DetachVectorArea(source)
	source.Free(mp)

	destination := NewOffHeapVecWithType(types.T_varchar.ToType())
	// Unaccounted buffers may serve either backing because there is no
	// allocation-site provenance to preserve.
	require.True(t, data.CanAttachTo(destination, DetachedAreaBuffer))
	require.NoError(t, area.AttachTo(destination, DetachedDataBuffer))
	require.NoError(t, data.AttachTo(destination, DetachedAreaBuffer))
	require.Zero(t, data.Capacity())
	require.Zero(t, area.Capacity())
	destination.Free(mp)
	require.Zero(t, mp.CurrNB())

	fixed := NewOffHeapVecWithType(types.T_date.ToType())
	require.NoError(t, AppendFixed(
		fixed,
		types.Date(1),
		false,
		mp,
	))
	require.NoError(t, fixed.SetTypeAndFixData(
		types.T_datetime.ToType(),
		mp,
	))
	require.Equal(t, types.T_datetime, fixed.GetType().Oid)
	require.Equal(t, 1, fixed.Length())
	require.Error(t, fixed.SetTypeAndFixData(
		types.T_varchar.ToType(),
		mp,
	))
	fixed.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestVectorAllocationAccountHelperBoundaries(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	var nilVec *Vector
	require.Nil(t, nilVec.AllocationAccountSelection())
	require.ErrorIs(
		t,
		nilVec.CanSetAllocationAccount(state.selection),
		mpool.ErrAllocationAccountInvalid,
	)

	vec := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	_, err := vec.allocOwned(mp, 1, false, true)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = vec.growData(nil, 1)
	require.Error(t, err)
	_, err = vec.growData(mp, mpool.CapLimit)
	require.Error(t, err)
	_, err = vec.growArea2(mp, []byte{1}, 0)
	require.Error(t, err)

	require.NoError(t, vec.PreExtendWithArea(1, 128, mp))
	used := state.account.Snapshot().Used
	zero := int32(0)
	size, area, err := vec.readSizeBytes(
		bytes.NewReader(types.EncodeInt32(&zero)),
		mp,
		false,
	)
	require.NoError(t, err)
	require.Zero(t, size)
	require.Empty(t, area)
	require.Equal(t, used, state.account.Snapshot().Used)

	negative := int32(-1)
	_, _, err = vec.readSizeBytes(
		bytes.NewReader(types.EncodeInt32(&negative)),
		mp,
		false,
	)
	require.Error(t, err)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestUnionAllPreservesConstGrouping(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			state := newTestVectorAllocationAccount(t, 1<<20, 32)
			mp := mpool.MustNewZero()
			destination := newAccountedTestVector(t, typ, state.selection)
			if typ.IsVarlen() {
				require.NoError(t, AppendBytes(destination, []byte("prefix"), false, mp))
			} else {
				require.NoError(t, AppendFixed(destination, int64(1), false, mp))
			}

			rollup := NewRollupConst(typ, 3, mp)
			require.NoError(t, GetUnionAllFunction(typ, mp)(destination, rollup))
			require.Equal(t, 4, destination.Length())
			for row := uint64(1); row < 4; row++ {
				require.True(t, destination.GetGrouping().Contains(row))
			}
			rollup.Free(mp)

			var ordinary *Vector
			var err error
			if typ.IsVarlen() {
				ordinary, err = NewConstBytes(typ, []byte("value"), 3, mp)
			} else {
				ordinary, err = NewConstFixed(typ, int64(2), 3, mp)
			}
			require.NoError(t, err)
			ordinary.GetGrouping().Add(1)
			require.NoError(t, GetUnionAllFunction(typ, mp)(destination, ordinary))
			require.Equal(t, 7, destination.Length())
			require.True(t, destination.GetGrouping().Contains(5))
			require.False(t, destination.GetGrouping().Contains(4))
			require.False(t, destination.GetGrouping().Contains(6))
			ordinary.Free(mp)

			require.True(t,
				destination.GetGrouping().GetBitmap().HasExternalStorage())
			destination.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestUnmarshalBinaryRejectsOwnedDestinationWithoutLosingBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	target := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, bytes.Repeat([]byte("s"), 64), false, mp))
	targetValue := bytes.Repeat([]byte("t"), 64)
	require.NoError(t, AppendBytes(target, targetValue, false, mp))
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	before := mp.CurrNB()
	require.Positive(t, before)

	err = target.UnmarshalBinary(encoded)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.Equal(t, targetValue, target.GetBytesAt(0))
	require.Equal(t, before, mp.CurrNB())

	source.Free(mp)
	target.Free(mp)
	require.Zero(t, mp.CurrNB())
}
