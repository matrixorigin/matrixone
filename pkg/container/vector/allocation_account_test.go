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
	"fmt"
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

func TestAllocationAccountSelectionsEqual(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	equivalent, err := NewAllocationAccountSelection(
		state.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		testVectorAreaAllocationSite,
		testVectorNullAllocationSite,
		testVectorGroupAllocationSite,
	)
	require.NoError(t, err)
	differentSite, err := NewAllocationAccountSelection(
		state.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite+1,
		testVectorAreaAllocationSite,
		testVectorNullAllocationSite,
		testVectorGroupAllocationSite,
	)
	require.NoError(t, err)

	require.NotSame(t, state.selection, equivalent)
	require.True(t, AllocationAccountSelectionsEqual(state.selection, equivalent))
	require.False(t, AllocationAccountSelectionsEqual(state.selection, differentSite))
	require.False(t, AllocationAccountSelectionsEqual(state.selection, nil))
	require.True(t, AllocationAccountSelectionsEqual(nil, nil))

	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	require.NoError(t, vec.CanSetAllocationAccount(equivalent))
	require.NoError(t, vec.SetAllocationAccount(equivalent))
	// Equivalent provenance is a no-op: existing physical ownership remains
	// attached to the original immutable selection.
	require.Same(t, state.selection, vec.AllocationAccountSelection())
	vec.Free(mp)

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

func TestVectorAllocationAccountBitmapUsesExclusiveRowBoundary(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 4)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)

	require.NoError(t, vec.PreExtendBitmap(64, mp))
	require.Equal(t, 1, vec.nsp.GetBitmap().ExternalStorageCapacity())
	require.Equal(t, 1, vec.gsp.GetBitmap().ExternalStorageCapacity())
	require.Equal(t, uint64(16), state.account.Snapshot().Used)
	vec.SetLength(64)
	vec.SetAllNulls(64)
	vec.GetGrouping().AddRange(0, 64)
	require.Equal(t, 64, vec.GetNulls().Count())
	require.Equal(t, 64, vec.GetGrouping().Count())

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

func TestVectorAllocationAccountDupPreservesSparseBitmapRowDomain(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, AppendFixed(vec, int64(0), true, mp))
	for i := 1; i < 130; i++ {
		require.NoError(t, AppendFixed(vec, int64(i), false, mp))
	}
	require.NoError(t, vec.ensureGroupingCapacity(1, mp))
	vec.GetGrouping().Add(0)
	require.Equal(t, 1, vec.GetNulls().Count())
	require.Equal(t, 1, vec.GetGrouping().Count())
	dup, err := vec.Dup(mp)
	require.NoError(t, err)
	require.GreaterOrEqual(
		t,
		dup.GetNulls().GetBitmap().ExternalStorageCapacity(),
		3,
	)
	require.GreaterOrEqual(
		t,
		dup.GetGrouping().GetBitmap().ExternalStorageCapacity(),
		3,
	)
	sels := make([]int64, 129)
	for i := range sels {
		sels[i] = int64(i + 1)
	}
	require.NotPanics(t, func() {
		dup.Shrink(sels, false)
	})
	require.Equal(t, 129, dup.Length())
	require.Zero(t, dup.GetNulls().Count())
	require.Zero(t, dup.GetGrouping().Count())

	dup.Free(mp)
	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountDupPreservesStaleBitmapExtent(t *testing.T) {
	t.Run("flat null and grouping", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 8<<20, 16)
		mp := mpool.MustNewZero()
		vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		require.NoError(t, vec.PreExtend(130, mp))
		require.NoError(t, vec.PreExtendBitmap(130, mp))
		for i := range 130 {
			require.NoError(t, AppendFixed(vec, int64(i), false, mp))
		}
		vec.SetNull(129)
		vec.GetGrouping().Add(128)
		vec.SetLength(1)

		dup, err := vec.Dup(mp)
		require.NoError(t, err)
		require.Equal(t, 1, dup.Length())
		require.True(t, dup.GetNulls().Contains(129))
		require.True(t, dup.GetGrouping().Contains(128))

		dup.Free(mp)
		vec.Free(mp)
		finalizeTestVectorAllocationAccount(t, state)
	})

	t.Run("constant grouping", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 8<<20, 16)
		mp := mpool.MustNewZero()
		vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		require.NoError(t, AppendFixed(vec, int64(1), false, mp))
		vec.SetClass(CONSTANT)
		require.NoError(t, vec.PreExtendGrouping(130, mp))
		vec.GetGrouping().Add(129)
		vec.SetLength(1)

		dup, err := vec.Dup(mp)
		require.NoError(t, err)
		require.Equal(t, 1, dup.Length())
		require.True(t, dup.GetGrouping().Contains(129))

		dup.Free(mp)
		vec.Free(mp)
		finalizeTestVectorAllocationAccount(t, state)
	})

	t.Run("empty stale extent", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 8<<20, 16)
		mp := mpool.MustNewZero()
		vec := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		require.NoError(t, AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vec.PreExtendBitmap(130, mp))
		vec.SetNull(129)
		vec.GetGrouping().Add(129)
		vec.UnsetNull(129)
		vec.GetGrouping().Del(129)
		require.Zero(t, vec.GetNulls().Count())
		require.Zero(t, vec.GetGrouping().Count())

		dup, err := vec.Dup(mp)
		require.NoError(t, err)
		require.Equal(t, int64(130), dup.GetNulls().GetBitmap().Len())
		require.Equal(t, int64(130), dup.GetGrouping().GetBitmap().Len())

		dup.Free(mp)
		vec.Free(mp)
		finalizeTestVectorAllocationAccount(t, state)
	})
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

func TestVectorAllocationAccountPrepareParamShuffleFailureIsTransactional(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 2)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	for _, value := range []string{"1", "2", "3", "4"} {
		require.NoError(t, AppendBytes(vec, []byte(value), false, mp))
	}
	require.NoError(t, vec.SetPrepareParamKindsWithMP([]PrepareParamKind{
		PrepareParamInteger,
		PrepareParamFloat,
		PrepareParamNone,
		PrepareParamDecimal,
	}, mp))
	before := state.account.Snapshot()
	beforeBytes := make([][]byte, vec.Length())
	for row := range beforeBytes {
		beforeBytes[row] = vec.CloneBytesAt(row)
	}
	beforeKinds := append([]PrepareParamKind(nil), vec.GetPrepareParamKinds()...)

	var err error
	require.NotPanics(t, func() {
		err = vec.Shuffle([]int64{3, 1, 0, 3, 2}, mp)
	})
	require.Error(t, err)
	require.Equal(t, before.Used, state.account.Snapshot().Used)
	require.Equal(t, 4, vec.Length())
	require.Equal(t, beforeBytes, InefficientMustBytesCol(vec))
	require.Equal(t, beforeKinds, vec.GetPrepareParamKinds())

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountPrepareParamReaderFailureIsTransactional(t *testing.T) {
	// The vector data consumes the sole admitted allocation slot. The reader
	// path must reject the sidecar allocation without changing either the
	// vector's metadata or the account's charged bytes.
	state := newTestVectorAllocationAccount(t, 1<<20, 1)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_int8.ToType(), state.selection)
	require.NoError(t, AppendFixedList(vec, []int8{1, 2}, nil, mp))
	before := state.account.Snapshot()

	err := vec.SetPrepareParamKindsFromReader(
		bytes.NewReader([]byte{byte(PrepareParamInteger), byte(PrepareParamFloat)}),
		2, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Equal(t, before.Used, state.account.Snapshot().Used)
	require.Nil(t, vec.GetPrepareParamKinds())
	require.Equal(t, PrepareParamNone, vec.GetPrepareParamKind())

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

func TestWindowPreservesOffsetConstNull(t *testing.T) {
	mp := mpool.MustNewZero()
	source, err := NewConstFixed(
		types.T_int64.ToType(), int64(7), 4, mp,
	)
	require.NoError(t, err)
	source.SetNull(0)

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.True(t, window.IsConstNull())
	require.Equal(t, 2, window.Length())
	window.Free(mp)
	source.Free(mp)
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

func TestConstSetPreservesSelectedBinaryString(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytesList(
		source, [][]byte{[]byte("binary"), []byte("text"), nil}, []bool{false, false, true}, mp))
	require.NoError(t, source.SetBinaryStringRows([]bool{true, false, false}))
	destination := NewVec(types.T_text.ToType())
	set := GetConstSetFunction(types.T_text.ToType(), mp)

	require.NoError(t, set(destination, source, 0, 4))
	require.True(t, destination.GetIsBinaryString())
	for row := range 4 {
		require.True(t, destination.GetIsBinaryStringAt(row))
	}
	require.NoError(t, set(destination, source, 1, 4))
	require.False(t, destination.GetIsBinaryString())
	require.NoError(t, set(destination, source, 2, 4))
	require.True(t, destination.IsConstNull())
	require.False(t, destination.GetIsBinaryString())

	destination.Free(mp)
	source.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestConstSetRuntimeStringDomainAdmissionIsFailureAtomic(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 2)
	mp := mpool.MustNewZero()
	destination := newAccountedTestVector(t, types.T_varbinary.ToType(), state.selection)
	require.NoError(t, AppendBytes(destination, []byte("old"), false, mp))

	source := NewVec(types.T_varbinary.ToType())
	require.NoError(t, AppendBytes(source, []byte("new"), false, mp))
	require.NoError(t, source.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))

	set := GetConstSetFunction(types.T_varbinary.ToType(), mp)
	err := set(destination, source, 0, 4)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Equal(t, 1, destination.Length())
	require.False(t, destination.IsConst())
	require.Equal(t, "old", destination.GetStringAt(0))
	require.Equal(t, types.RuntimeStringInherit, destination.GetRuntimeStringDomainAt(0))

	source.Free(mp)
	destination.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
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

func TestVectorAccountedUnionPreservesHeterogeneousPrepareParamKinds(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	for range 2 {
		require.NoError(t, AppendBytes(source, []byte("5"), false, mp))
	}
	source.SetPrepareParamKinds([]PrepareParamKind{PrepareParamFloat, PrepareParamNone})

	for _, name := range []string{"union-one", "union-batch", "union-all"} {
		t.Run(name, func(t *testing.T) {
			destination := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			switch name {
			case "union-one":
				require.NoError(t, destination.UnionOne(source, 0, mp))
				require.NoError(t, destination.UnionOne(source, 1, mp))
			case "union-batch":
				require.NoError(t, destination.UnionBatch(source, 0, 2, nil, mp))
			case "union-all":
				require.NoError(t, GetUnionAllFunction(types.T_text.ToType(), mp)(destination, source))
			}
			require.Equal(t, PrepareParamFloat, destination.GetPrepareParamKindAt(0))
			require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(1))
			destination.Free(mp)
		})
	}
	source.Free(mp)
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

func BenchmarkVectorElementAccounting(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(b, 1<<40, 64)
	source := NewOffHeapVecWithType(types.T_int64.ToType())
	for i := range rows {
		if err := AppendFixed(source, int64(i), false, mp); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		source.Free(mp)
		finalizeTestVectorAllocationAccount(b, state)
	})

	for _, accounted := range []bool{false, true} {
		mode := "unaccounted"
		if accounted {
			mode = "accounted"
		}
		b.Run("union-one/"+mode, func(b *testing.B) {
			destination := NewOffHeapVecWithType(types.T_int64.ToType())
			if accounted {
				if err := destination.SetAllocationAccount(state.selection); err != nil {
					b.Fatal(err)
				}
			}
			if err := destination.PreExtend(1, mp); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				destination.ResetWithSameType()
				if err := destination.UnionOne(source, int64(i%rows), mp); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			destination.Free(mp)
		})

		b.Run("copy/"+mode, func(b *testing.B) {
			destination := NewOffHeapVecWithType(types.T_int64.ToType())
			if accounted {
				if err := destination.SetAllocationAccount(state.selection); err != nil {
					b.Fatal(err)
				}
			}
			if err := destination.PreExtend(1, mp); err != nil {
				b.Fatal(err)
			}
			destination.SetLength(1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := destination.Copy(source, 0, int64(i%rows), mp); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			destination.Free(mp)
		})
	}
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

	equivalent, err := NewAllocationAccountSelection(
		state.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		testVectorAreaAllocationSite,
		testVectorNullAllocationSite,
		testVectorGroupAllocationSite,
	)
	require.NoError(t, err)
	require.NotSame(t, state.selection, equivalent)
	destination := newAccountedTestVector(
		t,
		types.T_varchar.ToType(),
		equivalent,
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

func TestBinaryStringBitmapUsesVectorAllocationAccount(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	before := state.account.Snapshot().Used
	require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))
	require.Greater(t, state.account.Snapshot().Used, before)
	require.True(t, vec.GetIsBinaryStringAt(0))
	require.False(t, vec.GetIsBinaryStringAt(1))
	require.NoError(t, vec.PreExtendBitmap(63, mp))
	require.Equal(t,
		vec.GetNulls().GetBitmap().ExternalStorageCapacity(),
		vec.binaryStringRows.ExternalStorageCapacity())
	vec.SetIsBinaryString(false)
	require.Equal(t, int(state.account.Snapshot().Used), vec.Allocated())
	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestAccountedMixedBinaryStringPreExtendCoversSetLength(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))

	require.NoError(t, vec.PreExtend(65, mp))
	require.NotPanics(t, func() { vec.SetLength(65) })
	require.True(t, vec.GetIsBinaryStringAt(0))
	require.False(t, vec.GetIsBinaryStringAt(1))
	require.False(t, vec.GetIsBinaryStringAt(64))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedBinaryStringCreatedAfterPayloadGrowthCoversSetLength(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 4<<20, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(65536, mp))
	vec.SetLength(2)
	require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))

	require.NotPanics(t, func() { vec.SetLength(65536) })
	require.True(t, vec.GetIsBinaryStringAt(0))
	require.False(t, vec.GetIsBinaryStringAt(65535))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedConstRuntimeStringDomainUsesPhysicalRowCapacity(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	vec, err := NewConstBytesWithAllocation(
		types.T_varbinary.ToType(), []byte("selected"), 1, mp, state.selection,
	)
	require.NoError(t, err)
	require.NoError(t, vec.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
	used := state.account.Snapshot().Used
	binaryCapacity := vec.binaryStringRows.ExternalStorageCapacity()
	textCapacity := vec.textStringRows.ExternalStorageCapacity()
	require.Equal(t, 1, binaryCapacity)
	require.Equal(t, 1, textCapacity)

	require.NotPanics(t, func() { vec.SetLength(65) })
	require.Equal(t, used, state.account.Snapshot().Used)
	require.Equal(t, binaryCapacity, vec.binaryStringRows.ExternalStorageCapacity())
	require.Equal(t, textCapacity, vec.textStringRows.ExternalStorageCapacity())
	require.Equal(t, types.RuntimeStringText, vec.GetRuntimeStringDomainAt(0))
	require.Equal(t, types.RuntimeStringText, vec.GetRuntimeStringDomainAt(64))

	vec.SetLength(0)
	require.Equal(t, types.RuntimeStringInherit, vec.GetRuntimeStringDomainAt(0))
	require.NotPanics(t, func() { vec.SetLength(129) })
	require.Equal(t, types.RuntimeStringText, vec.GetRuntimeStringDomainAt(128))
	require.Equal(t, used, state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedBinaryStringInplaceSortHasPreflightedBitmaps(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%t", compact), func(t *testing.T) {
			state := newTestVectorAllocationAccount(t, 1<<20, 16)
			mp := mpool.MustNewZero()
			vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			require.NoError(t, vec.PreExtend(128, mp))
			vec.SetLength(128)
			rows := make([]bool, 128)
			rows[0] = true
			require.NoError(t, vec.SetBinaryStringRowsWithMP(rows, mp))

			require.NotPanics(t, func() {
				if compact {
					vec.InplaceSortAndCompact()
				} else {
					vec.InplaceSort()
				}
			})

			vec.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestCombinedMetadataReaderSecondAllocationFailureIsAtomic(t *testing.T) {
	const rows = 3
	// The varlena backing rounds to 80 bytes. Leave exactly three more bytes
	// for the staged kinds so the following bitmap allocation is the failure.
	state := newTestVectorAllocationAccount(t, 83, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(rows, mp))
	vec.SetLength(rows)
	vec.SetPrepareParamKind(PrepareParamBoolean)
	vec.SetIsBinaryString(true)
	before := state.account.Snapshot().Used

	err := vec.SetPrepareParamKindsAndBinaryStringFromReader(
		bytes.NewReader([]byte{
			byte(PrepareParamInteger) | 0x80,
			byte(PrepareParamFloat),
			byte(PrepareParamNone) | 0x80,
		}),
		rows,
		mp,
		0x80,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, before, state.account.Snapshot().Used)
	for row := range rows {
		require.Equal(t, PrepareParamBoolean, vec.GetPrepareParamKindAt(row))
		require.True(t, vec.GetIsBinaryStringAt(row))
	}

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestBinaryStringBitmapAllocationFailureIsAtomic(t *testing.T) {
	const twoVarlenaRowsBytes = 2 * types.VarlenaSize
	state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes, 16)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	before := state.account.Snapshot().Used
	require.Equal(t, uint64(twoVarlenaRowsBytes), before)

	err := vec.SetIsBinaryStringAt(0, true, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, before, state.account.Snapshot().Used)
	require.False(t, vec.GetIsBinaryString())
	require.False(t, vec.HasBinaryStringRows())
	require.False(t, vec.GetIsBinaryStringAt(0))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestSelectedRowsBinaryStringDecodeAllocationFailureIsAtomic(t *testing.T) {
	const twoVarlenaRowsBytes = 2 * types.VarlenaSize
	state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_text.ToType())
	require.NoError(t, AppendBytes(source, []byte("binary"), false, mp))
	require.NoError(t, AppendBytes(source, []byte("text"), false, mp))
	require.NoError(t,
		source.SetBinaryStringRowsWithMP([]bool{true, false}, mp))
	var encoded bytes.Buffer
	require.NoError(t, source.MarshalSelectedRowsTo(&encoded, []int32{0, 1}))

	destination := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	err := destination.UnmarshalSelectedRowsFrom(&encoded, 2, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, destination.Length())
	require.False(t, destination.HasBinaryStringMetadata())
	require.Equal(t, uint64(twoVarlenaRowsBytes), state.account.Snapshot().Used)

	destination.Free(mp)
	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestSelectedBatchBinaryStringPreflightClosesExactCapacity(t *testing.T) {
	run := func(limit uint64) (uint64, bool, error) {
		state := newTestVectorAllocationAccount(t, limit, 16)
		mp := mpool.MustNewZero()
		source := NewVec(types.T_text.ToType())
		require.NoError(t, AppendBytes(source, []byte("binary"), false, mp))
		require.NoError(t, AppendBytes(source, []byte("text"), false, mp))
		require.NoError(t,
			source.SetBinaryStringRowsWithMP([]bool{true, false}, mp))
		destination := newAccountedTestVector(t, types.T_text.ToType(), state.selection)

		err := destination.PreExtendSelectedBatch(
			source, 0, 2, []uint8{1, 1}, 2, mp)
		admitted := state.account.Snapshot().Used
		published := false
		if err == nil {
			err = destination.UnionBatchPreflighted(
				source, 0, 2, []uint8{1, 1}, mp)
			published = err == nil
			if published {
				require.Equal(t, admitted, state.account.Snapshot().Used)
				require.True(t, destination.GetBinaryStringMetadataAt(0))
				require.False(t, destination.GetBinaryStringMetadataAt(1))
			}
		}

		destination.Free(mp)
		source.Free(mp)
		finalizeTestVectorAllocationAccount(t, state)
		require.Zero(t, mp.CurrNB())
		return admitted, published, err
	}

	admitted, published, err := run(1 << 20)
	require.NoError(t, err)
	require.True(t, published)
	require.Positive(t, admitted)

	exact, published, err := run(admitted)
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, admitted, exact)

	_, published, err = run(admitted - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.False(t, published, "capacity rejection must happen before publication")
}

func TestBinaryStringConstantMarkerStaysScalar(t *testing.T) {
	state := newTestVectorAllocationAccount(t, types.VarlenaSize, 4)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
	require.NoError(t, AppendBytes(vec, []byte("value"), false, mp))
	vec.SetClass(CONSTANT)
	vec.SetLength(4096)

	require.NoError(t, vec.SetIsBinaryStringAt(4095, true, mp))
	require.True(t, vec.GetIsBinaryString())
	require.True(t, vec.GetIsBinaryStringAt(0))
	require.True(t, vec.GetIsBinaryStringAt(4095))
	require.False(t, vec.HasBinaryStringRows())
	require.Equal(t, uint64(types.VarlenaSize), state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestRetainedBinaryStringBitmapPreventsAccountReplacement(t *testing.T) {
	first := newTestVectorAllocationAccount(t, 1<<20, 8)
	second := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	vec := newAccountedTestVector(t, types.T_text.ToType(), first.selection)
	vec.SetLength(2)
	require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))
	require.Positive(t, first.account.Snapshot().Used)
	vec.SetIsBinaryString(false)
	require.False(t, vec.HasBinaryStringRows())

	require.ErrorIs(t, vec.CanSetAllocationAccount(second.selection), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, vec.SetAllocationAccount(second.selection), mpool.ErrAllocationAccountInvalid)
	require.Same(t, first.selection, vec.AllocationAccountSelection())
	require.Zero(t, second.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, first)
	finalizeTestVectorAllocationAccount(t, second)
	require.Zero(t, mp.CurrNB())
}

func TestBinaryStringShuffleAllocationFailureIsAtomic(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Vector, []int64, *mpool.MPool) error
	}{
		{name: "shuffle", run: func(vec *Vector, sels []int64, mp *mpool.MPool) error {
			return vec.Shuffle(sels, mp)
		}},
		{name: "shuffle with buffer fallback", run: func(vec *Vector, sels []int64, mp *mpool.MPool) error {
			var buf []byte
			return vec.ShuffleWithBuf(sels, mp, &buf)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const (
				oldRows = 2
				newRows = 1024
			)
			limit := uint64((oldRows+newRows)*types.VarlenaSize + 8)
			state := newTestVectorAllocationAccount(t, limit, 16)
			mp := mpool.MustNewZero()
			vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			require.NoError(t, vec.PreExtend(oldRows, mp))
			require.NoError(t, AppendBytesList(
				vec, [][]byte{[]byte("z"), []byte("a")}, nil, mp))
			require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))
			sels := make([]int64, newRows)
			for row := range sels {
				sels[row] = int64(row % oldRows)
			}

			err := test.run(vec, sels, mp)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, oldRows, vec.Length())
			require.Equal(t, []byte("z"), vec.GetBytesAt(0))
			require.Equal(t, []byte("a"), vec.GetBytesAt(1))
			require.True(t, vec.GetIsBinaryStringAt(0))
			require.False(t, vec.GetIsBinaryStringAt(1))

			vec.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestBinaryStringRawAppendAllocationFailureIsAtomic(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Vector, *mpool.MPool) error
	}{
		{name: "bytes", run: func(vec *Vector, mp *mpool.MPool) error {
			return AppendBytes(vec, []byte("ordinary"), false, mp)
		}},
		{name: "multi bytes", run: func(vec *Vector, mp *mpool.MPool) error {
			return AppendMultiBytes(vec, []byte("ordinary"), false, 1, mp)
		}},
		{name: "bytes list", run: func(vec *Vector, mp *mpool.MPool) error {
			return AppendBytesList(vec, [][]byte{[]byte("ordinary")}, nil, mp)
		}},
		{name: "string list", run: func(vec *Vector, mp *mpool.MPool) error {
			return AppendStringList(vec, []string{"ordinary"}, nil, mp)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const twoVarlenaRowsBytes = 2 * types.VarlenaSize
			state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes, 16)
			mp := mpool.MustNewZero()
			vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			require.NoError(t, vec.PreExtend(2, mp))
			require.NoError(t, AppendBytes(vec, []byte("binary"), false, mp))
			vec.SetIsBinaryString(true)

			err := test.run(vec, mp)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, 1, vec.Length())
			require.Equal(t, []byte("binary"), vec.GetBytesAt(0))
			require.True(t, vec.GetIsBinaryString())
			require.True(t, vec.GetIsBinaryStringAt(0))
			require.False(t, vec.HasBinaryStringRows())

			vec.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestBinaryStringRawAppendPayloadFailureRollsBackMetadata(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Vector, []byte, *mpool.MPool) error
	}{
		{name: "bytes", run: func(vec *Vector, value []byte, mp *mpool.MPool) error {
			return AppendBytes(vec, value, false, mp)
		}},
		{name: "multi bytes", run: func(vec *Vector, value []byte, mp *mpool.MPool) error {
			return AppendMultiBytes(vec, value, false, 1, mp)
		}},
		{name: "bytes list", run: func(vec *Vector, value []byte, mp *mpool.MPool) error {
			return AppendBytesList(vec, [][]byte{value}, nil, mp)
		}},
		{name: "string list", run: func(vec *Vector, value []byte, mp *mpool.MPool) error {
			return AppendStringList(vec, []string{string(value)}, nil, mp)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const dataBytes = 2 * types.VarlenaSize
			state := newTestVectorAllocationAccount(t, dataBytes+8, 16)
			mp := mpool.MustNewZero()
			vec := newAccountedTestVector(t, types.T_text.ToType(), state.selection)
			require.NoError(t, vec.PreExtend(2, mp))
			require.NoError(t, AppendBytes(vec, []byte("binary"), false, mp))
			vec.SetIsBinaryString(true)

			err := test.run(vec, bytes.Repeat([]byte{'x'}, types.VarlenaInlineSize+1), mp)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, 1, vec.Length())
			require.Equal(t, []byte("binary"), vec.GetBytesAt(0))
			require.True(t, vec.GetIsBinaryString())
			require.False(t, vec.HasBinaryStringRows())

			vec.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestBinaryStringUnionAllocationFailureDoesNotPublishRows(t *testing.T) {
	tests := []struct {
		name string
		run  func(destination, source *Vector, mp *mpool.MPool) error
	}{
		{name: "union-one", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return destination.UnionOne(source, 0, mp)
		}},
		{name: "union-multi", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return destination.UnionMulti(source, 0, 1, mp)
		}},
		{name: "union-int64", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return destination.Union(source, []int64{0}, mp)
		}},
		{name: "union-int32", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return destination.UnionInt32(source, []int32{0}, mp)
		}},
		{name: "union-batch", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return destination.UnionBatch(source, 0, 1, nil, mp)
		}},
		{name: "union-all", run: func(destination, source *Vector, mp *mpool.MPool) error {
			return GetUnionAllFunction(types.T_varchar.ToType(), mp)(destination, source)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const twoVarlenaRowsBytes = 2 * types.VarlenaSize
			state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes, 16)
			mp := mpool.MustNewZero()
			destination := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
			require.NoError(t, destination.PreExtend(2, mp))
			require.NoError(t, AppendBytes(destination, []byte("old"), false, mp))

			source := NewOffHeapVecWithType(types.T_varchar.ToType())
			require.NoError(t, AppendBytes(source, []byte("new"), false, mp))
			source.SetIsBinaryString(true)
			before := state.account.Snapshot().Used

			err := test.run(destination, source, mp)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, 1, destination.Length())
			require.Equal(t, []byte("old"), destination.GetBytesAt(0))
			require.False(t, destination.GetIsBinaryStringAt(0))
			require.Equal(t, before, state.account.Snapshot().Used)

			source.Free(mp)
			destination.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestBinaryStringCopyAllocationFailureDoesNotOverwriteRow(t *testing.T) {
	const twoVarlenaRowsBytes = 2 * types.VarlenaSize
	state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes, 16)
	mp := mpool.MustNewZero()
	destination := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, destination.PreExtend(2, mp))
	require.NoError(t, AppendBytes(destination, []byte("old"), false, mp))
	require.NoError(t, AppendBytes(destination, []byte("text"), false, mp))

	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, []byte("new"), false, mp))
	source.SetIsBinaryString(true)
	before := state.account.Snapshot().Used

	err := destination.Copy(source, 0, 0, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, []byte("old"), destination.GetBytesAt(0))
	require.Equal(t, []byte("text"), destination.GetBytesAt(1))
	require.False(t, destination.GetIsBinaryStringAt(0))
	require.Equal(t, before, state.account.Snapshot().Used)

	source.Free(mp)
	destination.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestBinaryStringCopyAndUnsetNullAllocationFailureIsAtomic(t *testing.T) {
	const twoVarlenaRowsBytes = 2 * types.VarlenaSize
	state := newTestVectorAllocationAccount(t, twoVarlenaRowsBytes+8, 16)
	mp := mpool.MustNewZero()
	destination := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, destination.PreExtend(2, mp))
	require.NoError(t, AppendBytes(destination, []byte("text"), false, mp))
	require.NoError(t, AppendBytes(destination, nil, true, mp))

	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(source, []byte("binary"), false, mp))
	source.SetIsBinaryString(true)
	before := state.account.Snapshot().Used

	err := destination.SetRawBytesAtFromAndUnsetNull(1, source, 0, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.True(t, destination.IsNull(1))
	require.False(t, destination.GetBinaryStringMetadataAt(0))
	require.False(t, destination.GetBinaryStringMetadataAt(1))
	require.Equal(t, before, state.account.Snapshot().Used)

	source.Free(mp)
	destination.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
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

func TestPrepareParamKindUnionAllocationFailureDoesNotPublishRows(t *testing.T) {
	tests := []struct {
		name string
		run  func(destination, source *Vector, mp *mpool.MPool) error
	}{
		{
			name: "union-one",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.UnionOne(source, 0, mp)
			},
		},
		{
			name: "union-multi",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.UnionMulti(source, 0, 2, mp)
			},
		},
		{
			name: "union-int64",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.Union(source, []int64{0, 1}, mp)
			},
		},
		{
			name: "union-int32",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.UnionInt32(source, []int32{0, 1}, mp)
			},
		},
		{
			name: "union-batch",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.UnionBatch(source, 0, 2, nil, mp)
			},
		},
		{
			name: "union-batch-flags",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return destination.UnionBatch(source, 0, 2, []uint8{0, 1}, mp)
			},
		},
		{
			name: "union-all",
			run: func(destination, source *Vector, mp *mpool.MPool) error {
				return GetUnionAllFunction(types.T_varchar.ToType(), mp)(destination, source)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newTestVectorAllocationAccount(t, 1<<20, 1)
			mp := mpool.MustNewZero()
			destination := newAccountedTestVector(
				t, types.T_varchar.ToType(), state.selection)
			require.NoError(t, destination.PreExtend(4, mp))
			require.NoError(t, AppendBytes(destination, []byte("6"), false, mp))

			source := NewOffHeapVecWithType(types.T_varchar.ToType())
			require.NoError(t, AppendBytesList(
				source, [][]byte{[]byte("5"), []byte("5")}, nil, mp))
			source.SetPrepareParamKind(PrepareParamFloat)

			before := state.account.Snapshot().Used
			err := test.run(destination, source, mp)
			require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
			require.Equal(t, 1, destination.Length())
			require.Equal(t, []byte("6"), destination.GetBytesAt(0))
			require.Nil(t, destination.GetPrepareParamKinds())
			require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(0))
			require.Equal(t, before, state.account.Snapshot().Used)

			source.Free(mp)
			destination.Free(mp)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestPrepareParamKindUnionAllConstAllocationFailureDoesNotPublishRows(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 1)
	mp := mpool.MustNewZero()
	destination := newAccountedTestVector(t, types.T_varchar.ToType(), state.selection)
	require.NoError(t, destination.PreExtend(4, mp))
	require.NoError(t, AppendBytes(destination, []byte("6"), false, mp))
	source, err := NewConstBytes(types.T_varchar.ToType(), []byte("5"), 2, mp)
	require.NoError(t, err)
	source.SetPrepareParamKind(PrepareParamFloat)

	before := state.account.Snapshot().Used
	err = GetUnionAllFunction(types.T_varchar.ToType(), mp)(destination, source)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Equal(t, 1, destination.Length())
	require.Equal(t, []byte("6"), destination.GetBytesAt(0))
	require.Nil(t, destination.GetPrepareParamKinds())
	require.Equal(t, before, state.account.Snapshot().Used)

	source.Free(mp)
	destination.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
}

func TestPrepareParamKindCopyAllocationFailureDoesNotOverwriteRow(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 1)
	mp := mpool.MustNewZero()
	destination := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	require.NoError(t, destination.PreExtend(2, mp))
	require.NoError(t, AppendFixedList(destination, []int64{6, 7}, nil, mp))
	source := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixed(source, int64(5), false, mp))
	source.SetPrepareParamKind(PrepareParamFloat)

	before := state.account.Snapshot().Used
	err := destination.Copy(source, 1, 0, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Equal(t, []int64{6, 7}, MustFixedColWithTypeCheck[int64](destination))
	require.Nil(t, destination.GetPrepareParamKinds())
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(0))
	require.Equal(t, PrepareParamNone, destination.GetPrepareParamKindAt(1))
	require.Equal(t, before, state.account.Snapshot().Used)

	source.Free(mp)
	destination.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
	require.Zero(t, mp.CurrNB())
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

func TestUnmarshalBinaryReplacesBorrowedAliases(t *testing.T) {
	mp := mpool.MustNewZero()
	first := NewVec(types.T_int64.ToType())
	second := NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(first, []int64{1, 2}, nil, mp))
	require.NoError(t, AppendFixedList(second, []int64{3, 4}, []bool{true, false}, mp))
	firstData, err := first.MarshalBinary()
	require.NoError(t, err)
	secondData, err := second.MarshalBinary()
	require.NoError(t, err)

	var target Vector
	require.NoError(t, target.UnmarshalBinary(firstData))
	require.Equal(t, []int64{1, 2}, MustFixedColWithTypeCheck[int64](&target))
	require.NoError(t, target.UnmarshalBinaryTrusted(secondData))
	require.Equal(t, []int64{0, 4}, MustFixedColWithTypeCheck[int64](&target))
	require.True(t, target.GetNulls().Contains(0))

	first.Free(mp)
	second.Free(mp)
	target.Free(mp)
	require.Zero(t, mp.CurrNB())
}
