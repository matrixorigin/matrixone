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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// newAccountedTestVector returns an empty vector of mp for a test that
// allocates it through an accounted view of mp (state.view).
func newAccountedTestVector(t testing.TB, typ types.Type, mp *mpool.MPool) *Vector {
	t.Helper()
	return NewVec(typ, mp)
}

func TestUnionBatchFailedGroupedAppendRollbackAndReuse(t *testing.T) {
	const limit = int64(1 << 20)
	state := newTestVectorAllocationAccount(t, 2<<20, 16)
	mp, err := mpool.NewMPool("grouped-append-rollback", limit, mpool.NoFixed)
	require.NoError(t, err)
	srcMP := mpool.MustNewZero()
	view := state.view(t, mp)
	dst := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	src := NewVec(types.T_varchar.ToType(), srcMP)
	replacement := NewVec(types.T_varchar.ToType(), srcMP)
	var ballast [][]byte
	t.Cleanup(func() {
		dst.Free(view)
		src.Free(srcMP)
		replacement.Free(srcMP)
		for _, b := range ballast {
			mp.Free(b)
		}
		require.Zero(t, mp.CurrNB())
		require.Zero(t, srcMP.CurrNB())
		mpool.DeleteMPool(mp)
		finalizeTestVectorAllocationAccount(t, state)
	})

	require.NoError(t, AppendBytes(dst, []byte("kept"), false, view))
	require.NoError(t, dst.PreExtend(2, view))
	require.NoError(t, dst.PreExtendGrouping(2, view))
	dst.GetGrouping().Set(0)
	require.NoError(t, AppendBytes(src, bytes.Repeat([]byte("x"), 32<<10), false, srcMP))
	src.GetGrouping().Set(0)
	require.NoError(t, AppendBytes(replacement, []byte("new"), false, srcMP))
	checkpoint := dst.MakeAppendCheckpoint()

	for limit-mp.CurrNB() >= 32<<10 {
		b, allocErr := mp.Alloc(4096, true)
		require.NoError(t, allocErr)
		ballast = append(ballast, b)
	}
	err = dst.UnionBatch(src, 0, 1, nil, view)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "error: %v", err)
	require.Equal(t, 1, dst.Length())
	require.True(t, dst.GetGrouping().Contains(1), "failure must occur after grouping publication")
	dst.RollbackAppend(checkpoint, 1)
	for _, b := range ballast {
		mp.Free(b)
	}
	ballast = nil

	require.NoError(t, dst.UnionBatch(replacement, 0, 1, nil, view))
	require.Equal(t, []string{"kept", "new"}, InefficientMustStrCol(dst))
	require.False(t, dst.IsNull(0))
	require.False(t, dst.IsNull(1))
	require.True(t, dst.GetGrouping().Contains(0))
	require.False(t, dst.GetGrouping().Contains(1))
}

func TestVectorAllocationAccountConfiguration(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	base := mpool.MustNewZero()

	_, err := base.WithAccount(
		nil,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		mpool.AllocationCapacityClassDefault,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	mp := state.view(t, base)
	// A view charges only off-heap allocations; vector allocations through a
	// view are always off-heap, even for an on-heap vector.
	onHeap := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(onHeap, int64(1), false, mp))
	require.Positive(t, state.account.Snapshot().Used)
	onHeap.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)

	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(vec, int64(1), false, mp))
	sourceUsed := state.account.Snapshot().Used
	dup, err := vec.Dup(mp)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	dup.Free(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	compact, err := vec.CloneToFlatCompact(mp)
	require.NoError(t, err)
	require.Greater(t, state.account.Snapshot().Used, sourceUsed)
	compact.Free(mp)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)

	// Through the base pool a copy is unaccounted.
	unaccounted, err := vec.Dup(base)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, state.account.Snapshot().Used)
	unaccounted.Free(base)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestShuffleWithAllocationAccountUsesAlternateCapacityClass(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	ordinaryController := &rejectNextVectorAllocation{}
	account, err := registry.OpenWithController(1<<20, ordinaryController)
	require.NoError(t, err)
	ordinary, err := mp.WithAccount(
		account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		mpool.AllocationCapacityClassDefault,
	)
	require.NoError(t, err)
	recoveryController := &rejectNextVectorAllocation{}
	class, err := account.RegisterCapacityController(recoveryController)
	require.NoError(t, err)
	recovery, err := mp.WithAccount(
		account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		class,
	)
	require.NoError(t, err)

	// A vector allocates only through the pool it was created with: a vector
	// of the recovery class charges that class, and another view (even of
	// the same account) is rejected.
	vec := newAccountedTestVector(t, types.T_int64.ToType(), recovery)
	require.NoError(t, AppendFixedList(
		vec, []int64{3, 1, 2}, nil, recovery))
	require.Error(t, vec.Shuffle([]int64{1, 2, 0}, ordinary))
	ordinaryController.failAt = ordinaryController.calls + 1
	require.NoError(t, vec.Shuffle([]int64{1, 2, 0}, recovery))
	require.Equal(t, []int64{1, 2, 3},
		MustFixedColWithTypeCheck[int64](vec))
	require.False(t, ordinaryController.rejected)
	require.Zero(t, ordinaryController.used)
	require.Positive(t, recoveryController.used)

	vec.Free(recovery)
	require.Zero(t, recoveryController.used)
	require.NoError(t,
		account.UnregisterCapacityController(class, recoveryController))
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}

func TestUnionBatchWithAllocationAccountDoesNotPropagateCapacityClass(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	ordinaryController := &rejectNextVectorAllocation{}
	account, err := registry.OpenWithController(1<<20, ordinaryController)
	require.NoError(t, err)
	ordinary, err := mp.WithAccount(
		account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		mpool.AllocationCapacityClassDefault,
	)
	require.NoError(t, err)
	recoveryController := &rejectNextVectorAllocation{}
	class, err := account.RegisterCapacityController(recoveryController)
	require.NoError(t, err)
	recovery, err := mp.WithAccount(
		account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite,
		class,
	)
	require.NoError(t, err)

	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixedList(
		source, []int64{3, 1, 2}, nil, mp))
	destination := newAccountedTestVector(t, types.T_int64.ToType(), recovery)
	ordinaryController.failAt = ordinaryController.calls + 1
	require.NoError(t, destination.UnionBatch(
		source, 0, source.Length(), nil, recovery))
	require.Equal(t, []int64{3, 1, 2},
		MustFixedColWithTypeCheck[int64](destination))
	require.False(t, ordinaryController.rejected)
	require.Zero(t, ordinaryController.used)
	require.Positive(t, recoveryController.used)

	// The private class applies only to the retained copy. A downstream copy
	// through the ordinary view cannot borrow the producer's recovery floor.
	_, err = destination.Dup(ordinary)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.True(t, ordinaryController.rejected)

	destination.Free(recovery)
	source.Free(mp)
	require.Zero(t, recoveryController.used)
	require.NoError(t,
		account.UnregisterCapacityController(class, recoveryController))
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}

func TestAllocationAccountViewsEqual(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	equivalent := state.view(t, mp)
	differentSite, err := mp.WithAccount(
		state.account,
		testVectorAllocationOwner,
		testVectorDataAllocationSite+1,
		mpool.AllocationCapacityClassDefault,
	)
	require.NoError(t, err)

	require.NotSame(t, view, equivalent)
	require.True(t, mpool.SameView(view, equivalent))
	require.False(t, mpool.SameView(view, differentSite))
	require.False(t, mpool.SameView(view, nil))
	require.True(t, mpool.SameView(nil, nil))

	vec := newAccountedTestVector(t, types.T_int64.ToType(), view)
	require.NoError(t, AppendFixed(vec, int64(1), false, view))
	// A vector grows only through its own pool; an equivalent view is a
	// different pool.
	used := state.account.Snapshot().Used
	require.Error(t, vec.PreExtend(1024, equivalent))
	require.NoError(t, vec.PreExtend(1024, view))
	require.Greater(t, state.account.Snapshot().Used, used)
	vec.Free(view)

	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountFixedResetReuseAndFree(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)

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
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_varchar.ToType(), mp)

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
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)

	require.NoError(t, vec.PreExtend(32*1024, mp))
	require.NoError(t, vec.PreExtendBitmap(32*1024, mp))
	vec.SetLength(32 * 1024)
	vec.SetAllNulls(32 * 1024)
	vec.GetGrouping().AddRange(0, 32*1024)
	require.Equal(t, 32*1024, vec.GetNulls().Count())
	require.Equal(t, 32*1024, vec.GetGrouping().Count())

	// Bitmaps are Go-owned; only the data buffer is charged to the account.
	initial := state.account.Snapshot()
	require.Equal(t, uint64(cap(vec.data)), initial.Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

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
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapUsesExclusiveRowBoundary(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 4)
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)

	require.NoError(t, vec.PreExtendBitmap(64, mp))
	require.Equal(t, 1, vec.nsp.GetBitmap().CapWords())
	require.Equal(t, 1, vec.gsp.GetBitmap().CapWords())
	require.Zero(t, state.account.Snapshot().Used, "bitmaps are not charged to the account")
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
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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

	require.NoError(t, vec.Shrink([]int64{0, 2, 64, 65, 129}, false, mp))
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

func TestVectorAllocationAccountShrinkSparseBitmaps(t *testing.T) {
	for _, decoded := range []bool{false, true} {
		for _, mode := range []string{"slice", "mask", "offset"} {
			for _, rows := range []int{63, 64, 65, 129} {
				t.Run(fmt.Sprintf("decoded=%t/%s/rows=%d", decoded, mode, rows), func(t *testing.T) {
					state := newTestVectorAllocationAccount(t, 1<<20, 16)
					t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
					base := mpool.MustNewZero()
					t.Cleanup(func() { mpool.DeleteMPool(base) })
					t.Cleanup(func() { require.Zero(t, base.CurrNB()) })
					mp := state.view(t, base)
					source := newAccountedTestVector(t, types.T_int64.ToType(), mp)
					t.Cleanup(func() { source.Free(mp) })
					vec := source
					for row := range rows + 2 {
						require.NoError(t, AppendFixed(vec, int64(row), row == 2, mp))
					}
					require.NoError(t, vec.PreExtendGrouping(3, mp))
					vec.GetGrouping().Add(1)
					if decoded {
						data, err := vec.MarshalBinary()
						require.NoError(t, err)
						var grouping bytes.Buffer
						require.NoError(t, vec.MarshalGroupingTo(&grouping))
						restored := newAccountedTestVector(t, types.T_int64.ToType(), mp)
						t.Cleanup(func() { restored.Free(mp) })
						require.NoError(t, restored.UnmarshalBinaryWithCopy(data, mp))
						require.NoError(t, restored.UnmarshalGroupingFromReader(&grouping, grouping.Len(), mp))
						vec = restored
					}
					require.Equal(t, 1, vec.GetNulls().GetBitmap().CapWords())
					require.Equal(t, 1, vec.GetGrouping().GetBitmap().CapWords())
					sels := make([]int64, rows)
					var mask bitmap.Bitmap
					mask.InitWithSize(int64(rows + 1))
					var offset uint64
					if mode == "offset" {
						offset = 1
					}
					for i := range sels {
						sels[i] = int64(i + 1)
						mask.Add(uint64(i+1) - offset)
					}
					want := append([]int64(nil), MustFixedColNoTypeCheck[int64](vec)[1:rows+1]...)
					before := state.account.Snapshot()
					if mode == "slice" {
						require.NoError(t, vec.Shrink(sels, false, mp))
					} else {
						require.NoError(t, vec.ShrinkByMask(&mask, false, offset, mp))
					}
					require.Equal(t, rows, vec.Length())
					require.Equal(t, want, MustFixedColNoTypeCheck[int64](vec))
					require.Equal(t, []uint64{1}, vec.GetNulls().ToArray())
					require.Equal(t, []uint64{0}, vec.GetGrouping().ToArray())
					require.NoError(t, vec.GetNulls().GetBitmap().Validate())
					require.NoError(t, vec.GetGrouping().GetBitmap().Validate())
					require.Equal(t, before.Used, state.account.Snapshot().Used)
					require.Equal(t, before.Peak, state.account.Snapshot().Peak)
				})
			}
		}
	}
}

func TestVectorAllocationAccountDupPreservesSparseBitmapRowDomain(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 8<<20, 16)
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
	sels := make([]int64, 129)
	for i := range sels {
		sels[i] = int64(i + 1)
	}
	require.NotPanics(t, func() {
		require.NoError(t, dup.Shrink(sels, false, mp))
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
		mp := state.view(t, mpool.MustNewZero())
		vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
		mp := state.view(t, mpool.MustNewZero())
		vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
		mp := state.view(t, mpool.MustNewZero())
		vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
	require.GreaterOrEqual(t, after.Peak, before.Peak)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())

	var goScratch []byte
	require.NoError(t, vec.ShuffleWithBuf([]int64{6, 5, 4, 3, 2, 1, 0}, mp, &goScratch))
	require.Nil(t, goScratch)
	require.Equal(t, []int64{63, 128, 2, 1, 64, 1, 129}, MustFixedColWithTypeCheck[int64](vec))

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapGrowthIsUnaccounted(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
	vec.GetNulls().Add(0)
	require.NoError(t, vec.PreExtendNulls(1024, mp))
	require.True(t, vec.GetNulls().Contains(0))
	require.Zero(t, state.account.Snapshot().Used)

	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountBitmapCopyDecode(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 32)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 128; i++ {
		require.NoError(t, AppendFixed(source, int64(i), i%7 == 0, mp))
	}
	source.GetGrouping().Add(2, 9, 64)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)

	copied := newAccountedTestVector(t, types.T_int64.ToType(), view)
	require.NoError(t, copied.UnmarshalBinaryWithCopy(encoded, view))
	require.Equal(t, source.Length(), copied.Length())
	require.True(t, copied.GetNulls().IsSame(source.GetNulls()))
	require.Positive(t, state.account.Snapshot().Used)
	copied.Free(view)
	require.Zero(t, state.account.Snapshot().Used)

	fromReader := newAccountedTestVector(t, types.T_int64.ToType(), view)
	require.NoError(t, fromReader.UnmarshalWithReader(bytes.NewReader(encoded), view))
	require.Equal(t, source.Length(), fromReader.Length())
	require.True(t, fromReader.GetNulls().IsSame(source.GetNulls()))
	fromReader.Free(view)

	duplicate, err := source.Dup(view)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)
	require.True(t, duplicate.GetNulls().IsSame(source.GetNulls()))
	require.True(t, duplicate.GetGrouping().IsSame(source.GetGrouping()))
	duplicate.Free(view)

	window, err := source.CloneWindow(1, 65, view)
	require.NoError(t, err)
	require.Equal(t, 64, window.Length())
	require.True(t, window.IsNull(6))
	require.True(t, window.IsNull(13))
	window.Free(view)

	rollup := NewVec(types.T_int64.ToType(), mp)
	rollup.SetLength(128)
	rollup.GetGrouping().AddRange(0, 128)
	rollup.ToConst()
	rollupCopy, err := rollup.Dup(view)
	require.NoError(t, err)
	require.True(t, rollupCopy.IsConstNull())
	require.True(t, rollupCopy.GetGrouping().IsSame(rollup.GetGrouping()))
	rollupCopy.Free(view)
	rollup.Free(mp)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountViewAndDeepCopy(t *testing.T) {
	stateA := newTestVectorAllocationAccount(t, 1<<20, 32)
	stateB := newTestVectorAllocationAccount(t, 1<<20, 32)
	mp := mpool.MustNewZero()
	viewA := stateA.view(t, mp)
	viewB := stateB.view(t, mp)

	source := newAccountedTestVector(t, types.T_varchar.ToType(), viewA)
	for _, value := range [][]byte{
		[]byte("first value that is not inline"),
		[]byte("second value that is not inline"),
		[]byte("third value that is not inline"),
	} {
		require.NoError(t, AppendBytes(source, value, false, viewA))
	}
	sourceUsed := stateA.account.Snapshot().Used

	// A window is a read-only reference: it allocates and charges nothing.
	view, err := source.Window(1, 3)
	require.NoError(t, err)
	require.True(t, view.IsReadOnly())
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)
	view.Free(nil)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)

	dup, err := source.Dup(viewA)
	require.NoError(t, err)
	require.Greater(t, stateA.account.Snapshot().Used, sourceUsed)
	dup.Free(viewA)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)

	crossOwner, err := source.Dup(viewB)
	require.NoError(t, err)
	require.Equal(t, sourceUsed, stateA.account.Snapshot().Used)
	require.NotZero(t, stateB.account.Snapshot().Used)
	crossOwner.Free(viewB)
	require.Zero(t, stateB.account.Snapshot().Used)

	window, err := source.CloneWindow(1, 3, viewB)
	require.NoError(t, err)
	require.Equal(t, source.GetBytesAt(1), window.GetBytesAt(0))
	require.Equal(t, source.GetBytesAt(2), window.GetBytesAt(1))
	require.NotZero(t, stateB.account.Snapshot().Used)
	window.Free(viewB)
	require.Zero(t, stateB.account.Snapshot().Used)

	compact, err := source.CloneToFlatCompact(viewB)
	require.NoError(t, err)
	require.Equal(t, source.Length(), compact.Length())
	require.NotZero(t, stateB.account.Snapshot().Used)
	compact.Free(viewB)
	require.Zero(t, stateB.account.Snapshot().Used)

	source.Free(viewA)
	finalizeTestVectorAllocationAccount(t, stateA)
	finalizeTestVectorAllocationAccount(t, stateB)
}

func TestWindowPreservesGroupingProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		source := NewVec(typ, mp)
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
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixedList(source, []int64{0, 1, 2, 3}, []bool{false, true, false, true}, mp))
	source.GetGrouping().Add(1, 2)

	window, err := source.Window(1, 4)
	require.NoError(t, err)
	require.True(t, window.GetGrouping().Contains(0))
	require.True(t, window.GetGrouping().Contains(1))
	require.False(t, window.GetGrouping().Contains(2))
	require.True(t, window.IsNull(0))
	require.False(t, window.IsNull(1))
	require.True(t, window.IsNull(2))
	// The window borrows source data; its own bitmaps are Go-owned.
	require.Zero(t, state.account.Snapshot().Used)
	window.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestIsGroupingRejectsOutOfRangeBits(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		mp := mpool.MustNewZero()
		vec := NewVec(typ, mp)
		vec.SetLength(1)
		vec.GetGrouping().Add(5)
		require.False(t, vec.IsGrouping())
		vec.Free(mp)
	}
}

func TestConstSetPreservesSelectedGrouping(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()} {
		source := NewVec(typ, mp)
		if typ.IsVarlen() {
			require.NoError(t, AppendBytesList(source, [][]byte{[]byte("ordinary"), []byte("grouping")}, nil, mp))
		} else {
			require.NoError(t, AppendFixedList(source, []int64{1, 2}, nil, mp))
		}
		source.GetGrouping().Add(1)
		destination := NewVec(typ, mp)
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
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(
		t,
		AppendBytes(
			source,
			bytes.Repeat([]byte("payload"), 32),
			false,
			mp,
		),
	)

	_, err := source.Dup(state.view(t, mp))
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountCrossPoolFreeAndSeal(t *testing.T) {
	t.Run("cross pool rejected", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 1<<20, 8)
		ownerPool := mpool.MustNewZero()
		otherPool := mpool.MustNewZero()
		view := state.view(t, ownerPool)
		vec := newAccountedTestVector(t, types.T_int64.ToType(), view)
		require.Error(t, vec.PreExtend(128, otherPool))
		require.Zero(t, state.account.Snapshot().Used)
		require.NoError(t, vec.PreExtend(128, view))
		require.NotZero(t, state.account.Snapshot().Used)
		require.Panics(t, func() { vec.Free(otherPool) })

		vec.Free(view)
		require.Zero(t, state.account.Snapshot().Used)
		finalizeTestVectorAllocationAccount(t, state)
	})

	t.Run("sealed before allocation", func(t *testing.T) {
		state := newTestVectorAllocationAccount(t, 1<<20, 8)
		mp := state.view(t, mpool.MustNewZero())
		vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
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
	mp := state.view(t, mpool.MustNewZero())
	rng := rand.New(rand.NewSource(26459))

	fixed := newAccountedTestVector(t, types.T_int64.ToType(), mp)
	var fixedExpected []int64
	for i := 0; i < 2_000; i++ {
		value := rng.Int63()
		fixedExpected = append(fixedExpected, value)
		require.NoError(t, AppendFixed(fixed, value, false, mp))
	}
	require.Equal(t, fixedExpected, MustFixedColNoTypeCheck[int64](fixed))

	varlen := newAccountedTestVector(t, types.T_varchar.ToType(), mp)
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

	selected := newAccountedTestVector(t, types.T_varchar.ToType(), mp)
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
	base := mpool.MustNewZero()
	mp := state.view(t, base)

	for _, typ := range []types.Type{
		types.T_int32.ToType(),
		types.T_varchar.ToType(),
	} {
		t.Run(typ.String(), func(t *testing.T) {
			source := NewVec(typ, base)
			for i := range 6 {
				if typ.IsVarlen() {
					require.NoError(t, AppendBytes(source, []byte{byte('a' + i)}, false, base))
				} else {
					require.NoError(t, AppendFixed(source, int32(i), false, base))
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
					dst := newAccountedTestVector(t, typ, mp)
					require.NoError(t, test.run(dst))
					for row, want := range test.want {
						require.Equal(t, want, dst.GetGrouping().Contains(uint64(row)))
					}
					require.Positive(t, state.account.Snapshot().Used)
					dst.Free(mp)
					require.Zero(t, state.account.Snapshot().Used)
				})
			}
			source.Free(base)
		})
	}
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAllocationAccountDecodeCopyAndReader(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(
		t,
		AppendBytes(source, []byte("decoded payload that is not inline"), false, mp),
	)
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	view := state.view(t, mp)

	copied := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	require.NoError(t, copied.UnmarshalBinaryWithCopy(encoded, view))
	require.Equal(t, source.GetBytesAt(0), copied.GetBytesAt(0))
	require.Equal(t, uint64(2), state.registry.LiveAllocationMetadata())
	require.ErrorIs(
		t,
		copied.UnmarshalBinaryWithCopy(encoded, view),
		mpool.ErrAllocationAccountInvalid,
	)
	require.ErrorIs(
		t,
		copied.UnmarshalBinary(encoded),
		mpool.ErrAllocationAccountInvalid,
	)
	copied.Free(view)

	readerDecoded := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	require.NoError(t, readerDecoded.UnmarshalWithReader(bytes.NewReader(encoded), view))
	require.Positive(t, state.account.Snapshot().Used)
	require.Equal(t, source.GetBytesAt(0), readerDecoded.GetBytesAt(0))
	readerDecoded.Free(view)

	short := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	dataHeader := 1 + types.TSize + 4 + 4
	require.Error(
		t,
		short.UnmarshalWithReader(
			bytes.NewReader(encoded[:dataHeader+1]),
			view,
		),
	)
	require.Zero(t, state.account.Snapshot().Used)
	short.Free(view)

	source.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestVectorAccountedReaderRejectsMalformedWire(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 16)
	mp := mpool.MustNewZero()
	source := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, AppendFixed(source, int64(7), true, mp))
	encoded, err := source.MarshalBinary()
	require.NoError(t, err)
	view := state.view(t, mp)

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
			decoded := newAccountedTestVector(t, types.T_int64.ToType(), view)
			require.NotPanics(t, func() {
				require.Error(t, decoded.UnmarshalWithReader(bytes.NewReader(data), view))
			})
			decoded.Free(view)
			require.Zero(t, state.account.Snapshot().Used)

			copied := newAccountedTestVector(t, types.T_int64.ToType(), view)
			require.NotPanics(t, func() {
				require.Error(t, copied.UnmarshalBinaryWithCopy(data, view))
			})
			copied.Free(view)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
	for end := range encoded {
		accounted := newAccountedTestVector(t, types.T_int64.ToType(), view)
		require.NotPanics(t, func() {
			require.Error(t, accounted.UnmarshalBinaryWithCopy(encoded[:end], view))
		})
		accounted.Free(view)
		require.Zero(t, state.account.Snapshot().Used)

		unaccounted := NewVec(types.T_int64.ToType(), mp)
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
	view := state.view(b, mp)

	b.Run("unaccounted-fixed-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewVec(types.T_int64.ToType(), mp)
			if err := vec.PreExtend(rows, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(view)
		}
	})
	b.Run("accounted-fixed-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewVec(types.T_int64.ToType(), mp)
			if err := vec.PreExtend(rows, view); err != nil {
				b.Fatal(err)
			}
			vec.Free(view)
		}
	})
	b.Run("unaccounted-varlen-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewVec(types.T_varchar.ToType(), mp)
			if err := vec.PreExtendWithArea(rows, 1<<20, mp); err != nil {
				b.Fatal(err)
			}
			vec.Free(view)
		}
	})
	b.Run("accounted-varlen-preextend-free", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := NewVec(types.T_varchar.ToType(), mp)
			if err := vec.PreExtendWithArea(rows, 1<<20, view); err != nil {
				b.Fatal(err)
			}
			vec.Free(view)
		}
	})
	b.Run("accounted-fixed-reset-reuse", func(b *testing.B) {
		vec := newAccountedTestVector(b, types.T_int64.ToType(), view)
		if err := vec.PreExtend(rows, view); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec.ResetWithSameType()
		}
		b.StopTimer()
		vec.Free(view)
	})
	finalizeTestVectorAllocationAccount(b, state)
}

func BenchmarkVectorElementAccounting(b *testing.B) {
	const rows = 8192
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(b, 1<<40, 64)
	source := NewVec(types.T_int64.ToType(), mp)
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
			destination := NewVec(types.T_int64.ToType(), mp)
			mp := mp
			if accounted {
				mp = state.view(b, mp)
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
			destination := NewVec(types.T_int64.ToType(), mp)
			mp := mp
			if accounted {
				mp = state.view(b, mp)
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
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_int64.ToType(), mp)
	err := vec.PreExtend(1, mp)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity))
	vec.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestDetachedBufferPreservesAllocationProvenance(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	mp := mpool.MustNewZero()
	view := state.view(t, mp)
	source := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	require.NoError(t, AppendBytes(
		source,
		[]byte("detached allocation payload that uses the vector area"),
		false,
		view,
	))
	used := state.account.Snapshot().Used
	require.Positive(t, used)

	data := DetachVectorData(source)
	area := DetachVectorArea(source)
	require.Positive(t, data.Capacity())
	require.Positive(t, area.Capacity())
	source.Free(view)
	require.Equal(t, used, state.account.Snapshot().Used)

	// The detached allocations keep their own charge; reattaching never
	// relabels memory, and either buffer may serve either backing of a vector
	// of the same pool. A vector of another pool cannot take them.
	require.False(t, data.CanAttachTo(NewVec(types.T_varchar.ToType(), mp), DetachedDataBuffer))
	destination := newAccountedTestVector(t, types.T_varchar.ToType(), view)
	require.True(t, data.CanAttachTo(destination, DetachedDataBuffer))
	require.True(t, data.CanAttachTo(destination, DetachedAreaBuffer))
	require.NoError(t, data.AttachTo(destination, DetachedDataBuffer))
	require.NoError(t, area.AttachTo(destination, DetachedAreaBuffer))
	require.Zero(t, data.Capacity())
	require.Zero(t, area.Capacity())
	require.Equal(t, used, state.account.Snapshot().Used)

	destination.Free(view)
	require.Zero(t, state.account.Snapshot().Used)
	data.Free(mp)
	area.Free(mp)
	finalizeTestVectorAllocationAccount(t, state)
}

func TestSetTypeAndFixDataAllocationFailureIsAtomic(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 512, 4)
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_date.ToType(), mp)
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
	source := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(
		source,
		[]byte("unaccounted detached allocation payload"),
		false,
		mp,
	))
	data := DetachVectorData(source)
	area := DetachVectorArea(source)
	source.Free(mp)

	destination := NewVec(types.T_varchar.ToType(), mp)
	// Unaccounted buffers may serve either backing because there is no
	// allocation-site provenance to preserve.
	require.True(t, data.CanAttachTo(destination, DetachedAreaBuffer))
	require.NoError(t, area.AttachTo(destination, DetachedDataBuffer))
	require.NoError(t, data.AttachTo(destination, DetachedAreaBuffer))
	require.Zero(t, data.Capacity())
	require.Zero(t, area.Capacity())
	destination.Free(mp)
	require.Zero(t, mp.CurrNB())

	fixed := NewVec(types.T_date.ToType(), mp)
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
	mp := state.view(t, mpool.MustNewZero())

	vec := newAccountedTestVector(t, types.T_varchar.ToType(), mp)
	// An allocation through a view is off-heap and charged.
	owned, err := vec.allocOwned(mp, 1)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)
	mp.Free(owned)
	require.Zero(t, state.account.Snapshot().Used)
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
			view := state.view(t, mp)
			destination := newAccountedTestVector(t, typ, view)
			if typ.IsVarlen() {
				require.NoError(t, AppendBytes(destination, []byte("prefix"), false, view))
			} else {
				require.NoError(t, AppendFixed(destination, int64(1), false, view))
			}

			rollup := NewRollupConst(typ, 3, mp)
			require.NoError(t, GetUnionAllFunction(typ, view)(destination, rollup))
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
			require.NoError(t, GetUnionAllFunction(typ, view)(destination, ordinary))
			require.Equal(t, 7, destination.Length())
			require.True(t, destination.GetGrouping().Contains(5))
			require.False(t, destination.GetGrouping().Contains(4))
			require.False(t, destination.GetGrouping().Contains(6))
			ordinary.Free(mp)

			destination.Free(view)
			finalizeTestVectorAllocationAccount(t, state)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestUnmarshalBinaryRejectsOwnedDestinationWithoutLosingBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	target := NewVec(types.T_varchar.ToType(), mp)
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

func TestVectorAllocationAccountConstNullCloneReuse(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 8)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	mp := state.view(t, mpool.MustNewZero())
	typ := types.T_int64.ToType()
	dst := newAccountedTestVector(t, typ, mp)
	t.Cleanup(func() { dst.Free(mp) })
	src := NewConstNull(typ, 2, mp)
	t.Cleanup(func() { src.Free(mp) })
	require.NoError(t, dst.PreExtend(2, mp))
	before := state.account.Snapshot().Used
	require.Positive(t, before)
	require.NoError(t, src.CloneWindowTo(dst, 0, 2, mp))
	require.True(t, dst.IsConstNull())
	require.Equal(t, before, state.account.Snapshot().Used)
	require.Equal(t, uint64(1), state.registry.LiveAllocationMetadata())
	dst.ResetWithNewType(&typ)
	require.NoError(t, AppendFixed(dst, int64(7), false, mp))
	require.Equal(t, before, state.account.Snapshot().Used)
	dst.Free(mp)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	require.Zero(t, mp.CurrNB())
}

func TestAccountedMixedStringShuffleThenSort(t *testing.T) {
	state := newTestVectorAllocationAccount(t, 1<<20, 32)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
	mp := state.view(t, mpool.MustNewZero())
	vec := newAccountedTestVector(t, types.T_text.ToType(), mp)
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, AppendBytesList(vec, [][]byte{[]byte("z"), []byte("a")}, nil, mp))
	vec.GetGrouping().Add(0)
	sels := make([]int64, 65)
	for i := range sels {
		sels[i] = int64(i % 2)
	}
	require.NoError(t, vec.Shuffle(sels, mp))
	require.NoError(t, vec.InplaceSort(mp))
	for i := range 65 {
		value := string(vec.GetBytesAt(i))
		if i < 32 {
			require.Equal(t, "a", value)
		} else {
			require.Equal(t, "z", value)
		}
		require.Equal(t, value == "z", vec.GetGrouping().Contains(uint64(i)))
		require.False(t, vec.IsNull(uint64(i)))
	}
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedSortExpandsSparseNullAndGrouping(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 32)
	defer func() {
		require.Zero(t, mp.CurrNB())
		finalizeTestVectorAllocationAccount(t, state)
		mpool.DeleteMPool(mp)
	}()
	view := state.view(t, mp)
	vec := newAccountedTestVector(t, types.T_int64.ToType(), view)
	defer vec.Free(view)
	values := make([]int64, 65)
	for i := range values {
		values[i] = int64(len(values) - i)
	}
	require.NoError(t, AppendFixedList(vec, values, nil, view))
	require.NoError(t, vec.ensureNullCapacity(1, mp))
	require.NoError(t, vec.ensureGroupingCapacity(2, mp))
	vec.GetNulls().Add(0)
	vec.GetGrouping().Add(1)
	require.NoError(t, vec.InplaceSort(view))
	require.True(t, vec.IsNull(0))
	require.Equal(t, int64(65), GetFixedAtNoTypeCheck[int64](vec, 0))
	require.True(t, vec.GetGrouping().Contains(64))
	require.Equal(t, int64(64), GetFixedAtNoTypeCheck[int64](vec, 64))
}
