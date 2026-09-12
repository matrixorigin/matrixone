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
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type nonComparableBufferLease []byte

func (l nonComparableBufferLease) Retain() bool          { return true }
func (l nonComparableBufferLease) Release()              {}
func (l nonComparableBufferLease) Bytes() []byte         { return l }
func (l nonComparableBufferLease) AccountedBytes() int64 { return int64(cap(l)) }

func TestRefCountedBufferLeaseTerminalState(t *testing.T) {
	var cleanup atomic.Int32
	lease, err := NewRefCountedBufferLease([]byte{1, 2, 3}, 64, func() {
		cleanup.Add(1)
	})
	require.NoError(t, err)
	require.Equal(t, int64(64), lease.AccountedBytes())
	require.True(t, lease.Retain())

	lease.Release()
	require.Equal(t, int32(0), cleanup.Load())
	require.Equal(t, []byte{1, 2, 3}, lease.Bytes())
	lease.Release()
	require.Equal(t, int32(1), cleanup.Load())
	require.Nil(t, lease.Bytes())
	require.False(t, lease.Retain())
	require.Panics(t, lease.Release)
}

func TestRefCountedBufferLeaseConcurrentRetainRelease(t *testing.T) {
	var cleanup atomic.Int32
	lease, err := NewRefCountedBufferLease(make([]byte, 32), 32, func() {
		cleanup.Add(1)
	})
	require.NoError(t, err)

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			require.True(t, lease.Retain())
			lease.Release()
		}()
	}
	wg.Wait()
	lease.Release()
	require.Equal(t, int32(1), cleanup.Load())
}

func TestBorrowedFixedVectorWindowAndCleanRelease(t *testing.T) {
	data := types.EncodeSlice([]int64{11, 22, 33, 44})
	var cleanup atomic.Int32
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() {
		cleanup.Add(1)
	})
	require.NoError(t, err)

	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
	require.NoError(t, err)
	lease.Release() // transfer the source reference to vec
	require.Equal(t, BorrowedLease, vec.DataBackingKind())
	// Borrowed storage has a stable retained lifetime, but it is still
	// immutable and non-owning. Legacy ownership/COW boundaries must duplicate
	// it before mutation or receiver reuse.
	require.True(t, vec.NeedDup())
	require.False(t, vec.CanDetach(BackingData))
	detached := DetachVectorData(vec)
	require.Zero(t, detached.Capacity())
	require.Equal(t, []int64{11, 22, 33, 44}, MustFixedColNoTypeCheck[int64](vec))

	window, err := vec.Window(1, 3)
	require.NoError(t, err)
	require.Equal(t, []int64{22, 33}, MustFixedColNoTypeCheck[int64](window))
	vec.CleanOnlyData()
	require.Zero(t, vec.Length())
	require.Equal(t, int32(0), cleanup.Load())
	require.Equal(t, []int64{22, 33}, MustFixedColNoTypeCheck[int64](window))
	window.Free(nil)
	require.Equal(t, int32(1), cleanup.Load())
}

func TestBorrowedVectorMaterializeOwnedIsTransactional(t *testing.T) {
	values := make([]int64, mpool.MB/8+1)
	copy(values, []int64{7, 8, 9})
	data := types.EncodeSlice(values)
	var cleanup atomic.Int32
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() {
		cleanup.Add(1)
	})
	require.NoError(t, err)
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data, lease)
	require.NoError(t, err)
	lease.Release()

	tooSmall, err := mpool.NewMPool("borrowed-cow-failure", mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	require.Error(t, vec.MaterializeOwned(tooSmall))
	require.Equal(t, BorrowedLease, vec.DataBackingKind())
	require.Equal(t, []int64{7, 8, 9}, MustFixedColNoTypeCheck[int64](vec)[:3])
	require.Equal(t, int32(0), cleanup.Load())

	mp := mpool.MustNewZero()
	require.NoError(t, vec.MaterializeOwned(mp))
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
	require.Equal(t, []int64{7, 8, 9}, MustFixedColNoTypeCheck[int64](vec)[:3])
	require.Equal(t, int32(1), cleanup.Load())
	require.True(t, vec.CanDetach(BackingData))
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBorrowedVectorMaterializeOwnedCopiesValidityIntoAccount(t *testing.T) {
	data := types.EncodeSlice([]int64{11, 22, 33})
	dataLease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	validity := []byte{0b00000101} // row one is NULL
	validityLease, err := NewRefCountedBufferLease(validity, int64(cap(validity)), nil)
	require.NoError(t, err)

	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := NewAllocationAccountSelection(
		account, mpool.AllocationOwnerExternal, 1, 2, 3, 4,
	)
	require.NoError(t, err)
	vec, err := NewBorrowedFixedVectorWithAllocation(
		types.T_int64.ToType(), 3, data, dataLease, selection,
	)
	require.NoError(t, err)
	require.NoError(t, vec.GetNulls().InstallBorrowedValidity(
		validity, 0, 3, 1, validityLease,
	))
	dataLease.Release()
	validityLease.Release()

	mp := mpool.MustNewZero()
	clone, err := vec.CloneWindow(0, vec.Length(), mp)
	require.NoError(t, err)
	require.False(t, clone.HasBorrowedBacking())
	require.Equal(t, []uint64{1}, clone.GetNulls().ToArray())
	clone.Free(mp)

	require.NoError(t, vec.MaterializeOwned(mp))
	require.False(t, vec.HasBorrowedBacking())
	require.Equal(t, []uint64{1}, vec.GetNulls().ToArray())
	require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](vec))
	require.Greater(t, account.Snapshot().Used, uint64(0))
	vec.Free(mp)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestBorrowedValidityLegacyMaterializationUsesReservedMPoolStorage(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := NewAllocationAccountSelection(
		account, mpool.AllocationOwnerExternal, 1, 2, 3, 4,
	)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	vec, err := NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), selection)
	require.NoError(t, err)
	require.NoError(t, vec.PrepareBorrowedValidity(3, mp))
	admittedBytes := mp.CurrNB()
	require.Positive(t, admittedBytes)
	require.Positive(t, account.Snapshot().Used)

	validity := []byte{0b00000101} // row one is NULL
	var releases atomic.Int32
	lease, err := NewRefCountedBufferLease(validity, int64(cap(validity)), func() {
		releases.Add(1)
	})
	require.NoError(t, err)
	require.NoError(t, vec.GetNulls().InstallBorrowedValidity(validity, 0, 3, 1, lease))
	lease.Release()
	require.True(t, vec.GetNulls().HasBorrowedValidity())

	bitmap := vec.GetNulls().GetBitmap()
	require.False(t, vec.GetNulls().HasBorrowedValidity())
	require.Equal(t, int32(1), releases.Load())
	require.True(t, bitmap.HasExternalStorage())
	require.Equal(t, []uint64{1}, vec.GetNulls().ToArray())
	require.Equal(t, admittedBytes, mp.CurrNB(), "legacy materialization must not allocate outside admission")

	vec.Free(mp)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, mp.CurrNB())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestRetainedReadonlyViewWithMPCopiesOwnedDescriptorsAndRetainsArea(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	first := []byte("first payload longer than twenty three bytes")
	second := []byte("second payload longer than twenty three bytes")
	area := append(append([]byte(nil), first...), second...)
	descriptors := MustFixedColNoTypeCheck[types.Varlena](vec)
	descriptors[0].SetOffsetLen(0, uint32(len(first)))
	descriptors[1].SetOffsetLen(uint32(len(first)), uint32(len(second)))
	lease, err := NewRefCountedBufferLease(area, int64(cap(area)), nil)
	require.NoError(t, err)
	require.NoError(t, vec.InstallBorrowedArea(area, lease))
	lease.Release()

	dataPointer := uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetData())))
	areaPointer := uintptr(unsafe.Pointer(unsafe.SliceData(vec.GetArea())))
	view, err := vec.RetainedReadonlyViewWithMP(mp)
	require.NoError(t, err)
	require.NotEqual(t, dataPointer, uintptr(unsafe.Pointer(unsafe.SliceData(view.GetData()))))
	require.Equal(t, areaPointer, uintptr(unsafe.Pointer(unsafe.SliceData(view.GetArea()))))
	require.Equal(t, BorrowedLease, view.AreaBackingKind())
	require.Equal(t, OwnedMPoolUnique, view.DataBackingKind())

	vec.Free(mp)
	require.Equal(t, string(first), view.GetStringAt(0))
	require.Equal(t, string(second), view.GetStringAt(1))
	require.NotNil(t, lease.Bytes())
	view.Free(mp)
	require.Nil(t, lease.Bytes())
	require.Zero(t, mp.CurrNB())
}

func TestBorrowedVectorResetReleasesEachBacking(t *testing.T) {
	dataLease, err := NewRefCountedBufferLease([]byte{1}, 1, nil)
	require.NoError(t, err)
	areaLease, err := NewRefCountedBufferLease([]byte("long-value"), 10, nil)
	require.NoError(t, err)
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.InstallBorrowedData([]byte{1}, dataLease))
	require.NoError(t, vec.InstallBorrowedArea([]byte("long-value"), areaLease))
	dataLease.Release()
	areaLease.Release()

	vec.ResetWithSameType()
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
	require.Equal(t, OwnedMPoolUnique, vec.AreaBackingKind())
	require.Nil(t, dataLease.Bytes())
	require.Nil(t, areaLease.Bytes())
}

func TestBorrowedAccountedBytesDoesNotCompareNonComparableLease(t *testing.T) {
	lease := nonComparableBufferLease(make([]byte, 0, 7))
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.InstallBorrowedData(lease, lease))
	require.NoError(t, vec.InstallBorrowedArea(lease, lease))
	require.Equal(t, int64(14), vec.BorrowedAccountedBytes(),
		"unknown lease identity must be accounted conservatively")
	vec.Free(nil)
}

func TestBorrowedVarlenMarshalUsesCanonicalCompactedArea(t *testing.T) {
	first := []byte("first payload longer than twenty three bytes")
	second := []byte("second payload longer than twenty three bytes")
	area := append([]byte("unused-prefix"), first...)
	secondOffset := len(area)
	area = append(area, second...)
	area = append(area, []byte("unused-suffix")...)
	descriptors := make([]types.Varlena, 3)
	descriptors[0].SetOffsetLen(uint32(len("unused-prefix")), uint32(len(first)))
	descriptors[1][0] = 5
	copy(descriptors[1][1:], "short")
	descriptors[2].SetOffsetLen(uint32(secondOffset), uint32(len(second)))
	data := types.EncodeSlice(descriptors)
	dataLease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	areaLease, err := NewRefCountedBufferLease(area, int64(cap(area)), nil)
	require.NoError(t, err)
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.InstallBorrowedData(data, dataLease))
	require.NoError(t, vec.InstallBorrowedArea(area, areaLease))
	dataLease.Release()
	areaLease.Release()
	vec.SetLength(3)
	vec.GetNulls().Add(2)

	plan, err := vec.PrepareMarshalBinary()
	require.NoError(t, err)
	fullAreaPlanSize := 1 + types.TSize + 4 + 4 + len(data) + 4 + len(area) + 4 + vec.GetNulls().MarshalSize() + 1
	require.Equal(t, fullAreaPlanSize-len(area)+len(first), plan.Size(),
		"unused prefix/suffix and the NULL row payload must not enter canonical wire bytes")
	encoded, err := vec.MarshalBinary()
	require.NoError(t, err)
	var streamed bytes.Buffer
	require.NoError(t, plan.MarshalTo(&streamed))
	require.Equal(t, encoded, streamed.Bytes())

	decoded := NewVecFromReuse()
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Equal(t, string(first), decoded.GetStringAt(0))
	require.Equal(t, "short", decoded.GetStringAt(1))
	require.True(t, decoded.IsNull(2))
	require.Equal(t, first, decoded.GetArea())
	decoded.Free(nil)
	vec.Free(nil)
	require.Nil(t, dataLease.Bytes())
	require.Nil(t, areaLease.Bytes())
}
