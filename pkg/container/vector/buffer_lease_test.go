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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

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

func TestBorrowedFixedVectorWindowAndClean(t *testing.T) {
	data := types.EncodeSlice([]int64{11, 22, 33, 44})
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data)
	require.NoError(t, err)
	require.Equal(t, BorrowedAlias, vec.DataBackingKind())
	// Borrowed storage is immutable and non-owning. Ownership/COW boundaries
	// must duplicate it before mutation or receiver reuse.
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
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind(), "reuse drops the alias")
	require.Nil(t, vec.GetData())
	// The lender (this test) still keeps data alive for the window.
	require.Equal(t, []int64{22, 33}, MustFixedColNoTypeCheck[int64](window))
	window.Free(nil)
	require.Equal(t, []int64{11, 22, 33, 44}, types.DecodeSlice[int64](data))
}

func TestBorrowedVectorAppendWithinCapacity(t *testing.T) {
	for _, limit := range []uint64{16, 256} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, limit, 32)
			t.Cleanup(func() {
				require.Zero(t, mp.CurrNB())
				finalizeTestVectorAllocationAccount(t, state)
				mpool.DeleteMPool(mp)
			})
			values := []int64{11, 22, 33, 0}
			data := types.EncodeSlice(values)
			lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
			require.NoError(t, err)
			t.Cleanup(lease.Release)
			vec, err := NewBorrowedFixedVector(
				types.T_int64.ToType(), 3, data[:3*8],
			)
			require.NoError(t, err)
			t.Cleanup(func() { vec.Free(mp) })
			err = AppendFixed(vec, int64(44), false, state.view(t, mp))
			if limit == 16 {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Equal(t, BorrowedAlias, vec.DataBackingKind())
				require.Equal(t, 3, vec.Length())
				require.Zero(t, state.account.Snapshot().Used)
			} else {
				require.NoError(t, err)
				require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
				require.Equal(t, []int64{11, 22, 33, 44}, MustFixedColNoTypeCheck[int64](vec))
			}
			require.Equal(t, []int64{11, 22, 33, 0}, values, "spare leased capacity is still immutable")
		})
	}
}

func TestBorrowedVectorMaterializeOwnedIsTransactional(t *testing.T) {
	values := make([]int64, mpool.MB/8+1)
	copy(values, []int64{7, 8, 9})
	data := types.EncodeSlice(values)
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data)
	require.NoError(t, err)

	tooSmall, err := mpool.NewMPool("borrowed-cow-failure", mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	require.Error(t, vec.MaterializeOwned(tooSmall))
	require.Equal(t, BorrowedAlias, vec.DataBackingKind())
	require.Equal(t, []int64{7, 8, 9}, MustFixedColNoTypeCheck[int64](vec)[:3])

	mp := mpool.MustNewZero()
	require.NoError(t, vec.MaterializeOwned(mp))
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
	require.Equal(t, []int64{7, 8, 9}, MustFixedColNoTypeCheck[int64](vec)[:3])
	require.True(t, vec.CanDetach(BackingData))
	MustFixedColNoTypeCheck[int64](vec)[0] = 70
	require.Equal(t, int64(7), values[0], "the lent backing is never written")
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestBorrowedVectorMaterializeOwnedCopiesValidityIntoAccount(t *testing.T) {
	data := types.EncodeSlice([]int64{11, 22, 33})
	dataLease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)

	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	vec, err := NewBorrowedFixedVector(
		types.T_int64.ToType(), 3, data,
	)
	require.NoError(t, err)
	vec.GetNulls().Add(1)
	dataLease.Release()

	mp := mpool.MustNewZero()
	view, err := mp.WithAccount(
		account, mpool.AllocationOwnerExternal, 1, mpool.AllocationCapacityClassDefault,
	)
	require.NoError(t, err)
	clone, err := vec.CloneWindow(0, vec.Length(), mp)
	require.NoError(t, err)
	require.False(t, clone.HasBorrowedBacking())
	require.Equal(t, []uint64{1}, clone.GetNulls().ToArray())
	clone.Free(mp)

	require.NoError(t, vec.MaterializeOwned(view))
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

func TestUnionOneBorrowedFixedValidity(t *testing.T) {
	for _, withNull := range []bool{false, true} {
		t.Run(fmt.Sprintf("null=%t", withNull), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			data := types.EncodeSlice([]int64{11, 22, 33})
			source, err := NewBorrowedFixedVector(types.T_int64.ToType(), 3, data)
			require.NoError(t, err)
			t.Cleanup(func() { source.Free(nil) })
			if withNull {
				source.GetNulls().Add(1)
			}
			destination := NewVec(types.T_int64.ToType())
			t.Cleanup(func() { destination.Free(mp) })
			require.NoError(t, destination.PreExtend(3, mp))
			for row, sel := range []int64{1, 2, 0} {
				require.NoError(t, destination.UnionOne(source, sel, mp))
				require.Equal(t, withNull && sel == 1, destination.IsNull(uint64(row)))
				if !destination.IsNull(uint64(row)) {
					require.Equal(t, []int64{11, 22, 33}[sel], GetFixedAtWithTypeCheck[int64](destination, row))
				}
			}
			require.Equal(t, []int64{11, 22, 33}, types.DecodeSlice[int64](data))
		})
	}
}

func TestBorrowedVectorResetDropsEachAlias(t *testing.T) {
	data, area := []byte{1}, []byte("long-value")
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.InstallBorrowedData(data))
	require.NoError(t, vec.InstallBorrowedArea(area))

	vec.ResetWithSameType()
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
	require.Equal(t, OwnedMPoolUnique, vec.AreaBackingKind())
	require.Nil(t, vec.GetData())
	require.Nil(t, vec.GetArea())
	require.Equal(t, []byte("long-value"), area, "dropping an alias never touches the lent bytes")
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
	require.NoError(t, vec.InstallBorrowedData(data))
	require.NoError(t, vec.InstallBorrowedArea(area))
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

	decoded := new(Vector)
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

func TestCloneWindowToConstNullDropsDataAlias(t *testing.T) {
	for _, sharedArea := range []bool{false, true} {
		t.Run(fmt.Sprintf("sharedArea=%v", sharedArea), func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_int64.ToType()
			if sharedArea {
				typ = types.T_varchar.ToType()
			}
			backing := make([]byte, typ.TypeSize()+32)
			dst := NewOffHeapVecWithType(typ)
			t.Cleanup(func() { dst.Free(mp) })
			require.NoError(t, dst.InstallBorrowedData(backing[:typ.TypeSize()]))
			if sharedArea {
				require.NoError(t, dst.InstallBorrowedArea(backing[typ.TypeSize():]))
			}
			src := NewConstNull(typ, 1, mp)
			t.Cleanup(func() { src.Free(mp) })
			require.NoError(t, src.CloneWindowTo(dst, 0, 1, nil))
			require.True(t, dst.IsConstNull())
			require.Equal(t, OwnedMPoolUnique, dst.DataBackingKind())
			require.Nil(t, dst.GetData())
			if sharedArea {
				require.Equal(t, BorrowedAlias, dst.AreaBackingKind(), "dropping the data alias keeps the area alias")
				require.Equal(t, backing[typ.TypeSize():], dst.GetArea())
			} else {
				dst.ResetWithNewType(&typ)
				require.NoError(t, AppendFixed(dst, int64(7), false, mp))
				require.Equal(t, int64(7), GetFixedAtNoTypeCheck[int64](dst, 0))
			}
			dst.Free(mp)
			require.Equal(t, make([]byte, len(backing)), backing, "the lent backing is never written")
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestPrepareDataForInplaceMutation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	values := []int64{11, 22, 33}
	data := types.EncodeSlice(values)
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	defer lease.Release()
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data)
	require.NoError(t, err)
	defer vec.Free(mp)

	require.Error(t, vec.PrepareDataForInplaceMutation(nil))
	require.Equal(t, BorrowedAlias, vec.DataBackingKind())
	require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](vec))
	require.NoError(t, vec.PrepareDataForInplaceMutation(mp))
	require.Equal(t, OwnedMPoolUnique, vec.DataBackingKind())
	MustFixedColNoTypeCheck[int64](vec)[0] = 99
	require.Equal(t, []int64{11, 22, 33}, values)
}

func TestPrepareDataForInplaceMutationAccountRejection(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 16, 32)
	defer func() {
		require.Zero(t, mp.CurrNB())
		finalizeTestVectorAllocationAccount(t, state)
		mpool.DeleteMPool(mp)
	}()
	values := []int64{11, 22, 33}
	data := types.EncodeSlice(values)
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	defer lease.Release()
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data)
	require.NoError(t, err)
	defer vec.Free(mp)

	require.ErrorIs(t, vec.PrepareDataForInplaceMutation(state.view(t, mp)), mpool.ErrAllocationAccountCapacity)
	require.Equal(t, BorrowedAlias, vec.DataBackingKind())
	require.Equal(t, values, MustFixedColNoTypeCheck[int64](vec))
	require.Zero(t, state.account.Snapshot().Used)
}

func TestFixedSortCompactKeepsOwnedStorage(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	require.NoError(t, AppendFixedList(vec, []int64{3, 1, 3, 2}, nil, mp))
	require.NoError(t, vec.InplaceSortAndCompact(mp))
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](vec))
	require.True(t, vec.GetSorted())
	require.NoError(t, AppendFixed(vec, int64(4), false, mp))
	require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColNoTypeCheck[int64](vec))
}

// Every copying API applied to an aliased vector yields a result that is
// independent of the lent backing: it survives the lender reusing the bytes.
func TestCopiesOfBorrowedVectorsDoNotAliasTheLender(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	long := "a payload that is longer than the inline varlena capacity"
	desc := make([]types.Varlena, 2)
	desc[0].SetOffsetLen(0, uint32(len(long)))
	desc[1][0] = 5
	copy(desc[1][1:], "short")
	data := types.EncodeSlice(desc)
	area := []byte(long)
	source := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, source.InstallBorrowedData(data))
	require.NoError(t, source.InstallBorrowedArea(area))
	source.SetLength(2)
	defer source.Free(mp)

	copies := map[string]func() (*Vector, error){
		"Dup":                func() (*Vector, error) { return source.Dup(mp) },
		"CloneWindow":        func() (*Vector, error) { return source.CloneWindow(0, 2, mp) },
		"CloneToFlatCompact": func() (*Vector, error) { return source.CloneToFlatCompact(mp) },
		"UnionBatch": func() (*Vector, error) {
			w := NewOffHeapVecWithType(types.T_varchar.ToType())
			return w, w.UnionBatch(source, 0, 2, nil, mp)
		},
	}
	results := make(map[string]*Vector, len(copies))
	for name, copyFn := range copies {
		w, err := copyFn()
		require.NoError(t, err, name)
		require.False(t, w.HasBorrowedBacking(), name)
		results[name] = w
	}
	// The lender reuses its bytes.
	for i := range data {
		data[i] = 0xFF
	}
	for i := range area {
		area[i] = 0xFF
	}
	for name, w := range results {
		require.Equal(t, long, w.GetStringAt(0), name)
		require.Equal(t, "short", w.GetStringAt(1), name)
		w.Free(mp)
	}
}

func TestWindowCopyingOwnedPartsKeepsOnlyLentAliases(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	first := "first payload longer than twenty three bytes"
	second := "second payload longer than twenty three bytes"
	area := []byte(first + second)
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.PreExtend(2, mp))
	vec.SetLength(2)
	desc := MustFixedColNoTypeCheck[types.Varlena](vec)
	desc[0].SetOffsetLen(0, uint32(len(first)))
	desc[1].SetOffsetLen(uint32(len(first)), uint32(len(second)))
	require.NoError(t, vec.InstallBorrowedArea(area))

	view, err := vec.WindowCopyingOwnedParts(0, 2, mp)
	require.NoError(t, err)
	require.Equal(t, OwnedMPoolUnique, view.DataBackingKind(), "owned descriptors are copied")
	require.Equal(t, BorrowedAlias, view.AreaBackingKind(), "the lent area stays aliased")
	require.Same(t, &area[0], &view.GetArea()[0])
	vec.Free(mp)
	require.Equal(t, first, view.GetStringAt(0))
	require.Equal(t, second, view.GetStringAt(1))
	view.Free(mp)
}
