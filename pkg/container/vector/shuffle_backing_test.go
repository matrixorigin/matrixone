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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Both input forms must transfer ownership to the shuffled allocation.
func TestShuffleBackingOwnership(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		name := "owned"
		if borrowed {
			name = "borrowed"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			var v *Vector
			var releases int
			if borrowed {
				data := types.EncodeSlice([]int64{11, 22, 33, 44})
				lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
				require.NoError(t, err)
				v, err = NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
				require.NoError(t, err)
				lease.Release()
			} else {
				v = NewOffHeapVecWithType(types.T_int64.ToType())
				for _, value := range []int64{11, 22, 33, 44} {
					require.NoError(t, AppendFixed(v, value, false, mp))
				}
			}
			require.NoError(t, v.Shuffle([]int64{3, 1}, mp))
			require.Equal(t, []int64{44, 22}, MustFixedColNoTypeCheck[int64](v))
			t.Logf("backing after shuffle: %v; mpool bytes: %d", v.DataBackingKind(), mp.CurrNB())
			require.Equal(t, int64(16), mp.CurrNB())
			shuffledData := v.data
			if v.DataBackingKind() != OwnedMPoolUnique {
				t.Errorf("shuffle backing = %v, want owned", v.DataBackingKind())
			}
			v.Free(mp)
			if borrowed {
				require.Equal(t, 1, releases)
			}
			remaining := mp.CurrNB()
			t.Logf("after free: releases=%d mpool bytes=%d", releases, remaining)
			if remaining != 0 {
				t.Errorf("mpool leaked %d bytes", remaining)
				mp.Free(shuffledData) // Recover the orphan in the unfixed implementation.
			}
			mpool.DeleteMPool(mp)
		})
	}
}

type failingRetainLease struct{ releases int }

func (*failingRetainLease) Retain() bool          { return false }
func (l *failingRetainLease) Release()            { l.releases++ }
func (*failingRetainLease) Bytes() []byte         { return nil }
func (*failingRetainLease) AccountedBytes() int64 { return 0 }

func TestWindowPartialRetainFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	data := types.EncodeSlice([]types.Varlena{{}})
	dataLease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() {})
	require.NoError(t, err)
	areaLease := &failingRetainLease{}
	v := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, v.InstallBorrowedData(data, dataLease))
	dataLease.Release()
	v.area, v.areaLease, v.cantFreeArea, v.length = []byte("payload"), areaLease, true, 1
	_, err = v.Window(0, 1)
	require.Error(t, err)
	require.Equal(t, 0, areaLease.releases)
	require.Equal(t, data, v.data)
	v.Free(mp)
	require.Equal(t, 1, areaLease.releases)
	require.Zero(t, mp.CurrNB())
}

func TestDetachedBufferRejectsEmptyLease(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixed(source, int64(7), false, mp))
	buffer := DetachVectorData(source)
	defer buffer.Free(mp)
	defer source.Free(mp)
	var releases int
	lease, err := NewRefCountedBufferLease(nil, 0, func() { releases++ })
	require.NoError(t, err)
	dest := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, dest.InstallBorrowedData(nil, lease))
	lease.Release()
	require.Error(t, buffer.AttachTo(dest, DetachedDataBuffer))
	require.NotZero(t, buffer.Capacity())
	dest.Free(mp)
	require.Equal(t, 1, releases)
}

func TestShrinkBorrowedSnapshot(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	data := types.EncodeSlice([]int64{11, 22, 33})
	releases := 0
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
	require.NoError(t, err)
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
	require.NoError(t, err)
	lease.Release()
	t.Cleanup(func() { v.Free(mp) })
	view, err := v.RetainedReadonlyViewWithMP(mp)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })
	require.Error(t, v.Shrink([]int64{1, 2}, false, nil))
	require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](v))
	require.NoError(t, v.Shrink([]int64{1, 2}, false, mp))
	require.Equal(t, []int64{22, 33}, MustFixedColNoTypeCheck[int64](v))
	require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](view))
	require.Zero(t, releases)
	view.Free(mp)
	require.Equal(t, 1, releases)
}

func TestShrinkBorrowedVarlenMaskSnapshot(t *testing.T) {
	for _, negate := range []bool{false, true} {
		t.Run(fmt.Sprint(negate), func(t *testing.T) {
			mp := mpool.MustNewZero()
			first := []byte("first payload longer than twenty three bytes")
			third := []byte("third payload longer than twenty three bytes")
			area := append(append([]byte(nil), first...), third...)
			descriptors := make([]types.Varlena, 3)
			descriptors[0].SetOffsetLen(0, uint32(len(first)))
			descriptors[1][0] = 5
			copy(descriptors[1][1:], "short")
			descriptors[2].SetOffsetLen(uint32(len(first)), uint32(len(third)))
			data := types.EncodeSlice(descriptors)
			dataReleases, areaReleases := 0, 0
			dl, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { dataReleases++ })
			require.NoError(t, err)
			al, err := NewRefCountedBufferLease(area, int64(cap(area)), func() { areaReleases++ })
			require.NoError(t, err)
			v := NewOffHeapVecWithType(types.T_varchar.ToType())
			require.NoError(t, v.InstallBorrowedData(data, dl))
			dl.Release()
			require.NoError(t, v.InstallBorrowedArea(area, al))
			al.Release()
			v.SetLength(3)
			v.GetNulls().Add(1)
			t.Cleanup(func() {
				v.Free(mp)
				require.Equal(t, 1, dataReleases)
				require.Equal(t, 1, areaReleases)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			view, err := v.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			t.Cleanup(func() { view.Free(mp) })
			var mask bitmap.Bitmap
			mask.InitWithSize(2)
			mask.AddMany([]uint64{0, 1})
			require.Error(t, v.ShrinkByMask(&mask, negate, 1, nil))
			require.Equal(t, string(first), v.GetStringAt(0))
			require.True(t, v.IsNull(1))
			require.NoError(t, v.ShrinkByMask(&mask, negate, 1, mp))
			if negate {
				require.Equal(t, 1, v.Length())
				require.Equal(t, string(first), v.GetStringAt(0))
			} else {
				require.Equal(t, 2, v.Length())
				require.True(t, v.IsNull(0))
				require.Equal(t, string(third), v.GetStringAt(1))
			}
			require.Equal(t, string(first), view.GetStringAt(0))
			require.True(t, view.IsNull(1))
			require.Equal(t, string(third), view.GetStringAt(2))
			require.Equal(t, 0, dataReleases)
			require.Equal(t, 0, areaReleases)
		})
	}
}

func TestLegacyUnmarshalRejectsBorrowedBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(source, []int64{9}, nil, mp))
	t.Cleanup(func() { source.Free(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	var encoded bytes.Buffer
	require.NoError(t, source.MarshalBinaryWithBufferV1(&encoded))
	data := types.EncodeSlice([]int64{11, 22, 33})
	releases := 0
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
	require.NoError(t, err)
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
	require.NoError(t, err)
	lease.Release()
	t.Cleanup(func() { v.Free(mp); require.Equal(t, 1, releases) })
	require.Error(t, v.UnmarshalBinaryV1(encoded.Bytes()))
	require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](v))
	require.True(t, v.HasBorrowedBacking())
	require.Zero(t, releases)
}

func TestCloneWindowWithoutPoolRejectsBorrowedDestination(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(src, []int64{9}, nil, mp))
	t.Cleanup(func() { src.Free(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	data := types.EncodeSlice([]int64{11, 22})
	releases := 0
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
	require.NoError(t, err)
	dst, err := NewBorrowedFixedVector(types.T_int64.ToType(), 2, data, lease)
	require.NoError(t, err)
	lease.Release()
	t.Cleanup(func() { dst.Free(mp); require.Equal(t, 1, releases) })
	require.Error(t, src.CloneWindowTo(dst, 0, 1, nil))
	require.Equal(t, []int64{11, 22}, MustFixedColNoTypeCheck[int64](dst))
	require.True(t, dst.HasBorrowedBacking())
	require.Zero(t, releases)
}

func TestUnmarshalWithCopyRejectsEmptyLease(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(src, []int64{9}, nil, mp))
	t.Cleanup(func() { src.Free(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	encoded, err := src.MarshalBinary()
	require.NoError(t, err)
	releases := 0
	lease, err := NewRefCountedBufferLease(nil, 0, func() { releases++ })
	require.NoError(t, err)
	dst := NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, dst.InstallBorrowedData(nil, lease))
	lease.Release()
	t.Cleanup(func() { dst.Free(mp); require.Equal(t, 1, releases) })
	require.Error(t, dst.UnmarshalBinaryWithCopy(encoded, mp))
	require.True(t, dst.HasBorrowedBacking())
	require.Zero(t, releases)
}

func TestSortBorrowedSnapshot(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint(compact), func(t *testing.T) {
			mp := mpool.MustNewZero()
			data := types.EncodeSlice([]int64{33, 11, 22})
			releases := 0
			t.Cleanup(func() { require.Equal(t, 1, releases); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
			require.NoError(t, err)
			v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
			require.NoError(t, err)
			lease.Release()
			t.Cleanup(func() { v.Free(mp) })
			view, err := v.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			t.Cleanup(func() { view.Free(mp) })
			sortVector := v.InplaceSort
			if compact {
				sortVector = v.InplaceSortAndCompact
			}
			require.Error(t, sortVector(nil))
			require.Equal(t, []int64{33, 11, 22}, MustFixedColNoTypeCheck[int64](v))
			require.NoError(t, sortVector(mp))
			require.Equal(t, []int64{11, 22, 33}, MustFixedColNoTypeCheck[int64](v))
			require.Equal(t, []int64{33, 11, 22}, MustFixedColNoTypeCheck[int64](view))
		})
	}
}
