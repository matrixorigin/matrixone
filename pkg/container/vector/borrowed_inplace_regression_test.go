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

package vector

import (
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBorrowedDataShrinkPreservesRetainedView(t *testing.T) {
	mp := mpool.MustNewZero()
	data := types.EncodeSlice([]int64{10, 20, 30, 40})
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(mp) })
	view, err := v.Window(0, 4)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })

	require.NoError(t, v.Shrink([]int64{1, 3}, false, mp))
	require.Equal(t, []int64{20, 40}, MustFixedColWithTypeCheck[int64](v))
	require.Equal(t, []int64{10, 20, 30, 40}, MustFixedColWithTypeCheck[int64](view))
}

func TestBorrowedInplaceMutationIsolationAndFailure(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Vector, *mpool.MPool) error
		want   []int64
	}{
		{"shrink", func(v *Vector, mp *mpool.MPool) error { return v.Shrink([]int64{1, 3}, false, mp) }, []int64{1, 2}},
		{"mask", func(v *Vector, mp *mpool.MPool) error {
			var b bitmap.Bitmap
			b.InitWithSize(4)
			b.AddMany([]uint64{1, 3})
			return v.ShrinkByMask(&b, false, 0, mp)
		}, []int64{1, 2}},
		{"sort", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSort(mp) }, []int64{1, 2, 3, 3}},
		{"compact", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSortAndCompact(mp) }, []int64{1, 2, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			values := []int64{3, 1, 3, 2}
			data := types.EncodeSlice(values)
			lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
			require.NoError(t, err)
			defer lease.Release()
			vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data, lease)
			require.NoError(t, err)
			defer vec.Free(mp)
			view, err := vec.Window(0, len(values))
			require.NoError(t, err)
			defer view.Free(mp)
			require.Error(t, tc.mutate(vec, nil))
			require.Equal(t, BorrowedLease, vec.DataBackingKind())
			require.Equal(t, []int64{3, 1, 3, 2}, MustFixedColNoTypeCheck[int64](vec))
			require.NoError(t, tc.mutate(vec, mp))
			require.Equal(t, tc.want, MustFixedColNoTypeCheck[int64](vec))
			require.Equal(t, []int64{3, 1, 3, 2}, MustFixedColNoTypeCheck[int64](view))
		})
	}
}

func TestBorrowedShrinkWithoutDataMovementDoesNotAllocate(t *testing.T) {
	values := []int64{1, 2, 3}
	data := types.EncodeSlice(values)
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	defer lease.Release()
	vec, err := NewBorrowedFixedVector(types.T_int64.ToType(), len(values), data, lease)
	require.NoError(t, err)
	defer vec.Free(nil)
	require.NoError(t, vec.Shrink([]int64{0, 1}, false, nil))
	require.Equal(t, BorrowedLease, vec.DataBackingKind())
	require.Equal(t, []int64{1, 2}, MustFixedColNoTypeCheck[int64](vec))
	require.Equal(t, []int64{1, 2, 3}, values)
}

func TestInplaceSortMovesNullAndGroupingRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	require.NoError(t, AppendFixedList(vec, []int64{2, 1, 3}, nil, mp))
	vec.GetNulls().Add(2)
	vec.GetGrouping().Add(1)
	require.NoError(t, vec.InplaceSort(mp))
	require.Equal(t, []int64{3, 1, 2}, MustFixedColNoTypeCheck[int64](vec))
	require.True(t, vec.IsNull(0))
	require.False(t, vec.IsNull(1))
	require.True(t, vec.GetGrouping().Contains(1))
	require.False(t, vec.GetGrouping().Contains(0))
}

func TestBorrowedVarlenInplacePreservesAreaAndView(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Vector, *mpool.MPool) error
		want   []string
	}{
		{"shrink", func(v *Vector, mp *mpool.MPool) error { return v.Shrink([]int64{1, 3}, false, mp) }, []string{"one-long-payload-that-is-out-of-line", "two-long-payload-that-is-out-of-line"}},
		{"mask", func(v *Vector, mp *mpool.MPool) error {
			var b bitmap.Bitmap
			b.InitWithSize(4)
			b.AddMany([]uint64{1, 3})
			return v.ShrinkByMask(&b, false, 0, mp)
		}, []string{"one-long-payload-that-is-out-of-line", "two-long-payload-that-is-out-of-line"}},
		{"sort", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSort(mp) }, []string{"one-long-payload-that-is-out-of-line", "three-long-payload-that-is-out-of-line", "three-long-payload-that-is-out-of-line", "two-long-payload-that-is-out-of-line"}},
		{"compact", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSortAndCompact(mp) }, []string{"one-long-payload-that-is-out-of-line", "three-long-payload-that-is-out-of-line", "two-long-payload-that-is-out-of-line"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			values := []string{"three-long-payload-that-is-out-of-line", "one-long-payload-that-is-out-of-line", "three-long-payload-that-is-out-of-line", "two-long-payload-that-is-out-of-line"}
			var area []byte
			desc := make([]types.Varlena, len(values))
			for i, value := range values {
				desc[i].SetOffsetLen(uint32(len(area)), uint32(len(value)))
				area = append(area, value...)
			}
			data := types.EncodeSlice(desc)
			var dataReleased, areaReleased atomic.Int32
			dataLease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { dataReleased.Add(1) })
			require.NoError(t, err)
			areaLease, err := NewRefCountedBufferLease(area, int64(cap(area)), func() { areaReleased.Add(1) })
			require.NoError(t, err)
			vec := NewOffHeapVecWithType(types.T_varchar.ToType())
			require.NoError(t, vec.InstallBorrowedData(data, dataLease))
			require.NoError(t, vec.InstallBorrowedArea(area, areaLease))
			dataLease.Release()
			areaLease.Release()
			vec.SetLength(len(values))
			view, err := vec.Window(0, len(values))
			require.NoError(t, err)
			require.Error(t, tc.mutate(vec, nil))
			require.Same(t, areaLease, vec.areaLease)
			require.NoError(t, tc.mutate(vec, mp))
			require.Same(t, areaLease, vec.areaLease)
			for i, want := range tc.want {
				require.Equal(t, want, string(vec.GetBytesAt(i)))
			}
			for i, want := range values {
				require.Equal(t, want, string(view.GetBytesAt(i)))
			}
			require.Zero(t, dataReleased.Load())
			require.Zero(t, areaReleased.Load())
			vec.Free(mp)
			require.Zero(t, areaReleased.Load())
			view.Free(mp)
			require.EqualValues(t, 1, dataReleased.Load())
			require.EqualValues(t, 1, areaReleased.Load())
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestLegacyAliasInplaceLeavesEncodedBytesUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Vector, *mpool.MPool) error
	}{
		{"shrink", func(v *Vector, mp *mpool.MPool) error { return v.Shrink([]int64{1, 3}, false, mp) }},
		{"mask", func(v *Vector, mp *mpool.MPool) error {
			var b bitmap.Bitmap
			b.InitWithSize(4)
			b.AddMany([]uint64{1, 3})
			return v.ShrinkByMask(&b, false, 0, mp)
		}},
		{"sort", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSort(mp) }},
		{"compact", func(v *Vector, mp *mpool.MPool) error { return v.InplaceSortAndCompact(mp) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			source := NewVec(types.T_int64.ToType())
			defer source.Free(mp)
			require.NoError(t, AppendFixedList(source, []int64{3, 1, 3, 2}, nil, mp))
			encoded, err := source.MarshalBinary()
			require.NoError(t, err)
			before := append([]byte(nil), encoded...)
			vec := NewVec(types.T_any.ToType())
			defer vec.Free(mp)
			require.NoError(t, vec.UnmarshalBinary(encoded))
			require.Equal(t, LegacyAlias, vec.DataBackingKind())
			require.NoError(t, tc.mutate(vec, mp))
			require.Equal(t, before, encoded)
		})
	}
}

func TestInplaceDataCOWKeepsSharedAreaLease(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	payload := []byte("payload-longer-than-the-inline-varlena-capacity")
	desc := make([]types.Varlena, 2)
	desc[0].SetOffsetLen(0, uint32(len(payload)))
	desc[1].SetOffsetLen(0, uint32(len(payload)))
	encoded := types.EncodeSlice(desc)
	backing := append(append([]byte(nil), encoded...), payload...)
	var released atomic.Int32
	lease, err := NewRefCountedBufferLease(backing, int64(cap(backing)), func() { released.Add(1) })
	require.NoError(t, err)
	vec := NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vec.InstallBorrowedData(backing[:len(encoded)], lease))
	require.NoError(t, vec.InstallBorrowedArea(backing[len(encoded):], lease))
	lease.Release()
	vec.SetLength(2)
	view, err := vec.Window(0, 2)
	require.NoError(t, err)
	require.NoError(t, vec.Shrink([]int64{1}, false, mp))
	require.Same(t, lease, vec.areaLease)
	require.Equal(t, payload, vec.GetBytesAt(0))
	require.Equal(t, payload, view.GetBytesAt(0))
	vec.Free(mp)
	require.Zero(t, released.Load())
	view.Free(mp)
	require.EqualValues(t, 1, released.Load())
	require.Zero(t, mp.CurrNB())
}
