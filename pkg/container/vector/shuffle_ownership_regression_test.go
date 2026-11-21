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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestShuffleBorrowedDataRetiresOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	data := types.EncodeSlice([]int64{1, 2, 3, 4})
	releases := 0
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
	require.NoError(t, err)
	t.Cleanup(func() {
		if lease != nil {
			lease.Release()
		}
	})
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data)
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(mp) })
	view, err := v.Window(0, 4)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })
	require.NoError(t, v.Shuffle([]int64{3, 1}, mp))
	require.Equal(t, []int64{4, 2}, MustFixedColWithTypeCheck[int64](v))
	require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
	// Preserve a handle to the orphaned allocation for cleanup on the broken baseline.
	replacement := v.GetData()
	orphan := v.DataBackingKind() == BorrowedAlias
	v.Free(mp)
	remaining := mp.CurrNB()
	if orphan {
		mp.Free(replacement)
	}
	require.Zero(t, remaining, "replacement must be freed by Vector.Free")
	view.Free(mp)
	require.Zero(t, releases, "a vector never releases its lender's lease")
	lease.Release()
	lease = nil
	require.Equal(t, 1, releases, "the lender's release is the last one")
	require.False(t, orphan, "replacement must not keep aliasing the old data")
}

func TestShuffleBorrowedSortedContiguous(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			mp := mpool.MustNewZero()
			data := types.EncodeSlice([]int64{1, 2, 3, 4})
			releases := 0
			lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
			require.NoError(t, err)
			v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data)
			require.NoError(t, err)
			t.Cleanup(func() { v.Free(mp) })
			view, err := v.Window(0, 4)
			require.NoError(t, err)
			t.Cleanup(func() { view.Free(mp) })
			v.SetSorted(true)
			sels := []int64{0, 1, 2, 3}
			if buffered {
				var buf []byte
				err = v.ShuffleWithBuf(sels, mp, &buf)
			} else {
				err = v.Shuffle(sels, mp)
			}
			require.NoError(t, err)
			require.True(t, v.GetSorted())
			require.NotEqual(t, BorrowedAlias, v.DataBackingKind())
			require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](v))
			v.Free(mp)
			require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
			view.Free(mp)
			require.Zero(t, releases, "a vector never releases its lender's lease")
			lease.Release()
			require.Equal(t, 1, releases)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestShuffleVarlenSharedLeaseAndLegacyAlias(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, buffered := range []bool{false, true} {
			for _, identity := range []bool{false, true} {
				t.Run(fmt.Sprintf("legacy=%v/buffered=%v/identity=%v", legacy, buffered, identity), func(t *testing.T) {
					mp := mpool.MustNewZero()
					first, second := "first payload longer than twenty three bytes", "second payload longer than twenty three bytes"
					area := []byte(first + second)
					desc := make([]types.Varlena, 2)
					desc[0].SetOffsetLen(0, uint32(len(first)))
					desc[1].SetOffsetLen(uint32(len(first)), uint32(len(second)))
					data := types.EncodeSlice(desc)
					v := NewOffHeapVecWithType(types.T_varchar.ToType())
					t.Cleanup(func() { v.Free(mp) })
					releases := 0
					if legacy {
						v.data, v.area = data, area
						v.cantFreeData, v.cantFreeArea = true, true
					} else {
						lease, err := NewRefCountedBufferLease(data, int64(cap(data)+cap(area)), func() { releases++ })
						require.NoError(t, err)
						t.Cleanup(func() {
							if lease != nil {
								lease.Release()
							}
						})
						require.NoError(t, v.InstallBorrowedData(data))
						require.NoError(t, v.InstallBorrowedArea(area))
					}
					v.SetLength(2)
					view, err := v.Window(0, 2)
					require.NoError(t, err)
					t.Cleanup(func() { view.Free(mp) })
					var buf []byte
					sels := []int64{1, 0}
					if identity {
						sels = []int64{0, 1}
					}
					v.SetSorted(true)
					if buffered {
						err = v.ShuffleWithBuf(sels, mp, &buf)
					} else {
						err = v.Shuffle(sels, mp)
					}
					require.NoError(t, err)
					require.Equal(t, identity, v.GetSorted())
					for i, source := range sels {
						require.Equal(t, []string{first, second}[source], v.GetStringAt(i))
					}
					require.NotEqual(t, BorrowedAlias, v.DataBackingKind())
					v.Free(mp)
					require.Equal(t, first, view.GetStringAt(0))
					require.Equal(t, second, view.GetStringAt(1))
					view.Free(mp)
					require.Zero(t, releases, "a vector never releases its lender's lease")
					require.Zero(t, mp.CurrNB())
				})
			}
		}
	}
}
