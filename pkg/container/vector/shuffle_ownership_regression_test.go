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
	"sync"
	"sync/atomic"
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
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
	require.NoError(t, err)
	lease.Release()
	lease = nil
	t.Cleanup(func() { v.Free(mp) })
	view, err := v.Window(0, 4)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })
	require.NoError(t, v.Shuffle([]int64{3, 1}, mp))
	require.Equal(t, []int64{4, 2}, MustFixedColWithTypeCheck[int64](v))
	require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
	// Preserve a handle to the orphaned allocation for cleanup on the broken baseline.
	replacement := v.GetData()
	orphan := v.DataBackingKind() == BorrowedLease
	v.Free(mp)
	remaining := mp.CurrNB()
	if orphan {
		mp.Free(replacement)
	}
	require.Zero(t, remaining, "replacement must be freed by Vector.Free")
	require.Zero(t, releases, "retained view still owns the original backing")
	view.Free(mp)
	require.Equal(t, 1, releases)
	require.False(t, orphan, "replacement must not retain the old data lease")
}

func TestShuffleBorrowedSortedContiguous(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			mp := mpool.MustNewZero()
			data := types.EncodeSlice([]int64{1, 2, 3, 4})
			releases := 0
			lease, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
			require.NoError(t, err)
			v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
			lease.Release()
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
			require.NotEqual(t, BorrowedLease, v.DataBackingKind())
			require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](v))
			v.Free(mp)
			require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
			require.Zero(t, releases)
			view.Free(mp)
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
						require.NoError(t, v.InstallBorrowedData(data, lease))
						require.NoError(t, v.InstallBorrowedArea(area, lease))
						lease.Release()
						lease = nil
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
					require.NotEqual(t, BorrowedLease, v.DataBackingKind())
					v.Free(mp)
					require.Equal(t, first, view.GetStringAt(0))
					require.Equal(t, second, view.GetStringAt(1))
					require.Zero(t, releases)
					view.Free(mp)
					if !legacy {
						require.Equal(t, 1, releases)
					}
					require.Zero(t, mp.CurrNB())
				})
			}
		}
	}
}

// pauseRelease intercepts the vector's lease retirement without blocking other owners.
type pauseRelease struct {
	BufferLease
	armed   atomic.Bool
	entered chan struct{}
	resume  chan struct{}
}

func (l *pauseRelease) Release() {
	if l.armed.CompareAndSwap(true, false) {
		close(l.entered)
		<-l.resume
	}
	l.BufferLease.Release()
}

func TestShuffleBorrowedLeaseLifecycle(t *testing.T) {
	for _, order := range []string{"no-view", "view-first", "vector-first", "interleaved"} {
		t.Run(order, func(t *testing.T) {
			mp := mpool.MustNewZero()
			data := types.EncodeSlice([]int64{1, 2, 3, 4})
			var releases atomic.Int32
			base, err := NewRefCountedBufferLease(data, int64(cap(data)), func() { releases.Add(1) })
			require.NoError(t, err)
			initial := true
			t.Cleanup(func() {
				if initial {
					base.Release()
				}
			})
			lease := &pauseRelease{BufferLease: base, entered: make(chan struct{}), resume: make(chan struct{})}
			v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
			require.NoError(t, err)
			t.Cleanup(func() { v.Free(mp) })
			base.Release()
			initial = false
			var view *Vector
			if order != "no-view" {
				view, err = v.Window(0, 4)
				require.NoError(t, err)
				t.Cleanup(func() { view.Free(mp) })
			}
			if order == "interleaved" {
				var once sync.Once
				unblock := func() { once.Do(func() { close(lease.resume) }) }
				done := make(chan error, 1)
				lease.armed.Store(true)
				go func() { done <- v.Shuffle([]int64{3, 1}, mp) }()
				t.Cleanup(func() { unblock(); <-done })
				<-lease.entered
				require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
				view.Free(mp)
				require.Zero(t, releases.Load())
				unblock()
				err := <-done
				// Leave a completion token for failure-safe cleanup.
				done <- nil
				require.NoError(t, err)
			} else {
				require.NoError(t, v.Shuffle([]int64{3, 1}, mp))
			}
			require.Equal(t, []int64{4, 2}, MustFixedColWithTypeCheck[int64](v))
			require.Equal(t, OwnedMPoolUnique, v.DataBackingKind())
			if view != nil && order != "interleaved" {
				require.Equal(t, []int64{1, 2, 3, 4}, MustFixedColWithTypeCheck[int64](view))
				require.Zero(t, releases.Load())
				if order == "view-first" {
					view.Free(mp)
					require.EqualValues(t, 1, releases.Load())
				}
			}
			v.Free(mp)
			if order == "vector-first" {
				require.Zero(t, releases.Load())
				view.Free(mp)
			}
			require.EqualValues(t, 1, releases.Load())
			require.Zero(t, mp.CurrNB())
		})
	}
}
