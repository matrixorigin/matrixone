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
	"testing"

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
