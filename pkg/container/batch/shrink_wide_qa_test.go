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

package batch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestUniqueVectorThresholdOrderAndError(t *testing.T) {
	for _, width := range []int{1024, 1025, 4096, 4097} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			storage := make([]vector.Vector, width/2)
			input := make([]*vector.Vector, width)
			expected := make([]*vector.Vector, 0, len(storage))
			seen := make(map[*vector.Vector]bool)
			for i := range input {
				input[i] = &storage[(i*37)%len(storage)]
				if !seen[input[i]] {
					seen[input[i]] = true
					expected = append(expected, input[i])
				}
			}
			for _, fail := range []bool{false, true} {
				sentinel := errors.New("stop")
				var got []*vector.Vector
				err := forEachUniqueVector(input, func(v *vector.Vector) error {
					got = append(got, v)
					if fail && len(got) == len(expected)-1 {
						return sentinel
					}
					return nil
				})
				want := expected
				if fail {
					require.ErrorIs(t, err, sentinel)
					want = expected[:len(expected)-1]
				} else {
					require.NoError(t, err)
				}
				require.Len(t, got, len(want))
				for i := range want {
					require.Same(t, want[i], got[i])
				}
			}
		})
	}
}

func TestShrinkWideDuplicateBorrowedRetainedView(t *testing.T) {
	for _, width := range []int{1025, 4097} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			mp := mpool.MustNewZero()
			data := types.EncodeSlice([]int64{11, 22, 33})
			releases := 0
			lease, err := vector.NewRefCountedBufferLease(data, int64(cap(data)), func() { releases++ })
			require.NoError(t, err)
			vec, err := vector.NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
			lease.Release()
			require.NoError(t, err)
			bat := NewWithSize(width)
			for i := range bat.Vecs {
				bat.Vecs[i] = vec
			}
			bat.SetRowCount(3)
			t.Cleanup(func() {
				bat.Clean(mp)
				require.Zero(t, mp.CurrNB())
				require.Equal(t, 1, releases)
				mpool.DeleteMPool(mp)
			})
			view, err := vec.RetainedReadonlyViewWithMP(mp)
			require.NoError(t, err)
			t.Cleanup(func() { view.Free(mp) })
			require.NoError(t, bat.Shrink([]int64{1, 2}, false, mp))
			require.Equal(t, 2, bat.RowCount())
			require.Equal(t, []int64{22, 33}, vector.MustFixedColNoTypeCheck[int64](vec))
			require.Equal(t, []int64{11, 22, 33}, vector.MustFixedColNoTypeCheck[int64](view))
			bat.Clean(mp)
			require.Equal(t, []int64{11, 22, 33}, vector.MustFixedColNoTypeCheck[int64](view))
			require.Zero(t, releases)
		})
	}
}
