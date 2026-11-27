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

func TestShuffleSortedness(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, typ := range []types.T{types.T_int64, types.T_varchar} {
		for _, buffered := range []bool{false, true} {
			for _, tc := range []struct {
				name                   string
				sels                   []int64
				sorted, want, nullable bool
			}{
				{"identity", []int64{0, 1, 2, 3}, true, true, false},
				{"subset", []int64{0, 2}, true, true, false},
				{"duplicates", []int64{0, 0, 3, 3}, true, true, false},
				{"permuted", []int64{0, 2, 3, 1}, true, false, false},
				{"bitmap_permuted", []int64{2, 3, 0, 1}, true, false, true},
				{"unproven", []int64{0, 1, 2, 3}, false, false, false},
				{"empty", nil, true, true, false},
				{"single", []int64{3}, true, true, false},
			} {
				t.Run(fmt.Sprintf("%s/buffered=%v/%s", typ, buffered, tc.name), func(t *testing.T) {
					v := NewVec(typ.ToType())
					defer v.Free(mp)
					values := []string{"", "a", "b", "c"}
					for i := range 4 {
						if typ == types.T_int64 {
							require.NoError(t, AppendFixed(v, int64(i*10), tc.nullable && i == 0, mp))
						} else {
							require.NoError(t, AppendBytes(v, []byte(values[i]), tc.nullable && i == 0, mp))
						}
					}
					v.SetSorted(tc.sorted)
					if buffered {
						var scratch []byte
						require.NoError(t, v.ShuffleWithBuf(tc.sels, mp, &scratch))
					} else {
						require.NoError(t, v.Shuffle(tc.sels, mp))
					}
					require.Equal(t, tc.want, v.GetSorted())
					require.Equal(t, len(tc.sels), v.Length())
					for i, pos := range tc.sels {
						require.Equal(t, tc.nullable && pos == 0, v.IsNull(uint64(i)))
						if typ == types.T_int64 {
							require.Equal(t, pos*10, MustFixedColWithTypeCheck[int64](v)[i])
						} else {
							require.Equal(t, values[pos], string(v.GetBytesAt(i)))
						}
					}
				})
			}
			t.Run(fmt.Sprintf("%s/buffered=%v/constant", typ, buffered), func(t *testing.T) {
				var v *Vector
				var err error
				if typ == types.T_int64 {
					v, err = NewConstFixed(typ.ToType(), int64(7), 4, mp)
				} else {
					v, err = NewConstBytes(typ.ToType(), []byte("a"), 4, mp)
				}
				defer v.Free(mp)
				require.NoError(t, err)
				v.SetSorted(true)
				if buffered {
					var scratch []byte
					require.NoError(t, v.ShuffleWithBuf([]int64{3, 1, 0, 2}, mp, &scratch))
				} else {
					require.NoError(t, v.Shuffle([]int64{3, 1, 0, 2}, mp))
				}
				require.True(t, v.GetSorted())
				require.True(t, v.IsConst())
			})
		}
	}
	require.Zero(t, mp.CurrNB())
}

func TestShuffleSharedSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, buffered := range []bool{false, true} {
		for _, shared := range []bool{false, true} {
			for _, tc := range []struct {
				name   string
				values []int64
				sorted bool
			}{
				{"monotone", []int64{0, 0, 1, 3}, true},
				{"permuted", []int64{1, 0, 3, 2}, false},
			} {
				t.Run(fmt.Sprintf("buffered=%v/shared=%v/%s", buffered, shared, tc.name), func(t *testing.T) {
					v := NewVec(types.T_int64.ToType())
					defer v.Free(mp)
					require.NoError(t, AppendFixedList(v, tc.values, nil, mp))
					v.GetNulls().Add(0)
					v.GetGrouping().Add(1)
					v.SetSorted(tc.sorted)

					var encoded, original []byte
					if !buffered {
						// Keep borrowed input storage alive and immutable while Shuffle
						// installs its own replacement data.
						var err error
						encoded, err = v.MarshalBinary()
						require.NoError(t, err)
						original = append([]byte(nil), encoded...)
						borrowed := NewVec(types.T_int64.ToType())
						defer borrowed.Free(mp)
						require.NoError(t, borrowed.UnmarshalBinary(encoded))
						borrowed.GetGrouping().Add(1)
						v = borrowed
					}
					sels := MustFixedColWithTypeCheck[int64](v)
					if !shared {
						sels = append([]int64(nil), sels...)
					}
					selection := append([]int64(nil), sels...)
					if buffered {
						var scratch []byte
						require.NoError(t, v.ShuffleWithBuf(sels, mp, &scratch))
					} else {
						require.NoError(t, v.Shuffle(sels, mp))
					}
					require.Equal(t, tc.sorted, v.GetSorted())
					require.Equal(t, len(selection), v.Length())
					for row, source := range selection {
						require.Equal(t, tc.values[source], MustFixedColWithTypeCheck[int64](v)[row])
						require.Equal(t, source == 0, v.IsNull(uint64(row)), "NULL row %d", row)
						require.Equal(t, source == 1, v.GetGrouping().Contains(uint64(row)), "grouping row %d", row)
					}
					if !buffered {
						require.Equal(t, original, encoded)
					}
				})
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}
