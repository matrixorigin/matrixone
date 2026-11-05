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

package function

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBetweenAfterShuffleInvalidatesSorted(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_int64.ToType()
			v := vector.NewVec(typ)
			t.Cleanup(func() { v.Free(mp) })
			require.NoError(t, vector.AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
			v.SetSorted(true)
			sels := []int64{2, 0}
			if buffered {
				sels = []int64{2, 0, 1}
				var buf []byte
				require.NoError(t, v.ShuffleWithBuf(sels, mp, &buf))
			} else {
				require.NoError(t, v.Shuffle(sels, mp))
			}
			n := len(sels)
			lower, err := vector.NewConstFixed(typ, int64(1), n, mp)
			require.NoError(t, err)
			t.Cleanup(func() { lower.Free(mp) })
			upper, err := vector.NewConstFixed(typ, int64(2), n, mp)
			require.NoError(t, err)
			t.Cleanup(func() { upper.Free(mp) })
			result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
			t.Cleanup(result.Free)
			require.NoError(t, result.PreExtendAndReset(n))
			require.NoError(t, betweenImpl([]*vector.Vector{v, lower, upper}, result, nil, n, nil))
			actual := append([]bool(nil), vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())...)
			sorted := v.GetSorted()
			v.SetSorted(false)
			require.NoError(t, betweenImpl([]*vector.Vector{v, lower, upper}, result, nil, n, nil))
			want := []bool{false, true}
			if buffered {
				want = append(want, true)
			}
			require.Equal(t, want, vector.MustFixedColWithTypeCheck[bool](result.GetResultVector()), "unsorted control")
			require.Equal(t, want, actual)
			require.False(t, sorted)
		})
	}
}

func TestSortedConsumersAfterShuffle(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			sels   []int64
			sorted bool
		}{
			{"identity", []int64{0, 1, 2, 3}, true},
			{"gaps_duplicates", []int64{0, 1, 1, 3}, true},
			{"inversion", []int64{0, 2, 1, 3}, false},
		} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, tc.name), func(t *testing.T) {
				mp := mpool.MustNewZero()
				ints := vector.NewVec(types.T_int64.ToType())
				strs := vector.NewVec(types.T_varchar.ToType())
				t.Cleanup(func() { ints.Free(mp); strs.Free(mp); require.Zero(t, mp.CurrNB()) })
				values := []int64{1, 2, 3, 4}
				words := []string{"aa long external value 0", "ab long external value 1", "ab long external value 2", "cc long external value 3"}
				require.NoError(t, vector.AppendFixedList(ints, values, nil, mp))
				for _, word := range words {
					require.NoError(t, vector.AppendBytes(strs, []byte(word), false, mp))
				}
				ints.SetSorted(true)
				strs.SetSorted(true)
				for _, v := range []*vector.Vector{ints, strs} {
					if buffered {
						var buf []byte
						require.NoError(t, v.ShuffleWithBuf(tc.sels, mp, &buf))
					} else {
						require.NoError(t, v.Shuffle(tc.sels, mp))
					}
					require.Equal(t, tc.sorted, v.GetSorted())
				}
				n := len(tc.sels)
				lower, err := vector.NewConstFixed(types.T_int64.ToType(), int64(2), n, mp)
				require.NoError(t, err)
				t.Cleanup(func() { lower.Free(mp) })
				upper, err := vector.NewConstFixed(types.T_int64.ToType(), int64(3), n, mp)
				require.NoError(t, err)
				t.Cleanup(func() { upper.Free(mp) })
				prefix, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("ab"), n, mp)
				require.NoError(t, err)
				t.Cleanup(func() { prefix.Free(mp) })
				result := vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
				t.Cleanup(result.Free)
				require.NoError(t, result.PreExtendAndReset(n))
				require.NoError(t, betweenImpl([]*vector.Vector{ints, lower, upper}, result, nil, n, nil))
				for i, sel := range tc.sels {
					require.Equal(t, values[sel] >= 2 && values[sel] <= 3, vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())[i])
				}
				require.NoError(t, result.PreExtendAndReset(n))
				require.NoError(t, PrefixEq([]*vector.Vector{strs, prefix}, result, nil, n, nil))
				for i, sel := range tc.sels {
					require.Equal(t, strings.HasPrefix(words[sel], "ab"), vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())[i])
				}
			})
		}
	}
}
