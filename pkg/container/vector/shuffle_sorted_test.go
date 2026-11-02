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

func TestShuffleSorted(t *testing.T) {
	cases := []struct {
		name         string
		sels         []int64
		sorted, want bool
	}{
		{"empty", nil, true, true}, {"single", []int64{1}, true, true},
		{"increasing", []int64{0, 2}, true, true}, {"repeat", []int64{1, 1, 2}, true, true},
		{"descending", []int64{2, 1, 0}, true, false}, {"already unsorted", []int64{0, 1, 2}, false, false},
	}
	for _, buffered := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, tc.name), func(t *testing.T) {
				mp := mpool.MustNewZero()
				v := NewVec(types.T_int64.ToType())
				t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
				require.NoError(t, AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
				v.SetSorted(tc.sorted)
				var buf []byte
				var err error
				if buffered {
					err = v.ShuffleWithBuf(tc.sels, mp, &buf)
				} else {
					err = v.Shuffle(tc.sels, mp)
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, v.GetSorted())
			})
		}
	}
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("const/buffered=%v", buffered), func(t *testing.T) {
			mp := mpool.MustNewZero()
			v, err := NewConstFixed(types.T_int64.ToType(), int64(1), 3, mp)
			require.NoError(t, err)
			defer v.Free(mp)
			v.SetSorted(true)
			var buf []byte
			if buffered {
				err = v.ShuffleWithBuf([]int64{2, 0}, mp, &buf)
			} else {
				err = v.Shuffle([]int64{2, 0}, mp)
			}
			require.NoError(t, err)
			require.True(t, v.GetSorted())
			require.Equal(t, 3, v.Length())
		})
	}
}

// BenchmarkShuffle measures reusable-buffer and allocation paths with equal values,
// so each iteration starts with a legitimately sorted vector without rebuilding it.
func BenchmarkShuffle(b *testing.B) {
	for _, n := range []int{64, 8192} {
		for _, varlen := range []bool{false, true} {
			for _, buffered := range []bool{false, true} {
				for _, order := range []string{"unsorted", "ascending", "descending"} {
					b.Run(fmt.Sprintf("n=%d/varlen=%v/buffered=%v/%s", n, varlen, buffered, order), func(b *testing.B) {
						mp := mpool.MustNewZero()
						typ := types.T_int64.ToType()
						if varlen {
							typ = types.T_varchar.ToType()
						}
						v := NewVec(typ)
						defer v.Free(mp)
						if varlen {
							vals := make([][]byte, n)
							for i := range vals {
								vals[i] = []byte("external payload longer than inline limit")
							}
							if err := AppendBytesList(v, vals, nil, mp); err != nil {
								b.Fatal(err)
							}
						} else {
							if err := AppendFixedList(v, make([]int64, n), nil, mp); err != nil {
								b.Fatal(err)
							}
						}
						sels := make([]int64, n)
						for i := range sels {
							sels[i] = int64(i)
							if order == "descending" {
								sels[i] = int64(n - 1 - i)
							}
						}
						var buf []byte
						if buffered {
							if err := v.ShuffleWithBuf(sels, mp, &buf); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							v.SetSorted(order != "unsorted")
							var err error
							if buffered {
								err = v.ShuffleWithBuf(sels, mp, &buf)
							} else {
								err = v.Shuffle(sels, mp)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
