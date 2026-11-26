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

func TestPermuteSorted(t *testing.T) {
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
				v := NewVec(types.T_int64.ToType(), mp)
				t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
				require.NoError(t, AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
				v.SetSorted(tc.sorted)
				var buf []byte
				var err error
				if buffered {
					err = v.PermuteWithScratch(tc.sels, mp, &buf)
				} else {
					err = v.Permute(tc.sels, mp)
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
				err = v.PermuteWithScratch([]int64{2, 0}, mp, &buf)
			} else {
				err = v.Permute([]int64{2, 0}, mp)
			}
			require.NoError(t, err)
			require.True(t, v.GetSorted())
			require.Equal(t, 3, v.Length())
		})
	}
}

// Exercise both the contiguous prefix and its boundary with the general gather.
func TestShuffleSortedGather(t *testing.T) {
	cases := []struct {
		name string
		sels []int64
		want bool
	}{
		{"identity", []int64{0, 1, 2, 3}, true},
		{"last repeat", []int64{0, 1, 2, 2}, true},
		{"subrange", []int64{1, 2, 3}, true},
		{"gaps and duplicates", []int64{0, 0, 2, 3}, true},
		{"first inversion", []int64{1, 0, 2, 3}, false},
		{"prefix boundary inversion", []int64{1, 2, 0, 3}, false},
		{"last inversion", []int64{0, 1, 3, 2}, false},
	}
	for _, buffered := range []bool{false, true} {
		for _, varlen := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("buffered=%v/varlen=%v/%s", buffered, varlen, tc.name), func(t *testing.T) {
					mp := mpool.MustNewZero()
					typ := types.T_int64.ToType()
					if varlen {
						typ = types.T_varchar.ToType()
					}
					v := NewVec(typ, mp)
					t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
					values := []string{"a long external value 0", "a long external value 1", "a long external value 2", "a long external value 3"}
					if varlen {
						for _, value := range values {
							require.NoError(t, AppendBytes(v, []byte(value), false, mp))
						}
					} else {
						require.NoError(t, AppendFixedList(v, []int64{10, 20, 30, 40}, nil, mp))
					}
					v.SetSorted(true)
					var buf []byte
					var err error
					if buffered {
						err = v.PermuteWithScratch(tc.sels, mp, &buf)
					} else {
						err = v.Permute(tc.sels, mp)
					}
					require.NoError(t, err)
					require.Equal(t, tc.want, v.GetSorted())
					for row, source := range tc.sels {
						if varlen {
							require.Equal(t, values[source], string(v.GetBytesAt(row)))
						} else {
							require.Equal(t, (source+1)*10, MustFixedColWithTypeCheck[int64](v)[row])
						}
					}
				})
			}
		}
	}
}

// A scratch that overlaps v's own data is replaced, so v ends up right for
// any permutation and the caller does not keep the alias.
func TestPermuteWithScratchAliasFallsBack(t *testing.T) {
	for _, perm := range [][]int64{{0, 1, 2, 3}, {3, 1, 2, 0}} {
		for _, tc := range []struct {
			name                     string
			offset, length, capacity int
		}{
			{"same", 0, 32, 32}, {"offset", 8, 32, 32}, {"last byte", 31, 32, 32}, {"zero length", 8, 0, 32},
		} {
			t.Run(fmt.Sprintf("%v/%s", perm, tc.name), func(t *testing.T) {
				mp := mpool.MustNewZero()
				v := NewVec(types.T_int64.ToType(), mp)
				t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
				require.NoError(t, v.PreExtend(8, mp))
				require.NoError(t, AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp))
				v.SetSorted(true)
				backing := v.GetData()[:64]
				buf := backing[tc.offset : tc.offset+tc.length : tc.offset+tc.capacity]
				require.NoError(t, v.PermuteWithScratch(perm, mp, &buf))
				want := make([]int64, len(perm))
				for i, r := range perm {
					want[i] = r + 1
				}
				require.Equal(t, want, MustFixedColWithTypeCheck[int64](v))
				require.Equal(t, perm[0] == 0, v.GetSorted())
				if perm[0] != 0 {
					// A scratch that was written no longer aliases v.
					require.False(t, byteSlicesOverlap(buf, v.GetData()[:cap(v.GetData())]))
				}
			})
		}
	}
}

func TestPermuteWithScratchVarlenAlias(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_text.ToType(), mp)
	t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
	require.NoError(t, v.PreExtend(5, mp))
	values := [][]byte{[]byte("first value exceeding the inline storage limit"), []byte("second value exceeding the inline storage limit"), []byte("third value exceeding the inline storage limit"), []byte("fourth value exceeding the inline storage limit")}
	require.NoError(t, AppendBytesList(v, values, nil, mp))
	sz := v.GetType().TypeSize()
	buf := v.GetData()[sz : 5*sz]
	require.NoError(t, v.PermuteWithScratch([]int64{3, 2, 1, 0}, mp, &buf))
	for i := range values {
		require.Equal(t, values[3-i], v.GetBytesAt(i))
	}
}

// The scratch is kept and reused across calls.
func TestPermuteWithScratchReuse(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
	require.NoError(t, AppendFixedList(v, []int64{1, 2, 3, 4}, nil, mp))
	v.SetSorted(true)
	data := &v.GetData()[0]
	buf := make([]byte, 0, 32)
	// The identity permutation of sorted input changes nothing.
	require.NoError(t, v.PermuteWithScratch([]int64{0, 1, 2, 3}, mp, &buf))
	require.True(t, v.GetSorted())
	require.Empty(t, buf)
	require.NoError(t, v.PermuteWithScratch([]int64{1, 0, 2, 3}, mp, &buf))
	first := &buf[0]
	require.NoError(t, v.PermuteWithScratch([]int64{3, 0, 2, 1}, mp, &buf))
	require.Equal(t, []int64{4, 2, 3, 1}, MustFixedColWithTypeCheck[int64](v))
	require.False(t, v.GetSorted())
	require.Len(t, buf, 32)
	require.True(t, first == &buf[0])
	// v's data was not reallocated.
	require.True(t, data == &v.GetData()[0])
}

func TestByteSlicesOverlapBoundaries(t *testing.T) {
	b := make([]byte, 8)
	for _, tc := range []struct {
		name string
		a, b []byte
		want bool
	}{
		{"same", b[:4], b[:4], true}, {"forward", b[:4], b[3:6], true},
		{"reverse", b[3:6], b[:4], true}, {"adjacent", b[:4], b[4:], false},
		{"empty", b[:0], b[:4], false}, {"nil", nil, b[:4], false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, byteSlicesOverlap(tc.a, tc.b)) })
	}
}

func TestPermuteWithScratchStringMetadata(t *testing.T) {
	for _, typ := range []types.Type{types.T_text.ToType(), types.T_varbinary.ToType()} {
		for _, identity := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/identity=%v", typ.Oid, identity), func(t *testing.T) {
				mp := mpool.MustNewZero()
				v := NewVec(typ, mp)
				t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
				values := [][]byte{
					[]byte("first payload longer than the inline limit"),
					[]byte("second payload longer than the inline limit"),
					[]byte("third payload longer than the inline limit"),
				}
				require.NoError(t, AppendBytesList(v, values, nil, mp))
				v.SetNull(1)
				sels := []int64{2, 0, 1}
				if identity {
					sels = []int64{0, 1, 2}
					v.SetSorted(true)
				}
				var buf []byte
				require.NoError(t, v.PermuteWithScratch(sels, mp, &buf))
				for row, source := range sels {
					require.Equal(t, values[source], v.GetBytesAt(row))
					require.Equal(t, source == 1, v.IsNull(uint64(row)))
				}
				if identity {
					require.True(t, v.GetSorted())
				}
			})
		}
	}
}

// BenchmarkPermuteSorted measures reusable-buffer and allocation paths with equal values,
// so each iteration starts with a legitimately sorted vector without rebuilding it.
func BenchmarkPermuteSorted(b *testing.B) {
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
						v := NewVec(typ, mp)
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
							if err := v.PermuteWithScratch(sels, mp, &buf); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							v.SetSorted(order != "unsorted")
							var err error
							if buffered {
								err = v.PermuteWithScratch(sels, mp, &buf)
							} else {
								err = v.Permute(sels, mp)
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
