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

func TestShuffleWithBufStringMetadata(t *testing.T) {
	for _, metadata := range []string{"uniform", "source", "kind", "domain"} {
		t.Run(metadata, func(t *testing.T) {
			mp := mpool.MustNewZero()
			v := NewVec(types.T_text.ToType())
			t.Cleanup(func() { v.Free(mp); require.Zero(t, mp.CurrNB()) })
			values := [][]byte{
				[]byte("first payload longer than the inline limit"),
				[]byte("second payload longer than the inline limit"),
				[]byte("third payload longer than the inline limit"),
			}
			require.NoError(t, AppendBytesList(v, values, nil, mp))
			v.SetNull(1)
			v.GetGrouping().Add(2)
			v.areaDisjoint = true
			require.NoError(t, v.SetStringSource(types.StringSourceLiteral))
			v.SetPrepareParamKind(PrepareParamInteger)
			require.NoError(t, v.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp))
			switch metadata {
			case "source":
				require.NoError(t, v.SetStringSourceAtWithMP(0, types.StringSourceCOMStmt, mp))
			case "kind":
				require.NoError(t, v.SetPrepareParamKindAtWithMP(0, PrepareParamFloat, mp))
			case "domain":
				require.NoError(t, v.SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, mp))
			}
			var sources [3]types.StringSource
			var kinds [3]PrepareParamKind
			var domains [3]types.RuntimeStringDomain
			for row := range values {
				sources[row] = v.GetStringSourceAt(row)
				kinds[row] = v.GetPrepareParamKindAt(row)
				domains[row] = v.GetRuntimeStringDomainAt(row)
			}
			sels := []int64{2, 0, 1}
			var buf []byte
			require.NoError(t, v.ShuffleWithBuf(sels, mp, &buf))
			for row, source := range sels {
				require.Equal(t, values[source], v.GetBytesAt(row))
				require.Equal(t, source == 1, v.IsNull(uint64(row)))
				require.Equal(t, source == 2, v.GetGrouping().Contains(uint64(row)))
				require.Equal(t, sources[source], v.GetStringSourceAt(row))
				require.Equal(t, kinds[source], v.GetPrepareParamKindAt(row))
				require.Equal(t, domains[source], v.GetRuntimeStringDomainAt(row))
			}
			require.False(t, v.areaDisjoint)
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
