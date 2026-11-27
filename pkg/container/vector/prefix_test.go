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
	"bytes"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestUnmarshalPrefixInVector(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, tc := range []struct {
		name        string
		input, want []string
		sorted      bool
	}{
		{"empty_set", nil, nil, false},
		{"canonical", []string{"ab", "cd"}, []string{"ab", "cd"}, true},
		{"unsorted", []string{"cd", "ab"}, []string{"ab", "cd"}, false},
		{"false_sorted_flag", []string{"cd", "ab"}, []string{"ab", "cd"}, true},
		{"duplicates_and_overlap", []string{"az", "aa", "a", "a", "b"}, []string{"a", "b"}, true},
		{"empty_prefix", []string{"zzz", "", "aaa"}, []string{""}, false},
		{"zero_bytes", []string{"\x00x", "\x00", "ab"}, []string{"\x00", "ab"}, false},
		{"area", []string{"a-long-prefix-outside-the-inline-area-x", "a-long-prefix-outside-the-inline-area"}, []string{"a-long-prefix-outside-the-inline-area"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := NewVec(types.T_varchar.ToType())
			defer v.Free(mp)
			for _, s := range tc.input {
				require.NoError(t, AppendBytes(v, []byte(s), false, mp))
			}
			v.SetSorted(tc.sorted)
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			original := bytes.Clone(data)
			for range 2 {
				got, err := UnmarshalPrefixInVector(data)
				require.NoError(t, err)
				defer got.Free(mp)
				require.True(t, got.GetSorted() || got.Length() == 0)
				var values []string
				for i := range got.Length() {
					values = append(values, string(got.GetBytesAt(i)))
				}
				require.Equal(t, tc.want, values)
				// Independent predicate oracle, including values outside the set.
				for _, row := range append([]string{"", "abacus", "az", "b", "other", "\x00xy"}, tc.input...) {
					match := func(prefixes []string) bool {
						for _, prefix := range prefixes {
							if bytes.HasPrefix([]byte(row), []byte(prefix)) {
								return true
							}
						}
						return false
					}
					require.Equal(t, match(tc.input), match(values), row)
				}
				require.Equal(t, original, data, "shared expression bytes must stay immutable")
			}
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestUnmarshalPrefixInVectorConstants(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, length := range []int{0, 1, math.MaxInt32} {
		func() {
			v, err := NewConstBytes(types.T_varchar.ToType(), []byte("ab"), 1, mp)
			defer v.Free(mp)
			require.NoError(t, err)
			v.SetLength(length) // zero length still retains the physical slot
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			got, err := UnmarshalPrefixInVector(data)
			defer got.Free(mp)
			require.NoError(t, err)
			if length == 0 {
				require.False(t, got.IsConst())
				require.Zero(t, got.Length())
			} else {
				require.Equal(t, 1, got.Length(), "constant work must not scale with logical length")
				require.Equal(t, []byte("ab"), got.GetBytesAt(0))
			}
		}()
	}
	require.Zero(t, mp.CurrNB())
}

func TestUnmarshalPrefixInVectorRejectsUnsafeInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	data := func() []byte {
		valid := NewVec(types.T_varchar.ToType())
		defer valid.Free(mp)
		require.NoError(t, AppendBytes(valid, []byte("a"), false, mp))
		data, err := valid.MarshalBinary()
		require.NoError(t, err)
		return data
	}()
	badClass, dictionary := bytes.Clone(data), bytes.Clone(data)
	badClass[0], dictionary[0] = 255, DIST
	for _, input := range [][]byte{nil, data[:len(data)-1], badClass, dictionary} {
		got, err := UnmarshalPrefixInVector(input)
		require.Error(t, err)
		require.Nil(t, got)
	}
	for _, kind := range []string{"fixed", "bitmap_null", "out_of_range_null", "const_null", "bad_area"} {
		t.Run(kind, func(t *testing.T) {
			v := NewVec(types.T_varchar.ToType())
			defer func() { v.Free(mp) }()
			switch kind {
			case "fixed":
				v = NewVec(types.T_int64.ToType())
				require.NoError(t, AppendFixed(v, int64(1), false, mp))
			case "const_null":
				v = NewConstNull(types.T_varchar.ToType(), 1, mp)
			default:
				require.NoError(t, AppendBytes(v, []byte("payload-longer-than-the-inline-varlena-storage"), false, mp))
				switch kind {
				case "bitmap_null":
					v.GetNulls().Add(0)
				case "out_of_range_null":
					v.GetNulls().Add(100)
				case "bad_area":
					col, _ := MustVarlenaRawData(v)
					col[0].SetOffsetLen(math.MaxUint32, math.MaxUint32)
				}
			}
			wire, err := v.MarshalBinary()
			require.NoError(t, err)
			got, err := UnmarshalPrefixInVector(wire)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
	require.Zero(t, mp.CurrNB())
}
