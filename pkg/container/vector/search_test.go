// Copyright 2025 Matrix Origin
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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestVarlenBinarySearchOffsetByValFactorySmallSet(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	input := []string{"aa", "aa", "ab", "ac", "ac", "ad"}
	for _, v := range input {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	vals := [][]byte{[]byte("aa"), []byte("ac"), []byte("ac"), []byte("az")}
	fn := VarlenBinarySearchOffsetByValFactory(vals)

	require.Equal(t, []int64{0, 1, 3, 4}, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryLargeSet(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	values := make([]string, 0, 400)
	for i := 0; i < 200; i++ {
		s := fmt.Sprintf("%04d", i)
		values = append(values, s, s)
	}
	for _, v := range values {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	var targets [][]byte
	targets = append(targets, []byte("0000"))
	for i := 0; i < 200; i += 3 {
		targets = append(targets, []byte(fmt.Sprintf("%04d", i)))
	}
	targets = append(targets, []byte("0199"), []byte("9999"), []byte("9999"))

	expected := make([]int64, 0, vec.Length())
	targetSet := make(map[string]struct{})
	for _, v := range targets {
		targetSet[string(v)] = struct{}{}
	}
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	fn := VarlenBinarySearchOffsetByValFactory(targets)
	require.Equal(t, expected, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryEmptyInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	fn := VarlenBinarySearchOffsetByValFactory(nil)
	require.Nil(t, fn(vec))

	require.NoError(t, AppendBytes(vec, []byte("a"), false, mp))
	fn = VarlenBinarySearchOffsetByValFactory(nil)
	require.Nil(t, fn(vec))

	fn = VarlenBinarySearchOffsetByValFactory([][]byte{[]byte("b")})
	require.Len(t, fn(vec), 0)
}

func BenchmarkVarlenBinarySearchOffsetByValFactory(b *testing.B) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())

	zeroPadded := func(i int) []byte { return []byte(fmt.Sprintf("%05d", i)) }
	for i := 0; i < 8192; i++ {
		require.NoError(b, AppendBytes(vec, zeroPadded(i), false, mp))
	}

	toBytes := func(ss []string) [][]byte {
		res := make([][]byte, len(ss))
		for i := range ss {
			res[i] = []byte(ss[i])
		}
		return res
	}

	cases := []struct {
		name string
		vals [][]byte
	}{
		{
			name: "single_hit",
			vals: [][]byte{zeroPadded(4096)},
		},
		{
			name: "single_miss",
			vals: [][]byte{[]byte("99999")},
		},
		{
			name: "few_hits",
			vals: toBytes([]string{"01000", "02000", "03000"}),
		},
		{
			name: "many_hits",
			vals: func() [][]byte {
				vals := make([][]byte, 0, 4096)
				for i := 0; i < 8192; i += 2 {
					vals = append(vals, zeroPadded(i))
				}
				return vals
			}(),
		},
		{
			name: "all_hits",
			vals: func() [][]byte {
				vals := make([][]byte, 0, 8192)
				for i := 0; i < 8192; i++ {
					vals = append(vals, zeroPadded(i))
				}
				return vals
			}(),
		},
	}

	for _, tc := range cases {
		fn := VarlenBinarySearchOffsetByValFactory(tc.vals)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = fn(vec)
			}
		})
	}

	b.Cleanup(func() {
		vec.Free(mp)
		require.Equal(b, int64(0), mp.CurrNB())
	})
}

func TestOrderedBinarySearchOffsetByValFactoryWithDuplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)

	base := []int64{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]int64, 0, 12*16)
	for i := 0; i < 16; i++ {
		for _, v := range base {
			values = append(values, v+int64(i*1000))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendFixed(vec, v, false, mp))
	}

	vals := []int64{1, 2, 55, 55, 301, 301, 1034, 50000}
	fn := OrderedBinarySearchOffsetByValFactory(vals)

	targetSet := make(map[int64]struct{})
	for _, v := range vals {
		targetSet[v] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}

func TestFixedSizedBinarySearchOffsetByValFactoryWithDuplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_int64.ToType())
	defer vec.Free(mp)

	base := []int64{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]int64, 0, 12*12)
	for i := 0; i < 12; i++ {
		for _, v := range base {
			values = append(values, v+int64(i*500))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendFixed(vec, v, false, mp))
	}

	vals := []int64{1, 2, 55, 55, 279, 301, 301, 1500, 6000}
	cmp := func(a, b int64) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	}
	fn := FixedSizedBinarySearchOffsetByValFactory(vals, cmp)

	targetSet := make(map[int64]struct{})
	for _, v := range vals {
		targetSet[v] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryWithDuplicateNumbers(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	vec := NewVec(types.T_varchar.ToType())
	defer vec.Free(mp)

	base := []int{1, 1, 2, 33, 34, 41, 55, 55, 279, 299, 301, 301}
	values := make([]string, 0, 12*10)
	for i := 0; i < 10; i++ {
		for _, v := range base {
			values = append(values, fmt.Sprintf("%06d", v+i*700))
		}
	}
	for _, v := range values {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}

	vals := [][]byte{
		[]byte("000001"),
		[]byte("000002"),
		[]byte("000055"),
		[]byte("000055"),
		[]byte("000279"),
		[]byte("000301"),
		[]byte("000301"),
		[]byte("003500"),
		[]byte("010000"),
	}
	fn := VarlenBinarySearchOffsetByValFactory(vals)

	targetSet := make(map[string]struct{})
	for _, v := range vals {
		targetSet[string(v)] = struct{}{}
	}
	var expected []int64
	for idx, v := range values {
		if _, ok := targetSet[v]; ok {
			expected = append(expected, int64(idx))
		}
	}

	require.Equal(t, expected, fn(vec))
}

func TestVarlenBinarySearchOffsetByValFactoryDistributions(t *testing.T) {
	for _, tc := range []struct {
		name, encoding             string
		keys, step, offset, repeat int
		miss                       bool
	}{
		{name: "sparse", keys: 65, step: 97, repeat: 1},
		{name: "clustered", keys: 300, step: 1, offset: 7000, repeat: 1},
		{name: "dense", keys: 8192, step: 1, repeat: 1},
		{name: "alternating", keys: 4096, step: 2, repeat: 1},
		{name: "two-row gaps", keys: 2731, step: 3, repeat: 1},
		{name: "duplicate rows and needles", keys: 300, step: 7, repeat: 2},
		{name: "absent between rows", keys: 300, step: 17, repeat: 1, miss: true},
		{name: "outside range", keys: 300, step: 1, offset: 9000, repeat: 1},
		{name: "long values", encoding: "long", keys: 300, step: 19, repeat: 1},
		{name: "variable lengths", encoding: "variable", keys: 300, step: 19, repeat: 1},
		{name: "binary boundaries", encoding: "binary", keys: 300, step: 19, repeat: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			vec := NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { vec.Free(mp); require.Zero(t, mp.CurrNB()) })
			encode := func(n int) []byte {
				v := fmt.Sprintf("%08d", n)
				switch tc.encoding {
				case "long":
					v = strings.Repeat("long", 20) + v
				case "variable":
					v = strings.Repeat("x", n%40) + v
				case "binary":
					v = string([]byte{0, 255, 0}) + v
				}
				return []byte(v)
			}
			var values []string
			for i := 0; i < 8192; i++ {
				values = append(values, string(encode(2*(i/tc.repeat))))
			}
			var keys [][]byte
			for i := 0; i < tc.keys; i++ {
				n := 2 * (tc.offset + i*tc.step)
				if tc.miss {
					n++
				}
				key := encode(n)
				keys = append(keys, key)
				if tc.repeat > 1 {
					keys = append(keys, bytes.Clone(key))
				}
			}
			if tc.encoding == "binary" {
				for _, v := range [][]byte{nil, {}, {0}, {0, 0}, []byte("a"), []byte("ab"), []byte("abc"), {255}} {
					values = append(values, string(v))
					keys = append(keys, bytes.Clone(v))
				}
			}
			sort.Strings(values)
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
			for _, v := range values {
				require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
			}
			// A scalar membership set is independent of the sorted search strategy.
			members := make(map[string]bool, len(keys))
			for _, key := range keys {
				members[string(key)] = true
			}
			var want []int64
			for i, v := range values {
				if members[v] {
					want = append(want, int64(i))
				}
			}
			beforeData, beforeArea := bytes.Clone(vec.GetData()), bytes.Clone(vec.GetArea())
			require.Equal(t, want, VarlenBinarySearchOffsetByValFactory(keys)(vec))
			require.Equal(t, beforeData, vec.GetData())
			require.Equal(t, beforeArea, vec.GetArea())
		})
	}
}

func TestGallopVarlenGEComparison(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := NewVec(types.T_varchar.ToType())
	defer func() { vec.Free(mp); require.Zero(t, mp.CurrNB()) }()
	values := []string{"", "", "a", "a", "aa", "ab", "abc", strings.Repeat("x", 80), "\xff"}
	for _, v := range values {
		require.NoError(t, AppendBytes(vec, []byte(v), false, mp))
	}
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	for lo := 0; lo <= len(values); lo++ {
		for hi := lo; hi <= len(values); hi++ {
			for _, key := range []string{"", "\x00", "a", "a\x00", "aa", "ac", "x", "\xff", "\xff\x00"} {
				want := lo + sort.Search(hi-lo, func(i int) bool { return bytes.Compare([]byte(values[lo+i]), []byte(key)) >= 0 })
				got, cmp := gallopVarlenGE(col, vec.GetArea(), []byte(key), lo, hi)
				require.Equal(t, want, got)
				if got < hi {
					require.Equal(t, bytes.Compare([]byte(values[got]), []byte(key)), cmp)
				}
			}
		}
	}
}
