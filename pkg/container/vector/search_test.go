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
	"math"
	"math/rand"
	"sort"
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

	vec := NewVec(types.T_varchar.ToType(), mp)
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

	vec := NewVec(types.T_varchar.ToType(), mp)
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

	vec := NewVec(types.T_varchar.ToType(), mp)
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
	vec := NewVec(types.T_varchar.ToType(), mp)

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

	vec := NewVec(types.T_int64.ToType(), mp)
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

	vec := NewVec(types.T_int64.ToType(), mp)
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

	vec := NewVec(types.T_varchar.ToType(), mp)
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

func TestOrderedLinearSearchEqualityAndComparator(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	vec := NewVec(types.T_float64.ToType(), mp)
	t.Cleanup(func() { vec.Free(mp) })
	for _, value := range []float64{-2, math.Copysign(0, -1), 0, math.NaN(), 2, 9} {
		require.NoError(t, AppendFixed(vec, value, false, mp))
	}
	values := []float64{0, 2, 2, math.NaN()}
	require.Equal(t, []int64{1, 2, 4}, OrderedLinearSearchOffsetByValFactory(values, nil)(vec))
	// A deliberately asymmetric comparator proves argument order is preserved.
	compare := func(row, candidate float64) int {
		if row == candidate-2 {
			return 0
		}
		return 1
	}
	require.Equal(t, []int64{0, 1, 2}, OrderedLinearSearchOffsetByValFactory(values, compare)(vec))
	require.Nil(t, OrderedLinearSearchOffsetByValFactory[float64](nil, nil)(vec))
	require.Nil(t, OrderedLinearSearchOffsetByValFactory[float64](nil, compare)(vec))
	empty := NewVec(types.T_float64.ToType(), mp)
	t.Cleanup(func() { empty.Free(mp) })
	require.Nil(t, OrderedLinearSearchOffsetByValFactory(values, nil)(empty))
	require.Nil(t, OrderedLinearSearchOffsetByValFactory(values, compare)(empty))
}

// Galloping prefix-IN search (CollectOffsetsByPrefixInFactory) against the
// linear and merge references.

func buildSortedBytesVec(t *testing.T, mp *mpool.MPool, vals [][]byte) *Vector {
	v := NewVec(types.T_varchar.ToType(), mp)
	for _, b := range vals {
		require.NoError(t, AppendBytes(v, b, false, mp))
	}
	return v
}

// normInt64 treats a nil slice and an empty slice as equal: the galloping merge
// returns a non-nil empty slice on no-match (matching the original merge), while
// the Linear oracle returns nil.
func normInt64(s []int64) []int64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

// TestCollectOffsetsByPrefixInGallopMatchesLinear is a differential test: the
// galloping merge in CollectOffsetsByPrefixInFactory must produce byte-identical
// output to the brute-force LinearCollectOffsetsByPrefixInFactory oracle for any
// sorted (lvec, rvec). The tiny alphabet forces dense prefix collisions, gaps of
// every size, duplicate needles, and prefix-of-prefix needles.
func TestCollectOffsetsByPrefixInGallopMatchesLinear(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(0xC0FFEE))
	alphabet := []byte("ab") // small -> many prefix collisions and gaps

	randStr := func(maxLen int) []byte {
		n := rng.Intn(maxLen) + 1
		s := make([]byte, n)
		for i := range s {
			s[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return s
	}

	for round := 0; round < 4000; round++ {
		ln := rng.Intn(40)
		lvals := make([][]byte, ln)
		for i := range lvals {
			lvals[i] = randStr(5)
		}
		sort.Slice(lvals, func(i, j int) bool { return bytes.Compare(lvals[i], lvals[j]) < 0 })

		rn := rng.Intn(8)
		rvals := make([][]byte, rn)
		for i := range rvals {
			rvals[i] = randStr(4) // shorter -> exercises prefix (not just exact) matches
		}
		sort.Slice(rvals, func(i, j int) bool { return bytes.Compare(rvals[i], rvals[j]) < 0 })

		lvec := buildSortedBytesVec(t, mp, lvals)
		rvec := buildSortedBytesVec(t, mp, rvals)

		got := CollectOffsetsByPrefixInFactory(rvec)(lvec)
		want := LinearCollectOffsetsByPrefixInFactory(rvec)(lvec)
		require.Equalf(t, normInt64(want), normInt64(got),
			"round %d\n lvals=%q\n rvals=%q", round, lvals, rvals)

		lvec.Free(mp)
		rvec.Free(mp)
	}
}

// mergeRefByPrefixIn is a verbatim copy of the original (pre-gallop) merge — the
// reference for what CollectOffsetsByPrefixInFactory must reproduce exactly. Used
// to prove the gallop is faithful to the original for ANY needle (rvec) order.
func mergeRefByPrefixIn(rvec, lvec *Vector) []int64 {
	lvlen := lvec.Length()
	rvlen := rvec.Length()
	if lvlen == 0 || rvlen == 0 {
		return nil
	}
	lcol, larea := MustVarlenaRawData(lvec)
	rcol, rarea := MustVarlenaRawData(rvec)
	rval := rcol[0].GetByteSlice(rarea)
	rpos := 0
	sels := make([]int64, 0, rvlen)
	for i := 0; i < lvlen; i++ {
		lval := lcol[i].GetByteSlice(larea)
		for types.PrefixCompare(lval, rval) > 0 {
			rpos++
			if rpos == rvlen {
				return sels
			}
			rval = rcol[rpos].GetByteSlice(rarea)
		}
		if bytes.HasPrefix(lval, rval) {
			sels = append(sels, int64(i))
		}
	}
	return sels
}

// TestCollectOffsetsByPrefixInGallopEqualsMergeAnyNeedleOrder proves the gallop is
// byte-identical to the original merge even when rvec is NOT sorted. lvec is always
// sorted (the one enforced precondition); rvec is shuffled half the time. Identical
// output confirms the optimization depends only on lvec's order, never rvec's — so
// "only one slice is sorted" does not affect the change's faithfulness. (Whether the
// merge itself is *correct* on unsorted needles is a separate, pre-existing property
// of basePKFilter.Vec, shared with the sibling binary-search IN paths.)
func TestCollectOffsetsByPrefixInGallopEqualsMergeAnyNeedleOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(0xBADBEEF))
	alphabet := []byte("ab")

	randStr := func(maxLen int) []byte {
		n := rng.Intn(maxLen) + 1
		s := make([]byte, n)
		for i := range s {
			s[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return s
	}

	for round := 0; round < 4000; round++ {
		ln := rng.Intn(40)
		lvals := make([][]byte, ln)
		for i := range lvals {
			lvals[i] = randStr(5)
		}
		sort.Slice(lvals, func(i, j int) bool { return bytes.Compare(lvals[i], lvals[j]) < 0 }) // lvec ALWAYS sorted

		rn := rng.Intn(8)
		rvals := make([][]byte, rn)
		for i := range rvals {
			rvals[i] = randStr(4)
		}
		if round%2 == 0 {
			// sorted needles (the contract); other half left shuffled (unsorted)
			sort.Slice(rvals, func(i, j int) bool { return bytes.Compare(rvals[i], rvals[j]) < 0 })
		}

		lvec := buildSortedBytesVec(t, mp, lvals)
		rvec := buildSortedBytesVec(t, mp, rvals) // helper appends as-is; "sorted" name aside

		got := CollectOffsetsByPrefixInFactory(rvec)(lvec)
		want := mergeRefByPrefixIn(rvec, lvec)
		require.Equalf(t, normInt64(want), normInt64(got),
			"round %d (rvec sorted=%v)\n lvals=%q\n rvals=%q", round, round%2 == 0, lvals, rvals)

		lvec.Free(mp)
		rvec.Free(mp)
	}
}

func TestCollectOffsetsByPrefixInGallopEdgeCases(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		name   string
		lvals  [][]byte
		rvals  [][]byte
		expect []int64
	}{
		{
			name:   "empty lvec",
			lvals:  nil,
			rvals:  [][]byte{[]byte("a")},
			expect: nil,
		},
		{
			name:   "empty rvec (no needles) -> nil, no panic",
			lvals:  [][]byte{[]byte("a"), []byte("b")},
			rvals:  nil,
			expect: nil,
		},
		{
			name:   "single needle matching a contiguous run (prefix)",
			lvals:  [][]byte{[]byte("a1"), []byte("b1"), []byte("b2"), []byte("b3"), []byte("c1")},
			rvals:  [][]byte{[]byte("b")},
			expect: []int64{1, 2, 3},
		},
		{
			name:   "large gap before the only match (forces a long gallop)",
			lvals:  [][]byte{[]byte("a1"), []byte("a2"), []byte("a3"), []byte("a4"), []byte("a5"), []byte("a6"), []byte("a7"), []byte("z9")},
			rvals:  [][]byte{[]byte("z")},
			expect: []int64{7},
		},
		{
			name:   "unit gaps between matches (gallop must not regress)",
			lvals:  [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
			rvals:  [][]byte{[]byte("a"), []byte("c"), []byte("e")},
			expect: []int64{0, 2, 4},
		},
		{
			name:   "prefix-of-prefix needles: each row counted once",
			lvals:  [][]byte{[]byte("abc"), []byte("abd"), []byte("xyz")},
			rvals:  [][]byte{[]byte("ab"), []byte("abc")},
			expect: []int64{0, 1},
		},
		{
			name:   "no matches",
			lvals:  [][]byte{[]byte("aaa"), []byte("bbb")},
			rvals:  [][]byte{[]byte("c"), []byte("d")},
			expect: nil,
		},
		{
			name:   "needle longer than data row (cannot be a prefix)",
			lvals:  [][]byte{[]byte("ab"), []byte("ac")},
			rvals:  [][]byte{[]byte("abc")},
			expect: nil,
		},
		{
			name:   "all rows match the single needle",
			lvals:  [][]byte{[]byte("a1"), []byte("a2"), []byte("a3")},
			rvals:  [][]byte{[]byte("a")},
			expect: []int64{0, 1, 2},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lvec := buildSortedBytesVec(t, mp, c.lvals)
			rvec := buildSortedBytesVec(t, mp, c.rvals)
			defer lvec.Free(mp)
			defer rvec.Free(mp)

			got := CollectOffsetsByPrefixInFactory(rvec)(lvec)
			require.Equal(t, c.expect, normInt64(got))
			// cross-check against the oracle too
			want := LinearCollectOffsetsByPrefixInFactory(rvec)(lvec)
			require.Equal(t, normInt64(want), normInt64(got))
		})
	}
}

func TestCollectOffsetsByPrefixEqFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType(), mp)
	defer v1.Free(mp)

	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1221"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)

	prefix1 := []byte("01")
	prefix2 := []byte("12")
	prefix3 := []byte("14")
	prefix4 := []byte("113")

	fn1 := CollectOffsetsByPrefixEqFactory(prefix1)
	fn2 := CollectOffsetsByPrefixEqFactory(prefix2)
	fn3 := CollectOffsetsByPrefixEqFactory(prefix3)
	fn4 := CollectOffsetsByPrefixEqFactory(prefix4)
	off1 := fn1(v1)
	off2 := fn2(v1)
	off3 := fn3(v1)
	off4 := fn4(v1)
	require.Equal(t, 0, len(off1))
	require.Equal(t, []int64{2, 3, 4}, off2)
	require.Equal(t, 0, len(off3))
	require.Equal(t, 0, len(off4))
}

func TestCollectOffsetsByPrefixBetweenFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType(), mp)
	defer v1.Free(mp)

	require.NoError(t, AppendStringList(v1, []string{"1111", "1121", "1211", "1221", "1231", "1311"}, nil, mp))
	cases := []struct {
		name        string
		left, right string
		want        []int64
	}{
		{"forward", "11", "12", []int64{0, 1, 2, 3, 4}},
		{"forward_gap", "113", "124", []int64{2, 3, 4}},
		{"equal_prefix", "12", "12", []int64{2, 3, 4}},
		{"equal_key", "1221", "1221", []int64{3}},
		{"short_upper_prefix", "1221", "12", []int64{3, 4}},
		{"reversed", "13", "11", nil},
		{"reversed_gap", "125", "113", nil},
		{"reversed_to_before_block", "12", "01", nil},
		{"reversed_within_prefix", "1230", "121", nil},
		{"empty_gap", "113", "114", nil},
		{"before_block", "00", "01", nil},
		{"after_block", "14", "15", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right := []byte(tc.left), []byte(tc.right)
			got := CollectOffsetsByPrefixBetweenFactory(left, right)(v1)
			require.Equal(t, tc.want, got)
			require.Equal(t, LinearCollectOffsetsByPrefixBetweenFactory(left, right)(v1), got)
		})
	}
	t.Run("empty_vector", func(t *testing.T) {
		v := NewVec(types.T_char.ToType(), mp)
		defer v.Free(mp)
		require.Nil(t, CollectOffsetsByPrefixBetweenFactory([]byte("13"), []byte("11"))(v))
	})
}

func TestCollectOffsetsByPrefixInRangeFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType(), mp)
	defer v1.Free(mp)

	// Sorted data: keys with prefixes "11", "11", "12", "12", "12", "13"
	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1221"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)

	lb := []byte("11")
	ub := []byte("12")

	// hint=0: [lb, ub] — both closed
	fn0 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, fn0(v1))

	// hint=1: (lb, ub] — left open
	fn1 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 1)
	require.Equal(t, []int64{2, 3, 4}, fn1(v1))

	// hint=2: [lb, ub) — right open
	fn2 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 2)
	require.Equal(t, []int64{0, 1}, fn2(v1))

	// hint=3: (lb, ub) — both open
	fn3 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 3)
	require.Nil(t, fn3(v1))

	// Asymmetric bounds: short prefix fence as UB
	lb2 := []byte("1121")
	ub2 := []byte("12")
	fn4 := CollectOffsetsByPrefixInRangeFactory(lb2, ub2, 1) // (lb2, ub2]
	require.Equal(t, []int64{2, 3, 4}, fn4(v1))

	// Empty vector
	v2 := NewVec(types.T_char.ToType(), mp)
	defer v2.Free(mp)
	fn5 := CollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Nil(t, fn5(v2))
}

func TestLinearCollectOffsetsByPrefixInRangeFactory(t *testing.T) {
	mp := mpool.MustNewZero()
	v1 := NewVec(types.T_char.ToType(), mp)
	defer v1.Free(mp)

	// Unsorted data
	AppendBytes(v1, []byte("1211"), false, mp)
	AppendBytes(v1, []byte("1111"), false, mp)
	AppendBytes(v1, []byte("1311"), false, mp)
	AppendBytes(v1, []byte("1121"), false, mp)
	AppendBytes(v1, []byte("1231"), false, mp)

	lb := []byte("11")
	ub := []byte("12")

	// hint=0: [lb, ub]
	fn0 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 0)
	require.Equal(t, []int64{0, 1, 3, 4}, fn0(v1))

	// hint=1: (lb, ub]
	fn1 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 1)
	require.Equal(t, []int64{0, 4}, fn1(v1))

	// hint=2: [lb, ub)
	fn2 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 2)
	require.Equal(t, []int64{1, 3}, fn2(v1))

	// hint=3: (lb, ub)
	fn3 := LinearCollectOffsetsByPrefixInRangeFactory(lb, ub, 3)
	require.Nil(t, fn3(v1))
}
