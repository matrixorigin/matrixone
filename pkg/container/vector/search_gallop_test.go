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
	"math/rand"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func buildSortedBytesVec(t *testing.T, mp *mpool.MPool, vals [][]byte) *Vector {
	v := NewVec(types.T_varchar.ToType())
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
