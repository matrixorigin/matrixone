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

package fulltext2

import (
	"sort"
	"testing"

	"github.com/blevesearch/vellum"
	"github.com/stretchr/testify/require"
)

// buildDict sorts the given terms (as the segment does), assigns each a distinct
// value, and builds + loads an FST term dict. Returns the dict and the expected
// term→value map. Terms must be unique.
func buildDict(t *testing.T, terms ...string) (*termDict, map[string]uint64) {
	t.Helper()
	sorted := append([]string(nil), terms...)
	sort.Strings(sorted)
	vals := make([]uint64, len(sorted))
	exp := make(map[string]uint64, len(sorted))
	for i, term := range sorted {
		vals[i] = uint64(i*100 + 7) // distinct, non-trivial
		exp[term] = vals[i]
	}
	data, err := buildTermDictFST(sorted, vals)
	require.NoError(t, err)
	d, err := loadTermDict(data)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d, exp
}

// collectPrefix drains prefixIter into a slice of terms (ascending).
func collectPrefix(t *testing.T, d *termDict, prefix string) []string {
	t.Helper()
	it, ok, err := d.prefixIter(prefix)
	require.NoError(t, err)
	if !ok {
		return nil
	}
	defer func() { _ = it.Close() }()
	var got []string
	for {
		term, _ := it.Current()
		got = append(got, string(term))
		if err := it.Next(); err == vellum.ErrIteratorDone {
			break
		} else {
			require.NoError(t, err)
		}
	}
	return got
}

func TestTermDictGet(t *testing.T) {
	d, exp := buildDict(t, "apple", "apply", "banana", "band")
	require.Equal(t, 4, d.len())
	for term, want := range exp {
		got, ok, err := d.get(term)
		require.NoError(t, err)
		require.True(t, ok, term)
		require.Equal(t, want, got, term)
	}
	// misses
	for _, miss := range []string{"", "app", "avocado", "bandit", "z"} {
		_, ok, err := d.get(miss)
		require.NoError(t, err)
		require.False(t, ok, miss)
	}
}

func TestTermDictPrefixASCII(t *testing.T) {
	d, _ := buildDict(t, "app", "apple", "apply", "banana", "band", "bat")

	require.Equal(t, []string{"app", "apple", "apply"}, collectPrefix(t, d, "app"))
	require.Equal(t, []string{"apple", "apply"}, collectPrefix(t, d, "appl"))
	require.Equal(t, []string{"banana", "band"}, collectPrefix(t, d, "ban"))
	require.Equal(t, []string{"bat"}, collectPrefix(t, d, "bat"))
	require.Equal(t, []string{"banana", "band", "bat"}, collectPrefix(t, d, "b"))
	// empty prefix → all terms, ascending
	require.Equal(t, []string{"app", "apple", "apply", "banana", "band", "bat"}, collectPrefix(t, d, ""))
	// no matches — before, between, after
	require.Empty(t, collectPrefix(t, d, "aa"))
	require.Empty(t, collectPrefix(t, d, "az"))
	require.Empty(t, collectPrefix(t, d, "z"))
}

// TestTermDictChinese is the load-bearing case: CJK terms need no special
// handling because vellum is byte-oriented and UTF-8 byte order == code-point
// order, so Go's string sort is already vellum's required insertion order, and
// prefix iteration works on whole-character prefixes.
func TestTermDictChinese(t *testing.T) {
	// 中(U+4E2D) 国(U+56FD) 文(U+6587) — bigrams share a leading character.
	d, exp := buildDict(t, "中", "中国", "中文", "国", "文", "国家")
	require.Equal(t, 6, d.len())

	// exact lookup of a multi-byte term
	got, ok, err := d.get("中文")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, exp["中文"], got)

	// prefix over a shared leading character → the two bigrams + the unigram
	require.Equal(t, []string{"中", "中国", "中文"}, collectPrefix(t, d, "中"))
	require.Equal(t, []string{"国", "国家"}, collectPrefix(t, d, "国"))
	require.Equal(t, []string{"文"}, collectPrefix(t, d, "文"))
	// full ascending order matches byte/code-point order
	require.Equal(t, []string{"中", "中国", "中文", "国", "国家", "文"}, collectPrefix(t, d, ""))
	require.Empty(t, collectPrefix(t, d, "日"))
}

func TestPrefixSuccessor(t *testing.T) {
	require.Equal(t, []byte{'a', 'p', 'q'}, prefixSuccessor([]byte("app")))
	require.Equal(t, []byte{0x01}, prefixSuccessor([]byte{0x00}))
	// trailing 0xFF bytes are dropped, the first < 0xFF is incremented
	require.Equal(t, []byte{0x02}, prefixSuccessor([]byte{0x01, 0xFF, 0xFF}))
	// empty prefix and all-0xFF have no finite successor → nil (open upper bound)
	require.Nil(t, prefixSuccessor([]byte{}))
	require.Nil(t, prefixSuccessor([]byte{0xFF, 0xFF}))
}
