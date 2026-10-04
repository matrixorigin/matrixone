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

// Large-scale term-dict test: build the vellum FST from the REAL gojieba
// dictionary (~349k Chinese/mixed words) and exercise exact lookup + `word*`
// prefix enumeration at scale, proving UTF-8 byte order == code-point order (so
// CJK prefix ranges are correct) on production vocabulary rather than a toy set.
package fulltext2

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// readJiebaWords loads the unique words (first column) of jieba.dict.utf8, or
// skips the test if the dictionary is not available in this environment.
func readJiebaWords(t *testing.T) []string {
	t.Helper()
	dir := tokenizer.JiebaDictDir()
	if dir == "" {
		t.Skip("jieba dict dir not found")
	}
	f, err := os.Open(filepath.Join(dir, "jieba.dict.utf8"))
	if err != nil {
		t.Skipf("cannot open jieba dict: %v", err)
	}
	defer f.Close()

	seen := make(map[string]struct{})
	var terms []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 0 {
			continue
		}
		w := fields[0]
		if _, dup := seen[w]; dup {
			continue
		}
		seen[w] = struct{}{}
		terms = append(terms, w)
	}
	require.NoError(t, sc.Err())
	return terms
}

// TestTermDictFromJiebaDict builds the FST from the whole jieba vocabulary and
// checks exact get() + prefixTerms() over it.
func TestTermDictFromJiebaDict(t *testing.T) {
	terms := readJiebaWords(t)
	require.Greater(t, len(terms), 100000, "real jieba dict has ~349k words")

	// The FST requires ascending byte order + unique keys (the Segment invariant).
	sort.Strings(terms)
	values := make([]uint64, len(terms))
	want := make(map[string]uint64, len(terms))
	for i, w := range terms {
		values[i] = uint64(i)
		want[w] = uint64(i)
	}

	data, err := buildTermDictFST(terms, values)
	require.NoError(t, err)
	d, err := loadTermDict(data)
	require.NoError(t, err)
	defer d.Close()

	require.Equal(t, len(terms), d.len())

	// Exact lookups for well-known dictionary words return their assigned value.
	for _, w := range []string{"中文", "北京", "清华大学", "苹果", "香蕉", "天安门"} {
		exp, in := want[w]
		if !in {
			continue // tolerate dict variations across environments
		}
		val, ok, gerr := d.get(w)
		require.NoError(t, gerr)
		require.True(t, ok, "%q must be present", w)
		require.Equal(t, exp, val, "value mismatch for %q", w)
	}

	// A word that cannot be in the dictionary is absent.
	_, ok, err := d.get("绝对不存在的怪词ZzQq")
	require.NoError(t, err)
	require.False(t, ok)

	// Prefix enumeration: every 中文* term begins with 中文, is a real dict word,
	// and the results are ascending — the CJK `word*` path over real vocabulary.
	pref, err := d.prefixTerms("中文")
	require.NoError(t, err)
	require.NotEmpty(t, pref)
	require.Contains(t, pref, "中文")
	require.True(t, sort.StringsAreSorted(pref), "prefix results must be ascending")
	seen := make(map[string]struct{}, len(terms))
	for _, w := range terms {
		seen[w] = struct{}{}
	}
	for _, w := range pref {
		require.True(t, strings.HasPrefix(w, "中文"), "%q lacks prefix 中文", w)
		_, in := seen[w]
		require.True(t, in, "%q not in source vocabulary", w)
	}

	// The prefix scan found the same count as a brute-force filter (no misses).
	brute := 0
	for _, w := range terms {
		if strings.HasPrefix(w, "中文") {
			brute++
		}
	}
	require.Equal(t, brute, len(pref))
}
