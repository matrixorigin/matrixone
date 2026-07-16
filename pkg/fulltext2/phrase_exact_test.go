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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestExactPhraseSemantics pins fulltext2's NL and boolean-phrase EXACT-MATCH contract —
// the regression that previously escaped every unit test because the old tests used only
// English (where the ngram tokenizer degenerates to whole words, so a CJK-specific bug is
// invisible). It exercises the production query path (SearchQuery) so it covers the query
// tokenization + byte-position positional phrase matcher end to end.
//
//   - NL multi-term is an EXACT ORDERED phrase: a query matches a doc ONLY where the
//     terms appear in order and adjacent — NOT reversed, NOT with a word between. The
//     old CJK "bag-of-words" path matched all three (a false positive on reversed /
//     non-adjacent docs); a naive raw-ngram phrase matched none of the substring cases.
//   - A query that is a contiguous SUBSTRING of a longer doc matches (中文學習 ⊂
//     中文學習教材) — the case the raw-ngram phrase missed because the query's trailing
//     partial n-grams are absent from the longer doc's FST.
//   - Byte positions are consistent across the build-side AND the serialized/loaded
//     segment (so a query works the same after a restart/merge).
func TestExactPhraseSemantics(t *testing.T) {
	docs := []Doc{
		{int64(1), []byte("全文 索引")},        // in order, adjacent
		{int64(2), []byte("索引 全文")},        // reversed
		{int64(3), []byte("全文 系统 索引")},   // in order but NOT adjacent
		{int64(4), []byte("中文學習教材")},     // superset of 中文學習
		{int64(5), []byte("quick brown fox")}, // English, in order
		{int64(6), []byte("brown quick")},     // English, reversed
	}

	run := func(t *testing.T, idx *Index) {
		nl := func(q string) []any {
			rs, err := idx.SearchQuery([]byte(q), false, ParserNgram, BM25, 100, nil)
			require.NoError(t, err)
			return resultIDs(rs)
		}
		bl := func(q string) []any {
			rs, err := idx.SearchQuery([]byte(q), true, ParserNgram, BM25, 100, nil)
			require.NoError(t, err)
			return resultIDs(rs)
		}

		// NL exact ordered phrase — matches ONLY the in-order-adjacent doc.
		require.ElementsMatch(t, []any{int64(1)}, nl("全文 索引"), "CJK NL: exact ordered phrase only")
		require.ElementsMatch(t, []any{int64(5)}, nl("quick brown"), "English NL: order-sensitive")
		require.ElementsMatch(t, []any{int64(6)}, nl("brown quick"), "English NL: reversed is a different phrase")
		require.Empty(t, nl("索引 系统"), "not adjacent in doc 3 in this order")

		// Contiguous substring of a longer doc matches.
		require.ElementsMatch(t, []any{int64(4)}, nl("中文學習"), "CJK substring of a longer doc")
		require.ElementsMatch(t, []any{int64(4)}, nl("中文學"))
		require.ElementsMatch(t, []any{int64(4)}, nl("中文"))

		// Boolean explicit "phrase" is exact-ordered; a bare boolean operand is OR.
		require.ElementsMatch(t, []any{int64(1)}, bl(`"全文 索引"`), "boolean explicit phrase: exact")
		require.ElementsMatch(t, []any{int64(1), int64(2), int64(3)}, bl("全文 索引"), "boolean bare: OR")
	}

	seg, err := BuildSegmentFromDocsParser("ex", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	t.Run("build-side", func(t *testing.T) { run(t, NewIndex([]*Segment{seg}, nil)) })

	// Serialize → deserialize, so the loaded (mmap posRaw) byte positions are exercised too.
	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("ex", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	t.Run("loaded-side", func(t *testing.T) { run(t, NewIndex([]*Segment{loaded}, nil)) })
}
