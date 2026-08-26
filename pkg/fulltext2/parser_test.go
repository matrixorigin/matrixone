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

// Parser support: the ngram (default), gojieba, and json parsers over a fulltext2
// index. CJK is the focus — NL exact-phrase over overlapping 3-grams is unreliable,
// so ngram/json NL routes CJK through classic fulltext's redundant-ngram removal
// (bag-of-words), while gojieba word-segmentation keeps exact-phrase.
package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// zhCorpus indexes five CJK docs under parser and returns a queryable Index.
//
//	0: 遠東兒童中文學習教材   (contains 中文學習, 兒童)
//	1: 中文短篇小說適合初學者  (contains 中文, no 學習/兒童)
//	2: 兒童學習樂趣          (contains 兒童, 學習)
//	3: 教學指引與生字卡       (none of the above)
//	4: 中文學習中文學習       (contains 中文學習 twice)
func zhCorpus(t *testing.T, parser string) *Index {
	docs := []Doc{
		{int64(0), []byte("遠東兒童中文學習教材")},
		{int64(1), []byte("中文短篇小說適合初學者")},
		{int64(2), []byte("兒童學習樂趣")},
		{int64(3), []byte("教學指引與生字卡")},
		{int64(4), []byte("中文學習中文學習")},
	}
	seg, err := BuildSegmentFromDocsParser("zh", int32(types.T_int64), docs, parser)
	require.NoError(t, err)
	return NewIndex([]*Segment{seg}, nil)
}

func nlIDs(t *testing.T, idx *Index, parser, q string) []any {
	res, err := idx.SearchQuery([]byte(q), false, parser, BM25, 100, nil)
	require.NoError(t, err)
	return resultIDs(res)
}

func boolIDs(t *testing.T, idx *Index, parser, q string) []any {
	res, err := idx.SearchQuery([]byte(q), true, parser, BM25, 100, nil)
	require.NoError(t, err)
	return resultIDs(res)
}

func resultIDs(res []Result) []any {
	out := make([]any, len(res))
	for i, r := range res {
		out[i] = r.Pk
	}
	return out
}

func requireJieba(t *testing.T) {
	if _, err := DocTokenizer(ParserGojieba); err != nil {
		t.Skipf("gojieba dictionary unavailable: %v", err)
	}
}

// --- ngram (default) parser: CJK NL is bag-of-words over redundant-removed 3-grams ---

func TestNgramNLChinese(t *testing.T) {
	idx := zhCorpus(t, ParserNgram)
	// 中文學習 -> ngrams 中文學, 文學習 (bag-of-words); docs 0 and 4 contain both.
	require.ElementsMatch(t, []any{int64(0), int64(4)}, nlIDs(t, idx, ParserNgram, "中文學習"))
	// 兒童 (<3 chars) -> prefix 兒童* ; matches 3-grams starting 兒童 in docs 0 and 2.
	require.ElementsMatch(t, []any{int64(0), int64(2)}, nlIDs(t, idx, ParserNgram, "兒童"))
	// unknown compound -> no hit.
	require.Empty(t, nlIDs(t, idx, ParserNgram, "西班牙語"))
}

func TestNgramBooleanChinese(t *testing.T) {
	idx := zhCorpus(t, ParserNgram)
	// +中文 +學習 : both compounds' ngrams present -> docs 0, 4 (2 has 學習 but not 中文).
	require.ElementsMatch(t, []any{int64(0), int64(4)}, boolIDs(t, idx, ParserNgram, "+中文 +學習"))
	// +兒童 -中文 : 兒童 present, 中文 absent -> only doc 2 (doc 0 has both).
	require.ElementsMatch(t, []any{int64(2)}, boolIDs(t, idx, ParserNgram, "+兒童 -中文"))
}

func TestNgramLatinStaysPhrase(t *testing.T) {
	// Latin under ngram keeps fulltext2 exact-phrase NL.
	docs := []Doc{
		{int64(0), []byte("the quick brown fox")},
		{int64(1), []byte("quick red fox")},
		{int64(2), []byte("brown fox jumps")},
	}
	seg, err := BuildSegmentFromDocsParser("en", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)
	// "brown fox" is contiguous only in docs 0 and 2 (not doc 1: quick red fox).
	require.ElementsMatch(t, []any{int64(0), int64(2)}, nlIDs(t, idx, ParserNgram, "brown fox"))
}

// --- gojieba parser: word segmentation, exact-phrase NL works for CJK ---

func TestGojiebaNLChinese(t *testing.T) {
	requireJieba(t)
	idx := zhCorpus(t, ParserGojieba)
	// jieba segments 中文學習 into words; exact-phrase matches docs 0 and 4.
	require.ElementsMatch(t, []any{int64(0), int64(4)}, nlIDs(t, idx, ParserGojieba, "中文學習"))
	// 兒童 is a jieba word present in docs 0 and 2.
	require.ElementsMatch(t, []any{int64(0), int64(2)}, nlIDs(t, idx, ParserGojieba, "兒童"))
}

func TestGojiebaBooleanChinese(t *testing.T) {
	requireJieba(t)
	idx := zhCorpus(t, ParserGojieba)
	require.ElementsMatch(t, []any{int64(0), int64(4)}, boolIDs(t, idx, ParserGojieba, "+中文 +學習"))
	require.ElementsMatch(t, []any{int64(2)}, boolIDs(t, idx, ParserGojieba, "+兒童 -中文"))
}

// --- json parser: values flattened to text, then indexed/queried as ngram ---

func TestJSONParser(t *testing.T) {
	docs := []Doc{
		{int64(0), []byte(`{"a":1, "b":"red apple"}`)},
		{int64(1), []byte(`{"a":2, "b":"中文學習教材"}`)},
		{int64(2), []byte(`{"a":3, "b":"red blue"}`)},
	}
	seg, err := BuildSegmentFromDocsParser("js", int32(types.T_int64), docs, ParserJSON)
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)
	// Latin value token, boolean.
	require.ElementsMatch(t, []any{int64(0), int64(2)}, boolIDs(t, idx, ParserJSON, "red"))
	// CJK value: 中文學習 -> ngram bag-of-words, only doc 1.
	require.ElementsMatch(t, []any{int64(1)}, nlIDs(t, idx, ParserJSON, "中文學習"))
}
