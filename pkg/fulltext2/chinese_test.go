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

// Chinese (CJK) search coverage over a jieba-friendly corpus (ported from the
// classic fulltext jieba tests' vocabulary: 北京/清华大学/苹果/香蕉/天安门). gojieba
// segments into dictionary words so NL is exact-phrase and boolean operands are
// whole words; ngram tiles CJK into 3-grams so NL is bag-of-words.
package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// zhSimplified indexes the corpus under parser and returns a queryable Index.
//
//	0: 我来到北京清华大学   (我 来到 北京 清华大学)
//	1: 苹果香蕉都好吃      (苹果 香蕉 都 好吃)
//	2: 我爱北京天安门      (我 爱 北京 天安门)
//	3: 清华大学在北京      (清华大学 在 北京)
//	4: 香蕉和苹果         (香蕉 和 苹果)
func zhSimplified(t *testing.T, parser string) *Index {
	docs := []Doc{
		{int64(0), []byte("我来到北京清华大学")},
		{int64(1), []byte("苹果香蕉都好吃")},
		{int64(2), []byte("我爱北京天安门")},
		{int64(3), []byte("清华大学在北京")},
		{int64(4), []byte("香蕉和苹果")},
	}
	seg, err := BuildSegmentFromDocsParser("zh", int32(types.T_int64), docs, parser)
	require.NoError(t, err)
	return NewIndex([]*Segment{seg}, nil)
}

// --- gojieba: dictionary word segmentation ---

func TestGojiebaWordSearch(t *testing.T) {
	requireJieba(t)
	idx := zhSimplified(t, ParserGojieba)
	// single jieba words.
	require.ElementsMatch(t, []any{int64(0), int64(2), int64(3)}, nlIDs(t, idx, ParserGojieba, "北京"))
	require.ElementsMatch(t, []any{int64(0), int64(3)}, nlIDs(t, idx, ParserGojieba, "清华大学"))
	require.ElementsMatch(t, []any{int64(1), int64(4)}, nlIDs(t, idx, ParserGojieba, "苹果"))
	require.ElementsMatch(t, []any{int64(2)}, nlIDs(t, idx, ParserGojieba, "天安门"))
}

func TestGojiebaPhrase(t *testing.T) {
	requireJieba(t)
	idx := zhSimplified(t, ParserGojieba)
	// NL is exact phrase of jieba words: 北京·清华大学 is contiguous only in doc 0
	// (doc 3 is 清华大学·在·北京 — the words appear in the other order).
	require.ElementsMatch(t, []any{int64(0)}, nlIDs(t, idx, ParserGojieba, "北京清华大学"))
}

func TestGojiebaBoolean(t *testing.T) {
	requireJieba(t)
	idx := zhSimplified(t, ParserGojieba)
	// +北京 -苹果: 北京 present, 苹果 absent.
	require.ElementsMatch(t, []any{int64(0), int64(2), int64(3)}, boolIDs(t, idx, ParserGojieba, "+北京 -苹果"))
	// +苹果 +香蕉: both present.
	require.ElementsMatch(t, []any{int64(1), int64(4)}, boolIDs(t, idx, ParserGojieba, "+苹果 +香蕉"))
	// bag-of-words OR.
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, boolIDs(t, idx, ParserGojieba, "苹果 天安门"))
	// group: +北京 +(清华大学 天安门).
	require.ElementsMatch(t, []any{int64(0), int64(2), int64(3)}, boolIDs(t, idx, ParserGojieba, "+北京 +(清华大学 天安门)"))
}

// --- ngram: 3-gram tiling, CJK NL = bag-of-words ---

func TestNgramChineseSearch(t *testing.T) {
	idx := zhSimplified(t, ParserNgram)
	// 清华大学 -> 3-grams 清华大/华大学 (bag); docs 0 and 3 contain them.
	require.ElementsMatch(t, []any{int64(0), int64(3)}, nlIDs(t, idx, ParserNgram, "清华大学"))
	// +苹果 +香蕉 boolean over ngrams: docs 1 and 4.
	require.ElementsMatch(t, []any{int64(1), int64(4)}, boolIDs(t, idx, ParserNgram, "+苹果 +香蕉"))
}

// --- non-int pk carrying Chinese (covers builderCopyPk/[]byte pk path) ---

func TestGojiebaBytesPk(t *testing.T) {
	requireJieba(t)
	docs := []Doc{
		{[]byte("doc-甲"), []byte("北京清华大学")},
		{[]byte("doc-乙"), []byte("上海交通大学")},
	}
	seg, err := BuildSegmentFromDocsParser("zhpk", int32(types.T_varchar), docs, ParserGojieba)
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)
	got := nlIDs(t, idx, ParserGojieba, "清华大学")
	require.Len(t, got, 1)
	require.Equal(t, "doc-甲", string(got[0].([]byte)))
}
