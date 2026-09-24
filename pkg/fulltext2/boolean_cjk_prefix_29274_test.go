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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// #29274: a BOOLEAN-mode CJK operand with a trailing `*` used to keep only the FIRST trigram and
// prefix-match it, so `苹果香蕉*` matched 苹果香瓜 (which shares 苹果香 but diverges at rune 4). All
// the ngrams fully contained in the prefix must be required -- the same positional phrase the
// unstarred operand builds -- so `苹果香蕉*` matches 苹果香蕉 / 苹果香蕉西瓜 but not 苹果香瓜. A stem
// shorter than one trigram keeps its prefix behavior.
func TestBooleanCJKTrailingStarKeepsWholeStem(t *testing.T) {
	docs := []Doc{
		{int64(1), []byte("苹果香蕉")},
		{int64(2), []byte("苹果香瓜")},
		{int64(3), []byte("苹果香蕉西瓜")},
		{int64(4), []byte("红苹果甜")},
	}
	// "" / "default" / "ngram" all normalize to the SimpleTokenizer that exhibits the bug.
	for _, parser := range []string{"", ParserDefault, ParserNgram} {
		t.Run("parser="+parser, func(t *testing.T) {
			seg, err := BuildSegmentFromDocsParser("zh", int32(types.T_int64), docs, parser)
			require.NoError(t, err)
			idx := NewIndex([]*Segment{seg}, nil)

			// The bug: `苹果香蕉*` must NOT return doc 2 (苹果香瓜).
			require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, parser, "苹果香蕉*"),
				"trailing * must keep the whole stem, not just the first trigram (#29274)")
			require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, parser, "+苹果香蕉*"))
			// Control: the unstarred phrase already returned the correct set.
			require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, parser, "+苹果香蕉"))
			// Control: natural-language (bag-of-words phrase) agrees.
			require.ElementsMatch(t, []any{int64(1), int64(3)}, nlIDs(t, idx, parser, "苹果香蕉"))

			// A stem == one trigram: `苹果香*` matches every doc containing 苹果香 (1,2,3).
			require.ElementsMatch(t, []any{int64(1), int64(2), int64(3)}, boolIDs(t, idx, parser, "苹果香*"))
			// A stem shorter than one trigram keeps genuine prefix behavior: `苹果*` matches all
			// docs with a token prefixed by 苹果 (including 红苹果甜's 苹果甜).
			require.ElementsMatch(t, []any{int64(1), int64(2), int64(3), int64(4)}, boolIDs(t, idx, parser, "苹果*"))
		})
	}
}

// TestBooleanGojiebaTrailingStarKeepsWholeStem: gojieba has the same class of bug via word
// segmentation -- `苹果香蕉*` -> words [苹果, 香蕉] kept only 苹果 and prefix-matched it, returning
// every 苹果* doc. A MULTI-word stem must require all words (phrase); a single-word stem keeps its
// word prefix.
func TestBooleanGojiebaTrailingStarKeepsWholeStem(t *testing.T) {
	requireJieba(t)
	docs := []Doc{
		{int64(1), []byte("苹果香蕉")},
		{int64(2), []byte("苹果香瓜")},
		{int64(3), []byte("苹果香蕉西瓜")},
		{int64(4), []byte("红苹果甜")},
	}
	seg, err := BuildSegmentFromDocsParser("zh", int32(types.T_int64), docs, ParserGojieba)
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	// Multi-word stem: require both words, do not drop 香蕉. Was {1,2,3,4}.
	require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, ParserGojieba, "苹果香蕉*"),
		"gojieba trailing * must require every word of the stem, not just the first (#29274)")
	require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, ParserGojieba, "+苹果香蕉*"))
	require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, ParserGojieba, "+苹果香蕉"))
	// Single-word stem keeps genuine word prefix (matches 红苹果甜's 苹果 as well).
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(3), int64(4)}, boolIDs(t, idx, ParserGojieba, "苹果*"))
}

// TestRawToClauseParserEdgeCases drives rawToClauseParser directly (it is unexported and some
// branches are unreachable from a well-formed AGAINST() string) over the operand shapes and empty
// inputs it must classify, including the `*`-with/without-CJK split this fix hinges on.
func TestRawToClauseParserEdgeCases(t *testing.T) {
	// group whose only child tokenizes to nothing -> no usable children -> ok=false. The empty
	// child also exercises the zero-term operand path.
	_, ok, err := rawToClauseParser(rawClause{group: true, children: []rawClause{{text: "   "}}}, ParserNgram)
	require.NoError(t, err)
	require.False(t, ok)

	// group with a usable child -> clauseGroup.
	c, ok, err := rawToClauseParser(rawClause{group: true, children: []rawClause{{text: "hello"}}}, ParserNgram)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clauseGroup, c.kind)

	// json_value: empty -> ok=false; whole-value exact term; value prefix on *.
	_, ok, err = rawToClauseParser(rawClause{text: ""}, ParserJSONValue)
	require.NoError(t, err)
	require.False(t, ok)
	c, ok, err = rawToClauseParser(rawClause{text: "abc"}, ParserJSONValue)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clauseTerm, c.kind)
	c, ok, err = rawToClauseParser(rawClause{text: "abc", star: true}, ParserJSONValue)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clausePrefix, c.kind)

	// non-CJK operand that tokenizes to zero terms -> ok=false.
	_, ok, err = rawToClauseParser(rawClause{text: "   "}, ParserNgram)
	require.NoError(t, err)
	require.False(t, ok)

	// Latin `*` stays a plain first-token prefix (the !hasCJK arm of the star fix).
	c, ok, err = rawToClauseParser(rawClause{text: "hello", star: true}, ParserNgram)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clausePrefix, c.kind)

	// Latin single term / multi-word phrase.
	c, ok, err = rawToClauseParser(rawClause{text: "hello"}, ParserNgram)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clauseTerm, c.kind)
	c, ok, err = rawToClauseParser(rawClause{text: "hello world"}, ParserNgram)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clausePhrase, c.kind)

	// CJK `*` becomes the positional phrase (the #29274 fix).
	c, ok, err = rawToClauseParser(rawClause{text: "苹果香蕉", star: true}, ParserNgram)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clausePhrase, c.kind)
}

// TestPhraseClause covers the shared phrase builder directly, including the no-slot guard an empty
// pattern hits (the callers pre-check the operand, so they never pass an empty pattern).
func TestPhraseClause(t *testing.T) {
	_, ok, err := phraseClause("", ParserNgram, 1.0)
	require.NoError(t, err)
	require.False(t, ok)

	c, ok, err := phraseClause("hello world", ParserNgram, 1.0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, clausePhrase, c.kind)
}
