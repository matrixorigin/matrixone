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

package fulltext

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// collectSQL flattens every Sql string in a []*SqlNode tree.
func collectSQL(ns []*SqlNode) string {
	var b strings.Builder
	var walk func(n *SqlNode)
	walk = func(n *SqlNode) {
		b.WriteString(n.Sql)
		b.WriteByte('\n')
		for _, c := range n.Children {
			walk(c)
		}
	}
	for _, n := range ns {
		walk(n)
	}
	return b.String()
}

// #29273: the star-phrase expansion must fire at EVERY SQL-gen site (GenJoinPlusSql, GenJoinSql,
// GenSql no-join, GenSql join-context), so a multi-trigram CJK stem becomes the positional phrase
// (word = '苹果香' + prefix_eq('蕉')) instead of prefix_eq of the never-stored 4-rune string.
func TestStarPhraseAllGenSites(t *testing.T) {
	mode := int64(tree.FULLTEXT_BOOLEAN)
	idx := "__idx"
	star := func() *Pattern { return &Pattern{Text: "苹果香蕉*", Operator: STAR, Index: 0} }
	wantPhrase := func(sql string) {
		require.Contains(t, sql, "word = '苹果香'", sql)
		require.Contains(t, sql, "prefix_eq(word,'蕉')", sql)
		require.NotContains(t, sql, "prefix_eq(word,'苹果香蕉')", sql)
	}

	ns, err := GenJoinPlusSql(star(), mode, idx, "")
	require.NoError(t, err)
	wantPhrase(collectSQL(ns))

	ns, err = GenJoinSql(star(), mode, idx, "")
	require.NoError(t, err)
	wantPhrase(collectSQL(ns))

	ns, err = GenSql(star(), mode, idx, nil, false, "")
	require.NoError(t, err)
	wantPhrase(collectSQL(ns))

	// join-context (joinsql non-empty, isJoin=false) hits the 4th site.
	joinsql := []*SqlNode{{Index: 0, Label: "t0", Sql: "SELECT doc_id, CAST(0 as int) FROM t0"}}
	ns, err = GenSql(star(), mode, idx, joinsql, false, "")
	require.NoError(t, err)
	wantPhrase(collectSQL(ns))
}

// #29273: truncateStarPrefix caps an over-long Latin star prefix at the stored-token byte limit on a
// rune boundary; short/CJK prefixes pass through untouched.
func TestTruncateStarPrefix(t *testing.T) {
	require.Equal(t, "abc", truncateStarPrefix("abc"))
	require.Equal(t, strings.Repeat("a", tokenizer.MAX_TOKEN_SIZE),
		truncateStarPrefix(strings.Repeat("a", tokenizer.MAX_TOKEN_SIZE)))
	require.Equal(t, strings.Repeat("b", tokenizer.MAX_TOKEN_SIZE),
		truncateStarPrefix(strings.Repeat("b", 30)))
	require.Equal(t, "蕉", truncateStarPrefix("蕉"))
	// 12 x 2-byte rune (24 bytes) must cap on a rune boundary, never mid-rune.
	got := truncateStarPrefix(strings.Repeat("é", 12))
	require.LessOrEqual(t, len(got), tokenizer.MAX_TOKEN_SIZE)
	require.True(t, utf8.ValidString(got))
}

// #29273: genStarPhraseSql returns ok=true only for a stem that must become a positional phrase (a
// >3-rune CJK stem, or a multi-word gojieba stem); everything else keeps the plain prefix_eq.
func TestGenStarPhraseSql(t *testing.T) {
	mode := int64(tree.FULLTEXT_BOOLEAN)
	// non-star kw -> ok=false (early return).
	_, ok, err := genStarPhraseSql("苹果香蕉", mode, "idx", "")
	require.NoError(t, err)
	require.False(t, ok)
	// >3-rune CJK -> ok=true.
	sql, ok, err := genStarPhraseSql("苹果香蕉*", mode, "idx", "")
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, sql, "prefix_eq(word,'蕉')")
	// exactly one trigram -> ok=false (plain prefix).
	_, ok, err = genStarPhraseSql("苹果香*", mode, "idx", "")
	require.NoError(t, err)
	require.False(t, ok)
	// Latin -> ok=false.
	_, ok, err = genStarPhraseSql("hello*", mode, "idx", "")
	require.NoError(t, err)
	require.False(t, ok)
}

func boolSQL(t *testing.T, q, parser string) string {
	t.Helper()
	s, err := NewSearchAccum("src", "idx", q, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
	require.NoError(t, err, q)
	sql, err := PatternToSql(s.Pattern, s.Mode, s.TblName, parser, ALGO_TFIDF)
	require.NoError(t, err, q)
	return sql
}

// #29273: a boolean trailing `*` on a CJK stem >1 trigram used to prefix_eq the whole (never-stored)
// stem and return nothing. It must expand to a positional phrase whose last token is a prefix_eq, so
// `苹果香蕉*` matches 苹果香蕉 / 苹果香蕉西瓜 but not 苹果香瓜 -- and `+苹果香蕉*` behaves identically.
func TestStarCJKExpandsToPositionalPhrase(t *testing.T) {
	for _, q := range []string{"苹果香蕉*", "+苹果香蕉*"} {
		sql := boolSQL(t, q, "")
		require.Contains(t, sql, "word = '苹果香'", q)              // leading trigram exact
		require.Contains(t, sql, "prefix_eq(word,'蕉')", q)       // tail rune as prefix
		require.Contains(t, sql, "HAVING COUNT(*) = 2", q)       // positional 2-token phrase
		require.NotContains(t, sql, "prefix_eq(word,'苹果香蕉')", q) // never the raw 4-rune stem
	}

	// 6-rune clean multiple: leading trigram + last trigram as the prefix.
	sql := boolSQL(t, "苹果香蕉西瓜*", "")
	require.Contains(t, sql, "word = '苹果香'")
	require.Contains(t, sql, "prefix_eq(word,'蕉西瓜')")

	// A stem <= one trigram stays a plain prefix (existing behavior).
	require.Contains(t, boolSQL(t, "苹果香*", ""), "prefix_eq(word,'苹果香')")
	require.NotContains(t, boolSQL(t, "苹果香*", ""), "HAVING COUNT")
	require.Contains(t, boolSQL(t, "苹果*", ""), "prefix_eq(word,'苹果')")
}

// #29273 (Latin facet): a Latin stem longer than the 23-byte stored-token cap must prefix_eq the
// truncated token, not the raw stem.
func TestStarLatinTruncatesToTokenCap(t *testing.T) {
	sql := boolSQL(t, strings.Repeat("b", 26)+"*", "")
	require.Contains(t, sql, "prefix_eq(word,'"+strings.Repeat("b", 23)+"')")
	require.NotContains(t, sql, strings.Repeat("b", 24))
}

// #29273 (gojieba facet): a multi-word stem segments and stars the last word.
func TestStarGojiebaLastSegment(t *testing.T) {
	if _, err := tokenizer.SharedJiebaTokenizer(false); err != nil {
		t.Skip("jieba dictionary not available:", err)
	}
	sql := boolSQL(t, "苹果香蕉*", "gojieba")
	require.Contains(t, sql, "word = '苹果'")
	require.Contains(t, sql, "prefix_eq(word,'香蕉')")
}
