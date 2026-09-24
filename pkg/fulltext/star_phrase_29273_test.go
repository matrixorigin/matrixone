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

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

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
