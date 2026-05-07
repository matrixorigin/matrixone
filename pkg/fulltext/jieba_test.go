// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func collectTexts(ps []*Pattern) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.Text)
	}
	return out
}

func TestParsePatternInNLModeJieba(t *testing.T) {
	ps, err := ParsePatternInNLMode("我来到北京清华大学", "gojieba")
	require.Nil(t, err)
	assert.Equal(t, []string{"我", "来到", "北京", "清华大学"}, collectTexts(ps))
	for _, p := range ps {
		// jieba path emits TEXT patterns only (no STAR rewrites).
		assert.Equal(t, TEXT, p.Operator)
	}
}

func TestParsePatternInNLModeNgramUnchanged(t *testing.T) {
	// Empty parser keeps the existing ngram behavior: short input → STAR prefix.
	ps, err := ParsePatternInNLMode("hi", "")
	require.Nil(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "hi*", ps[0].Text)
	assert.Equal(t, STAR, ps[0].Operator)
}

func TestParsePatternRoutesByParser(t *testing.T) {
	// gojieba: "苹果香蕉" segments cleanly into two TEXT tokens.
	ps, err := ParsePattern("苹果香蕉", int64(tree.FULLTEXT_NL), "gojieba")
	require.Nil(t, err)
	texts := collectTexts(ps)
	assert.Equal(t, []string{"苹果", "香蕉"}, texts)

	// ngram path on the same input would emit overlapping bigrams, which is
	// markedly different from a 2-element exact word list.
	ngramPs, err := ParsePattern("苹果香蕉", int64(tree.FULLTEXT_NL), "")
	require.Nil(t, err)
	assert.NotEqual(t, texts, collectTexts(ngramPs))
}

func TestNewSearchAccumExtractsParserFromParams(t *testing.T) {
	s, err := NewSearchAccum("src", "idx", "我爱北京",
		int64(tree.FULLTEXT_NL), `{"parser":"gojieba"}`, ALGO_TFIDF)
	require.Nil(t, err)
	require.Len(t, s.Pattern, 3)
	assert.Equal(t, []string{"我", "爱", "北京"}, collectTexts(s.Pattern))
}

func TestParserFromParams(t *testing.T) {
	cases := []struct {
		params string
		want   string
	}{
		{"", ""},
		{`{"parser":"gojieba"}`, "gojieba"},
		{`{"parser":"ngram"}`, "ngram"},
		{`{"async":"true"}`, ""},
	}
	for _, c := range cases {
		got, err := parserFromParams(c.params)
		require.Nil(t, err, c.params)
		assert.Equal(t, c.want, got, c.params)
	}

	_, err := parserFromParams("not json")
	assert.NotNil(t, err)
}

func TestPatternToSqlGojiebaBoolean(t *testing.T) {
	// Re-tokenization in boolean mode for a Chinese keyword must use jieba.
	s, err := NewSearchAccum("src", "idx", "+清华大学",
		int64(tree.FULLTEXT_BOOLEAN), `{"parser":"gojieba"}`, ALGO_TFIDF)
	require.Nil(t, err)

	sql, err := PatternToSql(s.Pattern, s.Mode, s.TblName, "gojieba", ALGO_TFIDF)
	require.Nil(t, err)
	// Jieba keeps "清华大学" as a single dictionary word, so the SQL must
	// look up that exact word — not 3-char overlapping bigrams.
	assert.Contains(t, sql, "word = '清华大学'")
	assert.False(t, strings.Contains(sql, "word = '清华大'"),
		"sql should not contain ngram bigram 清华大: %s", sql)
}
