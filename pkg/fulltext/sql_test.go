// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscape(t *testing.T) {
	assert.Equal(t, `ma\'trix`, escape("ma'trix"))
	assert.Equal(t, `back\\slash`, escape(`back\slash`))
	assert.Equal(t, `back\\slash\'s`, escape(`back\slash's`))
	assert.Equal(t, `normal`, escape("normal"))
}

func TestSqlPhraseBM25(t *testing.T) {
	tests := []TestCase{
		{
			pattern: "\"Ma'trix Origin\"",
			expect:  "select a.*, CAST(COALESCE(dl.pos, 0) AS INT) as doc_len from (SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'ma\\'trix' UNION ALL SELECT doc_id, pos - 8 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id) a LEFT JOIN `__mo_index_secondary_` dl ON a.doc_id = dl.doc_id AND dl.word = '__DocLen'",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), idxTable, "", ALGO_BM25)
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlPhrase(t *testing.T) {
	tests := []TestCase{
		{
			pattern: "\"Ma'trix Origin\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'ma\\'trix' UNION ALL SELECT doc_id, pos - 8 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id",
		},
		{
			pattern: "\"Matrix Origin\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id",
		},
		{
			pattern: "\"Matrix\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id",
		},
		{
			pattern: "\"    Matrix     \"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id",
		},
		{
			pattern: "\"Matrix     Origin\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 11 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id",
		},
		{
			pattern: "\"  你好嗎? Hello World  在一起  Happy  再见  \"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = '你好嗎?' UNION ALL SELECT doc_id, pos - 11 AS anchor FROM `__mo_index_secondary_` WHERE word = 'hello' UNION ALL SELECT doc_id, pos - 17 AS anchor FROM `__mo_index_secondary_` WHERE word = 'world' UNION ALL SELECT doc_id, pos - 24 AS anchor FROM `__mo_index_secondary_` WHERE word = '在一起' UNION ALL SELECT doc_id, pos - 35 AS anchor FROM `__mo_index_secondary_` WHERE word = 'happy' UNION ALL SELECT doc_id, pos - 42 AS anchor FROM `__mo_index_secondary_` WHERE word = '再见') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 6) phrase GROUP BY doc_id",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), idxTable, "", ALGO_TFIDF)
		require.Nil(t, err)
		//fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlBoolean(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma') UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = 'trix') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "Matrix Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "+Matrix Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
		},
		{
			pattern: "+Matrix -Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
		},
		{
			pattern: "Matrix ~Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "Matrix +(<Origin >One)",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'one' GROUP BY doc_id), t2 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t1.doc_id, CAST(1 as int) FROM t1 UNION ALL SELECT t0.doc_id, CAST(2 as int) FROM t2, t0 WHERE t0.doc_id = t2.doc_id UNION ALL SELECT t1.doc_id, CAST(2 as int) FROM t2, t1 WHERE t1.doc_id = t2.doc_id",
		},
		{
			pattern: "+Matrix +Origin",
			expect:  "WITH t00 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t01 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id), t0 AS (SELECT t00.doc_id FROM t00, t01 WHERE t00.doc_id = t01.doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0",
		},
		{
			pattern: "\"Matrix origin\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id",
		},
		{
			pattern: "Matrix Origin*",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM `__mo_index_secondary_` WHERE prefix_eq(word,'origin')",
		},
		{
			pattern: "+Matrix* Origin*",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'matrix')) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM `__mo_index_secondary_` as t1, t0 WHERE t0.doc_id = t1.doc_id AND prefix_eq(t1.word, 'origin')",
		},
		{
			pattern: "+Matrix +(Origin (One Two))",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id), t2 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'one' GROUP BY doc_id), t3 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'two' GROUP BY doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id UNION ALL SELECT t0.doc_id, CAST(2 as int) FROM t2, t0 WHERE t0.doc_id = t2.doc_id UNION ALL SELECT t0.doc_id, CAST(3 as int) FROM t3, t0 WHERE t0.doc_id = t3.doc_id",
		},
		{
			pattern: "+读写汉字 -学中文",
			expect:  "WITH t0 AS (SELECT doc_id FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = '读写汉' UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = '写汉字') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = '学中文' GROUP BY doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
		},
		{
			pattern: "+SGB11型号的检验报告",
			expect:  "WITH t0 AS (SELECT doc_id FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'sgb11' UNION ALL SELECT doc_id, pos - 5 AS anchor FROM `__mo_index_secondary_` WHERE word = '型号的' UNION ALL SELECT doc_id, pos - 14 AS anchor FROM `__mo_index_secondary_` WHERE word = '检验报' UNION ALL SELECT doc_id, pos - 17 AS anchor FROM `__mo_index_secondary_` WHERE word = '验报告') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 4) phrase GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0",
		},
		{
			pattern: "+读书会 +提效 +社群 +案例 +运营",
			expect:  "WITH t00 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = '读书会' GROUP BY doc_id), t01 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'提效') GROUP BY doc_id), t02 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'社群') GROUP BY doc_id), t03 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'案例') GROUP BY doc_id), t04 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'运营') GROUP BY doc_id), t0 AS (SELECT t00.doc_id FROM t00, t01, t02, t03, t04 WHERE t00.doc_id = t01.doc_id AND t00.doc_id = t02.doc_id AND t00.doc_id = t03.doc_id AND t00.doc_id = t04.doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_TFIDF)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), idxTable, "", ALGO_TFIDF)
		//fmt.Println(PatternListToStringWithPosition(s.Pattern))
		require.Nil(t, err)
		//fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlBooleanBM25(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "select a.*, CAST(COALESCE(dl.pos, 0) AS INT) as doc_len from (WITH t0 AS (SELECT doc_id FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma') UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = 'trix') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) phrase GROUP BY doc_id), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin' GROUP BY doc_id) SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1) a LEFT JOIN `__mo_index_secondary_` dl ON a.doc_id = dl.doc_id AND dl.word = '__DocLen'",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_BOOLEAN), idxTable, "", ALGO_BM25)
		require.Nil(t, err)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlNL(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma') UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = 'trix' UNION ALL SELECT doc_id, pos - 8 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 3",
		},
		{
			pattern: "Matrix Origin",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2",
		},
		{
			pattern: "读写汉字 学中文",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = '读写汉' UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = '写汉字' UNION ALL SELECT doc_id, pos - 13 AS anchor FROM `__mo_index_secondary_` WHERE word = '学中文') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 3",
		},
		{
			pattern: "读写",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE prefix_eq(word,'读写')",
		},
		{
			pattern: "肥胖的原因都是因为摄入脂肪多导致的吗",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = '肥胖的' UNION ALL SELECT doc_id, pos - 9 AS anchor FROM `__mo_index_secondary_` WHERE word = '原因都' UNION ALL SELECT doc_id, pos - 18 AS anchor FROM `__mo_index_secondary_` WHERE word = '是因为' UNION ALL SELECT doc_id, pos - 27 AS anchor FROM `__mo_index_secondary_` WHERE word = '摄入脂' UNION ALL SELECT doc_id, pos - 36 AS anchor FROM `__mo_index_secondary_` WHERE word = '肪多导' UNION ALL SELECT doc_id, pos - 45 AS anchor FROM `__mo_index_secondary_` WHERE word = '致的吗') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 6",
		},
		{
			pattern: "肥胖的原因都是因为摄入fat多导致的吗",
			expect:  "SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = '肥胖的' UNION ALL SELECT doc_id, pos - 9 AS anchor FROM `__mo_index_secondary_` WHERE word = '原因都' UNION ALL SELECT doc_id, pos - 18 AS anchor FROM `__mo_index_secondary_` WHERE word = '是因为' UNION ALL SELECT doc_id, pos - 24 AS anchor FROM `__mo_index_secondary_` WHERE word = '为摄入' UNION ALL SELECT doc_id, pos - 33 AS anchor FROM `__mo_index_secondary_` WHERE word = 'fat' UNION ALL SELECT doc_id, pos - 36 AS anchor FROM `__mo_index_secondary_` WHERE word = '多导致' UNION ALL SELECT doc_id, pos - 42 AS anchor FROM `__mo_index_secondary_` WHERE word = '致的吗') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 7",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_NL), "", ALGO_TFIDF)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_NL), idxTable, "", ALGO_TFIDF)
		require.Nil(t, err)
		//fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSqlNLBM25(t *testing.T) {

	tests := []TestCase{
		{
			pattern: "Ma'trix Origin",
			expect:  "select a.*, CAST(COALESCE(dl.pos, 0) AS INT) as doc_len from (SELECT doc_id, CAST(0 as int) FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma') UNION ALL SELECT doc_id, pos - 3 AS anchor FROM `__mo_index_secondary_` WHERE word = 'trix' UNION ALL SELECT doc_id, pos - 8 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 3) a LEFT JOIN `__mo_index_secondary_` dl ON a.doc_id = dl.doc_id AND dl.word = '__DocLen'",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, c := range tests {
		s, err := NewSearchAccum("src", "index", c.pattern, int64(tree.FULLTEXT_NL), "", ALGO_BM25)
		require.Nil(t, err)
		result, err := PatternToSql(s.Pattern, int64(tree.FULLTEXT_NL), idxTable, "", ALGO_BM25)
		require.Nil(t, err)
		//fmt.Println(result)
		assert.Equal(t, c.expect, result)
	}
}

func TestSingleKeywordTopKSQL(t *testing.T) {
	tests := []struct {
		pattern    string
		mode       int64
		expectOK   bool
		expectTopK string
	}{
		{
			pattern:    "Matrix",
			mode:       int64(tree.FULLTEXT_NL),
			expectOK:   true,
			expectTopK: "SELECT doc_id, tf, nmatch FROM (SELECT doc_id, tf, COUNT(*) OVER() AS nmatch FROM (SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id) a) ranked ORDER BY tf DESC LIMIT 5",
		},
		{
			pattern:    "Matrix",
			mode:       int64(tree.FULLTEXT_DEFAULT),
			expectOK:   true,
			expectTopK: "SELECT doc_id, tf, nmatch FROM (SELECT doc_id, tf, COUNT(*) OVER() AS nmatch FROM (SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id) a) ranked ORDER BY tf DESC LIMIT 5",
		},
		{
			pattern:  "+Matrix",
			mode:     int64(tree.FULLTEXT_BOOLEAN),
			expectOK: false,
		},
		{
			pattern:    "读写",
			mode:       int64(tree.FULLTEXT_NL),
			expectOK:   true,
			expectTopK: "SELECT doc_id, tf, nmatch FROM (SELECT doc_id, tf, COUNT(*) OVER() AS nmatch FROM (SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM `__mo_index_secondary_` WHERE prefix_eq(word,'读写') GROUP BY doc_id) a) ranked ORDER BY tf DESC LIMIT 5",
		},
	}

	idxTable := "`__mo_index_secondary_`"
	for _, tc := range tests {
		s, err := NewSearchAccum("src", "index", tc.pattern, tc.mode, "", ALGO_TFIDF)
		require.NoError(t, err)

		topKSQL, ok, err := SingleKeywordTopKSQL(s.Pattern, tc.mode, idxTable, 5)
		require.NoError(t, err)
		require.Equal(t, tc.expectOK, ok)
		if !tc.expectOK {
			assert.Empty(t, topKSQL)
			continue
		}
		assert.Equal(t, tc.expectTopK, topKSQL)
	}
}

func TestSingleKeywordTopKBM25SQL(t *testing.T) {
	idxTable := "`__mo_index_secondary_`"
	s, err := NewSearchAccum("src", "index", "Matrix", int64(tree.FULLTEXT_NL), "", ALGO_BM25)
	require.NoError(t, err)

	sql, ok, err := SingleKeywordTopKBM25SQL(s.Pattern, int64(tree.FULLTEXT_NL), idxTable, 10.5, 5)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t,
		"SELECT doc_id, score, nmatch FROM (SELECT a.doc_id, (a.tf * (1.5 + 1) / (a.tf + 1.5 * (1 - 0.75 + 0.75 * (CAST(COALESCE(dl.pos, 0) AS INT) / 10.5)))) AS score, COUNT(*) OVER() AS nmatch FROM (SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM `__mo_index_secondary_` WHERE word = 'matrix' GROUP BY doc_id) a LEFT JOIN `__mo_index_secondary_` dl ON a.doc_id = dl.doc_id AND dl.word = '__DocLen') ranked ORDER BY score DESC LIMIT 5",
		sql,
	)

	booleanAccum, err := NewSearchAccum("src", "index", "+Matrix", int64(tree.FULLTEXT_BOOLEAN), "", ALGO_BM25)
	require.NoError(t, err)

	sql, ok, err = SingleKeywordTopKBM25SQL(booleanAccum.Pattern, int64(tree.FULLTEXT_BOOLEAN), idxTable, 10.5, 5)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, sql)
}

func TestPhraseTopKSQL(t *testing.T) {
	idxTable := "`__mo_index_secondary_`"
	s, err := NewSearchAccum("src", "index", "Matrix Origin", int64(tree.FULLTEXT_NL), "", ALGO_TFIDF)
	require.NoError(t, err)

	countSQL, ok, err := PhraseCountSQL(s.Pattern, int64(tree.FULLTEXT_NL), idxTable)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "SELECT COUNT(DISTINCT doc_id) FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) ft", countSQL)

	topKSQL, ok, err := PhraseTopKSQL(s.Pattern, int64(tree.FULLTEXT_NL), idxTable, 5)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) ft GROUP BY doc_id ORDER BY tf DESC LIMIT 5", topKSQL)
}

func TestPhraseTopKBM25SQL(t *testing.T) {
	idxTable := "`__mo_index_secondary_`"
	s, err := NewSearchAccum("src", "index", "Matrix Origin", int64(tree.FULLTEXT_NL), "", ALGO_BM25)
	require.NoError(t, err)

	sql, ok, err := PhraseTopKBM25SQL(s.Pattern, int64(tree.FULLTEXT_NL), idxTable, 1.2345, 10.5, s.Nkeywords, 5)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "SELECT a.doc_id, 2 * 1.2344999999999999 * (a.tf * (1.5 + 1) / (a.tf + 1.5 * (1 - 0.75 + 0.75 * (CAST(COALESCE(dl.pos, 0) AS INT) / 10.5)))) AS score FROM (SELECT doc_id, CASE WHEN COUNT(*) > 255 THEN 255 ELSE COUNT(*) END AS tf FROM (SELECT doc_id FROM (SELECT doc_id, pos - 0 AS anchor FROM `__mo_index_secondary_` WHERE word = 'matrix' UNION ALL SELECT doc_id, pos - 7 AS anchor FROM `__mo_index_secondary_` WHERE word = 'origin') anchors GROUP BY doc_id, anchor HAVING COUNT(*) = 2) ft GROUP BY doc_id) a LEFT JOIN `__mo_index_secondary_` dl ON a.doc_id = dl.doc_id AND dl.word = '__DocLen' ORDER BY score DESC LIMIT 5", sql)

	single, err := NewSearchAccum("src", "index", "Matrix", int64(tree.FULLTEXT_NL), "", ALGO_BM25)
	require.NoError(t, err)

	sql, ok, err = PhraseTopKBM25SQL(single.Pattern, int64(tree.FULLTEXT_NL), idxTable, 1.2345, 10.5, single.Nkeywords, 5)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, sql)
}
