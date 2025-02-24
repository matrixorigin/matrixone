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

func TestSqlPhraseBM25(t *testing.T) {
	tests := []TestCase{
		{
			pattern: "\"Ma'trix Origin\"",
			expect:  "select a.*, b.pos as doc_len from (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'ma\\'trix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 8) a left join `__mo_index_secondary_` b on a.doc_id = b.doc_id and b.word = '__DocLen'",
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
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'ma\\'trix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 8",
		},
		{
			pattern: "\"Matrix Origin\"",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'matrix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 7",
		},
		{
			pattern: "\"Matrix\"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE word = 'matrix'",
		},
		{
			pattern: "\"    Matrix     \"",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE word = 'matrix'",
		},
		{
			pattern: "\"Matrix     Origin\"",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'matrix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 11",
		},
		{
			pattern: "\"  你好嗎? Hello World  在一起  Happy  再见  \"",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '你好嗎?'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'hello'), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'world'), kw3 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '在一起'), kw4 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'happy'), kw5 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '再见') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1, kw2, kw3, kw4, kw5 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 11 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 17 AND kw0.doc_id = kw3.doc_id AND kw3.pos - kw0.pos = 24 AND kw0.doc_id = kw4.doc_id AND kw4.pos - kw0.pos = 35 AND kw0.doc_id = kw5.doc_id AND kw5.pos - kw0.pos = 42",
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
			expect:  "WITH t0 AS (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma')), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'trix') SELECT kw0.doc_id FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "Matrix Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "+Matrix Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
		},
		{
			pattern: "+Matrix -Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
		},
		{
			pattern: "Matrix ~Origin",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1",
		},
		{
			pattern: "Matrix +(<Origin >One)",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'one'), t2 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix') SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t1.doc_id, CAST(1 as int) FROM t1 UNION ALL SELECT t0.doc_id, CAST(2 as int) FROM t2, t0 WHERE t0.doc_id = t2.doc_id UNION ALL SELECT t1.doc_id, CAST(2 as int) FROM t2, t1 WHERE t1.doc_id = t2.doc_id",
		},
		{
			pattern: "+Matrix +Origin",
			expect:  "WITH t00 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t01 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin'), t0 AS (SELECT t00.doc_id FROM t00, t01 WHERE t00.doc_id = t01.doc_id) SELECT t0.doc_id, CAST(0 as int) FROM t0",
		},
		{
			pattern: "\"Matrix origin\"",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'matrix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 7",
		},
		{
			pattern: "Matrix Origin*",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix') SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM `__mo_index_secondary_` WHERE prefix_eq(word,'origin')",
		},
		{
			pattern: "+Matrix* Origin*",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE prefix_eq(word,'matrix')) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM `__mo_index_secondary_` as t1, t0 WHERE t0.doc_id = t1.doc_id AND prefix_eq(t1.word, 'origin')",
		},
		{
			pattern: "+Matrix +(Origin (One Two))",
			expect:  "WITH t0 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'matrix'), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin'), t2 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'one'), t3 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'two') SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id UNION ALL SELECT t0.doc_id, CAST(2 as int) FROM t2, t0 WHERE t0.doc_id = t2.doc_id UNION ALL SELECT t0.doc_id, CAST(3 as int) FROM t3, t0 WHERE t0.doc_id = t3.doc_id",
		},
		{
			pattern: "+读写汉字 -学中文",
			expect:  "WITH t0 AS (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '读写汉'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '写汉字'), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'汉字')), kw3 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'字')) SELECT kw0.doc_id FROM kw0, kw1, kw2, kw3 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 6 AND kw0.doc_id = kw3.doc_id AND kw3.pos - kw0.pos = 9), t1 AS (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '学中文'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'中文')), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'文')) SELECT kw0.doc_id FROM kw0, kw1, kw2 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 6) SELECT t0.doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t1, t0 WHERE t0.doc_id = t1.doc_id",
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
			expect:  "select a.*, b.pos as doc_len from (WITH t0 AS (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma')), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'trix') SELECT kw0.doc_id FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3), t1 AS (SELECT doc_id FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT doc_id, CAST(0 as int) FROM t0 UNION ALL SELECT doc_id, CAST(1 as int) FROM t1) a left join `__mo_index_secondary_` b on a.doc_id = b.doc_id and b.word = '__DocLen'",
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
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma')), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'trix'), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1, kw2 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 8",
		},
		{
			pattern: "Matrix Origin",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'matrix'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 7",
		},
		{
			pattern: "读写汉字 学中文",
			expect:  "WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '读写汉'), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '写汉字'), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'汉字')), kw3 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'字')), kw4 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = '学中文'), kw5 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'中文')), kw6 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'文')) SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1, kw2, kw3, kw4, kw5, kw6 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 6 AND kw0.doc_id = kw3.doc_id AND kw3.pos - kw0.pos = 9 AND kw0.doc_id = kw4.doc_id AND kw4.pos - kw0.pos = 13 AND kw0.doc_id = kw5.doc_id AND kw5.pos - kw0.pos = 16 AND kw0.doc_id = kw6.doc_id AND kw6.pos - kw0.pos = 19",
		},
		{
			pattern: "读写",
			expect:  "SELECT doc_id, CAST(0 as int) FROM `__mo_index_secondary_` WHERE prefix_eq(word,'读写')",
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
			expect:  "select a.*, b.pos as doc_len from (WITH kw0 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE prefix_eq(word,'ma')), kw1 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'trix'), kw2 AS (SELECT doc_id, pos FROM `__mo_index_secondary_` WHERE word = 'origin') SELECT kw0.doc_id, CAST(0 as int) FROM kw0, kw1, kw2 WHERE kw0.doc_id = kw1.doc_id AND kw1.pos - kw0.pos = 3 AND kw0.doc_id = kw2.doc_id AND kw2.pos - kw0.pos = 8) a left join `__mo_index_secondary_` b on a.doc_id = b.doc_id and b.word = '__DocLen'",
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
