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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

/*
fulltext SQL generation

For natural language mode, it is a phrase search.  The search will be AND operation and with position as filter.

For boolean mode, the rule are the followings:
  - operation JOIN contains a list of + operators which is single TEXT or STAR values
  - operator + is considered as AND Operation.  + Operators can be a single TEXT/STAR or Group.  Operator + with children Group cannot be optimized to under JOIN.
  - operator - is not considered as AND operation because NOT filter is slow in SQL.
    Change the formula from A INTERSECT (NOT B) into A UNION (A INTERSECT B) and process the negative filter afterwards.
  - other operators are OR operation.

SQL generation from boolean search string is a Set theory.

e.g. search string +hello -world

the search string will convert into Pattern/Plan

((+ (TEXT hello)) (- (TEXT world)))

with set theory, we can formulae the above plans into

A\B = A - (A INTERSECT B)

Since NOT filter is slow in SQL, we change the plan into A UNION (A INTERSECT B) and process the negative later.

# The result SQL will be

A UNION ALL (A JOIN B)

In case there are multiple + operators like the search string "+A +B -(<C >D)"
JOIN will be used to optimize the SQL with Pattern/Plan

(((JOIN (+ (TEXT A)) (+ (TEXT B))) (- (GROUP (< (TEXT C)) (> (TEXT D)))))

# With plan above, we can formula the SQL like belows

(A INTERSECT B) UNION ((A INTERSECT B) INTERSECT C) UNION ((A INTERSECT B) INTERSECT D)

WITH t0 (A JOIN B)
(t0) UNION ALL (t0 JOIN C) UNION ALL (t0 JOIN D)

(A JOIN B) is the result contain both A and B. Consider it is the appearance of A and B (denoted as index 0 in SQL)).
(t0 JOIN C) is the result contain both A, B and C. Consider it is the appearance of A, B and C (denoted as index 1 in SQL).
(t0 JOIN D) is the result contain both A, B and D. Consider it is the appearance of A, B and D (denoted as index 2 in SQL).

For TD-IDF calculation,

Instead of caculating the TD-IDF from the words A, B, C and D,
we will calculate the score from word groups (A & B) -> "0" , (A & B & C) -> "1", (A & B & D) -> "2".
The ordering of the result remain the same and of course the score is different.

In case + operator with GROUP such as "+(A B) ~C -D",

Although operator + with GROUP cannot be optimized with JOIN Pattern node,  we can still optimize the SQL with JOIN like below

(+ (GROUP (TEXT A) (TEXT B))) (~ (TEXT C)) (- (TEXT D)))

# The generated SQL is like

A UNION B UNION (A INTERSECT C) UNION (A INTERSECT D) UNION (B INTERSECT C) UNION (B INTERSECT D)

(A) UNION ALL (B) UNION ALL (A JOIN C) UNION ALL (A JOIN D) UNION ALL (B JOIN C) UNION ALL (B JOIN D)

In case multiple + operator with single values and + operator with GROUP like "+A +B +(C D) E"

The plan like the followings:
((JOIN (+ (TEXT A)) (+ (TEXT B))) (+ (GROUP (TEXT C) (TEXT D))) (TEXT E))

To simplify the SQL, (+ (GROUP (TEXT C) (TEXT D))) will not considered as JOIN.

(A INTERSECT B) UNION (A INTERSECT B  INTERSECT C) UNION (A INTEREST B INTERSECT D) UNION (A INTERSECT B INTERSECT E)

WITH t0 (A JOIN B)
(t0) UNION ALL (t0 JOIN C) UNION ALL (t0 JOIN D) UNION ALL (t0 JOIN E)
*/
type SqlNode struct {
	Index    int32
	Label    string
	Sql      string
	IsJoin   bool
	Children []*SqlNode
}

func escape(src string) string {
	return strings.ReplaceAll(src, "'", `\'`)
}

// GenTextSql that support ngram in boolean mode
// TEXT is a word. For english, it is fine to have a simple filter word = 'word".
// For Chinese, word should be tokenize into ngram
func GenTextSql(p *Pattern, mode int64, idxtbl string, parser string) (string, error) {

	if parser == "json_value" {
		sql := fmt.Sprintf("SELECT doc_id FROM %s WHERE word = '%s'", idxtbl, escape(p.Text))
		return sql, nil
	}

	ps, err := ParsePatternInNLMode(p.Text)
	if err != nil {
		return "", err
	}

	sql, err := SqlPhrase(ps, mode, idxtbl, false)
	if err != nil {
		return "", err
	}

	return sql, nil
}

// PLUS node as JOIN.  Index is from TEXT/STAR node
// children of PLUS node can be a single TEXT/STAR or GROUP.
// In case of GROUP, mutiple SqlNodes will be generated.
func GenJoinPlusSql(p *Pattern, mode int64, idxtbl string, parser string) ([]*SqlNode, error) {

	var sql string
	var textps []*Pattern

	textps = findTextOrStarFromPattern(p, textps)
	sqlns := make([]*SqlNode, 0, len(textps))
	for _, tp := range textps {
		kw := tp.Text
		alias := fmt.Sprintf("t%d", tp.Index)
		sqlnode := &SqlNode{IsJoin: true, Index: tp.Index, Label: alias}
		if tp.Operator == TEXT {
			// TEXT
			textsql, err := GenTextSql(tp, mode, idxtbl, parser)
			if err != nil {
				return nil, err
			}
			sql = fmt.Sprintf("%s AS (%s)", alias, textsql)

			//sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE word = '%s')", alias, idxtbl, kw)
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: tp.Index, Label: alias, IsJoin: true, Sql: sql})

		} else {
			// STAR
			if kw[len(kw)-1] != '*' {
				return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
			}
			prefix := kw[0 : len(kw)-1]
			sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE prefix_eq(word,'%s'))", alias, idxtbl, escape(prefix))
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: tp.Index, Label: alias, IsJoin: true, Sql: sql})
		}

		sqlnode.Sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s", alias, sqlnode.Index, alias)
		sqlns = append(sqlns, sqlnode)
	}

	return sqlns, nil
}

// JOIN node.  Index is from JOIN node.  Index of TEXT/STAR is invalid
// Generate a JOIN subsql in children and union sql in Sql.
// subsql t0 is the table for all JOIN
func GenJoinSql(p *Pattern, mode int64, idxtbl string, parser string) ([]*SqlNode, error) {

	var sql string
	var textps []*Pattern
	tables := make([]string, 0)

	sqlnode := &SqlNode{IsJoin: true, Index: p.Index}
	idx := p.Index
	subidx := 0
	textps = findTextOrStarFromPattern(p, textps)

	for _, tp := range textps {
		kw := tp.Text
		alias := fmt.Sprintf("t%d%d", idx, subidx)
		tables = append(tables, alias)
		if tp.Operator == TEXT {
			textsql, err := GenTextSql(tp, mode, idxtbl, parser)
			if err != nil {
				return nil, err
			}
			sql = fmt.Sprintf("%s AS (%s)", alias, textsql)

			//sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE word = '%s')", alias, idxtbl, kw)
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: alias, IsJoin: true, Sql: sql})
			subidx++
		} else {
			if kw[len(kw)-1] != '*' {
				return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
			}
			prefix := kw[0 : len(kw)-1]
			sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE prefix_eq(word,'%s'))", alias, idxtbl, escape(prefix))
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: alias, IsJoin: true, Sql: sql})
			subidx++
		}
	}

	oncond := make([]string, 0, len(sqlnode.Children)-1)
	for i := range tables {
		if i > 0 {
			oncond = append(oncond, fmt.Sprintf("%s.doc_id = %s.doc_id", tables[0], tables[i]))
		}
	}

	label := fmt.Sprintf("t%d", p.Index)
	sql = fmt.Sprintf("%s AS (SELECT %s.doc_id FROM %s WHERE %s)", label, tables[0], strings.Join(tables, ", "),
		strings.Join(oncond, " AND "))
	sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: label, IsJoin: true, Sql: sql})

	sqlnode.Sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s", label, sqlnode.Index, label)
	sqlnode.Label = label

	return []*SqlNode{sqlnode}, nil
}

// generate the sql with the pattern.
// isJoin flag is true, geneate the SQL in JOIN mode (only for JOIN and PLUS node)
// if joinsql is not NILL, all the OR TEXT/STAR node will be joined with joinsql
func GenSql(p *Pattern, mode int64, idxtbl string, joinsql []*SqlNode, isJoin bool, parser string) ([]*SqlNode, error) {

	var sqls []*SqlNode
	var textps []*Pattern

	if isJoin {
		if p.Operator == JOIN {
			return GenJoinSql(p, mode, idxtbl, parser)
		} else {
			return GenJoinPlusSql(p, mode, idxtbl, parser)
		}
	}

	textps = findTextOrStarFromPattern(p, textps)

	if len(joinsql) == 0 {
		// NO JOIN

		for _, tp := range textps {
			var sql string
			idx := tp.Index
			kw := tp.Text
			alias := fmt.Sprintf("t%d", idx)
			sqlnode := &SqlNode{Index: idx, Label: alias, IsJoin: isJoin}
			if tp.Operator == TEXT {
				textsql, err := GenTextSql(tp, mode, idxtbl, parser)
				if err != nil {
					return nil, err
				}
				subsql := fmt.Sprintf("%s AS (%s)", alias, textsql)
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s", idx, alias)
				sqlnode.Sql = sql
				sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: subsql})

			} else {
				if kw[len(kw)-1] != '*' {
					return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
				}
				prefix := kw[0 : len(kw)-1]
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s')", idx, idxtbl, escape(prefix))
				sqlnode.Sql = sql

			}
			sqls = append(sqls, sqlnode)
		}

	} else {

		for _, jn := range joinsql {
			for _, tp := range textps {
				var sql string
				idx := tp.Index
				kw := tp.Text
				alias := fmt.Sprintf("t%d", idx)
				sqlnode := &SqlNode{Index: idx, Label: alias, IsJoin: isJoin}
				if tp.Operator == TEXT {
					textsql, err := GenTextSql(tp, mode, idxtbl, parser)
					if err != nil {
						return nil, err
					}
					subsql := fmt.Sprintf("%s AS (%s)", alias, textsql)
					sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s, %s WHERE %s.doc_id = %s.doc_id",
						jn.Label, idx, alias, jn.Label, jn.Label, alias)
					sqlnode.Sql = sql
					sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: subsql})

				} else {
					if kw[len(kw)-1] != '*' {
						return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
					}
					prefix := kw[0 : len(kw)-1]
					sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s as %s, %s WHERE %s.doc_id = %s.doc_id AND prefix_eq(%s.word, '%s')",
						jn.Label, idx, idxtbl, alias, jn.Label, jn.Label, alias, alias, escape(prefix))
					sqlnode.Sql = sql

				}
				sqls = append(sqls, sqlnode)
			}

		}
	}

	return sqls, nil
}

// Generate SQL in boolean mode
func SqlBoolean(ps []*Pattern, mode int64, idxtbl string, parser string) (string, error) {

	var err error
	var join []*SqlNode
	var sqls []*SqlNode
	// check JOIN

	if len(ps) == 1 {
		if ps[0].Operator == JOIN {
			join, err = GenSql(ps[0], mode, idxtbl, nil, true, parser)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
		} else {
			s, err := GenSql(ps[0], mode, idxtbl, nil, false, parser)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, s...)
		}
	} else {
		startidx := 0
		if ps[0].Operator == JOIN {
			join, err = GenSql(ps[0], mode, idxtbl, nil, true, parser)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
			startidx++
		} else if ps[0].Operator == PLUS {
			// Plus with Group also make as JOIN
			join, err = GenSql(ps[0], mode, idxtbl, nil, true, parser)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
			startidx++
		}

		for i := startidx; i < len(ps); i++ {
			p := ps[i]
			s, err := GenSql(p, mode, idxtbl, join, false, parser)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, s...)
		}
	}

	// generate final sql

	subsql := make([]string, 0)
	union := make([]string, 0)
	duplicate := make(map[string]bool)

	for _, s := range sqls {
		union = append(union, s.Sql)
		for _, c := range s.Children {
			_, ok := duplicate[c.Label]
			if !ok {
				subsql = append(subsql, c.Sql)
				duplicate[c.Label] = true
			}
		}
	}

	ret := ""
	if len(subsql) > 0 {
		ret = "WITH "
		ret += strings.Join(subsql, ", ")
		ret += " "
	}
	ret += strings.Join(union, " UNION ALL ")

	return ret, nil
}

// Generate SQL in phrase mode.  It is the same for natural language mode and phrase search in boolean mode
func SqlPhrase(ps []*Pattern, mode int64, idxtbl string, withIndex bool) (string, error) {

	var sql string
	var union []string

	// get plain text
	if len(ps) == 1 {
		tp := ps[0]
		kw := tp.Text

		if tp.Operator == TEXT {
			if withIndex {
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE word = '%s'",
					tp.Index, idxtbl, escape(kw))
			} else {
				sql = fmt.Sprintf("SELECT doc_id FROM %s WHERE word = '%s'",
					idxtbl, escape(kw))

			}
		} else {
			if kw[len(kw)-1] != '*' {
				return "", moerr.NewInternalErrorNoCtx("wildcard search without character *")
			}
			prefix := kw[0 : len(kw)-1]
			if withIndex {
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s')",
					tp.Index, idxtbl, escape(prefix))
			} else {
				sql = fmt.Sprintf("SELECT doc_id FROM %s WHERE prefix_eq(word,'%s')",
					idxtbl, escape(prefix))

			}

		}
	} else {

		oncond := make([]string, len(ps)-1)
		tables := make([]string, len(ps))
		for i, tp := range ps {
			var subsql string
			kw := tp.Text
			tblname := fmt.Sprintf("kw%d", i)
			tables[i] = tblname
			if tp.Operator == TEXT {
				subsql = fmt.Sprintf("%s AS (SELECT doc_id, pos FROM %s WHERE word = '%s')",
					tblname, idxtbl, escape(kw))
			} else {
				if kw[len(kw)-1] != '*' {
					return "", moerr.NewInternalErrorNoCtx("wildcard search without character *")
				}
				prefix := kw[0 : len(kw)-1]
				subsql = fmt.Sprintf("%s AS (SELECT doc_id, pos FROM %s WHERE prefix_eq(word,'%s'))",
					tblname, idxtbl, escape(prefix))

			}
			union = append(union, subsql)
			if i > 0 {
				oncond[i-1] = fmt.Sprintf("%s.doc_id = %s.doc_id AND %s.pos - %s.pos = %d",
					tables[0], tables[i], tables[i], tables[0], ps[i].Position-ps[0].Position)
			}
		}
		sql = "WITH "
		sql += strings.Join(union, ", ")
		if withIndex {
			sql += fmt.Sprintf(" SELECT %s.doc_id, CAST(0 as int) FROM ", tables[0])
		} else {
			sql += fmt.Sprintf(" SELECT %s.doc_id FROM ", tables[0])
		}

		sql += strings.Join(tables, ", ")
		sql += " WHERE "
		sql += strings.Join(oncond, " AND ")
	}

	//logutil.Infof("SQL is %s", sql)

	return sql, nil
}

// API for generate SQL from pattern
func PatternToSql(ps []*Pattern, mode int64, idxTable string, parser string, algo FullTextScoreAlgo) (string, error) {
	sql, err := patternToSql(ps, mode, idxTable, parser)
	if err != nil {
		return "", err
	}
	switch algo {
	case ALGO_BM25:
		newSql := genBM25SQL(sql, idxTable)
		return newSql, nil
	case ALGO_TFIDF:
	default:
		return "", moerr.NewInternalErrorNoCtx("invalid fulltext search mode")
	}
	return sql, nil
}

func genBM25SQL(sql string, idxTable string) string {
	return fmt.Sprintf("select a.*, b.pos as doc_len from (%s) a left join %s b on a.doc_id = b.doc_id and b.word = '%s'", sql, idxTable, DOC_LEN_WORD)
}

func patternToSql(ps []*Pattern, mode int64, idxtbl string, parser string) (string, error) {
	switch mode {
	case int64(tree.FULLTEXT_NL), int64(tree.FULLTEXT_DEFAULT):
		return SqlPhrase(ps, mode, idxtbl, true)
	case int64(tree.FULLTEXT_BOOLEAN):
		if ps[0].Operator == PHRASE {
			return SqlPhrase(ps[0].Children, mode, idxtbl, true)
		} else {
			return SqlBoolean(ps, mode, idxtbl, parser)
		}
	case int64(tree.FULLTEXT_QUERY_EXPANSION), int64(tree.FULLTEXT_NL_QUERY_EXPANSION):
		return "", moerr.NewInternalErrorNoCtx("Query Expansion mode not supported")
	default:
		return "", moerr.NewInternalErrorNoCtx("invalid fulltext search mode")
	}
}
