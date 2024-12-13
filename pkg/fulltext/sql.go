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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type SqlNode struct {
	Index    int32
	Label    string
	Sql      string
	IsJoin   bool
	Children []*SqlNode
}

// PLUS node as JOIN
func GenJoinPlusSql(p *Pattern, mode int64, idxtbl string, joinsql []*SqlNode, isJoin bool) ([]*SqlNode, error) {

	return nil, nil
}

// JOIN node
func GenJoinSql(p *Pattern, mode int64, idxtbl string, joinsql []*SqlNode, isJoin bool) ([]*SqlNode, error) {

	return nil, nil
}

func GenSql(p *Pattern, mode int64, idxtbl string, joinsql []*SqlNode, isJoin bool) ([]*SqlNode, error) {

	var sqls []*SqlNode
	var keywords []string
	var indexes []int32
	keywords, indexes = GetTextFromPattern(p, keywords, indexes)

	if isJoin {
		if p.Operator == JOIN {
			return GenJoinSql(p, mode, idxtbl, joinsql, isJoin)
		} else {
			return GenJoinPlusSql(p, mode, idxtbl, joinsql, isJoin)
		}
	}

	for i, kw := range keywords {
		var sql string
		idx := indexes[i]
		alias := fmt.Sprintf("t%d", idx)
		if len(joinsql) == 0 {
			sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE word = '%s'", idx, idxtbl, kw)
		} else {
			sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s as %s, %s WHERE %s.doc_id = %s.doc AND word = '%s'",
				idx, idxtbl, alias, joinsql[0].Label, joinsql[0].Label, alias, kw)
		}
		sqls = append(sqls, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: sql})
	}

	keywords = keywords[:0]
	indexes = indexes[:0]
	keywords, indexes = GetStarFromPattern(p, keywords, indexes)

	for i, kw := range keywords {
		var sql string
		idx := indexes[i]
		alias := fmt.Sprintf("t%d", idx)
		if kw[len(kw)-1] != '*' {
			return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
		}
		prefix := kw[0 : len(kw)-1]
		if len(joinsql) == 0 {
			sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s')", idx, idxtbl, prefix)
		} else {
			sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s as %s, %s WHERE %s.doc_id = %s.doc AND prefix_eq(word, '%s')",
				idx, idxtbl, alias, joinsql[0].Label, joinsql[0].Label, alias, prefix)
		}
		sqls = append(sqls, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: sql})
	}

	return sqls, nil
}

func SqlBoolean(ps []*Pattern, mode int64, idxtbl string) (string, error) {

	var err error
	var join []*SqlNode
	var sqls []*SqlNode
	// check JOIN

	if len(ps) == 1 {
		if ps[0].Operator == JOIN {
			join, err = GenSql(ps[0], mode, idxtbl, nil, true)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
		} else {
			s, err := GenSql(ps[0], mode, idxtbl, nil, false)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, s...)
		}
	} else {
		startidx := 0
		if ps[0].Operator == JOIN {
			join, err = GenSql(ps[0], mode, idxtbl, nil, true)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
			startidx++
		} else if ps[0].Operator == PLUS {
			// check if singe text.  Mark it as JOIN too
			if ps[0].Children[0].Operator == TEXT || ps[0].Children[0].Operator == STAR {
				// make as JOIN
				join, err = GenSql(ps[0], mode, idxtbl, nil, true)
				if err != nil {
					return "", err
				}
				sqls = append(sqls, join...)
				startidx++
			}
		}

		for i := startidx; i < len(ps); i++ {
			p := ps[i]
			s, err := GenSql(p, mode, idxtbl, join, false)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, s...)
		}
	}

	// generate final sql

	return "", nil
}

func SqlNL(ps []*Pattern, mode int64, idxtbl string) (string, error) {

	var sql string
	var union []string
	var keywords []string
	var indexes []int32
	var positions []int32

	// get plain text
	for _, p := range ps {
		keywords, indexes, positions = GetPhraseTextFromPattern(p, keywords, indexes, positions)
	}

	if len(keywords) == 1 {
		sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE word = '%s'",
			indexes[0], idxtbl, keywords[0])
	} else {
		oncond := make([]string, len(keywords)-1)
		tables := make([]string, len(keywords))
		for i, kw := range keywords {
			tblname := fmt.Sprintf("kw%d", i)
			tables[i] = tblname
			union = append(union, fmt.Sprintf("%s AS (SELECT doc_id, pos FROM %s WHERE word = '%s')",
				tblname, idxtbl, kw))
			if i > 0 {
				oncond[i-1] = fmt.Sprintf("%s.doc_id = %s.doc_id AND %s.pos > %s.pos",
					tables[0], tables[i], tables[i], tables[0])
			}
		}
		sql = "WITH "
		sql += strings.Join(union, ", ")
		sql += fmt.Sprintf(" SELECT %s.doc_id, CAST(0 as int) FROM ", tables[0])
		sql += strings.Join(tables, ", ")
		sql += " WHERE "
		sql += strings.Join(oncond, " AND ")
	}

	logutil.Infof("SQL is %s", sql)

	return "", nil
}

func SqlPhrase(ps []*Pattern, mode int64, idxtbl string) (string, error) {

	var sql string
	var union []string
	var keywords []string
	var indexes []int32
	var positions []int32

	// get plain text
	for _, p := range ps {
		keywords, indexes, positions = GetPhraseTextFromPattern(p, keywords, indexes, positions)
	}

	if len(keywords) == 1 {
		sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE word = '%s'",
			indexes[0], idxtbl, keywords[0])
	} else {
		oncond := make([]string, len(keywords)-1)
		tables := make([]string, len(keywords))
		for i, kw := range keywords {
			tblname := fmt.Sprintf("kw%d", i)
			tables[i] = tblname
			union = append(union, fmt.Sprintf("%s AS (SELECT doc_id, pos FROM %s WHERE word = '%s')",
				tblname, idxtbl, kw))
			if i > 0 {
				oncond[i-1] = fmt.Sprintf("%s.doc_id = %s.doc_id AND %s.pos - %s.pos = %d",
					tables[0], tables[i], tables[i], tables[0], positions[i]-positions[0])
			}
		}
		sql = "WITH "
		sql += strings.Join(union, ", ")
		sql += fmt.Sprintf(" SELECT %s.doc_id, CAST(0 as int) FROM ", tables[0])
		sql += strings.Join(tables, ", ")
		sql += " WHERE "
		sql += strings.Join(oncond, " AND ")
	}

	logutil.Infof("SQL is %s", sql)

	return "", nil
}

func PatternToSql(ps []*Pattern, mode int64, idxtbl string) (string, error) {

	switch mode {
	case int64(tree.FULLTEXT_NL), int64(tree.FULLTEXT_DEFAULT):
		return SqlNL(ps, mode, idxtbl)
	case int64(tree.FULLTEXT_BOOLEAN):
		if ps[0].Operator == PHRASE {
			return SqlPhrase(ps, mode, idxtbl)
		} else {
			return SqlBoolean(ps, mode, idxtbl)
		}
	case int64(tree.FULLTEXT_QUERY_EXPANSION), int64(tree.FULLTEXT_NL_QUERY_EXPANSION):
		return "", moerr.NewInternalErrorNoCtx("Query Expansion mode not supported")
	default:
		return "", moerr.NewInternalErrorNoCtx("invalid fulltext search mode")
	}
	return "", nil
}
