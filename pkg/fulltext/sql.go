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

type SqlNode struct {
	Index    int32
	Label    string
	Sql      string
	IsJoin   bool
	Children []*SqlNode
}

// PLUS node as JOIN.  Index is from TEXT/STAR node
func GenJoinPlusSql(p *Pattern, mode int64, idxtbl string) ([]*SqlNode, error) {

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
			sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE word = '%s')", alias, idxtbl, kw)
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: tp.Index, Label: alias, IsJoin: true, Sql: sql})

		} else {
			// STAR
			if kw[len(kw)-1] != '*' {
				return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
			}
			prefix := kw[0 : len(kw)-1]
			sql = fmt.Sprintf("%s AS (SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s'))", alias, tp.Index, idxtbl, prefix)
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: tp.Index, Label: alias, IsJoin: true, Sql: sql})
		}

		sqlnode.Sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s", alias, sqlnode.Index, alias)
		sqlns = append(sqlns, sqlnode)
	}

	return sqlns, nil
}

// JOIN node.  Index is from JOIN node.  Index of TEXT/STAR is invalid
func GenJoinSql(p *Pattern, mode int64, idxtbl string) ([]*SqlNode, error) {

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
			sql = fmt.Sprintf("%s AS (SELECT doc_id FROM %s WHERE word = '%s')", alias, idxtbl, kw)
			sqlnode.Children = append(sqlnode.Children, &SqlNode{Index: idx, Label: alias, IsJoin: true, Sql: sql})
			subidx++
		} else {
			if kw[len(kw)-1] != '*' {
				return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
			}
			prefix := kw[0 : len(kw)-1]
			sql = fmt.Sprintf("%s AS (SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s'))", alias, idx, idxtbl, prefix)
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

func GenSql(p *Pattern, mode int64, idxtbl string, joinsql []*SqlNode, isJoin bool) ([]*SqlNode, error) {

	var sqls []*SqlNode
	var textps []*Pattern

	if isJoin {
		if p.Operator == JOIN {
			return GenJoinSql(p, mode, idxtbl)
		} else {
			return GenJoinPlusSql(p, mode, idxtbl)
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
			if tp.Operator == TEXT {
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE word = '%s'", idx, idxtbl, kw)

			} else {
				if kw[len(kw)-1] != '*' {
					return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
				}
				prefix := kw[0 : len(kw)-1]
				sql = fmt.Sprintf("SELECT doc_id, CAST(%d as int) FROM %s WHERE prefix_eq(word,'%s')", idx, idxtbl, prefix)

			}
			sqls = append(sqls, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: sql})
		}

	} else {

		for _, jn := range joinsql {
			for _, tp := range textps {
				var sql string
				idx := tp.Index
				kw := tp.Text
				alias := fmt.Sprintf("t%d", idx)
				if tp.Operator == TEXT {
					sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s as %s, %s WHERE %s.doc_id = %s.doc_id AND %s.word = '%s'",
						jn.Label, idx, idxtbl, alias, jn.Label, jn.Label, alias, alias, kw)

				} else {
					if kw[len(kw)-1] != '*' {
						return nil, moerr.NewInternalErrorNoCtx("wildcard search without character *")
					}
					prefix := kw[0 : len(kw)-1]
					sql = fmt.Sprintf("SELECT %s.doc_id, CAST(%d as int) FROM %s as %s, %s WHERE %s.doc_id = %s.doc_id AND prefix_eq(%s.word, '%s')",
						jn.Label, idx, idxtbl, alias, jn.Label, jn.Label, alias, alias, prefix)

				}
				sqls = append(sqls, &SqlNode{Index: idx, Label: alias, IsJoin: isJoin, Sql: sql})
			}

		}
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
			// Plus with Group also make as JOIN
			join, err = GenSql(ps[0], mode, idxtbl, nil, true)
			if err != nil {
				return "", err
			}
			sqls = append(sqls, join...)
			startidx++
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

	subsql := make([]string, 0)
	union := make([]string, 0)

	for _, s := range sqls {
		union = append(union, s.Sql)
		for _, c := range s.Children {
			subsql = append(subsql, c.Sql)
		}
	}

	ret := "WITH "
	ret += strings.Join(subsql, ", ")
	ret += " "
	ret += strings.Join(union, " UNION ALL ")

	return ret, nil
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

	//logutil.Infof("SQL is %s", sql)

	return sql, nil
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

	//logutil.Infof("SQL is %s", sql)

	return sql, nil
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
