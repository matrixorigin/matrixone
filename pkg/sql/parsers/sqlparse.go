// Copyright 2021 Matrix Origin
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

package parsers

import (
	"context"
	"encoding/json"
	gotrace "runtime/trace"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/postgresql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Parse(ctx context.Context, dialectType dialect.DialectType, sql string, lower int64) ([]tree.Statement, error) {
	_, task := gotrace.NewTask(context.TODO(), "parser.Parse")
	defer task.End()
	switch dialectType {
	case dialect.MYSQL:
		return mysql.Parse(ctx, sql, lower)
	case dialect.POSTGRESQL:
		return postgresql.Parse(ctx, sql)
	default:
		return nil, moerr.NewInternalError(ctx, "type of dialect error")
	}
}

func ParseOne(ctx context.Context, dialectType dialect.DialectType, sql string, lower int64) (tree.Statement, error) {
	switch dialectType {
	case dialect.MYSQL:
		return mysql.ParseOne(ctx, sql, lower)
	case dialect.POSTGRESQL:
		return postgresql.ParseOne(ctx, sql)
	default:
		return nil, moerr.NewInternalError(ctx, "type of dialect error")
	}
}

const (
	stripCloudUser           = "/* cloud_user */"
	stripCloudUserContent    = "cloud_user"
	stripCloudNonUser        = "/* cloud_nonuser */"
	stripCloudNonUserContent = "cloud_nonuser"
	stripSaveQuery           = "/* save_result */"
	stripSaveQueryContent    = "save_result"
)

var stripContents = map[string]int8{
	stripCloudUserContent:    0,
	stripCloudNonUserContent: 0,
	stripSaveQueryContent:    0,
}

var HandleSqlForRecord = func(sql string) []string {
	split := SplitSqlBySemicolon(sql)
	for i := range split {
		stripScanner := mysql.NewScanner(dialect.MYSQL, split[i])
		//strip needed comment "/*XXX*/"
		var commentIdx [][]int
		for stripScanner.Pos < len(split[i]) {
			typ, comment := stripScanner.ScanComment()
			if typ == mysql.COMMENT {
				//only strip needed comment "/*XXX*/"
				if strings.HasPrefix(comment, "/*") && strings.HasSuffix(comment, "*/") {
					commentContent := strings.ToLower(strings.TrimSpace(comment[2 : len(comment)-2]))
					if _, ok := stripContents[commentContent]; ok {
						commentIdx = append(commentIdx, []int{stripScanner.Pos - len(comment), stripScanner.Pos})
					}
				}
			} else if typ == mysql.EofChar() || typ == mysql.LEX_ERROR {
				break
			}
		}

		if len(commentIdx) > 0 {
			var builder strings.Builder
			for j := 0; j < len(commentIdx); j++ {
				if j == 0 {
					builder.WriteString(split[i][0:commentIdx[j][0]])
				} else {
					builder.WriteString(split[i][commentIdx[j-1][1]:commentIdx[j][0]])
				}
			}

			builder.WriteString(split[i][commentIdx[len(commentIdx)-1][1]:len(split[i])])
			split[i] = strings.TrimSpace(builder.String())
		}

		// Hide secret key for split[i],
		// for example:
		// before: create account nihao admin_name 'admin' identified with '123'
		// after: create account nihao admin_name 'admin' identified with '******'

		// Slice indexes helps to get the final ranges from split[i],
		// for example:
		// Secret keys' indexes ranges in split[i] are:
		// 1, 2, 3, 3
		// These mean [1, 2] and [3, 3] in split[i] are secret keys
		// And if len(split[i]) is 10, then we get slice indexes:
		// -1, 1, 2, 3, 3, 10
		// These mean we need to get (-1, 1), (2, 3), (3, 10) from split[i]
		scanner := mysql.NewScanner(dialect.MYSQL, split[i])
		indexes := []int{-1}
		eq := int('=')
		for scanner.Pos < len(split[i]) {
			typ, s := scanner.Scan()
			if typ == mysql.IDENTIFIED {
				typ, _ = scanner.Scan()
				if typ == mysql.BY || typ == mysql.WITH {
					typ, s = scanner.Scan()
					if typ != mysql.RANDOM {
						indexes = append(indexes, scanner.Pos-len(s)-1, scanner.Pos-2)
					}
				}
			} else if strings.ToLower(s) == "access_key_id" || strings.ToLower(s) == "secret_access_key" {
				typ, _ = scanner.Scan()
				if typ == eq {
					_, s = scanner.Scan()
					indexes = append(indexes, scanner.Pos-len(s)-1, scanner.Pos-2)
				}
			}
		}
		indexes = append(indexes, len(split[i]))

		if len(indexes) > 2 {
			var builder strings.Builder
			for j := 0; j < len(indexes); j += 2 {
				builder.WriteString(split[i][indexes[j]+1 : indexes[j+1]])
				if j < len(indexes)-2 {
					builder.WriteString("******")
				}
			}
			split[i] = builder.String()
		}
		split[i] = strings.TrimSpace(split[i])
	}
	return split
}

func SplitSqlBySemicolon(sql string) []string {
	var ret []string
	if len(sql) == 0 {
		// case 1 : "" => [""]
		return []string{sql}
	}
	scanner := mysql.NewScanner(dialect.MYSQL, sql)
	lastEnd := 0
	endWithSemicolon := false
	for scanner.Pos < len(sql) {
		typ, _ := scanner.Scan()
		for scanner.Pos < len(sql) && typ != ';' {
			typ, _ = scanner.Scan()
		}
		if typ == ';' {
			ret = append(ret, sql[lastEnd:scanner.Pos-1])
			lastEnd = scanner.Pos
			endWithSemicolon = true
		} else {
			ret = append(ret, sql[lastEnd:scanner.Pos])
			endWithSemicolon = false
		}
	}

	if len(ret) == 0 {
		//!!!NOTE there is at least one element in ret slice
		panic("there is at least one element")
	}
	//handle whitespace characters in the front and end of the sql
	for i := range ret {
		ret[i] = strings.TrimSpace(ret[i])
	}
	// do nothing
	//if len(ret) == 1 {
	//	//case 1 : "   " => [""]
	//	//case 2 : " abc " = > ["abc"]
	//	//case 3 : " /* abc */  " = > ["/* abc */"]
	//}
	if len(ret) > 1 {
		last := len(ret) - 1
		if !endWithSemicolon && len(ret[last]) == 0 {
			//case 3 : "abc;   " => ["abc"]
			//if the last one is end empty, remove it
			ret = ret[:last]
		}
		//case 4 : "abc; def; /* abc */  " => ["abc", "def", "/* abc */"]
	}

	return ret
}

func extractLeadingHints(sql string) []string {
	if len(sql) == 0 {
		return []string{""}
	}

	stmts := SplitSqlBySemicolon(sql)
	results := make([]string, len(stmts))

	isSpace := func(b byte) bool {
		switch b {
		case ' ', '\t', '\n', '\r', '\f':
			return true
		default:
			return false
		}
	}

	for idx, stmt := range stmts {
		if len(stmt) == 0 {
			results[idx] = ""
			continue
		}
		i := 0
		for i < len(stmt) && isSpace(stmt[i]) {
			i++
		}

		var builder strings.Builder
		for i < len(stmt) {
			if i+2 < len(stmt) && stmt[i] == '/' && stmt[i+1] == '*' && (stmt[i+2] == '!' || stmt[i+2] == '+') {
				end := strings.Index(stmt[i+3:], "*/")
				// Determine content start: strip opening marker
				contentStart := i + 3
				if stmt[i+2] == '!' && i+3 < len(stmt) && stmt[i+3] == '+' {
					contentStart = i + 4 // for '/*!+' skip both
				}
				if stmt[i+2] == '+' {
					contentStart = i + 3 // for '/*+'
				}
				if end < 0 {
					// unterminated: append inner content
					if contentStart < len(stmt) {
						builder.WriteString(stmt[contentStart:])
					}
					break
				}
				end += i + 3
				contentEnd := end // exclude closing '*/' by stopping at its start
				if contentStart <= contentEnd {
					builder.WriteString(stmt[contentStart:contentEnd])
				}
				i = end
				for i < len(stmt) && isSpace(stmt[i]) {
					i++
				}
				continue
			}

			break
		}

		if builder.Len() > 0 {
			results[idx] = builder.String()
		} else {
			results[idx] = ""
		}
	}

	return results
}

type RewriteMap struct {
	// RawRewrites carries the raw SQL strings from JSON input.
	RawRewrites map[string]string `json:"rewrites"`
}

// AddRewriteHints The position of the hint at the beginning is different from that in MySQL.
// Placing it in the parser for parsing will lead to syntax conflicts,
// so it can only be parsed separately
func AddRewriteHints(ctx context.Context, stmts []tree.Statement, sql string) error {
	hints := extractLeadingHints(sql)
	if len(hints) != len(stmts) {
		return moerr.NewParseError(ctx, "parse hints bug")
	}
	for i, stmt := range stmts {
		switch stmt.(type) {
		case *tree.Select, *tree.ParenSelect:
			// ok
		default:
			continue
		}
		hint := strings.TrimSpace(hints[i])
		if hint == "" || (len(hint) > 0 && hint[0] != '{') {
			continue
		}

		var rewriteMap RewriteMap
		if err := json.Unmarshal([]byte(hint), &rewriteMap); err != nil {
			return moerr.NewParseError(ctx, err.Error())
		}
		if len(rewriteMap.RawRewrites) == 0 {
			continue
		}
		rewriteOption := &tree.RewriteOption{Rewrites: make(map[string]*tree.Rewrite)}
		for k, v := range rewriteMap.RawRewrites {
			key := strings.TrimSpace(k)
			if key == "" {
				return moerr.NewParseError(ctx, "empty table and database")
			}
			// key can be "table" or "db.table"
			parts := strings.Split(key, ".")
			db, table := "", ""
			if len(parts) == 2 {
				db = strings.TrimSpace(parts[0])
				table = strings.TrimSpace(parts[1])
			} else {
				return moerr.NewParseError(ctx, "the mapping name needs to include database name")
			}
			if table == "" || db == "" {
				return moerr.NewParseError(ctx, "empty table or database")
			}
			if v == "" {
				return moerr.NewParseError(ctx, "statement")
			}
			st, err := ParseOne(ctx, dialect.MYSQL, v, 1)
			if err != nil {
				return moerr.NewParseError(ctx, err.Error())
			}

			switch st.(type) {
			case *tree.Select, *tree.ParenSelect:
				// ok
			default:
				return moerr.NewParseError(ctx, "only accept SELECT-like statements as rewrites")
			}
			if _, ok := rewriteOption.Rewrites[key]; ok {
				return moerr.NewParseError(ctx, "duplicate mapping names")
			}
			rewriteOption.Rewrites[key] = &tree.Rewrite{TableName: table, DbName: db, Stmt: st}
		}
		if len(rewriteOption.Rewrites) > 0 {
			switch s := stmt.(type) {
			case *tree.Select:
				s.RewriteOption = rewriteOption
			case *tree.ParenSelect:
				if s.Select != nil {
					s.Select.RewriteOption = rewriteOption
				}
			}
		}
	}
	return nil
}
