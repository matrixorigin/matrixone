// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// containsJSONMergeCall is deliberately a small lexical check rather than a
// substring search. It is used before the normal plan-cache lookup, so quoted
// text and comments must not disable caching for an unrelated statement. A
// statement containing the deprecated function is not cacheable because its
// warning is a bind-time diagnostic and must be recreated for every COM_QUERY.
func containsJSONMergeCall(sql string) bool {
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'', '"', '`':
			i = skipJSONMergeQuoted(sql, i, sql[i])
			continue
		case '#':
			i = skipJSONMergeLineComment(sql, i+1)
			continue
		case '-':
			if i+2 < len(sql) && sql[i+1] == '-' && isJSONMergeSpace(sql[i+2]) {
				i = skipJSONMergeLineComment(sql, i+2)
				continue
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipJSONMergeBlockComment(sql, i+2)
				continue
			}
		}

		if isJSONMergeIdentifierStart(sql[i]) {
			start := i
			i++
			for i < len(sql) && isJSONMergeIdentifierPart(sql[i]) {
				i++
			}
			if strings.EqualFold(sql[start:i], "json_merge") {
				j := skipJSONMergeCallGap(sql, i)
				if j < len(sql) && sql[j] == '(' {
					return true
				}
			}
			continue
		}
		i++
	}
	return false
}

func skipJSONMergeCallGap(sql string, start int) int {
	i := start
	for i < len(sql) {
		for i < len(sql) && isJSONMergeSpace(sql[i]) {
			i++
		}
		if i >= len(sql) {
			return i
		}
		switch sql[i] {
		case '#':
			i = skipJSONMergeLineComment(sql, i+1)
			continue
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipJSONMergeBlockComment(sql, i+2)
				continue
			}
		case '-':
			if i+2 < len(sql) && sql[i+1] == '-' && isJSONMergeSpace(sql[i+2]) {
				i = skipJSONMergeLineComment(sql, i+2)
				continue
			}
		}
		return i
	}
	return i
}

func isJSONMergeIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isJSONMergeIdentifierPart(ch byte) bool {
	return isJSONMergeIdentifierStart(ch) || ch >= '0' && ch <= '9' || ch == '$'
}

func isJSONMergeSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func skipJSONMergeQuoted(sql string, start int, quote byte) int {
	for i := start + 1; i < len(sql); i++ {
		if sql[i] == '\\' && quote != '`' {
			i++
			continue
		}
		if sql[i] != quote {
			continue
		}
		if i+1 < len(sql) && sql[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return len(sql)
}

func skipJSONMergeLineComment(sql string, start int) int {
	if end := strings.IndexByte(sql[start:], '\n'); end >= 0 {
		return start + end + 1
	}
	return len(sql)
}

func skipJSONMergeBlockComment(sql string, start int) int {
	if end := strings.Index(sql[start:], "*/"); end >= 0 {
		return start + end + 2
	}
	return len(sql)
}

func beginJSONMergeWarningStatement(
	ses *Session,
	execCtx *ExecCtx,
	input *UserInput,
	stmt tree.Statement,
) {
	if ses == nil || execCtx == nil {
		return
	}
	// EXECUTE is intentionally excluded: a prepared statement already warned
	// when it was first bound, and any plan rebuild underneath EXECUTE is marked
	// internal by rebuildPreparePlan.
	if _, ok := stmt.(*tree.Execute); ok {
		return
	}
	isPrepare := IsPrepareStatement(stmt)
	if !isPrepare && (input == nil || !containsJSONMergeCall(input.getSql())) {
		return
	}
	ctx := execCtx.reqCtx
	if ctx == nil {
		ctx = context.Background()
	}
	execCtx.reqCtx = plan2.WithJSONMergeWarningContext(
		ctx, ses, plan2.JSONMergeWarningUser)
	if compilerCtx := ses.GetTxnCompileCtx(); compilerCtx != nil {
		compilerCtx.SetContext(execCtx.reqCtx)
	}
}
