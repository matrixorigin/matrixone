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

package frontend

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
)

// querySchedulingIntentForStatement applies the statement-scoped scheduling
// SET_VAR overrides without mutating session state. The first override for a
// variable wins, matching optimizer-hint duplicate semantics.
func querySchedulingIntentForStatement(ses FeSession, sql string) schedule.SchedulingIntent {
	if !hasSchedulingOptimizerHint(sql) {
		return querySchedulingIntent(ses)
	}
	return querySchedulingIntentForStatementWithSQLMode(ses, sql, sessionSQLModeForScheduling(ses))
}

func querySchedulingIntentForStatementWithSQLMode(
	ses FeSession,
	sql string,
	sqlMode string,
) schedule.SchedulingIntent {
	intent := querySchedulingIntent(ses)
	if !hasSchedulingOptimizerHint(sql) {
		return intent
	}
	flags := mysql.ParseSQLModeFlags(sqlMode)
	noBackslashEscapes := flags.Has(mysql.SQLModeNoBackslashEscapes)
	seen := make(map[string]struct{}, 2)
	invalid := false
	for _, hint := range optimizerHintComments(sql, noBackslashEscapes) {
		for _, assignment := range schedulingSetVarAssignments(hint, noBackslashEscapes) {
			name := strings.ToLower(strings.TrimSpace(assignment.name))
			if name != queryMaxWorkers && name != queryPoolStrict {
				continue
			}
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			intent.Explicit = true
			if !assignment.valid || !applySchedulingSetVar(&intent, name, assignment.value) {
				invalid = true
			}
		}
	}
	if invalid {
		// Preserve fail-closed behavior for an explicit scheduling request.
		// The placement boundary will report invalid-scheduling-intent.
		intent.PoolFallback = schedule.PoolFallbackPolicy(255)
	}
	return intent
}

func hasSchedulingOptimizerHint(sql string) bool {
	return strings.Contains(sql, "/*+") || strings.Contains(sql, "/*!+")
}

func sessionSQLModeForScheduling(ses FeSession) string {
	if ses == nil {
		return ""
	}
	return sessionSQLModeForParser(ses)
}

func applySchedulingSetVar(intent *schedule.SchedulingIntent, name, raw string) bool {
	raw = unquoteSchedulingHintScalar(strings.TrimSpace(raw))
	switch name {
	case queryMaxWorkers:
		value, err := gSysVarsDefs[queryMaxWorkers].Type.Convert(raw)
		if err != nil {
			return false
		}
		maxWorkers, ok := value.(int64)
		if !ok {
			return false
		}
		intent.WorkerSet = schedule.WorkerSetPolicy{Mode: schedule.WorkerSetAll}
		if maxWorkers > 0 {
			intent.WorkerSet.Mode = schedule.WorkerSetMax
			intent.WorkerSet.MaxWorkers = int(maxWorkers)
		}
	case queryPoolStrict:
		value, err := gSysVarsDefs[queryPoolStrict].Type.Convert(raw)
		if err != nil {
			return false
		}
		boolType, ok := gSysVarsDefs[queryPoolStrict].Type.(SystemVariableBoolType)
		if !ok {
			return false
		}
		intent.PoolFallback = schedule.PoolFallbackLegacyCompatible
		intent.EmptyWorkerPolicy = schedule.EmptyWorkerLocalFallback
		if boolType.IsTrue(value) {
			intent.PoolFallback = schedule.PoolFallbackStrict
			intent.EmptyWorkerPolicy = schedule.EmptyWorkerFail
		}
	default:
		return false
	}
	return true
}

type schedulingSetVarAssignment struct {
	name  string
	value string
	valid bool
}

// optimizerHintComments returns only /*+ ... */ and /*!+ ... */ comments that
// occur in SQL lexical space. Hint-looking text inside strings, identifiers,
// ordinary comments, and line comments is ignored.
func optimizerHintComments(sql string, noBackslashEscapes bool) []string {
	var hints []string
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'', '"', '`':
			i = skipSQLQuoted(sql, i, sql[i], noBackslashEscapes)
		case '#':
			i = skipSchedulingSQLLineComment(sql, i+1)
		case '-':
			if i+2 < len(sql) && sql[i+1] == '-' && isSQLSpace(sql[i+2]) {
				i = skipSchedulingSQLLineComment(sql, i+2)
			} else {
				i++
			}
		case '/':
			if i+1 >= len(sql) || sql[i+1] != '*' {
				i++
				continue
			}
			contentStart := i + 2
			isHint := contentStart < len(sql) && sql[contentStart] == '+'
			if contentStart+1 < len(sql) && sql[contentStart] == '!' && sql[contentStart+1] == '+' {
				contentStart++
				isHint = true
			}
			end := strings.Index(sql[contentStart:], "*/")
			if end < 0 {
				return hints
			}
			if isHint {
				hints = append(hints, sql[contentStart+1:contentStart+end])
			}
			i = contentStart + end + 2
		default:
			i++
		}
	}
	return hints
}

func schedulingSetVarAssignments(hint string, noBackslashEscapes bool) []schedulingSetVarAssignment {
	var assignments []schedulingSetVarAssignment
	depth := 0
	for pos := 0; pos < len(hint); {
		switch hint[pos] {
		case '\'', '"', '`':
			pos = skipSQLQuoted(hint, pos, hint[pos], noBackslashEscapes)
			continue
		case '(':
			depth++
			pos++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			pos++
			continue
		}
		if depth != 0 || pos+len("set_var") > len(hint) ||
			!strings.EqualFold(hint[pos:pos+len("set_var")], "set_var") {
			pos++
			continue
		}
		start := pos
		pos += len("set_var")
		if (start > 0 && isSQLIdentifierByte(hint[start-1])) ||
			(pos < len(hint) && isSQLIdentifierByte(hint[pos])) {
			continue
		}
		for pos < len(hint) && isSQLSpace(hint[pos]) {
			pos++
		}
		if pos >= len(hint) || hint[pos] != '(' {
			continue
		}
		end, ok := matchingSchedulingHintParen(hint, pos, noBackslashEscapes)
		if !ok {
			inner := hint[pos+1:]
			assignments = append(assignments, parseSchedulingSetVarAssignment(
				inner, false, noBackslashEscapes))
			break
		}
		assignments = append(assignments, parseSchedulingSetVarAssignment(
			hint[pos+1:end], true, noBackslashEscapes))
		pos = end + 1
	}
	return assignments
}

func parseSchedulingSetVarAssignment(
	inner string,
	closed bool,
	noBackslashEscapes bool,
) schedulingSetVarAssignment {
	equal := schedulingHintEqual(inner, noBackslashEscapes)
	if equal < 0 {
		return schedulingSetVarAssignment{name: strings.TrimSpace(inner), valid: false}
	}
	name := strings.TrimSpace(inner[:equal])
	value := strings.TrimSpace(inner[equal+1:])
	return schedulingSetVarAssignment{name: name, value: value, valid: closed && name != "" && value != ""}
}

func schedulingHintEqual(value string, noBackslashEscapes bool) int {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'', '"':
			i = skipSQLQuoted(value, i, value[i], noBackslashEscapes) - 1
		case '=':
			return i
		}
	}
	return -1
}

func matchingSchedulingHintParen(value string, open int, noBackslashEscapes bool) (int, bool) {
	depth := 1
	for i := open + 1; i < len(value); i++ {
		switch value[i] {
		case '\'', '"':
			i = skipSQLQuoted(value, i, value[i], noBackslashEscapes) - 1
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return len(value), false
}

func skipSQLQuoted(value string, start int, quote byte, noBackslashEscapes bool) int {
	for i := start + 1; i < len(value); i++ {
		if value[i] == '\\' && !noBackslashEscapes {
			i++
			continue
		}
		if value[i] != quote {
			continue
		}
		if i+1 < len(value) && value[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return len(value)
}

func skipSchedulingSQLLineComment(value string, start int) int {
	if end := strings.IndexByte(value[start:], '\n'); end >= 0 {
		return start + end + 1
	}
	return len(value)
}

func unquoteSchedulingHintScalar(value string) string {
	if len(value) < 2 || (value[0] != '\'' && value[0] != '"') || value[len(value)-1] != value[0] {
		return value
	}
	quote := string(value[0])
	return strings.ReplaceAll(value[1:len(value)-1], quote+quote, quote)
}

func isSQLSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', '\f':
		return true
	default:
		return false
	}
}

func isSQLIdentifierByte(value byte) bool {
	return value == '_' || value == '$' || value >= '0' && value <= '9' ||
		value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}
