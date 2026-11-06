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

package mysql

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Literal construction uses MySQL's fuzzy-date flags, not runtime CAST
// conversion/rounding. Keep these digest-only modes out of persisted AST flags.
type digestTemporalMode struct{ noZeroDate, noZeroInDate, invalidDates bool }

func digestTemporalSQLMode(mode string) (result digestTemporalMode) {
	for mode != "" {
		part, rest, _ := strings.Cut(mode, ",")
		mode = rest
		part = strings.TrimSpace(part)
		switch {
		case strings.EqualFold(part, "NO_ZERO_DATE"):
			result.noZeroDate = true
		case strings.EqualFold(part, "NO_ZERO_IN_DATE"):
			result.noZeroInDate = true
		case strings.EqualFold(part, "ALLOW_INVALID_DATES"):
			result.invalidDates = true
		case strings.EqualFold(part, "TRADITIONAL"):
			result.noZeroDate, result.noZeroInDate = true, true
		}
	}
	return
}
func digestTimeDigit(c byte) bool { return c >= '0' && c <= '9' }
func digestTimeSpace(c byte) bool { return c == ' ' || c >= '\t' && c <= '\r' }
func digestTimePunct(c byte) bool {
	return c >= '!' && c <= '/' || c >= ':' && c <= '@' || c >= '[' && c <= '`' || c >= '{' && c <= '~'
}
func digestTimeTail(s string) bool {
	for i := range len(s) {
		if !digestTimeSpace(s[i]) {
			return false
		}
	}
	return true
}
func digestTimeZone(s string) bool {
	if len(s) < 6 || s[0] != '+' && s[0] != '-' || s[3] != ':' || !digestTimeDigit(s[1]) || !digestTimeDigit(s[2]) || !digestTimeDigit(s[4]) || !digestTimeDigit(s[5]) {
		return false
	}
	h, m := int(s[1]-'0')*10+int(s[2]-'0'), int(s[4]-'0')*10+int(s[5]-'0')
	return m < 60 && h*60+m <= 14*60 && !(s[0] == '-' && h == 0 && m == 0) && digestTimeTail(s[6:])
}

// Callers have checked 1 <= month <= 12. MySQL treats year 0 as non-leap,
// independently of fuzzy-zero-date admission.
func digestDaysInMonth(year, month int) int {
	days := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	result := days[month-1]
	if month == 2 && year != 0 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		result++
	}
	return result
}

// Kind is -1 for a recognized but invalid date, 0 for no datetime, 1 for DATE,
// and 2 for DATETIME. TIME's initial datetime probe must distinguish these.
// Field recognition follows MySQL 8.4 str_to_datetime; deprecated separators
// are accepted, while conversion warnings (truncation/range/zero) are not.
func digestDateTimeKind(s string, mode digestTemporalMode, datetimeOnly bool) int {
	p := 0
	for p < len(s) && digestTimeSpace(s[p]) {
		p++
	}
	if p == len(s) || !digestTimeDigit(s[p]) {
		return 0
	}
	endDigits := p
	for endDigits < len(s) && (digestTimeDigit(s[endDigits]) || s[endDigits] == 'T') {
		endDigits++
	}
	digits := endDigits - p
	internal := endDigits == len(s) || s[endDigits] == '.'
	yearLength, width := 0, 4
	if internal {
		yearLength = 2
		if digits == 4 || digits == 8 || digits >= 14 {
			yearLength = 4
		}
		width = yearLength
	}
	var fields, lengths [7]int
	count, last := 0, p
	delimiter, space, zone := false, false, false
	for i := 0; i < 7 && p < len(s) && digestTimeDigit(s[p]); i++ {
		start := p
		for p < len(s) && digestTimeDigit(s[p]) && ((!internal && i != 6) || p-start < width) {
			fields[i] = fields[i]*10 + int(s[p]-'0')
			p++
			if fields[i] > 999999 {
				return 0
			}
		}
		lengths[i] = p - start
		count = i + 1
		last = p
		width = 2
		if p == len(s) {
			break
		}
		if i == 2 && s[p] == 'T' {
			p++
			continue
		}
		if i == 5 {
			if s[p] == '.' {
				p++
				last = p
				width = 6
			} else if digestTimeDigit(s[p]) {
				// Undelimited digits after seconds are not a fraction.
				break
			} else if s[p] == '+' || s[p] == '-' {
				if !digestTimeZone(s[p:]) {
					return 0
				}
				zone = true
				last = len(s)
				p = len(s)
			}
			continue
		}
		if i == 6 && (s[p] == '+' || s[p] == '-') {
			if !digestTimeZone(s[p:]) {
				return 0
			}
			zone = true
			p = len(s)
		}
		for p < len(s) && (digestTimePunct(s[p]) || digestTimeSpace(s[p])) {
			if digestTimeSpace(s[p]) {
				if i != 2 && i != 6 {
					return 0
				}
				space = true
			}
			delimiter = true
			p++
		}
		last = p
	}
	if delimiter && !space && datetimeOnly {
		return 0
	}
	p = last
	if !internal {
		yearLength = lengths[0]
	}
	any := 0
	for _, v := range fields {
		any |= v
	}
	if yearLength == 2 && any != 0 {
		if fields[0] < 70 {
			fields[0] += 2000
		} else {
			fields[0] += 1900
		}
	}
	y, m, d := fields[0], fields[1], fields[2]
	if count < 3 || y > 9999 || m > 12 || d > 31 || fields[3] > 23 || fields[4] > 59 || fields[5] > 59 {
		return -1
	}
	if any != 0 {
		if mode.noZeroInDate && (m == 0 || d == 0) {
			return -1
		}
		if !mode.invalidDates && m != 0 && d > digestDaysInMonth(y, m) {
			return -1
		}
	} else if mode.noZeroDate {
		return -1
	}
	if lengths[6] == 6 {
		if p < len(s) && s[p] >= '5' && s[p] <= '9' && fields[6] == 999999 {
			// The charset-aware constructor rounds nanoseconds. Carrying a
			// second rejects fuzzy/invalid dates even with ALLOW_INVALID_DATES.
			if m == 0 || d == 0 || (y == 9999 && m == 12 && d == 31 && fields[3] == 23 && fields[4] == 59 && fields[5] == 59) {
				return -1
			}
			if d > digestDaysInMonth(y, m) {
				return -1
			}
		}
		for p < len(s) && digestTimeDigit(s[p]) {
			p++
		}
	}
	if p < len(s) && (s[p] == '+' || s[p] == '-') {
		if !digestTimeZone(s[p:]) {
			return 0
		}
		zone = true
		p = len(s)
	}
	if !digestTimeTail(s[p:]) {
		return -1
	}
	if zone && (m == 0 || d == 0) {
		return -1
	}
	if count <= 3 && !zone {
		return 1
	}
	return 2
}

func digestTimeLiteralValid(s string) bool {
	p := 0
	for p < len(s) && digestTimeSpace(s[p]) {
		p++
	}
	if p < len(s) && s[p] == '-' {
		p++
	}
	if p == len(s) {
		return false
	}
	if len(s)-p >= 12 && digestDateTimeKind(s[p:], digestTemporalMode{}, true) != 0 {
		return false
	}
	start := p
	read := func() (uint64, bool) {
		// Match the native unsigned accumulator before its UINT_MAX check;
		// arbitrary-length fields remain bounded by the SQL/work budget.
		var value uint64
		for p < len(s) && digestTimeDigit(s[p]) {
			value = value*10 + uint64(s[p]-'0')
			p++
		}
		return value, value <= uint64(^uint32(0))
	}
	value, ok := read()
	if !ok {
		return false
	}
	endDays := p
	for p < len(s) && digestTimeSpace(s[p]) {
		p++
	}
	var fields [4]uint64
	state := 4
	if p+1 < len(s) && p != endDays && digestTimeDigit(s[p]) {
		fields[0] = value
		state = 1
	} else if p+1 < len(s) && s[p] == ':' && digestTimeDigit(s[p+1]) {
		fields[1] = value
		state = 2
		p++
	} else {
		fields[1], fields[2], fields[3] = value/10000, value/100%100, value%100
	}
	for state < 4 {
		value, ok = read()
		if !ok {
			return false
		}
		fields[state] = value
		state++
		if state == 4 || p+1 >= len(s) || s[p] != ':' || !digestTimeDigit(s[p+1]) {
			break
		}
		p++
	}
	fraction := false
	if p+1 < len(s) && s[p] == '.' && digestTimeDigit(s[p+1]) {
		p++
		n := 0
		for p < len(s) && digestTimeDigit(s[p]) {
			if n < 6 && s[p] != '0' {
				fraction = true
			}
			n++
			p++
		}
		if n > 6 && s[p-1] >= '5' {
			fraction = true
		}
	} else if p+1 == len(s) && s[p] == '.' {
		p++
	}
	h, m, sec := fields[0]*24+fields[1], fields[2], fields[3]
	return p > start && h <= 838 && m < 60 && sec < 60 && !(h == 838 && m == 59 && sec == 59 && fraction) && digestTimeTail(s[p:])
}

func digestTemporalLiteralValid(yylex yyLexer, kind, value string) bool {
	lexer := yylex.(*Lexer)
	if lexer.digest == nil {
		return true
	}
	if c := lexer.scanner.parseControl; c != nil {
		c.work += len(value)
		if c.work > c.limits.Work {
			panic(parseAbort{c, ErrParseLimit})
		}
		if err := context.Cause(c.ctx); err != nil {
			panic(parseAbort{c, err})
		}
	}
	valid := false
	switch digestKeywordUpper(kind) {
	case "DATE":
		valid = digestDateTimeKind(value, lexer.digest.temporalMode, false) == 1
	case "TIMESTAMP":
		valid = digestDateTimeKind(value, lexer.digest.temporalMode, false) == 2
	case "TIME":
		valid = digestTimeLiteralValid(value)
	}
	return valid
}

func digestTemporalLiteral(yylex yyLexer, kind, value string) *tree.FuncExpr {
	node := &tree.FuncExpr{
		IsTemporalLiteral: true,
		Func:              tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(kind)),
		FuncName:          tree.NewCStr(kind, 1),
		Exprs:             tree.Exprs{tree.NewNumVal(value, value, false, tree.P_char)},
	}
	if !digestTemporalLiteralValid(yylex, kind, value) {
		yylex.(*Lexer).digest.rejectExpression(node)
	}
	return node
}
