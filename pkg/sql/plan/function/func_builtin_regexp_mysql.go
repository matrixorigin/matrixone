// Copyright 2021 - 2026 Matrix Origin
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

package function

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/dlclark/regexp2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type mysqlRegexpMode struct {
	multiline bool
	dotAll    bool
}

const mysqlRegexpMatchTimeout = time.Second

func (rs *regexpSet) getMySQLRegexp(pattern, matchType, functionName string) (*regexp2.Regexp, error) {
	if pattern == "" {
		return nil, moerr.NewRegexpIllegalArgumentNoCtx()
	}
	mode, unixLines, ignoreCase, err := parseMySQLRegexpMatchType(matchType, functionName)
	if err != nil {
		return nil, err
	}
	rewritten := rewriteMySQLRegexpPattern(pattern, mode, unixLines)
	key := fmt.Sprintf("%t\x00%s", ignoreCase, rewritten)
	if reg, ok := rs.mysqlMp[key]; ok {
		return reg, nil
	}
	if len(rs.mysqlMp) == mapSizeForRegexp {
		for key := range rs.mysqlMp {
			delete(rs.mysqlMp, key)
		}
	}
	options := regexp2.RegexOptions(regexp2.RE2)
	if ignoreCase {
		options |= regexp2.IgnoreCase
	}
	reg, err := regexp2.Compile(rewritten, options)
	if err != nil {
		return nil, moerr.NewInvalidArgNoCtx(functionName+" have invalid regexp pattern arg", pattern)
	}
	// regexp2 is required for ICU-compatible zero-width CRLF boundaries. Bound
	// its backtracking work so a hostile pattern cannot monopolize a CN worker.
	reg.MatchTimeout = mysqlRegexpMatchTimeout
	rs.mysqlMp[key] = reg
	return reg, nil
}

func mysqlRegexpExecError(err error) error {
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "match timeout after ") {
		return moerr.NewRegexpTimeoutNoCtx()
	}
	return err
}

func (rs *regexpSet) validateMySQLRegexp(pattern, matchType, functionName string) error {
	_, err := rs.getMySQLRegexp(pattern, matchType, functionName)
	return err
}

func (rs *regexpSet) validateMySQLRegexpReplacement(pattern, replacement, matchType string) error {
	reg, err := rs.getMySQLRegexp(pattern, matchType, "regexp_replace")
	if err != nil {
		return err
	}
	_, err = parseMySQLRegexpReplacement(replacement, reg)
	return err
}

func parseMySQLRegexpMatchType(input, functionName string) (
	mode mysqlRegexpMode, unixLines bool, ignoreCase bool, err error,
) {
	for _, flag := range input {
		switch flag {
		case 'i':
			ignoreCase = true
		case 'c':
			ignoreCase = false
		case 'm':
			mode.multiline = true
		case 'n':
			mode.dotAll = true
		case 'u':
			unixLines = true
		default:
			return mysqlRegexpMode{}, false, false, moerr.NewWrongArguments(moerr.Context(), functionName)
		}
	}
	return mode, unixLines, ignoreCase, nil
}

// rewriteMySQLRegexpPattern expresses ICU's line-boundary rules without
// changing the subject. regexp2 is used here because the CRLF rule requires
// zero-width lookaround; positions therefore remain positions in the original
// rune slice.
func rewriteMySQLRegexpPattern(pattern string, initial mysqlRegexpMode, unixLines bool) string {
	runes := []rune(pattern)
	mode := initial
	stack := make([]mysqlRegexpMode, 0, 4)
	var result strings.Builder
	result.Grow(len(pattern))

	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\\':
			result.WriteRune(runes[i])
			if i+1 < len(runes) {
				i++
				result.WriteRune(runes[i])
			}
		case '[':
			result.WriteRune(runes[i])
			for i++; i < len(runes); i++ {
				result.WriteRune(runes[i])
				if runes[i] == '\\' && i+1 < len(runes) {
					i++
					result.WriteRune(runes[i])
				} else if runes[i] == ']' {
					break
				}
			}
		case '(':
			if updated, end, scoped, rendered, ok := parseInlineRegexpMode(runes, i, mode); ok {
				result.WriteString(rendered)
				if scoped {
					stack = append(stack, mode)
				}
				mode = updated
				i = end
				continue
			}
			stack = append(stack, mode)
			result.WriteRune('(')
		case ')':
			result.WriteRune(')')
			if len(stack) > 0 {
				mode = stack[len(stack)-1]
				stack = stack[:len(stack)-1]
			}
		case '.':
			if mode.dotAll {
				result.WriteString("(?s:.)")
			} else {
				result.WriteString("[^\\r\\n\\x{85}\\x{2028}\\x{2029}]")
			}
		case '^':
			if !mode.multiline {
				result.WriteRune('^')
			} else if unixLines {
				result.WriteString("(?:\\A|(?<=\\n))")
			} else {
				result.WriteString("(?:\\A|(?<=\\n)|(?<=\\x{85})|(?<=\\x{2028})|(?<=\\x{2029})|(?<=\\r)(?!\\n))")
			}
		case '$':
			if !mode.multiline {
				result.WriteRune('$')
			} else if unixLines {
				result.WriteString("(?:\\z|(?=\\n))")
			} else {
				result.WriteString("(?:\\z|(?=\\r\\n)|(?=\\r(?!\\n))|(?<!\\r)(?=\\n)|(?=[\\x{85}\\x{2028}\\x{2029}]))")
			}
		default:
			result.WriteRune(runes[i])
		}
	}
	return result.String()
}

func regexpPatternHasUnescapedAnchor(pattern string) bool {
	runes := []rune(pattern)
	inClass := false
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\\':
			i++
		case '[':
			inClass = true
		case ']':
			inClass = false
		case '^', '$':
			if !inClass {
				return true
			}
		}
	}
	return false
}

func parseInlineRegexpMode(
	pattern []rune, start int, current mysqlRegexpMode,
) (updated mysqlRegexpMode, end int, scoped bool, rendered string, ok bool) {
	updated = current
	if start+2 >= len(pattern) || pattern[start+1] != '?' {
		return mysqlRegexpMode{}, 0, false, "", false
	}
	i := start + 2
	enable := true
	var keptEnable, keptDisable strings.Builder
	seenFlag := false
	for ; i < len(pattern); i++ {
		flag := pattern[i]
		if flag == '-' {
			enable = false
			continue
		}
		if flag == ')' || flag == ':' {
			if !seenFlag {
				return mysqlRegexpMode{}, 0, false, "", false
			}
			scoped = flag == ':'
			kept := keptEnable.String()
			if keptDisable.Len() > 0 {
				kept += "-" + keptDisable.String()
			}
			if scoped {
				if kept == "" {
					rendered = "(?:"
				} else {
					rendered = "(?" + kept + ":"
				}
			} else if kept == "" {
				rendered = "(?:)"
			} else {
				rendered = "(?" + kept + ")"
			}
			return updated, i, scoped, rendered, true
		}
		if !unicode.IsLetter(flag) {
			return mysqlRegexpMode{}, 0, false, "", false
		}
		seenFlag = true
		switch flag {
		case 'm':
			updated.multiline = enable
		case 's':
			updated.dotAll = enable
		default:
			if enable {
				keptEnable.WriteRune(flag)
			} else {
				keptDisable.WriteRune(flag)
			}
		}
	}
	return mysqlRegexpMode{}, 0, false, "", false
}

type mysqlReplacementToken struct {
	literal string
	group   int
}

type mysqlRegexpReplacement []mysqlReplacementToken

func (replacement mysqlRegexpReplacement) literal() (string, bool) {
	var result strings.Builder
	for _, token := range replacement {
		if token.group >= 0 {
			return "", false
		}
		result.WriteString(token.literal)
	}
	return result.String(), true
}

func parseMySQLRegexpReplacement(input string, reg *regexp2.Regexp) (mysqlRegexpReplacement, error) {
	groups := make(map[int]struct{})
	for _, number := range reg.GetGroupNumbers() {
		groups[number] = struct{}{}
	}
	runes := []rune(input)
	tokens := make(mysqlRegexpReplacement, 0, 4)
	start := 0
	for i := 0; i < len(runes); i++ {
		if runes[i] != '$' {
			continue
		}
		if start < i {
			tokens = append(tokens, mysqlReplacementToken{literal: string(runes[start:i]), group: -1})
		}
		if i+1 >= len(runes) || runes[i+1] < '0' || runes[i+1] > '9' {
			return nil, moerr.NewRegexpInvalidCaptureGroupNoCtx()
		}
		i++
		group := int(runes[i] - '0')
		if _, ok := groups[group]; !ok {
			return nil, moerr.NewRegexpIndexOutOfBoundsNoCtx()
		}
		tokens = append(tokens, mysqlReplacementToken{group: group})
		start = i + 1
	}
	if start < len(runes) {
		tokens = append(tokens, mysqlReplacementToken{literal: string(runes[start:]), group: -1})
	}
	return tokens, nil
}

func (replacement mysqlRegexpReplacement) expand(match *regexp2.Match) string {
	var result strings.Builder
	for _, token := range replacement {
		if token.group < 0 {
			result.WriteString(token.literal)
			continue
		}
		group := match.GroupByNumber(token.group)
		if group != nil {
			result.WriteString(group.String())
		}
	}
	return result.String()
}
