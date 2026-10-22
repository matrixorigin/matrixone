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

package function

import (
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Wide-multiline REGEXP routing (issue #30210, mo-bot review on #30646).
//
// MySQL/ICU multiline treats CR, LF, CRLF, NEL (U+0085), LS (U+2028) and PS
// (U+2029) as one line terminator each, with CR/NEL/LS/PS matched by ^ only
// when not trailing the input. RE2 cannot express those anchors because Go's
// regexp engine has no lookarounds, and the previous regexp2 routing scanned
// every subject before RE2 could short-circuit (532x on a prefix hit),
// suffered regexp2 admission-memory failures for ordinary anchors, and
// silently degraded to LF-only semantics past 1 MiB.
//
// This file replaces that routing with a per-line evaluation strategy:
//
//   - Subjects without any wide terminator evaluate exactly as before via
//     Go RE2 `(?m)` (the ordinary multiline fast path is untouched).
//   - Wide-m patterns whose rewritten anchors cannot match a line terminator
//     are evaluated per line with the plain (non-multiline) pattern form.
//     Inside a line buffer, RE2's `^` equals the line start and `$` equals
//     the line end, which is exactly the MySQL multiline contract; per-line
//     iteration runs in O(subject), needs no regexp2 admission budget, and
//     holds at any subject size. After a trailing lone CR/NEL/LS/PS the
//     cursor stops (ICU's caret exclusion), while a trailing LF — including
//     the LF half of a CRLF pair — leaves one final empty line, preserving
//     Go RE2's native multiline end-of-input contract.
//   - Patterns that can match a line terminator (literal \n, `.`, \s, ...)
//     do not admit the per-line transform and fall back to regexp2 up to
//     regexp2MaxSubjectBytes, above which they are rejected explicitly
//     instead of returning a silently wrong LF-only result.

const (
	regexpRouteRE2 = iota
	regexpRouteRegexp2
	regexpRouteWideLines
	regexpRouteWideTooLarge
)

func regexpWideMultilineRoute(pat, pureMatchType, subject string, binary bool) int {
	if requiresRegexp2Pattern(pat) {
		return regexpRouteRegexp2
	}
	if !regexpMatchTypeWideMultiline(pureMatchType) {
		return regexpRouteRE2
	}
	if rewriteRegexpWideMultilineAnchors(pat, binary) == pat {
		return regexpRouteRE2
	}
	if !regexpWideMultilineSubjectNeedsICU(subject, binary) {
		return regexpRouteRE2
	}
	if regexpPatternMatchesLineTerminator(pat, pureMatchType, binary) {
		if len(subject) <= regexp2MaxSubjectBytes {
			return regexpRouteRegexp2
		}
		return regexpRouteWideTooLarge
	}
	return regexpRouteWideLines
}

func regexpWideMultilineTooLargeError(binary bool) error {
	mode := "text"
	if binary {
		mode = "binary"
	}
	return moerr.NewInvalidInputNoCtxf(
		"wide multiline REGEXP on %s input larger than %d bytes is not supported "+
			"when the pattern can match a line terminator", mode, regexp2MaxSubjectBytes)
}

// wideMultilineNextLine returns the next line segment [lineStart, lineEnd)
// and the terminator following it at [lineEnd, termEnd). CRLF is one
// terminator; interior consecutive terminators still yield empty lines. The
// cursor stops after a trailing lone CR/NEL/LS/PS (no caret at end of
// input), while a trailing LF — including the LF half of CRLF — yields one
// final empty line, preserving Go RE2's native multiline contract.
func wideMultilineNextLine(subject string, from int, binary bool) (lineStart, lineEnd, termEnd int, ok bool) {
	if from >= len(subject) {
		// A trailing \n (bare LF or the LF half of CRLF) leaves an empty
		// trailing line, matching Go RE2's native multiline ^/$ at \z, which
		// the MySQL compatibility contract keeps for that terminator. A
		// trailing lone CR/NEL/LS/PS terminates the input without a caret at
		// end of input, so no line follows them.
		if len(subject) > 0 && from == len(subject) && subject[len(subject)-1] == 0x0A {
			return len(subject), len(subject), len(subject), true
		}
		return 0, 0, 0, false
	}
	lineStart = from
	i := from
	for i < len(subject) {
		b := subject[i]
		if b == 0x0A {
			return lineStart, i, i + 1, true
		}
		if b == 0x0D {
			if i+1 < len(subject) && subject[i+1] == 0x0A {
				return lineStart, i, i + 2, true
			}
			return lineStart, i, i + 1, true
		}
		if binary {
			if b == 0x85 {
				return lineStart, i, i + 1, true
			}
			i++
			continue
		}
		// UTF-8: NEL=C2 85, LS=E2 80 A8, PS=E2 80 A9.
		if b == 0xC2 && i+1 < len(subject) && subject[i+1] == 0x85 {
			return lineStart, i, i + 2, true
		}
		if b == 0xE2 && i+2 < len(subject) && subject[i+1] == 0x80 &&
			(subject[i+2] == 0xA8 || subject[i+2] == 0xA9) {
			return lineStart, i, i + 3, true
		}
		_, size := utf8.DecodeRuneInString(subject[i:])
		if size < 1 {
			size = 1
		}
		i += size
	}
	return lineStart, len(subject), len(subject), true
}

func regexpLineTerminators(binary bool) []rune {
	if binary {
		return []rune{0x0A, 0x0D, 0x85}
	}
	return []rune{0x0A, 0x0D, 0x85, 0x2028, 0x2029}
}

// regexpPatternMatchesLineTerminator reports whether a pattern can match a
// string containing a line terminator. All line terminator runes are
// case-fold-invariant, so a FoldCase-expanded class never gains a terminator
// it did not already contain: plain intersection is exact.
func regexpPatternMatchesLineTerminator(pat, pureMatchType string, binary bool) bool {
	rule := regexpPatternWithPureMatchTypeRE2(pat, pureMatchType)
	re, err := syntax.Parse(rule, syntax.Perl)
	if err != nil {
		// The compile path reports the real error; stay off the line route.
		return true
	}
	return regexpSyntaxHitsLineTerminator(re, binary)
}

func regexpSyntaxHitsLineTerminator(re *syntax.Regexp, binary bool) bool {
	switch re.Op {
	case syntax.OpLiteral:
		for _, t := range regexpLineTerminators(binary) {
			for _, r := range re.Rune {
				if r == t {
					return true
				}
			}
		}
		return false
	case syntax.OpCharClass:
		for i := 0; i+1 < len(re.Rune); i += 2 {
			lo, hi := re.Rune[i], re.Rune[i+1]
			for _, t := range regexpLineTerminators(binary) {
				if lo <= t && t <= hi {
					return true
				}
			}
		}
		return false
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		return true
	default:
		for _, sub := range re.Sub {
			if regexpSyntaxHitsLineTerminator(sub, binary) {
				return true
			}
		}
		return false
	}
}

// getWideLineMatcher compiles the per-line (non-multiline) RE2 form of a
// wide-m pattern. Inside one terminator-free line buffer Go's `^`/$ equal
// the line bounds, which is exactly the MySQL multiline anchor contract.
func (rs *regexpSet) getWideLineMatcher(pat, pureMatchType string, binary bool) (*regexp.Regexp, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return nil, err
	}
	lineFlags := strings.ReplaceAll(pureMatchType, "m", "")
	rule := regexpPatternWithPureMatchTypeRE2(pat, lineFlags)
	binaryCaseFold := binary && strings.ContainsRune(lineFlags, 'i')
	reg, _, err := rs.getRegularMatcherInfoWithBinaryCaseFold(rule, binary, binaryCaseFold)
	return reg, err
}

// regexpWideLike evaluates REGEXP_LIKE under wide multiline anchors by
// testing each line; the first matching line settles TRUE.
func (rs *regexpSet) regexpWideLike(pat, str, pureMatchType string, binary bool) (bool, error) {
	reg, err := rs.getWideLineMatcher(pat, pureMatchType, binary)
	if err != nil {
		return false, err
	}
	binaryCaseFold := binary && strings.ContainsRune(pureMatchType, 'i')
	from := 0
	for {
		lineStart, lineEnd, termEnd, ok := wideMultilineNextLine(str, from, binary)
		if !ok {
			return false, nil
		}
		if regexpMatchCompiled(reg, str[lineStart:lineEnd], binary, binaryCaseFold) {
			return true, nil
		}
		from = termEnd
	}
}

// wideNthMatch returns the raw absolute byte range of the occurrence-th
// wide-multiline match at or after startByte, walking lines in order.
func (rs *regexpSet) wideNthMatch(
	reg *regexp.Regexp, pat, str string, startByte int, subjectIsBinary bool,
	occurrence int64, pureMatchType string,
) ([2]int, bool, error) {
	remaining := occurrence
	from := 0
	for {
		lineStart, lineEnd, termEnd, ok := wideMultilineNextLine(str, from, subjectIsBinary)
		if !ok {
			return [2]int{}, false, nil
		}
		line := str[lineStart:lineEnd]
		if lineEnd > startByte {
			localStart := 0
			if startByte > lineStart {
				localStart = startByte - lineStart
			}
			searchSubject := line
			searchStart := localStart
			if subjectIsBinary {
				if strings.ContainsRune(pureMatchType, 'i') {
					searchSubject, searchStart = encodeBinaryRegexpText(line, localStart)
				} else {
					searchSubject, searchStart = encodeBinaryRegexpBytes(line, localStart)
				}
			}
			selected := [2]int{}
			visited := int64(0)
			err := rs.regexpVisitAtOrAfterWithMatchType(
				reg, pat, searchSubject, searchStart, subjectIsBinary, remaining, pureMatchType,
				func(start, end int) { selected = [2]int{start, end}; visited++ })
			if err != nil {
				return [2]int{}, false, err
			}
			if visited >= remaining {
				if subjectIsBinary {
					selected = decodeBinaryRegexpMatch(searchSubject, selected)
				}
				return [2]int{lineStart + selected[0], lineStart + selected[1]}, true, nil
			}
			remaining -= visited
		}
		from = termEnd
	}
}

func (rs *regexpSet) regularSubstrWide(
	pat string, str string, pos, occurrence int64, subjectIsBinary bool, pureMatchType string,
) (match bool, substr string, err error) {
	reg, err := rs.getWideLineMatcher(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return false, "", regexpCompileError("regexp_substr", pat, err)
	}
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 1 {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	selected, found, err := rs.wideNthMatch(reg, pat, str, startByte, subjectIsBinary, occurrence, pureMatchType)
	if err != nil || !found {
		return false, "", err
	}
	return true, str[selected[0]:selected[1]], nil
}

func (rs *regexpSet) regularInstrWide(
	pat string, str string, pos, occurrence int64, retOption int8, subjectIsBinary bool, pureMatchType string,
) (index int64, err error) {
	reg, err := rs.getWideLineMatcher(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return 0, regexpCompileError("regexp_instr", pat, err)
	}
	startByte, ok := 0, pos >= 1 && len(str) == 0
	if len(str) != 0 {
		startByte, ok = regexpSearchStartByte(str, pos, subjectIsBinary)
	}
	if !ok {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	if retOption < 0 || retOption > 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return option %d", retOption)
	}
	// REGEXP_INSTR anchors are relative to the suffix at pos; the wide line
	// evaluation keeps that contract by scanning lines of the suffix.
	searchSubject := str[startByte:]
	match, found, err := rs.wideNthMatch(reg, pat, searchSubject, 0, subjectIsBinary, occurrence, pureMatchType)
	if err != nil || !found {
		return 0, err
	}
	matchOffset := match[retOption]
	return regexpSuffixByteOffsetToPosition(searchSubject, pos, matchOffset, subjectIsBinary), nil
}

func (rs *regexpSet) regularReplaceWide(
	pat string, str string, repl string, pos, occurrence int64, subjectIsBinary bool, pureMatchType string,
) (result string, err error) {
	var reg *regexp.Regexp
	if occurrence > 0 {
		var err error
		reg, err = rs.getWideLineMatcher(pat, pureMatchType, subjectIsBinary)
		if err != nil {
			return "", regexpCompileError("regexp_replace", pat, err)
		}
	}
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 0 {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	lineFlags := strings.ReplaceAll(pureMatchType, "m", "")
	remaining := occurrence
	var out strings.Builder
	out.Grow(len(str) + 8)
	from := 0
	for {
		lineStart, lineEnd, termEnd, ok := wideMultilineNextLine(str, from, subjectIsBinary)
		if !ok {
			break
		}
		line := str[lineStart:lineEnd]
		term := str[lineEnd:termEnd]
		writeLine := line
		if lineEnd > startByte && !(occurrence > 0 && remaining <= 0) {
			localStart := 0
			if startByte > lineStart {
				localStart = startByte - lineStart
			}
			localPos := int64(localStart + 1)
			if !subjectIsBinary {
				localPos = int64(utf8.RuneCountInString(str[lineStart:lineStart+localStart])) + 1
			}
			useOcc := int64(0)
			if occurrence > 0 {
				visited := int64(0)
				searchSubject := line
				searchStart := localStart
				if subjectIsBinary {
					if strings.ContainsRune(pureMatchType, 'i') {
						searchSubject, searchStart = encodeBinaryRegexpText(line, localStart)
					} else {
						searchSubject, searchStart = encodeBinaryRegexpBytes(line, localStart)
					}
				}
				if err = rs.regexpVisitAtOrAfterWithMatchType(
					reg, pat, searchSubject, searchStart, subjectIsBinary, 0, pureMatchType,
					func(int, int) { visited++ }); err != nil {
					return "", err
				}
				if visited >= remaining {
					useOcc = remaining
					remaining = 0
				} else {
					remaining -= visited
				}
			}
			writeLine, err = rs.regularReplaceWithMatchType(
				pat, line, repl, localPos, useOcc, subjectIsBinary, lineFlags)
			if err != nil {
				return "", err
			}
		}
		out.WriteString(writeLine)
		out.WriteString(term)
		from = termEnd
	}
	return out.String(), nil
}
