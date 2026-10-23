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
//   - Wide-m patterns admitted for per-line evaluation run with the plain
//     (non-multiline) pattern form against each terminator-free line buffer,
//     where RE2's `^` equals the line start and `$` equals the line end —
//     exactly the MySQL multiline contract. Admission inspects only the
//     pattern, so a prefix hit keeps its short-circuit and never pays a
//     whole-subject pre-scan. Iteration runs in O(subject), needs no regexp2
//     admission budget, and holds at any subject size. After a trailing lone
//     CR/NEL/LS/PS the cursor stops (ICU's caret exclusion), while a
//     trailing LF — including the LF half of a CRLF pair — leaves one final
//     empty line, yielded once, preserving Go RE2's native multiline
//     end-of-input contract.
//   - Patterns that can match a line terminator (literal \n, terminator
//     classes, \s, '.' only under match_type 'n') or that carry
//     whole-subject anchors (\A, \z, or anchors under a (?-m) scope) do not
//     admit the per-line transform and fall back to regexp2 up to
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
	// Line-route admission inspects the pattern only: no whole-subject
	// pre-scan may sit in front of a prefix short-circuit match (mo-bot
	// review on #30646). A subject scan happens only here, on the
	// terminator-capable fallback: when the subject itself carries no wide
	// terminator, the base engine's LF-only (?m) anchors enumerate exactly
	// the same line boundaries as ICU on this input, so the supported base
	// input domain is preserved instead of bouncing into regexp2 admission.
	if !regexpWideLineAdmissible(pat, pureMatchType, binary) {
		if !regexpSubjectHasWideLineTerminator(subject, binary) {
			return regexpRouteRE2
		}
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

// wideMultilineNextLine returns the next line segment [lineStart, lineEnd),
// the terminator following it at [lineEnd, termEnd), and caretOK: whether
// '^' may bind at lineStart. CRLF is one terminator; interior consecutive
// terminators still yield empty lines. A trailing LF — including the LF half
// of CRLF — yields one final empty line with the caret allowed, preserving
// Go RE2's native multiline contract. A trailing lone CR/NEL/LS/PS yields
// one final empty segment too, but with the caret suppressed: '$' still
// binds at the true end of input while '^' does not (mo-bot review on
// #30646).
func wideMultilineNextLine(subject string, from int, binary bool) (lineStart, lineEnd, termEnd int, caretOK, ok bool) {
	if from >= len(subject) {
		if from == len(subject) {
			if len(subject) == 0 || subject[len(subject)-1] == 0x0A {
				// The empty subject and the terminal empty line after a
				// trailing LF (bare or the LF half of CRLF) keep the caret,
				// matching Go RE2's native multiline ^/$ at the end of
				// input. The termEnd of len(subject)+1 is the exhausted
				// sentinel, so this segment is yielded exactly once.
				return len(subject), len(subject), len(subject) + 1, true, true
			}
			if regexpSubjectEndsWithLoneWideTerminator(subject, binary) {
				// The terminal segment after a trailing lone CR/NEL/LS/PS:
				// '$' still binds at the true end of input, but the MySQL
				// contract binds no '^' there. Yielded exactly once through
				// the same exhausted sentinel.
				return len(subject), len(subject), len(subject) + 1, false, true
			}
		}
		return 0, 0, 0, false, false
	}
	lineStart = from
	i := from
	for i < len(subject) {
		b := subject[i]
		if b == 0x0A {
			return lineStart, i, i + 1, true, true
		}
		if b == 0x0D {
			if i+1 < len(subject) && subject[i+1] == 0x0A {
				return lineStart, i, i + 2, true, true
			}
			return lineStart, i, i + 1, true, true
		}
		if binary {
			if b == 0x85 {
				return lineStart, i, i + 1, true, true
			}
			i++
			continue
		}
		// UTF-8: NEL=C2 85, LS=E2 80 A8, PS=E2 80 A9.
		if b == 0xC2 && i+1 < len(subject) && subject[i+1] == 0x85 {
			return lineStart, i, i + 2, true, true
		}
		if b == 0xE2 && i+2 < len(subject) && subject[i+1] == 0x80 &&
			(subject[i+2] == 0xA8 || subject[i+2] == 0xA9) {
			return lineStart, i, i + 3, true, true
		}
		_, size := utf8.DecodeRuneInString(subject[i:])
		if size < 1 {
			size = 1
		}
		i += size
	}
	return lineStart, len(subject), len(subject), true, true
}

// regexpSubjectEndsWithLoneWideTerminator reports whether subject ends with
// a lone wide line terminator: a final CR (not followed by LF, i.e. CR at
// the end of input), NEL (U+0085), LS (U+2028) or PS (U+2029). LF-terminated
// and CRLF-terminated subjects keep the RE2-native trailing empty line
// instead, and terminators end exactly at the subject end here.
func regexpSubjectEndsWithLoneWideTerminator(subject string, binary bool) bool {
	if len(subject) == 0 || subject[len(subject)-1] == 0x0A {
		return false
	}
	if subject[len(subject)-1] == 0x0D {
		return true
	}
	if binary {
		return subject[len(subject)-1] == 0x85
	}
	return strings.HasSuffix(subject, "\xc2\x85") ||
		strings.HasSuffix(subject, "\xe2\x80\xa8") ||
		strings.HasSuffix(subject, "\xe2\x80\xa9")
}

// regexpSubjectHasWideLineTerminator reports whether subject contains one of
// the ICU line terminators that Go RE2's native (?m) does not treat as a
// line boundary: CR, NEL (U+0085), LS (U+2028) or PS (U+2029). LF is common
// to both engines and excluded on purpose.
func regexpSubjectHasWideLineTerminator(subject string, binary bool) bool {
	if strings.IndexByte(subject, 0x0D) >= 0 {
		return true
	}
	if binary {
		return strings.IndexByte(subject, 0x85) >= 0
	}
	return strings.Contains(subject, "\xc2\x85") ||
		strings.Contains(subject, "\xe2\x80\xa8") ||
		strings.Contains(subject, "\xe2\x80\xa9")
}

// regexpWideCaretSuppressedEmptyMatch reports whether a wide-multiline
// pattern matches on the caret-suppressed terminal segment that follows a
// trailing lone CR/NEL/LS/PS. Every match on that zero-width segment is
// empty and '^' is forbidden there while '$' still binds, so a match exists
// iff the pattern language contains the empty string through a derivation
// that does not consume a begin-of-line node. The pattern is parsed with its
// match_type wrapper, so user '^' appears as OpBeginLine and user '$' as
// OpEndLine.
func regexpWideCaretSuppressedEmptyMatch(pat, pureMatchType string) bool {
	rule := regexpPatternWithPureMatchTypeRE2(pat, pureMatchType)
	re, err := syntax.Parse(rule, syntax.Perl)
	if err != nil {
		// The compile path reports the real error; treat as no match.
		return false
	}
	return regexpSyntaxCaretSuppressedEmptyMatch(re)
}

// regexpSyntaxCaretSuppressedEmptyMatch evaluates an empty match on the
// caret-suppressed terminal segment over the parsed pattern tree.
func regexpSyntaxCaretSuppressedEmptyMatch(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpEmptyMatch, syntax.OpEndLine, syntax.OpEndText, syntax.OpNoWordBoundary:
		// '$' still binds at the true end of input; \z cannot reach the line
		// route but would bind at the true end, and \B holds on the empty
		// segment with no adjacent word character.
		return true
	case syntax.OpBeginLine, syntax.OpBeginText, syntax.OpWordBoundary:
		// '^' (and \A, which cannot reach the line route) is forbidden on
		// the caret-suppressed segment, and \b cannot hold without an
		// adjacent word character.
		return false
	case syntax.OpConcat:
		for _, sub := range re.Sub {
			if !regexpSyntaxCaretSuppressedEmptyMatch(sub) {
				return false
			}
		}
		return true
	case syntax.OpAlternate:
		for _, sub := range re.Sub {
			if regexpSyntaxCaretSuppressedEmptyMatch(sub) {
				return true
			}
		}
		return false
	case syntax.OpStar, syntax.OpQuest:
		return true
	case syntax.OpRepeat:
		if re.Min == 0 {
			return true
		}
		return regexpSyntaxCaretSuppressedEmptyMatch(re.Sub[0])
	case syntax.OpCapture:
		return regexpSyntaxCaretSuppressedEmptyMatch(re.Sub[0])
	default:
		// Literals, character classes and dot need input bytes.
		return false
	}
}

// wideLikeChunkBytes bounds the line-prefix probe in regexpWideLike: a line
// longer than this is first examined through one bounded prefix chunk,
// restoring prefix short-circuit matching without paying a full-line
// terminator scan first (mo-bot review on #30646).
const wideLikeChunkBytes = 4096

// regexpWideChunkTerminator scans chunk — a bounded PREFIX of the current
// line — for the first wide line terminator, returning its position and byte
// length. ambiguous is true when the chunk ends inside what could be a
// multi-byte terminator (a lone CR, or an incomplete C2/E2 lead), in which
// case the caller falls back to the full-line walk.
func regexpWideChunkTerminator(chunk string) (pos, tailLen int, ambiguous bool) {
	i, n := 0, len(chunk)
	for i < n {
		b := chunk[i]
		switch {
		case b == 0x0A:
			return i, 1, false
		case b == 0x0D:
			if i+1 >= n {
				// Could be the CR half of a CRLF whose LF sits past the chunk.
				return -1, 0, true
			}
			if chunk[i+1] == 0x0A {
				return i, 2, false
			}
			return i, 1, false
		case b == 0xC2 && i+1 >= n:
			// Could start NEL (C2 85) with the second byte past the chunk.
			return -1, 0, true
		case b == 0xE2 && i+2 >= n:
			// Could start LS or PS (E2 80 A8/A9).
			return -1, 0, true
		case b == 0xC2 && chunk[i+1] == 0x85:
			return i, 2, false
		case b == 0xE2 && chunk[i+1] == 0x80 && (chunk[i+2] == 0xA8 || chunk[i+2] == 0xA9):
			return i, 3, false
		}
		_, size := utf8.DecodeRuneInString(chunk[i:])
		if size < 1 {
			size = 1
		}
		i += size
	}
	return -1, 0, false
}

func regexpLineTerminators(binary bool) []rune {
	if binary {
		return []rune{0x0A, 0x0D, 0x85}
	}
	return []rune{0x0A, 0x0D, 0x85, 0x2028, 0x2029}
}

// regexpWideLineAdmissible reports whether per-line evaluation preserves the
// pattern's whole-subject semantics; inadmissible patterns keep whole-subject
// routing (regexp2 within its budget, explicit rejection beyond it).
//
//   - Constructs that can match a line terminator (literal terminators,
//     terminator classes, \s, '.' only when match_type 'n' compiles it to
//     (?s)) need cross-line context. A plain '.' (OpAnyCharNotNL) matches no
//     line terminator under the MySQL/ICU default contract, and per-line
//     evaluation of a terminator-free line keeps exactly that meaning.
//   - Whole-subject anchors (\A, \z) and ^/$ compiled with multiline disabled
//     inside a (?-m:...) scope become OpBeginText/OpEndText and would gain
//     fake line boundaries when evaluated per line.
//
// All line terminator runes are case-fold-invariant, so a FoldCase-expanded
// class never gains a terminator it did not already contain: plain
// intersection is exact.
func regexpWideLineAdmissible(pat, pureMatchType string, binary bool) bool {
	rule := regexpPatternWithPureMatchTypeRE2(pat, pureMatchType)
	re, err := syntax.Parse(rule, syntax.Perl)
	if err != nil {
		// The compile path reports the real error; stay off the line route.
		return false
	}
	return !regexpSyntaxHasSubjectAnchor(re) && !regexpSyntaxHitsLineTerminator(re, binary)
}

// regexpSyntaxHasSubjectAnchor reports whether the parsed pattern contains a
// whole-subject anchor: \A, \z, or ^/$ evaluated with multiline disabled (for
// example inside a (?-m:...) scope). The pattern is parsed under the
// match_type wrapper, so top-level multiline anchors appear as
// OpBeginLine/OpEndLine and stay admissible.
func regexpSyntaxHasSubjectAnchor(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpBeginText, syntax.OpEndText:
		return true
	default:
		for _, sub := range re.Sub {
			if regexpSyntaxHasSubjectAnchor(sub) {
				return true
			}
		}
		return false
	}
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
	case syntax.OpAnyChar:
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
// testing each line; the first matching line settles TRUE. Lines longer
// than wideLikeChunkBytes first get one bounded prefix probe: a match
// ending strictly inside a terminator-free prefix is definitive without
// scanning the rest of the line, which restores the prefix short-circuit
// cost of the whole-subject engine (mo-bot review on #30646).
func (rs *regexpSet) regexpWideLike(pat, str, pureMatchType string, binary bool) (bool, error) {
	reg, err := rs.getWideLineMatcher(pat, pureMatchType, binary)
	if err != nil {
		return false, err
	}
	binaryCaseFold := binary && strings.ContainsRune(pureMatchType, 'i')
	from := 0
	for {
		if !binary && len(str)-from > wideLikeChunkBytes {
			pos, tailLen, ambiguous := regexpWideChunkTerminator(str[from : from+wideLikeChunkBytes])
			if !ambiguous {
				if pos >= 0 {
					// The terminator inside the prefix already resolves the
					// whole line; only its content can hold a match.
					if reg.MatchString(str[from : from+pos]) {
						return true, nil
					}
					from += pos + tailLen
					continue
				}
				if m := reg.FindStringIndex(str[from : from+wideLikeChunkBytes]); m != nil && m[1] < wideLikeChunkBytes-4 {
					return true, nil
				}
			}
		}
		lineStart, lineEnd, termEnd, caretOK, ok := wideMultilineNextLine(str, from, binary)
		if !ok {
			return false, nil
		}
		if !caretOK {
			// Terminal segment after a trailing lone CR/NEL/LS/PS: '^' is
			// suppressed here, so only an empty '$'-derivation can match.
			if regexpWideCaretSuppressedEmptyMatch(pat, pureMatchType) {
				return true, nil
			}
			from = termEnd
			continue
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
		lineStart, lineEnd, termEnd, caretOK, ok := wideMultilineNextLine(str, from, subjectIsBinary)
		if !ok {
			return [2]int{}, false, nil
		}
		line := str[lineStart:lineEnd]
		if !caretOK {
			// Terminal segment after a trailing lone CR/NEL/LS/PS: '^' is
			// suppressed here; the single possible match is an empty one at
			// lineStart through a '$'-derivation.
			if lineEnd >= startByte && regexpWideCaretSuppressedEmptyMatch(pat, pureMatchType) {
				if remaining <= 1 {
					return [2]int{lineStart, lineStart}, true, nil
				}
				remaining--
			}
			from = termEnd
			continue
		}
		// lineEnd >= startByte: a zero-width match may sit exactly at the line
		// end that equals the requested start (for example '^' on an initial
		// empty line), so such a line must still be searched.
		if lineEnd >= startByte {
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
	// MySQL returns an empty subject unchanged, even for a pattern such as
	// '^$' that can match an empty string; keep the same contract on the
	// wide route. Empty LINES inside a non-empty subject are not affected.
	if len(str) == 0 {
		return str, nil
	}
	lineFlags := strings.ReplaceAll(pureMatchType, "m", "")
	remaining := occurrence
	var out strings.Builder
	out.Grow(len(str) + 8)
	from := 0
	for {
		lineStart, lineEnd, termEnd, caretOK, ok := wideMultilineNextLine(str, from, subjectIsBinary)
		if !ok {
			break
		}
		line := str[lineStart:lineEnd]
		// termEnd may be len(str)+1 for the trailing segment after a final
		// line terminator (the exhausted sentinel); that segment has no
		// terminator bytes of its own.
		sliceTermEnd := termEnd
		if sliceTermEnd > len(str) {
			sliceTermEnd = len(str)
		}
		term := str[lineEnd:sliceTermEnd]
		writeLine := line
		// lineEnd >= startByte keeps zero-width matches sitting exactly at
		// the requested start inside the replaced range.
		if lineEnd >= startByte && !(occurrence > 0 && remaining <= 0) {
			localStart := 0
			if startByte > lineStart {
				localStart = startByte - lineStart
			}
			localPos := int64(localStart + 1)
			if !subjectIsBinary {
				localPos = int64(utf8.RuneCountInString(str[lineStart:lineStart+localStart])) + 1
			}
			useOcc := int64(0)
			skipReplace := false
			if occurrence > 0 {
				var visited int64
				if !caretOK && len(line) == 0 {
					// On the caret-suppressed terminal segment the only
					// possible visit is the single empty '$' match.
					if regexpWideCaretSuppressedEmptyMatch(pat, pureMatchType) {
						visited = 1
					}
				} else {
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
				}
				if visited >= remaining {
					useOcc = remaining
					remaining = 0
				} else {
					// The requested occurrence is on a later line; leave
					// this line unchanged and carry the remaining ordinal
					// over.
					remaining -= visited
					skipReplace = true
				}
			}
			if !skipReplace {
				if len(line) == 0 {
					// An empty line is not an empty subject: the base
					// shortcut that returns an empty subject unchanged must
					// not suppress '^'/'$' matches sitting on this line.
					writeLine, err = rs.regularReplaceWideEmptyLine(
						pat, repl, useOcc, subjectIsBinary, pureMatchType, lineFlags, caretOK,
					)
				} else {
					writeLine, err = rs.regularReplaceWithMatchType(
						pat, line, repl, localPos, useOcc, subjectIsBinary, lineFlags)
				}
				if err != nil {
					return "", err
				}
			}
		}
		// The per-line replacement-size check alone cannot enforce the
		// shared aggregate budget: each line may pass it while the
		// accumulated result overshoots, so budget the builder before every
		// append (mo-bot review on #30646).
		if err = regexpWriteReplacementString(&out, writeLine, regexpReplaceMaxResultBytes); err != nil {
			return "", err
		}
		if err = regexpWriteReplacementString(&out, term, regexpReplaceMaxResultBytes); err != nil {
			return "", err
		}
		from = termEnd
	}
	return out.String(), nil
}

// regularReplaceWideEmptyLine replaces an empty per-line segment. The base
// replace entry short-circuits an empty SUBJECT unchanged (the MySQL
// contract), which is right at the top level but suppresses legitimate
// matches on interior or terminal empty lines produced by line splitting:
// '\na' with '^' must become 'X\nXa', and the terminal segment after a
// trailing lone CR/NEL/LS/PS still binds '$' (mo-bot review on #30646).
func (rs *regexpSet) regularReplaceWideEmptyLine(
	pat, repl string, useOcc int64, subjectIsBinary bool, pureMatchType, lineFlags string, caretOK bool,
) (string, error) {
	reg, err := rs.getWideLineMatcher(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return "", regexpCompileError("regexp_replace", pat, err)
	}
	matched := false
	if caretOK {
		matched = regexpMatchCompiled(
			reg, "", subjectIsBinary, subjectIsBinary && strings.ContainsRune(pureMatchType, 'i'))
	} else {
		matched = regexpWideCaretSuppressedEmptyMatch(pat, pureMatchType)
	}
	if !matched {
		return "", nil
	}
	// The segment holds at most this one empty match at byte 0 (character
	// position 1); template expansion and output-budget enforcement reuse
	// the base engines.
	if regexpReplacementNeedsExpansion(repl) {
		return rs.regularReplaceWithTemplate(
			reg, pat, "", repl, 0, useOcc, subjectIsBinary, lineFlags, regexpReplaceMaxResultBytes,
		)
	}
	return rs.regularReplaceLiteralWithMatchType(
		reg, pat, "", repl, 0, useOcc, subjectIsBinary, lineFlags, true,
	)
}
