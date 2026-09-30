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

package function

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/clipperhouse/uax29/v2/graphemes"
	"github.com/dlclark/regexp2"
	regexp2syntax "github.com/dlclark/regexp2/syntax"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// regexp2MatchTimeout bounds the only path in which MatrixOne permits
// backtracking. Ordinary patterns continue to use Go's linear-time RE2
// implementation. A finite timeout is required here because a user supplied
// ICU pattern can otherwise consume an unbounded amount of CN CPU.
const regexp2MatchTimeout = time.Second

// regexp2's engine materializes the subject as []rune and keeps a backtracking
// stack. These admission limits prevent a complex user pattern from turning a
// single SQL value into an unbounded temporary allocation. The existing RE2
// path is unchanged and remains available for larger ordinary patterns.
const (
	regexp2MaxPatternBytes = 1 << 20
	// Backtracking is an exceptional compatibility path. Keep its temporary
	// subject bounded independently of the normal RE2 path; the latter still
	// handles larger ordinary values.
	regexp2MaxSubjectBytes = 1 << 20
	// Exact \X expansion is specialized against the current subject. It emits a
	// bounded alternative for each possible rune start, so keep that subject
	// smaller than the generic ICU path.
	regexp2MaxGraphemeSubjectBytes = 64 << 10
	// Each ICU \X expands to a bounded compatibility expression. Keep the
	// translated form bounded as well so a small source pattern cannot expand
	// into an unexpectedly large parser/compiler allocation.
	regexp2MaxTranslatedPatternBytes = 1 << 20
	regexp2MaxEvaluation             = 2 * regexp2MatchTimeout
	regexp2MaxCachedBytes            = 32 << 20
	regexp2MaxActiveSubjectBytes     = 64 << 20
	// regexp2 does not expose runner-stack capacity. Reserve a fixed
	// evaluation allowance and discard the per-evaluation regexp after use so
	// retained runner storage cannot accumulate in the matcher cache. The
	// static code/subject admission below makes this allowance executable: a
	// pattern is rejected before regexp2 runs when its worst-case runner state
	// budget cannot fit in this bound.
	regexp2MaxEvaluationMemoryBytes = 8 << 20
	regexp2MaxEvaluationStates      = 1 << 20
	regexp2MaxTrackCount            = 4096
	regexp2MaxRepeatCount           = 1 << 16
	regexp2MaxNullableRepeatCount   = 1 << 10
	regexp2MaxNullableRepeatStates  = 1 << 16
	regexp2RunnerBytesPerState      = 16
	regexp2NullableBytesPerState    = 128
	regexp2CaptureHistoryBytes      = 64
)

var regexp2ActiveSubjectBytes atomic.Int64

func regexp2InvalidInputf(format string, args ...any) error {
	return moerr.NewInvalidInputNoCtxf(format, args...)
}

func regexp2InternalErrorf(format string, args ...any) error {
	return moerr.NewInternalErrorNoCtxf(format, args...)
}

// regexp2Matcher is deliberately kept separate from *regexp.Regexp. The
// latter remains the hot path and the existing cache/test contracts continue
// to use it. This matcher is created only for patterns containing syntax Go's
// RE2 intentionally does not implement.
type regexp2Matcher struct {
	re                *regexp2.Regexp
	expression        string
	rule              string
	binary            bool
	hasGrapheme       bool
	userGroupToEngine []int
	groupName         map[string]int
	estimatedBytes    int
	evaluationBytes   int64
	releaseBudget     func()
	mu                sync.Mutex
}

func (rs *regexpSet) evictRegexpCacheEntry() {
	if len(rs.mp) > 0 {
		for key := range rs.mp {
			delete(rs.mp, key)
			delete(rs.mayMatchEmpty, key)
			return
		}
	}
	if len(rs.icu) > 0 {
		for key := range rs.icu {
			rs.icuBytes -= rs.icu[key].estimatedBytes
			delete(rs.icu, key)
			return
		}
	}
}

func (m *regexp2Matcher) NumSubexp() int {
	if m == nil || len(m.userGroupToEngine) < 1 {
		return 0
	}
	return len(m.userGroupToEngine) - 1
}

func (m *regexp2Matcher) SubexpIndex(name string) int {
	if m == nil {
		return -1
	}
	if index, ok := m.groupName[name]; ok {
		return index
	}
	return -1
}

// requiresRegexp2Pattern detects the ICU-visible constructs that are absent
// from Go regexp. It is syntax-aware enough not to route escaped text or
// character-class contents through the slower engine.
func requiresRegexp2Pattern(pattern string) bool {
	quoted := false
	class := false
	classFirst := false
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	for i := 0; i < len(pattern); {
		if quoted {
			if pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i += 2
				continue
			}
			_, size := utf8.DecodeRuneInString(pattern[i:])
			if size < 1 {
				size = 1
			}
			i += size
			continue
		}
		if class {
			if classFirst && pattern[i] == '^' {
				classFirst = false
				i++
				continue
			}
			if pattern[i] == '\\' && i+1 < len(pattern) {
				if pattern[i+1] == 'U' || pattern[i+1] == 'c' {
					return true
				}
				classFirst = false
				i += regexpEscapeTokenLen(pattern, i)
				continue
			}
			if pattern[i] == ']' && !classFirst {
				class = false
			}
			classFirst = false
			i++
			continue
		}
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				i++
				continue
			}
			if pattern[i] == '#' {
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				continue
			}
		}
		if pattern[i] == '(' {
			if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
				if scoped {
					freeSpacingStack = append(freeSpacingStack, freeSpacing)
					freeSpacing = enabled
				} else {
					freeSpacing = enabled
				}
				i = end
				continue
			}
			freeSpacingStack = append(freeSpacingStack, freeSpacing)
		}
		if pattern[i] == ')' {
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}

		switch pattern[i] {
		case '\\':
			if i+1 >= len(pattern) {
				i++
				continue
			}
			switch pattern[i+1] {
			case 'Q':
				quoted = true
				i += 2
				continue
			case 'X', 'U', 'c', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'k', 'g':
				return true
			default:
				i += regexpEscapeTokenLen(pattern, i)
				continue
			}
		case '[':
			class = true
			classFirst = true
			i++
			continue
		case '(':
			if i+2 < len(pattern) && pattern[i+1] == '?' {
				switch pattern[i+2] {
				case '=', '!', '>':
					return true
				case '<':
					// (?<= and (?<! are lookbehind; (?<name> is an
					// ICU named capture that Go regexp does not parse.
					return true
				}
			}
		case '*', '+', '?':
			if i+1 < len(pattern) && pattern[i+1] == '+' {
				return true
			}
		case '}':
			if i+1 < len(pattern) && pattern[i+1] == '+' {
				return true
			}
		}
		i++
	}
	return false
}

func regexp2PatternHasGrapheme(pattern string) bool {
	return regexp2GraphemeTokenCount(pattern) > 0
}

func regexp2GraphemeTokenCount(pattern string) int {
	quoted := false
	class := false
	classFirst := false
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	count := 0
	for i := 0; i < len(pattern); {
		if quoted {
			if pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i += 2
				continue
			}
			_, size := utf8.DecodeRuneInString(pattern[i:])
			if size < 1 {
				size = 1
			}
			i += size
			continue
		}
		if class {
			if classFirst && pattern[i] == '^' {
				classFirst = false
				i++
				continue
			}
			if pattern[i] == '\\' && i+1 < len(pattern) {
				classFirst = false
				i += regexpEscapeTokenLen(pattern, i)
				continue
			}
			if pattern[i] == ']' && !classFirst {
				class = false
			}
			classFirst = false
			i++
			continue
		}
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				i++
				continue
			}
			if pattern[i] == '#' {
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				continue
			}
		}
		if pattern[i] == '(' {
			if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
				if scoped {
					freeSpacingStack = append(freeSpacingStack, freeSpacing)
					freeSpacing = enabled
				} else {
					freeSpacing = enabled
				}
				i = end
				continue
			}
			freeSpacingStack = append(freeSpacingStack, freeSpacing)
		}
		if pattern[i] == ')' {
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}
		if pattern[i] == '\\' && i+1 < len(pattern) {
			if pattern[i+1] == 'Q' {
				quoted = true
				i += 2
				continue
			}
			if pattern[i+1] == 'X' {
				count++
			}
			i += regexpEscapeTokenLen(pattern, i)
			continue
		}
		if pattern[i] == '[' {
			class = true
			classFirst = true
		}
		i++
	}
	return count
}

func regexpEscapeTokenLen(pattern string, start int) int {
	if start+1 >= len(pattern) {
		return 1
	}
	if pattern[start+1] == 'p' || pattern[start+1] == 'P' || pattern[start+1] == 'x' {
		if start+2 < len(pattern) && pattern[start+2] == '{' {
			if close := strings.IndexByte(pattern[start+3:], '}'); close >= 0 {
				return close + 4
			}
		}
		if pattern[start+1] == 'x' && start+4 <= len(pattern) &&
			isHexDigit(pattern[start+2]) && isHexDigit(pattern[start+3]) {
			return 4
		}
	}
	if pattern[start+1] == 'u' && start+6 <= len(pattern) {
		valid := true
		for i := start + 2; i < start+6; i++ {
			valid = valid && isHexDigit(pattern[i])
		}
		if valid {
			return 6
		}
	}
	if pattern[start+1] == 'c' && start+2 < len(pattern) {
		return 3
	}
	if pattern[start+1] == 'U' && start+10 <= len(pattern) {
		valid := true
		for i := start + 2; i < start+10; i++ {
			valid = valid && isHexDigit(pattern[i])
		}
		if valid {
			return 10
		}
	}
	if pattern[start+1] >= '0' && pattern[start+1] <= '7' {
		end := start + 2
		for end < len(pattern) && end < start+4 && pattern[end] >= '0' && pattern[end] <= '7' {
			end++
		}
		return end - start
	}
	if pattern[start+1] == 'k' || pattern[start+1] == 'g' {
		if start+2 < len(pattern) &&
			(pattern[start+2] == '<' || pattern[start+2] == '\'' || pattern[start+2] == '{') {
			terminator := byte('>')
			if pattern[start+2] == '\'' {
				terminator = '\''
			} else if pattern[start+2] == '{' {
				terminator = '}'
			}
			if close := strings.IndexByte(pattern[start+3:], terminator); close >= 0 {
				return close + 4
			}
		}
	}
	if pattern[start+1] >= '1' && pattern[start+1] <= '9' {
		i := start + 2
		for i < len(pattern) && pattern[i] >= '0' && pattern[i] <= '9' {
			i++
		}
		return i - start
	}
	return 2
}

func isHexDigit(ch byte) bool {
	return ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F'
}

// regexpCaptureNamesInOrder returns the user-visible capture order. regexp2
// assigns unnamed slots before named slots, while ICU numbers every capturing
// parenthesis from left to right. Keeping this mapping explicit is necessary
// for mixed named/unnamed groups and for numeric backreferences.
func regexpCaptureNamesInOrder(pattern string) []string {
	var names []string
	quoted := false
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	for i := 0; i < len(pattern); {
		if quoted {
			if pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i += 2
				continue
			}
			_, size := utf8.DecodeRuneInString(pattern[i:])
			if size < 1 {
				size = 1
			}
			i += size
			continue
		}
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				i++
				continue
			}
			if pattern[i] == '#' {
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				continue
			}
		}
		if pattern[i] == '\\' {
			if i+1 < len(pattern) && pattern[i+1] == 'Q' {
				quoted = true
			}
			i += regexpEscapeTokenLen(pattern, i)
			continue
		}
		if pattern[i] == '[' {
			i = regexpCharClassEnd(pattern, i)
			continue
		}
		if pattern[i] == ')' {
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}
		if pattern[i] != '(' {
			_, size := utf8.DecodeRuneInString(pattern[i:])
			if size < 1 {
				size = 1
			}
			i += size
			continue
		}

		// Keep the same flag scopes used by regexp2. This is needed for
		// capture numbering: parentheses inside an x-mode comment are text,
		// not captures.
		if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
			if scoped {
				freeSpacingStack = append(freeSpacingStack, freeSpacing)
				freeSpacing = enabled
			} else {
				freeSpacing = enabled
			}
			i = end
			continue
		}
		freeSpacingStack = append(freeSpacingStack, freeSpacing)

		if i+1 >= len(pattern) || pattern[i+1] != '?' {
			names = append(names, "")
			i++
			continue
		}
		if i+3 < len(pattern) && pattern[i+2] == 'P' && pattern[i+3] == '<' {
			if close := strings.IndexByte(pattern[i+4:], '>'); close >= 0 {
				names = append(names, pattern[i+4:i+4+close])
				i += close + 5
				continue
			}
		}
		if i+2 < len(pattern) && pattern[i+2] == '<' {
			if i+3 < len(pattern) && (pattern[i+3] == '=' || pattern[i+3] == '!') {
				i++
				continue
			}
			if close := strings.IndexByte(pattern[i+3:], '>'); close >= 0 {
				names = append(names, pattern[i+3:i+3+close])
				i += close + 4
				continue
			}
		}
		if i+2 < len(pattern) && pattern[i+2] == '\'' {
			if close := strings.IndexByte(pattern[i+3:], '\''); close >= 0 {
				names = append(names, pattern[i+3:i+3+close])
				i += close + 4
				continue
			}
		}
		i++
	}
	return names
}

func isRegexpPatternWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '\f'
}

// regexpInlineFreeSpacing recognizes regexp2's complete inline option header.
// Only the x option affects this scanner, but all cimsx/u/d/e options must be
// consumed together so forms such as (?ix:...) do not make a comment scanner
// lose synchronization. enabled is the x state after applying the header.
func regexpInlineFreeSpacing(pattern string, start int, current bool) (end int, enabled, scoped, ok bool) {
	if start+2 >= len(pattern) || pattern[start] != '(' || pattern[start+1] != '?' {
		return 0, false, false, false
	}
	i := start + 2
	enabled = current
	off := false
	seenOption := false
	for i < len(pattern) {
		switch pattern[i] {
		case ')', ':':
			if !seenOption {
				return 0, false, false, false
			}
			return i + 1, enabled, pattern[i] == ':', true
		case '-':
			off = true
		case '+':
			off = false
		case 'x', 'X':
			enabled = !off
			seenOption = true
		case 'c', 'C', 'd', 'D', 'e', 'E', 'i', 'I', 'm', 'M', 'n', 'N', 's', 'S', 'u', 'U':
			seenOption = true
		default:
			return 0, false, false, false
		}
		i++
	}
	return 0, false, false, false
}

func regexp2CaptureMapping(pattern string, tree *regexp2syntax.RegexTree) ([]int, map[string]int, error) {
	names := regexpCaptureNamesInOrder(pattern)
	if len(tree.Capnames) == 0 {
		userToEngine := make([]int, len(names)+1)
		for i := range userToEngine {
			userToEngine[i] = i
		}
		return userToEngine, make(map[string]int), nil
	}
	allSlots := make([]int, 0, len(tree.Capnames))
	namedSlots := make(map[int]bool)
	for name, slot := range tree.Capnames {
		if slot > 0 {
			allSlots = append(allSlots, slot)
		}
		for _, captureName := range names {
			if captureName != "" && captureName == name {
				namedSlots[slot] = true
			}
		}
	}
	sort.Ints(allSlots)
	unnamedSlots := make([]int, 0, len(allSlots))
	for _, slot := range allSlots {
		if !namedSlots[slot] {
			unnamedSlots = append(unnamedSlots, slot)
		}
	}
	userToEngine := make([]int, len(names)+1)
	userToEngine[0] = 0
	groupName := make(map[string]int)
	unnamedIndex := 0
	for userIndex, name := range names {
		userIndex++
		if name != "" {
			slot, ok := tree.Capnames[name]
			if !ok || slot <= 0 {
				return nil, nil, regexp2InternalErrorf("regexp capture group %q is not addressable", name)
			}
			userToEngine[userIndex] = slot
			groupName[name] = userIndex
			continue
		}
		if unnamedIndex >= len(unnamedSlots) {
			return nil, nil, regexp2InternalErrorf("regexp capture group numbering is inconsistent")
		}
		userToEngine[userIndex] = unnamedSlots[unnamedIndex]
		unnamedIndex++
	}
	return userToEngine, groupName, nil
}

func remapRegexp2Backreferences(expression string, userToEngine []int) string {
	var out strings.Builder
	out.Grow(len(expression))
	quoted := false
	for i := 0; i < len(expression); {
		if quoted {
			if expression[i] == '\\' && i+1 < len(expression) && expression[i+1] == 'E' {
				out.WriteString(expression[i : i+2])
				quoted = false
				i += 2
				continue
			}
			_, size := utf8.DecodeRuneInString(expression[i:])
			if size < 1 {
				size = 1
			}
			out.WriteString(expression[i : i+size])
			i += size
			continue
		}
		if expression[i] == '[' {
			end := regexpCharClassEnd(expression, i)
			out.WriteString(expression[i:end])
			i = end
			continue
		}
		if expression[i] != '\\' || i+1 >= len(expression) {
			_, size := utf8.DecodeRuneInString(expression[i:])
			if size < 1 {
				size = 1
			}
			out.WriteString(expression[i : i+size])
			i += size
			continue
		}
		if expression[i+1] == 'Q' {
			quoted = true
			out.WriteString(expression[i : i+2])
			i += 2
			continue
		}
		if expression[i+1] >= '1' && expression[i+1] <= '9' {
			end := i + 2
			for end < len(expression) && expression[end] >= '0' && expression[end] <= '9' {
				end++
			}
			userGroup, parseErr := strconv.Atoi(expression[i+1 : end])
			if parseErr == nil && userGroup > 0 && userGroup < len(userToEngine) {
				out.WriteByte('\\')
				out.WriteString(strconv.Itoa(userToEngine[userGroup]))
			} else {
				out.WriteString(expression[i:end])
			}
			i = end
			continue
		}
		length := regexpEscapeTokenLen(expression, i)
		if i+length > len(expression) {
			length = len(expression) - i
		}
		out.WriteString(expression[i : i+length])
		i += length
	}
	return out.String()
}

// regexp2GraphemePatternForSubject builds an exact, subject-bounded \\X
// expression. The UAX #29 package is the source of truth for the cluster
// boundaries; the generated alternatives additionally assert both the start
// and end rune positions. This avoids approximating Extended_Pictographic,
// Indic conjunct, RI parity, or Prepend with general Unicode categories.
//
// The expression is intentionally per-evaluation rather than cached. A cached
// regexp must not retain a user subject through regexp2's runner pool, and a
// subject-specific expansion cannot be reused for a different value.
func regexp2GraphemePatternForSubject(subject string) (string, error) {
	if len(subject) > regexp2MaxGraphemeSubjectBytes {
		return "", regexp2InvalidInputf("regular expression subject with \\X exceeds the maximum supported size of %d bytes", regexp2MaxGraphemeSubjectBytes)
	}
	if !utf8.ValidString(subject) {
		return "", regexp2InvalidInputf("regular expression subject with \\X must be valid UTF-8")
	}
	totalRunes := utf8.RuneCountInString(subject)
	runeOffsets := make([]int, totalRunes+1)
	runeIndex := 0
	for byteOffset := range subject {
		runeOffsets[runeIndex] = byteOffset
		runeIndex++
	}
	runeOffsets[totalRunes] = len(subject)
	var out strings.Builder
	out.Grow(min(regexp2MaxTranslatedPatternBytes, len(subject)*16+64))
	if err := appendRegexp2TranslatedPart(&out, "(?>(?:"); err != nil {
		return "", err
	}
	first := true
	clusterStartRune := 0
	iter := graphemes.FromString(subject)
	for iter.Next() {
		cluster := iter.Value()
		clusterEndRune := clusterStartRune + utf8.RuneCountInString(cluster)
		clusterEndByte := iter.End()
		for startRune := clusterStartRune; startRune < clusterEndRune; startRune++ {
			// ICU keeps the full subject's grapheme boundaries while allowing a
			// search to begin inside a cluster. Thus e+acute started at acute
			// matches acute, and the second RI in a three-RI sequence stops at
			// the existing boundary instead of re-pairing with the third RI.
			clusterSuffix := subject[runeOffsets[startRune]:clusterEndByte]
			if !first {
				if err := appendRegexp2TranslatedPart(&out, "|"); err != nil {
					return "", err
				}
			}
			if startRune == 0 {
				if err := appendRegexp2TranslatedPart(&out, `\A`); err != nil {
					return "", err
				}
			} else {
				prefix := fmt.Sprintf(`(?<=\A(?s:.{%d}))`, startRune)
				if err := appendRegexp2TranslatedPart(&out, prefix); err != nil {
					return "", err
				}
			}
			if err := appendRegexp2TranslatedPart(&out, regexp2QuoteLiteral(clusterSuffix, false)); err != nil {
				return "", err
			}
			if clusterEndRune == totalRunes {
				if err := appendRegexp2TranslatedPart(&out, `\z`); err != nil {
					return "", err
				}
			} else {
				suffix := fmt.Sprintf(`(?=(?s:.{%d})\z)`, totalRunes-clusterEndRune)
				if err := appendRegexp2TranslatedPart(&out, suffix); err != nil {
					return "", err
				}
			}
			first = false
		}
		clusterStartRune = clusterEndRune
	}
	if first {
		// A \\X cannot match an empty subject. Keep a valid no-match atom so
		// callers can still compile a pattern containing it.
		if err := appendRegexp2TranslatedPart(&out, `(?s:.)`); err != nil {
			return "", err
		}
	}
	if err := appendRegexp2TranslatedPart(&out, "))"); err != nil {
		return "", err
	}
	return out.String(), nil
}

func appendRegexp2TranslatedPart(out *strings.Builder, value string) error {
	if len(value) > regexp2MaxTranslatedPatternBytes ||
		out.Len() > regexp2MaxTranslatedPatternBytes-len(value) {
		return regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
	}
	out.WriteString(value)
	return nil
}

func regexp2QuoteLiteral(value string, binary bool) string {
	var out strings.Builder
	out.Grow(len(value) + 8)
	if binary {
		out.Grow(len(value) * 6)
		for i := 0; i < len(value); i++ {
			// Keep binary quoted text byte-oriented until the common binary
			// encoder maps these byte escapes to its one-rune alphabet.
			fmt.Fprintf(&out, `\x{%02X}`, value[i])
		}
		return out.String()
	}
	for _, r := range value {
		if r < utf8.RuneSelf {
			out.WriteString(regexp2.Escape(string(r)))
			continue
		}
		// regexp2's Escape emits a variable-width \\u escape. A following
		// hexadecimal literal could therefore be consumed as part of the
		// same code point (for example \\u600A). Braced hex escapes are
		// unambiguous and are supported by the RE2 parser mode.
		fmt.Fprintf(&out, `\x{%04X}`, r)
	}
	return out.String()
}

// translateRegexp2Pattern converts syntax regexp2 does not understand while
// leaving ICU lookaround, backreferences, and atomic groups intact. The
// scanner tracks atom boundaries so possessive quantifiers can be rewritten
// as atomic groups without a fragile textual regexp rewrite.
func translateRegexp2Pattern(pattern string, binary bool) (string, error) {
	return translateRegexp2PatternWithGrapheme(pattern, binary, "")
}

func translateRegexp2PatternWithGrapheme(
	pattern string,
	binary bool,
	graphemePattern string,
) (string, error) {
	if err := validateRegexp2TranslationBudget(pattern, graphemePattern); err != nil {
		return "", err
	}
	var out strings.Builder
	out.Grow(len(pattern) + 32)
	groupStarts := make([]int, 0, 8)
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	lastAtomStart := -1

	for i := 0; i < len(pattern); {
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				out.WriteByte(pattern[i])
				i++
				continue
			}
			if pattern[i] == '#' {
				start := i
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				out.WriteString(pattern[start:i])
				continue
			}
		}
		if pattern[i] == '(' {
			if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
				start := out.Len()
				out.WriteString(pattern[i:end])
				if scoped {
					groupStarts = append(groupStarts, start)
					freeSpacingStack = append(freeSpacingStack, freeSpacing)
					freeSpacing = enabled
					lastAtomStart = start
				} else {
					freeSpacing = enabled
				}
				i = end
				continue
			}
			freeSpacingStack = append(freeSpacingStack, freeSpacing)
		}
		if pattern[i] == ')' {
			start := out.Len()
			out.WriteByte(pattern[i])
			if n := len(groupStarts); n > 0 {
				lastAtomStart = groupStarts[n-1]
				groupStarts = groupStarts[:n-1]
			} else {
				lastAtomStart = start
			}
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}
		if pattern[i] == '\\' {
			start := out.Len()
			if i+1 >= len(pattern) {
				out.WriteByte(pattern[i])
				i++
				lastAtomStart = start
				continue
			}
			if pattern[i+1] == 'Q' {
				end := strings.Index(pattern[i+2:], `\E`)
				literalEnd := len(pattern)
				if end >= 0 {
					literalEnd = i + 2 + end
				}
				hasLiteral := literalEnd > i+2
				out.WriteString(regexp2QuoteLiteral(pattern[i+2:literalEnd], binary))
				if end >= 0 {
					i = literalEnd + 2
				} else {
					i = literalEnd
				}
				if hasLiteral {
					lastAtomStart = start
				}
				continue
			}
			if pattern[i+1] == 'X' {
				if binary {
					out.WriteString(`(?s:.)`)
				} else if graphemePattern != "" {
					if err := appendRegexp2TranslatedPart(&out, graphemePattern); err != nil {
						return "", err
					}
					i += 2
					lastAtomStart = start
					continue
				} else {
					// The base cached matcher only needs a capture-compatible
					// placeholder. A text subject replaces this atom with the
					// exact UAX #29 expansion before evaluation.
					out.WriteString(`(?s:.)`)
				}
				i += 2
				lastAtomStart = start
				continue
			}
			length := regexpEscapeTokenLen(pattern, i)
			if i+length > len(pattern) {
				length = len(pattern) - i
			}
			if pattern[i+1] == 'g' {
				if end, ok := regexpNamedBackreferenceEnd(pattern, i); ok {
					out.WriteString(`\k<`)
					out.WriteString(pattern[i+3 : end-1])
					out.WriteByte('>')
					i = end
					lastAtomStart = start
					continue
				}
			}
			if !binary && pattern[i+1] == 'U' && length == 10 {
				out.WriteString("\\x{")
				out.WriteString(pattern[i+2 : i+10])
				out.WriteByte('}')
				i += length
				lastAtomStart = start
				continue
			}
			out.WriteString(pattern[i : i+length])
			i += length
			lastAtomStart = start
			continue
		}

		if pattern[i] == '[' {
			start := out.Len()
			end := regexpCharClassEnd(pattern, i)
			classPattern, err := translateRegexp2CharClass(pattern, i, end, binary)
			if err != nil {
				return "", err
			}
			if err := appendRegexp2TranslatedPart(&out, classPattern); err != nil {
				return "", err
			}
			i = end
			lastAtomStart = start
			continue
		}

		if pattern[i] == '{' {
			if _, ok := regexpQuantifierEnd(pattern, i); !ok {
				return "", regexp2InvalidInputf("invalid regular expression quantifier")
			}
		}
		if quantifierEnd, ok := regexpQuantifierEnd(pattern, i); ok {
			if err := validateRegexp2QuantifierModifiers(pattern, i, quantifierEnd); err != nil {
				return "", err
			}
			if quantifierEnd < len(pattern) && pattern[quantifierEnd] == '+' && lastAtomStart >= 0 {
				atom := out.String()[lastAtomStart:]
				prefix := out.String()[:lastAtomStart]
				out.Reset()
				out.Grow(len(prefix) + len(atom) + quantifierEnd - i + 4)
				out.WriteString(prefix)
				out.WriteString("(?>")
				out.WriteString(atom)
				out.WriteString(pattern[i:quantifierEnd])
				out.WriteByte(')')
				i = quantifierEnd + 1
				lastAtomStart = len(prefix)
				continue
			}
			out.WriteString(pattern[i:quantifierEnd])
			i = quantifierEnd
			continue
		}

		start := out.Len()
		switch pattern[i] {
		case '(':
			groupStarts = append(groupStarts, start)
			out.WriteByte(pattern[i])
			lastAtomStart = start
		case ')':
			out.WriteByte(pattern[i])
			if n := len(groupStarts); n > 0 {
				lastAtomStart = groupStarts[n-1]
				groupStarts = groupStarts[:n-1]
			} else {
				lastAtomStart = start
			}
		default:
			_, size := utf8.DecodeRuneInString(pattern[i:])
			if size < 1 {
				size = 1
			}
			out.WriteString(pattern[i : i+size])
			lastAtomStart = start
			i += size
			continue
		}
		i++
	}

	return out.String(), nil
}

func translateRegexp2CharClass(pattern string, start, end int, binary bool) (string, error) {
	var out strings.Builder
	out.Grow(end - start)
	for i := start; i < end; {
		if pattern[i] != '\\' || i+1 >= end {
			out.WriteByte(pattern[i])
			i++
			continue
		}
		length := regexpEscapeTokenLen(pattern, i)
		if i+length > end {
			length = end - i
		}
		if !binary && pattern[i+1] == 'U' && length == 10 {
			out.WriteString("\\x{")
			out.WriteString(pattern[i+2 : i+10])
			out.WriteByte('}')
		} else {
			out.WriteString(pattern[i : i+length])
		}
		i += length
	}
	return out.String(), nil
}

func validateRegexp2TranslationBudget(pattern, graphemePattern string) error {
	// Quoted bytes and possessive rewrites expand the source, and binary
	// encoding can add UTF-8 bytes for each mapped byte. Reserve a conservative
	// upper bound before building the expression; in particular, do not create
	// the same subject-specific \X expansion once per source token and only then
	// discover that the result is too large.
	perSourceByte := int64(8)
	upper := int64(len(pattern))*perSourceByte + 64
	if graphemePattern != "" {
		count := int64(regexp2GraphemeTokenCount(pattern))
		expansion := int64(len(graphemePattern)) - 2
		if expansion > 0 && count > (int64(regexp2MaxTranslatedPatternBytes)-upper)/expansion {
			return regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
		}
		upper += count * expansion
	}
	if upper > int64(regexp2MaxTranslatedPatternBytes) {
		return regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
	}
	return nil
}

func regexpNamedBackreferenceEnd(pattern string, start int) (int, bool) {
	if start+3 >= len(pattern) || pattern[start] != '\\' || pattern[start+1] != 'g' {
		return 0, false
	}
	open := pattern[start+2]
	close := byte('>')
	if open == '{' {
		close = '}'
	} else if open == '\'' {
		close = '\''
	} else if open != '<' {
		return 0, false
	}
	end := strings.IndexByte(pattern[start+3:], close)
	if end < 0 {
		return 0, false
	}
	return start + 3 + end + 1, true
}

func regexpCharClassEnd(pattern string, start int) int {
	i := start + 1
	if i < len(pattern) && pattern[i] == '^' {
		i++
	}
	first := true
	for i < len(pattern) {
		if pattern[i] == '\\' {
			i += regexpEscapeTokenLen(pattern, i)
			first = false
			continue
		}
		// POSIX classes contain a closing ] in [[:alpha:]].
		if pattern[i] == '[' && i+1 < len(pattern) &&
			(pattern[i+1] == ':' || pattern[i+1] == '.' || pattern[i+1] == '=') {
			terminator := string([]byte{pattern[i+1], ']'})
			if close := strings.Index(pattern[i+2:], terminator); close >= 0 {
				i += close + 4
			} else {
				return len(pattern)
			}
			first = false
			continue
		}
		if pattern[i] == ']' && !first {
			return i + 1
		}
		first = false
		i++
	}
	return len(pattern)
}

func regexpQuantifierEnd(pattern string, start int) (int, bool) {
	if start >= len(pattern) {
		return start, false
	}
	switch pattern[start] {
	case '*', '+', '?':
		end := start + 1
		if end < len(pattern) && pattern[end] == '?' {
			end++
		}
		return end, true
	case '{':
		close := strings.IndexByte(pattern[start+1:], '}')
		if close < 0 {
			return start, false
		}
		end := start + close + 2
		if end < len(pattern) && pattern[end] == '?' {
			end++
		}
		return end, true
	}
	return start, false
}

func validateRegexp2SourceRepeatBounds(pattern string) error {
	quoted := false
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	for i := 0; i < len(pattern); {
		if quoted {
			if pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i += 2
				continue
			}
			i++
			continue
		}
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				i++
				continue
			}
			if pattern[i] == '#' {
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				continue
			}
		}
		if pattern[i] == '\\' {
			if i+1 < len(pattern) && pattern[i+1] == 'Q' {
				quoted = true
			}
			i += regexpEscapeTokenLen(pattern, i)
			continue
		}
		if pattern[i] == '[' {
			i = regexpCharClassEnd(pattern, i)
			continue
		}
		if pattern[i] == '(' {
			if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
				if scoped {
					freeSpacingStack = append(freeSpacingStack, freeSpacing)
					freeSpacing = enabled
				} else {
					freeSpacing = enabled
				}
				i = end
				continue
			}
			freeSpacingStack = append(freeSpacingStack, freeSpacing)
			i++
			continue
		}
		if pattern[i] == ')' {
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}
		if pattern[i] == '{' {
			end, ok := regexpQuantifierEnd(pattern, i)
			if !ok {
				return regexp2InvalidInputf("invalid regular expression quantifier")
			}
			if err := validateRegexp2QuantifierModifiers(pattern, i, end); err != nil {
				return err
			}
			bodyEnd := regexpQuantifierBodyEnd(pattern, i, end)
			if err := validateRegexp2RepeatBody(pattern[i+1 : bodyEnd-1]); err != nil {
				return err
			}
			i = end
			continue
		}
		i++
	}
	return nil
}

func regexpQuantifierBodyEnd(pattern string, start, end int) int {
	if pattern[start] == '{' && end > start && pattern[end-1] == '?' {
		return end - 1
	}
	return end
}

func validateRegexp2RepeatBody(body string) error {
	comma := strings.IndexByte(body, ',')
	if comma < 0 {
		if body == "" {
			return regexp2InvalidInputf("invalid regular expression quantifier")
		}
		return validateRegexp2RepeatNumber(body)
	}
	if comma == 0 || strings.IndexByte(body[comma+1:], ',') >= 0 {
		return regexp2InvalidInputf("invalid regular expression quantifier")
	}
	if err := validateRegexp2RepeatNumber(body[:comma]); err != nil {
		return err
	}
	if body[comma+1:] == "" {
		return nil
	}
	return validateRegexp2RepeatNumber(body[comma+1:])
}

func validateRegexp2QuantifierModifiers(pattern string, start, end int) error {
	if end >= len(pattern) {
		return nil
	}
	if pattern[end] == '?' {
		return regexp2InvalidInputf("invalid regular expression quantifier")
	}
	if pattern[end] != '+' {
		return nil
	}
	if end > start+1 && pattern[end-1] == '?' ||
		end+1 < len(pattern) && (pattern[end+1] == '*' || pattern[end+1] == '+' || pattern[end+1] == '?') {
		return regexp2InvalidInputf("invalid regular expression quantifier")
	}
	if end+1 < len(pattern) && pattern[end+1] == '{' {
		if _, ok := regexpQuantifierEnd(pattern, end+1); ok {
			return regexp2InvalidInputf("invalid regular expression quantifier")
		}
	}
	return nil
}

func validateRegexp2RepeatNumber(value string) error {
	if value == "" {
		return nil
	}
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return regexp2InvalidInputf("invalid regular expression quantifier")
	}
	if n > regexp2MaxRepeatCount {
		return regexp2InvalidInputf("regular expression repetition exceeds the maximum supported count of %d", regexp2MaxRepeatCount)
	}
	return nil
}

func (rs *regexpSet) getRegexp2MatcherWithMatchType(
	pat, pureMatchType string,
	binary bool,
) (*regexp2Matcher, error) {
	rule := regexpPatternWithPureMatchType(pat, pureMatchType)
	binaryCaseFold := binary && strings.ContainsRune(pureMatchType, 'i')
	key := regexpCacheKey{pattern: rule, binary: binary, binaryCaseFold: binaryCaseFold}
	if rs.icu == nil {
		rs.icu = make(map[regexpCacheKey]*regexp2Matcher, mapSizeForRegexp)
	}
	if matcher, ok := rs.icu[key]; ok {
		return matcher, nil
	}
	if len(rule) > regexp2MaxPatternBytes {
		return nil, regexp2InvalidInputf("regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxPatternBytes)
	}
	if binary {
		if err := validateBinaryRegexpPattern(rule); err != nil {
			return nil, err
		}
	}
	if err := validateRegexp2SourceRepeatBounds(rule); err != nil {
		return nil, err
	}
	rule = strings.Clone(rule)
	key.pattern = rule
	expression, err := translateRegexp2Pattern(rule, binary)
	if err != nil {
		return nil, err
	}
	if len(expression) > regexp2MaxTranslatedPatternBytes {
		return nil, regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
	}
	if binary {
		expression, err = encodeBinaryRegexpPattern(expression, binaryCaseFold)
		if err != nil {
			return nil, err
		}
		if len(expression) > regexp2MaxTranslatedPatternBytes {
			return nil, regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
		}
	}
	estimatedBytes := regexp2EstimatedMatcherBytes(len(rule), len(expression))
	if estimatedBytes > regexp2MaxCachedBytes {
		return nil, regexp2InvalidInputf("compiled regular expression exceeds the maximum supported cache size of %d bytes", regexp2MaxCachedBytes)
	}
	if int64(estimatedBytes) > regexp2MaxEvaluationMemoryBytes {
		return nil, regexp2InvalidInputf("compiled regular expression exceeds the maximum supported evaluation size of %d bytes", regexp2MaxEvaluationMemoryBytes)
	}
	tree, _, release, err := regexp2PrepareExpression(expression)
	if err != nil {
		return nil, err
	}
	defer release()
	userGroupToEngine, groupName, err := regexp2CaptureMapping(pat, tree)
	if err != nil {
		return nil, err
	}
	_, err = compileRegexp2Expression(expression, userGroupToEngine)
	if err != nil {
		return nil, err
	}

	matcher := &regexp2Matcher{
		re:                nil,
		expression:        expression,
		rule:              rule,
		binary:            binary,
		hasGrapheme:       !binary && regexp2PatternHasGrapheme(pat),
		userGroupToEngine: userGroupToEngine,
		groupName:         groupName,
		estimatedBytes:    estimatedBytes,
	}
	for len(rs.mp)+len(rs.icu) >= mapSizeForRegexp ||
		rs.icuBytes+estimatedBytes > regexp2MaxCachedBytes {
		if len(rs.mp)+len(rs.icu) == 0 {
			break
		}
		rs.evictRegexpCacheEntry()
	}
	rs.icu[key] = matcher
	rs.icuBytes += estimatedBytes
	return matcher, nil
}

func regexp2EstimatedMatcherBytes(patternBytes, expressionBytes int) int {
	// regexp2 stores the source, code and character sets. The multiplier is a
	// conservative admission estimate; it is deliberately used for cache
	// admission only, while the independent expression limit bounds transient
	// compiler allocations.
	return 64<<10 + 16*(patternBytes+expressionBytes)
}

func regexp2PrepareExpression(
	expression string,
) (*regexp2syntax.RegexTree, *regexp2syntax.Code, func(), error) {
	compileBudget := int64(64<<10) + int64(len(expression))*8
	if compileBudget > int64(regexp2MaxEvaluationMemoryBytes) {
		return nil, nil, nil, regexp2InvalidInputf("compiled regular expression exceeds the maximum supported evaluation size of %d bytes", regexp2MaxEvaluationMemoryBytes)
	}
	reserved, err := acquireRegexp2SubjectBudget(0, compileBudget)
	if err != nil {
		return nil, nil, nil, err
	}
	release := func() {
		if reserved != 0 {
			regexp2ActiveSubjectBytes.Add(-reserved)
			reserved = 0
		}
	}
	tree, err := regexp2syntax.Parse(expression, regexp2syntax.RE2)
	if err != nil {
		release()
		return nil, nil, nil, err
	}
	code, err := regexp2syntax.Write(tree)
	if err != nil {
		release()
		return nil, nil, nil, err
	}
	if err := validateRegexp2CodeBudget(code); err != nil {
		release()
		return nil, nil, nil, err
	}
	return tree, code, release, nil
}

func validateRegexp2CodeBudget(code *regexp2syntax.Code) error {
	if code == nil {
		return regexp2InternalErrorf("regular expression compiler returned no code")
	}
	if code.TrackCount > regexp2MaxTrackCount {
		return regexp2InvalidInputf("regular expression backtracking state exceeds the maximum supported size")
	}
	for i := 0; i < len(code.Codes); {
		op := regexp2syntax.InstOp(code.Codes[i]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if i+size > len(code.Codes) {
			return regexp2InternalErrorf("regular expression compiler returned truncated code")
		}
		if regexp2OpcodeHasRepeatLimit(op) && code.Codes[i+size-1] != int(regexp2InfiniteRepeat) &&
			code.Codes[i+size-1] > regexp2MaxRepeatCount {
			return regexp2InvalidInputf("regular expression repetition exceeds the maximum supported count of %d", regexp2MaxRepeatCount)
		}
		if op == regexp2syntax.Setcount && code.Codes[i+1] < 1-regexp2MaxRepeatCount {
			return regexp2InvalidInputf("regular expression repetition exceeds the maximum supported count of %d", regexp2MaxRepeatCount)
		}
		i += size
	}
	if _, err := regexp2NullableRepeatStateBudget(code); err != nil {
		return err
	}
	return nil
}

const regexp2InfiniteRepeat = 1<<31 - 1

func regexp2OpcodeSize(op regexp2syntax.InstOp) int {
	switch op & regexp2syntax.Mask {
	case regexp2syntax.Onerep, regexp2syntax.Notonerep, regexp2syntax.Setrep,
		regexp2syntax.Oneloop, regexp2syntax.Notoneloop, regexp2syntax.Setloop,
		regexp2syntax.Onelazy, regexp2syntax.Notonelazy, regexp2syntax.Setlazy,
		regexp2syntax.Capturemark, regexp2syntax.Branchcount, regexp2syntax.Lazybranchcount:
		return 3
	case regexp2syntax.One, regexp2syntax.Notone, regexp2syntax.Multi,
		regexp2syntax.Ref, regexp2syntax.Testref, regexp2syntax.Goto,
		regexp2syntax.Nullcount, regexp2syntax.Setcount, regexp2syntax.Lazybranch,
		regexp2syntax.Branchmark, regexp2syntax.Lazybranchmark, regexp2syntax.Prune,
		regexp2syntax.Set:
		return 2
	default:
		return 1
	}
}

func regexp2OpcodeHasRepeatLimit(op regexp2syntax.InstOp) bool {
	switch op & regexp2syntax.Mask {
	case regexp2syntax.Onerep, regexp2syntax.Notonerep, regexp2syntax.Setrep,
		regexp2syntax.Oneloop, regexp2syntax.Notoneloop, regexp2syntax.Setloop,
		regexp2syntax.Onelazy, regexp2syntax.Notonelazy, regexp2syntax.Setlazy,
		regexp2syntax.Branchcount, regexp2syntax.Lazybranchcount:
		return true
	default:
		return false
	}
}

type regexp2Repeat struct {
	bodyStart      int
	bodyEnd        int
	maximum        int64
	infinite       bool
	nullable       bool
	assertionScope int
}

func regexp2NullableRepeatStateBudget(code *regexp2syntax.Code) (int64, error) {
	return regexp2RepeatStateBudget(code, 0)
}

func regexp2RepeatStateBudget(code *regexp2syntax.Code, subjectRunes int64) (int64, error) {
	if subjectRunes < 0 {
		return 0, regexp2InvalidInputf("invalid regular expression subject size")
	}
	repeats, err := regexp2CollectRepeats(code)
	if err != nil {
		return 0, err
	}
	stateBudget := int64(0)
	for i := range repeats {
		if regexp2RepeatHasParent(repeats, i) {
			continue
		}
		cost, ok := regexp2RepeatCost(repeats, i, subjectRunes)
		if !ok || cost > regexp2MaxNullableRepeatStates-stateBudget {
			return 0, regexp2InvalidInputf("nested nullable repetition exceeds the maximum supported state count of %d", regexp2MaxNullableRepeatStates)
		}
		stateBudget += cost
	}
	if stateBudget == 0 {
		stateBudget = 1
	}
	return stateBudget, nil
}

func regexp2CollectRepeats(code *regexp2syntax.Code) ([]regexp2Repeat, error) {
	repeats := make([]regexp2Repeat, 0, 4)
	for i := 0; i < len(code.Codes); {
		op := regexp2syntax.InstOp(code.Codes[i]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if i+size > len(code.Codes) {
			return nil, regexp2InternalErrorf("regular expression compiler returned truncated code")
		}
		if op == regexp2syntax.Branchcount || op == regexp2syntax.Branchmark ||
			op == regexp2syntax.Lazybranchcount || op == regexp2syntax.Lazybranchmark {
			if i+1 >= len(code.Codes) {
				return nil, regexp2InternalErrorf("regular expression compiler returned an invalid repetition target")
			}
			bodyStart := code.Codes[i+1]
			if bodyStart < 0 {
				return nil, regexp2InternalErrorf("regular expression compiler returned an invalid repetition body")
			}
			if bodyStart < i {
				maximum, infinite := regexp2RepeatedMaximum(code, i, op, bodyStart)
				nullable := !regexp2RepeatBodyAlwaysConsumes(code, bodyStart, i)
				if nullable && (infinite || maximum > regexp2MaxNullableRepeatCount) {
					return nil, regexp2InvalidInputf("regular expression nullable repetition exceeds the maximum supported count of %d", regexp2MaxNullableRepeatCount)
				}
				if infinite || maximum > 0 {
					repeats = append(repeats, regexp2Repeat{
						bodyStart: bodyStart,
						bodyEnd:   i,
						maximum:   maximum,
						infinite:  infinite,
						nullable:  nullable,
					})
				}
			}
		}
		i += size
	}
	scopes := regexp2AssertionScopes(code)
	for i := range repeats {
		repeats[i].assertionScope = regexp2InnermostAssertionScope(scopes, repeats[i].bodyStart, repeats[i].bodyEnd)
	}
	return repeats, nil
}

type regexp2AssertionScope struct {
	start int
	end   int
}

func regexp2AssertionScopes(code *regexp2syntax.Code) []regexp2AssertionScope {
	scopes := make([]regexp2AssertionScope, 0, 2)
	for pos := 0; pos < len(code.Codes); {
		op := regexp2syntax.InstOp(code.Codes[pos]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if pos+size > len(code.Codes) {
			break
		}
		if op == regexp2syntax.Setjump {
			if end, ok := regexp2ZeroWidthAssertionEnd(code, pos, len(code.Codes)); ok {
				scopes = append(scopes, regexp2AssertionScope{start: pos, end: end})
			}
		}
		pos += size
	}
	return scopes
}

func regexp2InnermostAssertionScope(scopes []regexp2AssertionScope, start, end int) int {
	best := -1
	bestWidth := int(^uint(0) >> 1)
	for i, scope := range scopes {
		if scope.start <= start && end <= scope.end && scope.end-scope.start < bestWidth {
			best = i
			bestWidth = scope.end - scope.start
		}
	}
	return best
}

func regexp2RepeatHasParent(repeats []regexp2Repeat, child int) bool {
	for i := range repeats {
		if i == child {
			continue
		}
		if repeats[i].bodyStart <= repeats[child].bodyStart &&
			repeats[child].bodyEnd <= repeats[i].bodyEnd &&
			(repeats[i].bodyStart < repeats[child].bodyStart || repeats[child].bodyEnd < repeats[i].bodyEnd) {
			return true
		}
	}
	return false
}

func regexp2RepeatCost(repeats []regexp2Repeat, parent int, subjectRunes int64) (int64, bool) {
	maximum := regexp2RepeatExecutionMaximum(repeats[parent], subjectRunes)
	if maximum <= 0 {
		return 0, false
	}
	cost := int64(0)
	if repeats[parent].nullable {
		cost = 1
	}
	for i := range repeats {
		if i == parent || !regexp2RepeatDirectChild(repeats, parent, i) {
			continue
		}
		childCost, ok := regexp2RepeatCost(repeats, i, subjectRunes)
		if !ok {
			return 0, false
		}
		if childCost == 0 {
			continue
		}
		if repeats[parent].nullable {
			if childCost > regexp2MaxNullableRepeatStates-cost {
				return 0, false
			}
			cost += childCost
			continue
		}
		if !repeats[i].nullable && repeats[parent].assertionScope == repeats[i].assertionScope {
			if childCost > regexp2MaxNullableRepeatStates-cost {
				return 0, false
			}
			cost += childCost
			continue
		}
		if childCost > regexp2MaxNullableRepeatStates/maximum {
			return 0, false
		}
		childCost *= maximum
		if childCost > regexp2MaxNullableRepeatStates-cost {
			return 0, false
		}
		cost += childCost
	}
	if cost == 0 {
		return 0, true
	}
	if repeats[parent].nullable && cost > regexp2MaxNullableRepeatStates/maximum {
		return 0, false
	}
	if repeats[parent].nullable {
		return maximum * cost, true
	}
	return cost, true
}

func regexp2CaptureHistoryBudget(code *regexp2syntax.Code, subjectRunes int64) (int64, error) {
	if subjectRunes < 0 {
		return 0, regexp2InvalidInputf("invalid regular expression subject size")
	}
	repeats, err := regexp2CollectRepeats(code)
	if err != nil {
		return 0, err
	}
	totalEntries := int64(0)
	for i := range repeats {
		if regexp2RepeatHasParent(repeats, i) {
			continue
		}
		entries, ok := regexp2RepeatCaptureHistoryCost(repeats, code, i, subjectRunes)
		if !ok || entries > regexp2MaxEvaluationMemoryBytes/regexp2CaptureHistoryBytes-totalEntries {
			return 0, regexp2InvalidInputf("regular expression evaluation exceeds the maximum supported memory of %d bytes", regexp2MaxEvaluationMemoryBytes)
		}
		totalEntries += entries
	}
	return totalEntries * regexp2CaptureHistoryBytes, nil
}

func regexp2RepeatCaptureHistoryCost(
	repeats []regexp2Repeat,
	code *regexp2syntax.Code,
	parent int,
	subjectRunes int64,
) (int64, bool) {
	maximum := regexp2RepeatExecutionMaximum(repeats[parent], subjectRunes)
	if maximum <= 0 {
		return 0, false
	}
	captureCount := regexp2RepeatCaptureCount(code, repeats, parent)
	entryLimit := int64(regexp2MaxEvaluationMemoryBytes / regexp2CaptureHistoryBytes)
	if captureCount > entryLimit/maximum {
		return 0, false
	}
	total := captureCount * maximum
	for i := range repeats {
		if i == parent || !regexp2RepeatDirectChild(repeats, parent, i) {
			continue
		}
		childCost, ok := regexp2RepeatCaptureHistoryCost(repeats, code, i, subjectRunes)
		if !ok {
			return 0, false
		}
		if !repeats[parent].nullable && !repeats[i].nullable &&
			repeats[parent].assertionScope == repeats[i].assertionScope {
			if childCost > entryLimit-total {
				return 0, false
			}
		} else {
			if childCost > (entryLimit-total)/maximum {
				return 0, false
			}
			childCost *= maximum
		}
		if childCost > entryLimit-total {
			return 0, false
		}
		total += childCost
	}
	return total, true
}

func regexp2RepeatExecutionMaximum(repeat regexp2Repeat, subjectRunes int64) int64 {
	if repeat.infinite {
		return subjectRunes + 1
	}
	return repeat.maximum
}

func regexp2RepeatCaptureCount(
	code *regexp2syntax.Code,
	repeats []regexp2Repeat,
	parent int,
) int64 {
	start, end := repeats[parent].bodyStart, repeats[parent].bodyEnd
	count := int64(0)
	for pos := start; pos < end; {
		op := regexp2syntax.InstOp(code.Codes[pos]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if pos+size > end || pos+size > len(code.Codes) {
			return count
		}
		nested := false
		for i := range repeats {
			if i == parent {
				continue
			}
			if repeats[parent].bodyStart <= repeats[i].bodyStart &&
				repeats[i].bodyEnd <= repeats[parent].bodyEnd &&
				(repeats[parent].bodyStart < repeats[i].bodyStart || repeats[i].bodyEnd < repeats[parent].bodyEnd) &&
				repeats[i].bodyStart <= pos && pos < repeats[i].bodyEnd {
				nested = true
				break
			}
		}
		if op == regexp2syntax.Capturemark && !nested {
			count++
		}
		pos += size
	}
	return count
}

func regexp2RepeatDirectChild(
	repeats []regexp2Repeat,
	parent, child int,
) bool {
	if repeats[parent].bodyStart > repeats[child].bodyStart ||
		repeats[child].bodyEnd > repeats[parent].bodyEnd || parent == child {
		return false
	}
	for i := range repeats {
		if i == parent || i == child {
			continue
		}
		if repeats[parent].bodyStart <= repeats[i].bodyStart &&
			repeats[i].bodyEnd <= repeats[parent].bodyEnd &&
			repeats[i].bodyStart <= repeats[child].bodyStart &&
			repeats[child].bodyEnd <= repeats[i].bodyEnd &&
			(repeats[i].bodyStart < repeats[child].bodyStart || repeats[child].bodyEnd < repeats[i].bodyEnd) {
			return false
		}
	}
	return true
}

func regexp2RepeatedMaximum(
	code *regexp2syntax.Code,
	branchIndex int,
	op regexp2syntax.InstOp,
	bodyStart int,
) (int64, bool) {
	if op == regexp2syntax.Branchmark || op == regexp2syntax.Lazybranchmark {
		return 0, true
	}
	limit := int64(code.Codes[branchIndex+2])
	if limit == regexp2InfiniteRepeat {
		return 0, true
	}
	minimum := int64(0)
	for i := regexp2PreviousOpcodeIndex(code, bodyStart); i >= 0; {
		previous := regexp2syntax.InstOp(code.Codes[i]) & regexp2syntax.Mask
		if previous == regexp2syntax.Setcount {
			minimum = int64(1 - code.Codes[i+1])
			break
		}
		if previous == regexp2syntax.Nullcount {
			break
		}
		if previous == regexp2syntax.Branchcount || previous == regexp2syntax.Branchmark ||
			previous == regexp2syntax.Lazybranchcount || previous == regexp2syntax.Lazybranchmark {
			break
		}
		i = regexp2PreviousOpcodeIndex(code, i)
	}
	return minimum + limit, false
}

func regexp2PreviousOpcodeIndex(code *regexp2syntax.Code, end int) int {
	previous := -1
	for i := 0; i < end && i < len(code.Codes); {
		previous = i
		size := regexp2OpcodeSize(regexp2syntax.InstOp(code.Codes[i]) & regexp2syntax.Mask)
		if i+size > end {
			break
		}
		i += size
	}
	return previous
}

func regexp2RepeatBodyAlwaysConsumes(code *regexp2syntax.Code, start, end int) bool {
	visited := make(map[int]struct{}, end-start)
	stack := []int{start}
	for len(stack) > 0 {
		pos := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if pos >= end {
			// A zero-cost path reached the loop exit, so the body is
			// nullable. This is the property that makes an unbounded
			// runner stack possible.
			return false
		}
		if pos < start || pos >= len(code.Codes) {
			continue
		}
		if _, ok := visited[pos]; ok {
			continue
		}
		visited[pos] = struct{}{}
		op := regexp2syntax.InstOp(code.Codes[pos]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if pos+size > end || pos+size > len(code.Codes) {
			return false
		}
		stack = append(stack, regexp2ZeroCostSuccessors(code, pos, end, op, size)...)
	}
	return true
}

func regexp2ZeroCostSuccessors(
	code *regexp2syntax.Code,
	pos, end int,
	op regexp2syntax.InstOp,
	size int,
) []int {
	next := pos + size
	toEnd := func(target int) int {
		if target >= end {
			return end
		}
		return target
	}
	switch op {
	case regexp2syntax.One, regexp2syntax.Notone, regexp2syntax.Set,
		regexp2syntax.Onerep, regexp2syntax.Notonerep, regexp2syntax.Setrep:
		if op == regexp2syntax.Onerep || op == regexp2syntax.Notonerep || op == regexp2syntax.Setrep {
			if code.Codes[pos+2] == 0 {
				return []int{next}
			}
		}
		return nil
	case regexp2syntax.Multi:
		if code.Codes[pos+1] >= 0 && code.Codes[pos+1] < len(code.Strings) &&
			len(code.Strings[code.Codes[pos+1]]) == 0 {
			return []int{next}
		}
		return nil
	case regexp2syntax.Oneloop, regexp2syntax.Notoneloop,
		regexp2syntax.Setloop, regexp2syntax.Onelazy,
		regexp2syntax.Notonelazy, regexp2syntax.Setlazy:
		// The operand is a rune or set index, not a jump target. The
		// remaining portion of a primitive loop may match zero runes.
		return []int{next}
	case regexp2syntax.Lazybranch, regexp2syntax.Branchmark,
		regexp2syntax.Lazybranchmark, regexp2syntax.Branchcount,
		regexp2syntax.Lazybranchcount:
		result := []int{next}
		if pos+1 < len(code.Codes) {
			result = append(result, toEnd(code.Codes[pos+1]))
		}
		return result
	case regexp2syntax.Goto:
		if pos+1 < len(code.Codes) {
			return []int{toEnd(code.Codes[pos+1])}
		}
		return []int{end}
	case regexp2syntax.Setjump:
		if target, ok := regexp2ZeroWidthAssertionEnd(code, pos, end); ok {
			return []int{target}
		}
		return []int{next}
	case regexp2syntax.Nothing, regexp2syntax.Stop:
		return nil
	default:
		// Capture, anchors, backreferences, and runner bookkeeping do
		// not prove that input is consumed. Keeping the zero-cost edge
		// is conservative for admission and safe for normal consuming
		// alternatives, which have their own positive-cost opcode.
		return []int{next}
	}
}

func regexp2ZeroWidthAssertionEnd(code *regexp2syntax.Code, start, end int) (int, bool) {
	setjumpSize := regexp2OpcodeSize(regexp2syntax.InstOp(code.Codes[start]) & regexp2syntax.Mask)
	markerPos := start + setjumpSize
	if markerPos >= end || markerPos >= len(code.Codes) ||
		regexp2syntax.InstOp(code.Codes[markerPos])&regexp2syntax.Mask != regexp2syntax.Setmark {
		return 0, false
	}

	nestedSetjumps := 0
	for pos := markerPos + regexp2OpcodeSize(regexp2syntax.Setmark); pos < end; {
		op := regexp2syntax.InstOp(code.Codes[pos]) & regexp2syntax.Mask
		size := regexp2OpcodeSize(op)
		if pos+size > end || pos+size > len(code.Codes) {
			return 0, false
		}
		switch op {
		case regexp2syntax.Setjump:
			nestedSetjumps++
		case regexp2syntax.Forejump:
			if nestedSetjumps > 0 {
				nestedSetjumps--
			}
		case regexp2syntax.Getmark:
			if nestedSetjumps == 0 {
				next := pos + size
				if next < end && next < len(code.Codes) &&
					regexp2syntax.InstOp(code.Codes[next])&regexp2syntax.Mask == regexp2syntax.Forejump {
					return next + regexp2OpcodeSize(regexp2syntax.Forejump), true
				}
			}
		}
		pos += size
	}
	return 0, false
}

func regexp2EvaluationMemoryBudget(
	code *regexp2syntax.Code,
	subjectRunes int,
	compiledBytes int,
) (int64, error) {
	if code == nil || subjectRunes < 0 || compiledBytes < 0 {
		return 0, regexp2InvalidInputf("invalid regular expression evaluation budget")
	}
	trackCount := code.TrackCount
	if trackCount < 1 {
		trackCount = 1
	}
	if trackCount > regexp2MaxTrackCount {
		return 0, regexp2InvalidInputf("regular expression backtracking state exceeds the maximum supported size")
	}
	states := int64(subjectRunes+1) * int64(trackCount)
	if states > regexp2MaxEvaluationStates {
		return 0, regexp2InvalidInputf("regular expression evaluation exceeds the maximum supported backtracking state")
	}
	nullableStates, err := regexp2RepeatStateBudget(code, int64(subjectRunes))
	if err != nil {
		return 0, err
	}
	captureHistoryBytes, err := regexp2CaptureHistoryBudget(code, int64(subjectRunes))
	if err != nil {
		return 0, err
	}
	compiled := int64(compiledBytes)
	if compiled < 64<<10 {
		compiled = 64 << 10
	}
	runnerBytes := states * regexp2RunnerBytesPerState
	nullableRunnerBytes := nullableStates * regexp2NullableBytesPerState
	captureBytes := int64(code.Capsize+1) * 32
	total := compiled + runnerBytes + nullableRunnerBytes + captureHistoryBytes + captureBytes
	if total > regexp2MaxEvaluationMemoryBytes {
		return 0, regexp2InvalidInputf("regular expression evaluation exceeds the maximum supported memory of %d bytes", regexp2MaxEvaluationMemoryBytes)
	}
	return total, nil
}

func regexp2EvaluationBudget(matcher *regexp2Matcher) int64 {
	if matcher != nil && matcher.evaluationBytes > 0 {
		return matcher.evaluationBytes
	}
	budget := int64(regexp2MaxEvaluationMemoryBytes)
	if matcher != nil && int64(matcher.estimatedBytes) > budget {
		budget = int64(matcher.estimatedBytes)
	}
	return budget
}

func compileRegexp2Expression(expression string, userGroupToEngine []int) (*regexp2.Regexp, error) {
	expression = remapRegexp2Backreferences(expression, userGroupToEngine)
	re, err := regexp2.Compile(expression, regexp2.RE2)
	if err != nil {
		return nil, err
	}
	re.MatchTimeout = regexp2MatchTimeout
	return re, nil
}

func (m *regexp2Matcher) forSubject(subject string) (*regexp2Matcher, error) {
	if m == nil {
		return nil, regexp2InternalErrorf("nil regular expression matcher")
	}
	if err := validateRegexp2Subject(subject); err != nil {
		return nil, err
	}
	expression := m.expression
	var err error
	if m.hasGrapheme {
		graphemePattern, err := regexp2GraphemePatternForSubject(subject)
		if err != nil {
			return nil, err
		}
		expression, err = translateRegexp2PatternWithGrapheme(m.rule, m.binary, graphemePattern)
		if err != nil {
			return nil, err
		}
	}
	if len(expression) > regexp2MaxTranslatedPatternBytes {
		return nil, regexp2InvalidInputf("translated regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxTranslatedPatternBytes)
	}
	_, code, release, err := regexp2PrepareExpression(expression)
	if err != nil {
		return nil, err
	}
	compiledBytes := regexp2EstimatedMatcherBytes(len(m.rule), len(expression))
	evaluationBytes, err := regexp2EvaluationMemoryBudget(
		code, utf8.RuneCountInString(subject), compiledBytes)
	if err != nil {
		release()
		return nil, err
	}
	re, err := compileRegexp2Expression(expression, m.userGroupToEngine)
	if err != nil {
		release()
		return nil, err
	}
	return &regexp2Matcher{
		re:                re,
		expression:        expression,
		rule:              m.rule,
		binary:            m.binary,
		hasGrapheme:       false,
		userGroupToEngine: m.userGroupToEngine,
		groupName:         m.groupName,
		estimatedBytes:    compiledBytes,
		evaluationBytes:   evaluationBytes,
		releaseBudget:     release,
	}, nil
}

func (m *regexp2Matcher) release() {
	if m == nil {
		return
	}
	m.mu.Lock()
	re := m.re
	releaseBudget := m.releaseBudget
	m.re = nil
	m.releaseBudget = nil
	m.mu.Unlock()
	if re != nil {
		// The last matching call clears its input. Run one final nil-input
		// search for early-return paths, then let the local regexp and all of
		// its retained runner storage become unreachable together.
		_, _ = re.FindRunesMatch(nil)
	}
	if releaseBudget != nil {
		releaseBudget()
	}
}

func regexp2MatchError(err error) error {
	if err == nil {
		return nil
	}
	// regexp2 includes the complete subject in its timeout error. Do not expose
	// or retain user data in a SQL error; all runtime failures are intentionally
	// collapsed to a bounded diagnostic.
	if strings.Contains(err.Error(), "match timeout") {
		return regexp2InvalidInputf("regexp match timed out")
	}
	return regexp2InternalErrorf("regexp match failed")
}

func validateRegexp2Subject(str string) error {
	if len(str) > regexp2MaxSubjectBytes {
		return regexp2InvalidInputf("regular expression subject exceeds the maximum supported size of %d bytes", regexp2MaxSubjectBytes)
	}
	return nil
}

type regexp2Input struct {
	value       string
	runes       []rune
	byteOffsets []int
	budget      int64
}

func acquireRegexp2SubjectBudget(valueBytes int, evaluationBytes int64) (int64, error) {
	// The runner needs a rune slice and the offset table needs one machine word
	// per rune. Reserve a conservative fixed multiplier before constructing
	// either, and reserve the compiler/runner allowance before compiling the
	// per-evaluation regexp, so concurrent ICU evaluations have a bounded
	// aggregate footprint.
	if evaluationBytes < 0 {
		return 0, regexp2InvalidInputf("invalid regular expression evaluation budget")
	}
	budget := int64(valueBytes)*16 + 64<<10 + evaluationBytes
	if budget > regexp2MaxActiveSubjectBytes {
		return 0, regexp2InvalidInputf("regular expression subject exceeds the maximum active memory budget")
	}
	for {
		active := regexp2ActiveSubjectBytes.Load()
		if active+budget > regexp2MaxActiveSubjectBytes {
			return 0, regexp2InvalidInputf("regular expression evaluation exceeds the maximum active memory budget")
		}
		if regexp2ActiveSubjectBytes.CompareAndSwap(active, active+budget) {
			return budget, nil
		}
	}
}

func newRegexp2Input(value string, binary bool, evaluationBytes int64) (*regexp2Input, error) {
	budget, err := acquireRegexp2SubjectBudget(len(value), evaluationBytes)
	if err != nil {
		return nil, err
	}
	input := &regexp2Input{
		value:  value,
		runes:  []rune(value),
		budget: budget,
	}
	if binary {
		input.byteOffsets = make([]int, len(input.runes)+1)
		for i := range input.runes {
			input.byteOffsets[i] = i
		}
		input.byteOffsets[len(input.runes)] = len(input.runes)
		return input, nil
	}
	input.byteOffsets = make([]int, len(input.runes)+1)
	runeIndex := 0
	for byteOffset := range value {
		input.byteOffsets[runeIndex] = byteOffset
		runeIndex++
	}
	input.byteOffsets[runeIndex] = len(value)
	return input, nil
}

func (input *regexp2Input) release() {
	if input == nil || input.budget == 0 {
		return
	}
	regexp2ActiveSubjectBytes.Add(-input.budget)
	input.value = ""
	input.runes = nil
	input.byteOffsets = nil
	input.budget = 0
}

func (input *regexp2Input) runeIndexAtByte(byteOffset int) int {
	if byteOffset <= 0 {
		return 0
	}
	if byteOffset >= len(input.value) {
		return len(input.runes)
	}
	return utf8.RuneCountInString(input.value[:byteOffset])
}

func (input *regexp2Input) byteOffsetAtRune(runeOffset int) int {
	if runeOffset <= 0 {
		return 0
	}
	if runeOffset >= len(input.byteOffsets) {
		return input.byteOffsets[len(input.byteOffsets)-1]
	}
	return input.byteOffsets[runeOffset]
}

func (m *regexp2Matcher) findAtOrAfter(input *regexp2Input, startRune int) (*regexp2.Match, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		// regexp2's runner cache retains its last input. Clear that reference
		// before returning so a cached matcher cannot pin a large SQL value.
		_, _ = m.re.FindRunesMatch(nil)
	}()
	if startRune <= 0 {
		match, err := m.re.FindRunesMatch(input.runes)
		return match, regexp2MatchError(err)
	}
	if startRune > len(input.runes) {
		return nil, nil
	}
	match, err := m.re.FindRunesMatchStartingAt(input.runes, startRune)
	return match, regexp2MatchError(err)
}

func regexp2MatchIndices(input *regexp2Input, match *regexp2.Match) [2]int {
	if match == nil {
		return [2]int{}
	}
	start := input.byteOffsetAtRune(match.Index)
	end := input.byteOffsetAtRune(match.Index + match.Length)
	return [2]int{start, end}
}

func regexp2SubmatchIndices(matcher *regexp2Matcher, input *regexp2Input, match *regexp2.Match) []int {
	if match == nil {
		return nil
	}
	groupCount := match.GroupCount()
	if matcher != nil && len(matcher.userGroupToEngine) > 0 {
		groupCount = len(matcher.userGroupToEngine)
	}
	indices := make([]int, 0, groupCount*2)
	for userGroup := 0; userGroup < groupCount; userGroup++ {
		engineGroup := userGroup
		if matcher != nil && userGroup < len(matcher.userGroupToEngine) {
			engineGroup = matcher.userGroupToEngine[userGroup]
		}
		value := match.GroupByNumber(engineGroup)
		if value == nil || len(value.Captures) == 0 {
			indices = append(indices, -1, -1)
			continue
		}
		start := input.byteOffsetAtRune(value.Index)
		end := input.byteOffsetAtRune(value.Index + value.Length)
		indices = append(indices, start, end)
	}
	return indices
}

func (rs *regexpSet) regexp2MatchWithMatchType(
	matcher *regexp2Matcher,
	str string,
	subjectIsBinary bool,
	pureMatchType string,
) (bool, error) {
	if err := validateRegexp2Subject(str); err != nil {
		return false, err
	}
	searchSubject, _ := regexpSearchSubjectAndStart(str, 0, subjectIsBinary, pureMatchType)
	evaluationMatcher, err := matcher.forSubject(searchSubject)
	if err != nil {
		return false, err
	}
	input, err := newRegexp2Input(
		searchSubject, subjectIsBinary, regexp2EvaluationBudget(evaluationMatcher))
	if err != nil {
		evaluationMatcher.release()
		return false, err
	}
	defer input.release()
	defer evaluationMatcher.release()
	match, err := evaluationMatcher.findAtOrAfter(input, 0)
	return match != nil, err
}

func (rs *regexpSet) regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
	matcher *regexp2Matcher,
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	limit int64,
	deadline time.Time,
	visit func(start, end int) error,
) (int64, error) {
	if err := validateRegexp2Subject(str); err != nil {
		return 0, err
	}
	searchSubject, searchStart := regexpSearchSubjectAndStart(
		str, startByte, subjectIsBinary, pureMatchType)
	evaluationMatcher, err := matcher.forSubject(searchSubject)
	if err != nil {
		return 0, err
	}
	input, err := newRegexp2Input(
		searchSubject, subjectIsBinary, regexp2EvaluationBudget(evaluationMatcher))
	if err != nil {
		evaluationMatcher.release()
		return 0, err
	}
	defer input.release()
	defer evaluationMatcher.release()
	visited := int64(0)
	nextStart := input.runeIndexAtByte(searchStart)
	for nextStart <= len(input.runes) {
		if time.Now().After(deadline) {
			return visited, regexp2InvalidInputf("regexp match timed out")
		}
		match, err := evaluationMatcher.findAtOrAfter(input, nextStart)
		if err != nil {
			return visited, err
		}
		if match == nil {
			break
		}
		span := regexp2MatchIndices(input, match)
		if err := visit(span[0], span[1]); err != nil {
			return visited, err
		}
		visited++
		if limit > 0 && visited >= limit {
			break
		}
		if match.Length == 0 {
			nextStart = match.Index + 1
		} else {
			nextStart = match.Index + match.Length
		}
	}
	return visited, nil
}

func (rs *regexpSet) regexp2VisitSubmatchesAtOrAfterWithMatchTypeAndDeadline(
	matcher *regexp2Matcher,
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	limit int64,
	deadline time.Time,
	visit func([]int) error,
) (int64, error) {
	if err := validateRegexp2Subject(str); err != nil {
		return 0, err
	}
	searchSubject, searchStart := regexpSearchSubjectAndStart(
		str, startByte, subjectIsBinary, pureMatchType)
	evaluationMatcher, err := matcher.forSubject(searchSubject)
	if err != nil {
		return 0, err
	}
	input, err := newRegexp2Input(
		searchSubject, subjectIsBinary, regexp2EvaluationBudget(evaluationMatcher))
	if err != nil {
		evaluationMatcher.release()
		return 0, err
	}
	defer input.release()
	defer evaluationMatcher.release()
	visited := int64(0)
	nextStart := input.runeIndexAtByte(searchStart)
	for nextStart <= len(input.runes) {
		if time.Now().After(deadline) {
			return visited, regexp2InvalidInputf("regexp match timed out")
		}
		match, err := evaluationMatcher.findAtOrAfter(input, nextStart)
		if err != nil {
			return visited, err
		}
		if match == nil {
			break
		}
		indices := regexp2SubmatchIndices(evaluationMatcher, input, match)
		if len(indices) < 2 {
			return visited, regexp2InternalErrorf("regexp match returned no whole-match span")
		}
		if err := visit(indices); err != nil {
			return visited, err
		}
		visited++
		if limit > 0 && visited >= limit {
			break
		}
		if match.Length == 0 {
			nextStart = match.Index + 1
		} else {
			nextStart = match.Index + match.Length
		}
	}
	return visited, nil
}

func (rs *regexpSet) regexp2NthMatchAtOrAfterWithMatchType(
	matcher *regexp2Matcher,
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	occurrence int64,
) ([2]int, bool, error) {
	return rs.regexp2NthMatchAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence,
		time.Now().Add(regexp2MaxEvaluation))
}

func (rs *regexpSet) regexp2NthMatchAtOrAfterWithMatchTypeAndDeadline(
	matcher *regexp2Matcher,
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	occurrence int64,
	deadline time.Time,
) ([2]int, bool, error) {
	var selected [2]int
	visited, err := rs.regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence,
		deadline,
		func(start, end int) error {
			selected = [2]int{start, end}
			return nil
		})
	if err != nil {
		return [2]int{}, false, err
	}
	return selected, visited >= occurrence, nil
}

func (rs *regexpSet) regexp2NthSubmatchesAtOrAfterWithMatchTypeAndDeadline(
	matcher *regexp2Matcher,
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	occurrence int64,
	deadline time.Time,
) ([]int, bool, error) {
	var selected []int
	visited, err := rs.regexp2VisitSubmatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence,
		deadline,
		func(indices []int) error {
			selected = append(selected[:0], indices...)
			return nil
		})
	if err != nil {
		return nil, false, err
	}
	return selected, visited >= occurrence, nil
}

func (rs *regexpSet) regexp2ReplaceWithMatchType(
	pat, str, replacement string,
	pos, occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
) (string, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return "", err
	}
	if err := validateRegexp2Subject(str); err != nil {
		return "", err
	}
	matcher, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return "", regexpCompileError("regexp_replace", pat, err)
	}
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return "", regexp2InvalidInputf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 0 {
		return "", regexp2InvalidInputf("regexp_replace have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	if len(str) == 0 {
		return str, nil
	}
	if regexpReplacementNeedsExpansion(replacement) {
		return rs.regexp2ReplaceWithTemplate(
			matcher, str, replacement, startByte, occurrence, subjectIsBinary,
			pureMatchType, regexpReplaceMaxResultBytes)
	}
	return rs.regexp2ReplaceLiteralWithLimit(
		matcher, str, replacement, startByte, occurrence, subjectIsBinary,
		pureMatchType, regexpReplaceMaxResultBytes)
}

func (rs *regexpSet) regexp2ReplaceLiteralWithLimit(
	matcher *regexp2Matcher,
	str, replacement string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
) (string, error) {
	return rs.regexp2ReplaceLiteralWithLimitAndDeadline(
		matcher, str, replacement, startByte, occurrence, subjectIsBinary,
		pureMatchType, maxBytes, time.Now().Add(regexp2MaxEvaluation))
}

func (rs *regexpSet) regexp2ReplaceLiteralWithLimitAndDeadline(
	matcher *regexp2Matcher,
	str, replacement string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
	deadline time.Time,
) (string, error) {
	if occurrence > 0 {
		match, found, err := rs.regexp2NthMatchAtOrAfterWithMatchTypeAndDeadline(
			matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence, deadline)
		if err != nil {
			return "", err
		}
		if !found {
			return str, nil
		}
		var size uint64
		for _, part := range []int{match[0], len(replacement), len(str) - match[1]} {
			var addErr error
			size, addErr = regexpAddReplacementSize(size, part, maxBytes)
			if addErr != nil {
				return "", addErr
			}
		}
		var result strings.Builder
		result.Grow(int(size))
		if err := regexpWriteReplacementString(&result, str[:match[0]], maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, replacement, maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, str[match[1]:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	var size uint64
	last := 0
	matched := false
	_, err := rs.regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, 0,
		deadline,
		func(start, end int) error {
			var addErr error
			size, addErr = regexpAddReplacementSize(size, start-last, maxBytes)
			if addErr != nil {
				return addErr
			}
			size, addErr = regexpAddReplacementSize(size, len(replacement), maxBytes)
			if addErr != nil {
				return addErr
			}
			last = end
			matched = true
			return nil
		})
	if err != nil {
		return "", err
	}
	if !matched {
		return str, nil
	}
	var addErr error
	size, addErr = regexpAddReplacementSize(size, len(str)-last, maxBytes)
	if addErr != nil {
		return "", addErr
	}
	var result strings.Builder
	result.Grow(int(size))
	last = 0
	_, err = rs.regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, 0,
		deadline,
		func(start, end int) error {
			if err := regexpWriteReplacementString(&result, str[last:start], maxBytes); err != nil {
				return err
			}
			if err := regexpWriteReplacementString(&result, replacement, maxBytes); err != nil {
				return err
			}
			last = end
			return nil
		})
	if err != nil {
		return "", err
	}
	if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
		return "", err
	}
	if uint64(result.Len()) != size {
		return "", regexp2InternalErrorf("regexp_replace: output size changed during replacement")
	}
	return result.String(), nil
}

func (rs *regexpSet) regexp2ReplaceWithTemplate(
	matcher *regexp2Matcher,
	str, replacement string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
) (string, error) {
	deadline := time.Now().Add(regexp2MaxEvaluation)
	if occurrence > 0 {
		indices, found, err := rs.regexp2NthSubmatchesAtOrAfterWithMatchTypeAndDeadline(
			matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence, deadline)
		if err != nil {
			return "", err
		}
		if !found {
			return str, nil
		}
		template, err := parseRegexpReplacementTemplate(replacement, matcher)
		if err != nil {
			return "", err
		}
		if !template.hasGroups {
			return rs.regexp2ReplaceLiteralWithLimitAndDeadline(
				matcher, str, regexpReplacementLiteral(template), startByte,
				occurrence, subjectIsBinary, pureMatchType, maxBytes, deadline)
		}
		size, err := regexpAddReplacementSize(0, indices[0], maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddTemplateSize(size, str, indices, template, maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddReplacementSize(size, len(str)-indices[1], maxBytes)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.Grow(int(size))
		if err := regexpWriteReplacementString(&result, str[:indices[0]], maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteTemplate(&result, str, indices, template, maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, str[indices[1]:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	upperBound, bounded := regexpReplaceOutputUpperBound(len(str), len(replacement))
	if bounded && upperBound <= uint64(maxBytes/2) {
		var result strings.Builder
		last := 0
		parsed := false
		var template regexpReplacementTemplate
		_, err := rs.regexp2VisitSubmatchesAtOrAfterWithMatchTypeAndDeadline(
			matcher, str, startByte, subjectIsBinary, pureMatchType, 0,
			deadline,
			func(indices []int) error {
				if !parsed {
					var parseErr error
					template, parseErr = parseRegexpReplacementTemplate(replacement, matcher)
					if parseErr != nil {
						return parseErr
					}
					result.Grow(min(len(str), 64<<10))
					parsed = true
				}
				if err := regexpWriteReplacementString(&result, str[last:indices[0]], maxBytes); err != nil {
					return err
				}
				if err := regexpWriteTemplate(&result, str, indices, template, maxBytes); err != nil {
					return err
				}
				last = indices[1]
				return nil
			})
		if err != nil {
			return "", err
		}
		if !parsed {
			return str, nil
		}
		if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	var template regexpReplacementTemplate
	var size uint64
	last := 0
	parsed := false
	found := false
	_, err := rs.regexp2VisitSubmatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, 0,
		deadline,
		func(indices []int) error {
			if !parsed {
				var parseErr error
				template, parseErr = parseRegexpReplacementTemplate(replacement, matcher)
				if parseErr != nil {
					return parseErr
				}
				parsed = true
			}
			var addErr error
			size, addErr = regexpAddReplacementSize(size, indices[0]-last, maxBytes)
			if addErr != nil {
				return addErr
			}
			size, addErr = regexpAddTemplateSize(size, str, indices, template, maxBytes)
			if addErr != nil {
				return addErr
			}
			last = indices[1]
			found = true
			return nil
		})
	if err != nil {
		return "", err
	}
	if !found {
		return str, nil
	}
	var addErr error
	size, addErr = regexpAddReplacementSize(size, len(str)-last, maxBytes)
	if addErr != nil {
		return "", addErr
	}
	var result strings.Builder
	result.Grow(int(size))
	last = 0
	_, err = rs.regexp2VisitSubmatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, str, startByte, subjectIsBinary, pureMatchType, 0,
		deadline,
		func(indices []int) error {
			if err := regexpWriteReplacementString(&result, str[last:indices[0]], maxBytes); err != nil {
				return err
			}
			if err := regexpWriteTemplate(&result, str, indices, template, maxBytes); err != nil {
				return err
			}
			last = indices[1]
			return nil
		})
	if err != nil {
		return "", err
	}
	if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
		return "", err
	}
	if uint64(result.Len()) != size {
		return "", regexp2InternalErrorf("regexp_replace: output size changed during replacement")
	}
	return result.String(), nil
}

func (rs *regexpSet) regexp2SubstrWithMatchType(
	pat, str string,
	pos, occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
) (bool, string, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return false, "", err
	}
	matcher, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return false, "", regexpCompileError("regexp_substr", pat, err)
	}
	if err := validateRegexp2Subject(str); err != nil {
		return false, "", err
	}
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return false, "", regexp2InvalidInputf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 1 {
		return false, "", regexp2InvalidInputf("regexp_substr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	match, found, err := rs.regexp2NthMatchAtOrAfterWithMatchType(
		matcher, str, startByte, subjectIsBinary, pureMatchType, occurrence)
	if err != nil {
		return false, "", err
	}
	if !found {
		return false, "", nil
	}
	return true, str[match[0]:match[1]], nil
}

func (rs *regexpSet) regexp2InstrWithMatchType(
	pat, str string,
	pos, occurrence int64,
	retOption int8,
	subjectIsBinary bool,
	pureMatchType string,
) (int64, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return 0, err
	}
	matcher, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return 0, regexpCompileError("regexp_instr", pat, err)
	}
	if err := validateRegexp2Subject(str); err != nil {
		return 0, err
	}
	startByte, ok := 0, pos >= 1 && len(str) == 0
	if len(str) != 0 {
		startByte, ok = regexpSearchStartByte(str, pos, subjectIsBinary)
	}
	if !ok {
		return 0, regexp2InvalidInputf("regexp_instr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 1 {
		return 0, regexp2InvalidInputf("regexp_instr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	if retOption < 0 || retOption > 1 {
		return 0, regexp2InvalidInputf("regexp_instr have Index out of bounds in regular expression search, return option %d", retOption)
	}
	searchSubject := str[startByte:]
	match, found, err := rs.regexp2NthMatchAtOrAfterWithMatchType(
		matcher, searchSubject, 0, subjectIsBinary, pureMatchType, occurrence)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return regexpSuffixByteOffsetToPosition(searchSubject, pos, match[retOption], subjectIsBinary), nil
}
