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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexp2PatternRouting(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		want    bool
	}{
		{pattern: `a(?=b)`, want: true},
		{pattern: `(?<!c)b`, want: true},
		{pattern: `(a)\1`, want: true},
		{pattern: `(?>a)`, want: true},
		{pattern: `a++`, want: true},
		{pattern: `a{2}+`, want: true},
		{pattern: `\X`, want: true},
		{pattern: `[?=]`, want: false},
		{pattern: `\Q(?=)\E`, want: false},
		{pattern: `\+\+`, want: false},
		{pattern: `a+`, want: false},
	} {
		require.Equal(t, tc.want, requiresRegexp2Pattern(tc.pattern), tc.pattern)
	}
}

func TestRegexp2PatternCompatibilityFunctions(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	for _, tc := range []struct {
		name    string
		pattern string
		subject string
		want    bool
	}{
		{name: "positive lookahead", pattern: `a(?=b)`, subject: "ab", want: true},
		{name: "negative lookahead", pattern: `a(?!c)`, subject: "ab", want: true},
		{name: "positive lookbehind", pattern: `(?<=a)b`, subject: "ab", want: true},
		{name: "negative lookbehind", pattern: `(?<!c)b`, subject: "ab", want: true},
		{name: "consuming alternation with lookahead", pattern: `^(ab|cd)+(?=e)`, subject: "abcde", want: true},
		{name: "primitive loop in consuming group", pattern: `^(a*b)+(?=c)`, subject: "abbcde", want: true},
		{name: "pattern backreference", pattern: `(a)\1`, subject: "aa", want: true},
		{name: "named backreference", pattern: `(?<left>a)\k<left>`, subject: "aa", want: true},
		{name: "atomic group", pattern: `(?>a|ab)c`, subject: "abc", want: false},
		{name: "possessive quantifier", pattern: `a++a`, subject: "aaa", want: false},
		{name: "bounded possessive quantifier", pattern: `a{2,3}+a`, subject: "aaa", want: false},
		{name: "combining grapheme", pattern: `^\X$`, subject: "e\u0301", want: true},
		{name: "grapheme after preceding atom", pattern: `.\X`, subject: "e\u0301", want: true},
		{name: "emoji zwj grapheme", pattern: `^\X$`, subject: "👩‍💻", want: true},
		{name: "regional-indicator grapheme", pattern: `^\X$`, subject: "🇺🇸", want: true},
		{name: "two graphemes", pattern: `^\X\X$`, subject: "e\u0301x", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rs.regularMatchWithMode(tc.pattern, tc.subject, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegexp2PatternFunctionsPreserveEntryPointSemantics(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	matched, got, err := rs.regularSubstrWithMatchType(`(?<=ab)c`, "abc", 3, 1, false, "")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "c", got)

	index, err := rs.regularInstrWithMatchType(`(?<=a)b`, "ab", 1, 1, 0, false, "")
	require.NoError(t, err)
	require.Equal(t, int64(2), index)
	index, err = rs.regularInstrWithMatchType(`(?<=a)b`, "ab", 2, 1, 0, false, "")
	require.NoError(t, err)
	require.Equal(t, int64(0), index, "INSTR searches a rebased suffix")

	got, err = rs.regularReplaceWithMatchType(`(a)\1`, "aa", "<$0>", 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, "<aa>", got)
	got, err = rs.regularReplaceWithMatchType(`(?<left>a)(b)\1`, "aba", "<$1:$2:${left}>", 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, "<a:b:a>", got)
	got, err = rs.regularReplaceWithMatchType(`a++`, "aaa", "X", 1, 0, false, "")
	require.NoError(t, err)
	require.Equal(t, "X", got)
	got, err = rs.regularReplaceWithMatchType(`\Qa.b\E(?=c)`, "a.bc", "X", 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, "Xc", got)
	matched, err = rs.regularMatchWithMode(`^(.*)\Qb\E++\1$`, "abbab", false)
	require.NoError(t, err)
	require.True(t, matched, "a non-empty quoted atom must own its following possessive quantifier")
	matched, err = rs.regularMatchWithMode(`^a\Q\E++$`, "aaa", false)
	require.NoError(t, err)
	require.True(t, matched, "an empty quoted literal preserves ICU's preceding-atom possessive binding")
	got, err = rs.regularReplaceWithMatchType("(?ix)(?<left>a) # (ghost)\n(b)\\2", "abb", "<$1:$2:${left}>", 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, "<a:b:a>", got)
	matched, err = rs.regularMatchWithMode(`\x61++`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`\u0061++`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^\U00000061++$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^\U00000061++$`, "ab", false)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = rs.regularMatchWithMode(`^\U00000061+$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^\cA++$`, "\x01\x01", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^\cA++$`, "\x01a", false)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = rs.regularMatchWithMode(`^\cA+$`, "\x01\x01", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^[\U00000061]++$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^[\U00000061]++$`, "UU", false)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = rs.regularMatchWithMode(`^[\c]]++$`, "\x1d\x1d", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^[\c]]++$`, "]]", false)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = rs.regularMatchWithMode(`^[[:alpha:]]+$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^[[:alpha:]]++$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^a*?$`, "aaa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^a{2,3}?$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^a{2,3}+$`, "aa", false)
	require.NoError(t, err)
	require.True(t, matched)
	got, err = rs.regularReplaceWithMatchType(`^([[:alpha:]]++)$`, "aa", "<$1>", 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, "<aa>", got)
	for _, pattern := range []string{`a*?+`, `a+?+`, `a??+`, `a*++`, `a*+?`, `a++*`, `a*+{2}`, `a{2}?+`, `a{2,3}?+`, `a{2,}?+`, `a{1,2}+*`, `a{,}+`, `a{1,2,3}+`, `a{2,65537}?+`, `a{2,65537}?`, `a{q}++`, `a{}++`, `a{1x}++`} {
		_, err = rs.regularMatchWithMode(pattern, "aaa", false)
		require.Error(t, err, pattern)
	}
	matched, got, err = rs.regularSubstrWithMatchType(`\X`, "e\u0301", 2, 1, false, "")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\u0301", got)
	matched, got, err = rs.regularSubstrWithMatchType(`\X`, "🇦🇧🇨", 2, 1, false, "")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "🇧", got)
}

func TestRegexp2CaptureMappingPreservesICUNumbering(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	for _, tc := range []struct {
		name        string
		pattern     string
		subject     string
		replacement string
		want        string
	}{
		{
			name:        "named before unnamed",
			pattern:     `(?<left>a)(b)\1`,
			subject:     "aba",
			replacement: `<$1:$2:${left}>`,
			want:        "<a:b:a>",
		},
		{
			name:        "nested unnamed before named",
			pattern:     `((a)a)(?<tail>b)\1`,
			subject:     "aabaa",
			replacement: `<$1:$2:${tail}>`,
			want:        "<aa:a:b>",
		},
		{
			name:        "free spacing comment does not create a group",
			pattern:     "(?x)(?<left>a) # (ghost)\n(b)\\2",
			subject:     "abb",
			replacement: "<$1:$2:${left}>",
			want:        "<a:b:a>",
		},
		{
			name:        "closing bracket at class start does not create a group",
			pattern:     `(?<n>a)[](](b)\2`,
			subject:     "a(bb",
			replacement: "<$1:$2:${n}>",
			want:        "<a:b:a>",
		},
		{
			name:        "escaped class element consumes the first position",
			pattern:     `(?<n>a)[\(](b)\2`,
			subject:     "a(bb",
			replacement: "<$1:$2:${n}>",
			want:        "<a:b:a>",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rs.regularReplaceWithMatchType(
				tc.pattern, tc.subject, tc.replacement, 1, 1, false, "")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegexp2BinaryAndPositionSemantics(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	binarySubject := string([]byte{0xff, 'a'})

	matched, err := rs.regularMatchWithMode(`\x{FF}(?=a)`, binarySubject, true)
	require.NoError(t, err)
	require.True(t, matched)

	matched, err = rs.regularLikeWithMode(`\x{E9}(?=a)`, string([]byte{0xc9, 'a'}), "i", true)
	require.NoError(t, err)
	require.True(t, matched)

	got, err := rs.regularReplaceWithMatchType(
		`(\x{FF})(?=a)`, binarySubject, "<$1>", 1, 0, true, "")
	require.NoError(t, err)
	require.Equal(t, append([]byte{'<'}, 0xff, '>', 'a'), []byte(got))

	index, err := rs.regularInstrWithMatchType(`a(?=b)`, "za", 2, 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, int64(0), index)
	index, err = rs.regularInstrWithMatchType(`(?<=a)b`, "ab", 1, 1, 1, false, "")
	require.NoError(t, err)
	require.Equal(t, int64(3), index)

	matched, got, err = rs.regularSubstrWithMatchType(`(?<=a)b`, "ab", 1, 1, false, "")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "b", got)
}

func TestRegexp2GraphemeCompatibilityCases(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	for _, tc := range []struct {
		name    string
		subject string
		want    bool
	}{
		{name: "crlf", subject: "\r\n", want: true},
		{name: "emoji modifier", subject: "👍🏼", want: true},
		{name: "keycap", subject: "1️⃣", want: true},
		{name: "hangul jamo", subject: "각", want: true},
		{name: "indic conjunct", subject: "क्ष", want: true},
		{name: "prepend", subject: "\u0600A", want: true},
		{name: "non pictographic zwj", subject: "a‍b", want: false},
		{name: "symbol is not an extender", subject: "a^", want: false},
		{name: "two clusters", subject: "👩‍💻x", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rs.regularMatchWithMode(`^\X$`, tc.subject, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegexp2CacheSharesTheExistingBound(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	for i := 0; i < mapSizeForRegexp; i++ {
		_, err := rs.getRegularMatcherWithMode("ordinary", false)
		require.NoError(t, err)
		_, err = rs.getRegexp2MatcherWithMatchType(fmt.Sprintf("(a)\\1(?:){%d}", i), "", false)
		require.NoError(t, err)
	}
	require.LessOrEqual(t, len(rs.mp)+len(rs.icu), mapSizeForRegexp)
	require.LessOrEqual(t, rs.icuBytes, regexp2MaxCachedBytes)
}

func TestRegexp2TranslatedPatternAdmissionLimit(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	pattern := `\X\X`
	matcher, err := rs.getRegexp2MatcherWithMatchType(pattern, "", false)
	require.NoError(t, err)
	_, err = matcher.forSubject(strings.Repeat("a", regexp2MaxGraphemeSubjectBytes))
	require.Error(t, err)
	require.Contains(t, err.Error(), "translated regular expression pattern exceeds")
}

func TestRegexp2EvaluationAdmissionRejectsUnboundedRunnerGrowth(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	subject := strings.Repeat("a", 128<<10) + "b"
	_, err := rs.regularMatchWithMode(`^(a)+(?=b)`, subject, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "regular expression evaluation exceeds")
	_, err = rs.regularMatchWithMode(`(a)\1{65537}`, "aa", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "repetition exceeds")
	_, err = rs.regularMatchWithMode(`(a)\1{2,65537}`, "aa", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "repetition exceeds")
	_, err = rs.regularMatchWithMode(`(?=a)(){65536}`, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nullable repetition")
	_, err = rs.regularMatchWithMode(`(?=a)*`, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nullable repetition")
	_, err = rs.regularMatchWithMode(`(?=a)(){65536}?`, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nullable repetition")
	_, err = rs.regularMatchWithMode(`(?=a)((){1024}){1024}`, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested nullable repetition")
	_, err = rs.regularMatchWithMode(`(?=a)(a(){1024}a)+`, strings.Repeat("a", 128), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested nullable repetition")
	captureHeavyPattern := `(?=a)` + strings.Repeat("(", 256) + strings.Repeat(")", 256) + `{1024}`
	_, err = rs.regularMatchWithMode(captureHeavyPattern, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "regular expression evaluation exceeds")
	captureNestedPattern := `(?=a)(` + strings.Repeat("(", 128) + strings.Repeat(")", 128) + `{64}){128}`
	_, err = rs.regularMatchWithMode(captureNestedPattern, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "regular expression evaluation exceeds")
	_, err = rs.regularMatchWithMode(`^(?:(?=(a)+)a)+$`, strings.Repeat("a", 1200), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "regular expression evaluation exceeds")
	_, err = rs.regularMatchWithMode(`(?=a)((()){256}){256}`, "a", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nested nullable repetition")
	matched, err := rs.regularMatchWithMode(`(?>a(?=b))+`, "ab", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`(?=a)((){256}(){256}){2}`, "a", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`(?=a)((){16}){128}`, "a", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`^(a(a(){2}a)+a)+(?=b)`, strings.Repeat("a", 1000)+"b", false)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = rs.regularMatchWithMode(`(?=a)(){1024}(){1024}`, "a", false)
	require.NoError(t, err)
	require.True(t, matched)
}

func TestRegexp2BinaryRejectsUnicodeCodePointEscapes(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	_, err := rs.regularMatchWithMode(`\uE080(?=.)`, string([]byte{0x80, 'a'}), true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unicode code-point escapes")
	_, err = rs.regularMatchWithMode(`^\U00000061++$`, "aa", true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unicode code-point escapes")
}

func TestRegexp2BinaryQuotedBytesRemainByteOriented(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	pattern := "\\Q" + string([]byte{0xc3, 0xa9}) + "\\E(?=a)"
	subject := string([]byte{0xc3, 0xa9, 'a'})
	matched, err := rs.regularMatchWithMode(pattern, subject, true)
	require.NoError(t, err)
	require.True(t, matched)

	pattern = "\\Q" + string([]byte{0xff, 0x00}) + "\\E(?=a)"
	subject = string([]byte{0xff, 0x00, 'a'})
	matched, err = rs.regularMatchWithMode(pattern, subject, true)
	require.NoError(t, err)
	require.True(t, matched)
}

func TestRegexp2MalformedCharacterClassesTerminate(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	for _, pattern := range []string{`(?=a)[[:`, `(?=a)[[.`, `(?=a)[[=`} {
		_, err := rs.regularMatchWithMode(pattern, "a", false)
		require.Error(t, err, pattern)
	}
}
