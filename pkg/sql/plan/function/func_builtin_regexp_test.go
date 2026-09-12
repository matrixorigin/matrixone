// Copyright 2022 Matrix Origin
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
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func Test_BuiltIn_RegularInstr(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat       string
		str       string
		pos       int64
		ocr       int64
		retOption int8
		expected  int64
	}{
		{pat: "at", str: "Cat", pos: 1, ocr: 1, retOption: 0, expected: 2},
		{pat: "^at", str: "at", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "Cat", str: "Cat Cat", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "Cat", str: "Cat Cat", pos: 3, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 6, ocr: 1, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 0, expected: 1},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 2, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 3, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 1, retOption: 0, expected: 5},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 2, retOption: 0, expected: 16},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 2, ocr: 3, retOption: 0, expected: 0},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 1, retOption: 1, expected: 4},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 2, retOption: 1, expected: 8},
		{pat: "C.t", str: "Cat City is SO Cute!", pos: 1, ocr: 3, retOption: 1, expected: 19},
	}

	for i, c := range cs {
		v, err := op.regMap.regularInstr(c.pat, c.str, c.pos, c.ocr, c.retOption)
		require.NoError(t, err)
		require.Equal(t, c.expected, v, i)
	}

	_, err := op.regMap.regularInstr("at", "Cat", 100, 1, 0)
	require.True(t, err != nil)
}

func Test_BuiltIn_RegexpMultibytePositions(t *testing.T) {
	op := newOpBuiltInRegexp()
	const subject = "甲😀乙😀丙"

	for _, tc := range []struct {
		pos        int64
		occurrence int64
		retOption  int8
		want       int64
	}{
		{pos: 1, occurrence: 1, retOption: 0, want: 2},
		{pos: 1, occurrence: 1, retOption: 1, want: 3},
		{pos: 1, occurrence: 2, retOption: 0, want: 4},
		{pos: 1, occurrence: 2, retOption: 1, want: 5},
		{pos: 3, occurrence: 1, retOption: 0, want: 4},
	} {
		got, err := op.regMap.regularInstr("😀", subject, tc.pos, tc.occurrence, tc.retOption)
		require.NoError(t, err, tc)
		require.Equal(t, tc.want, got, tc)
	}

	match, got, err := op.regMap.regularSubstr("😀", subject, 3, 1)
	require.NoError(t, err)
	require.True(t, match)
	require.Equal(t, "😀", got)

	got, err = op.regMap.regularReplace("😀", subject, "X", 3, 1)
	require.NoError(t, err)
	require.Equal(t, "甲😀乙X丙", got)
	got, err = op.regMap.regularReplace("😀", subject, "X", 3, 0)
	require.NoError(t, err)
	require.Equal(t, "甲😀乙X丙", got)

	for _, pos := range []int64{0, 6} {
		_, err = op.regMap.regularInstr("😀", subject, pos, 1, 0)
		require.Error(t, err, pos)
		_, _, err = op.regMap.regularSubstr("😀", subject, pos, 1)
		require.Error(t, err, pos)
		_, err = op.regMap.regularReplace("😀", subject, "X", pos, 1)
		require.Error(t, err, pos)
	}
}

func Test_BuiltIn_RegexpReplaceStartsAtRequestedPosition(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		name        string
		pattern     string
		subject     string
		replacement string
		position    int64
		occurrence  int64
		want        string
	}{
		{name: "overlap_first", pattern: "aa", subject: "aaa", replacement: "X", position: 2, occurrence: 1, want: "aX"},
		{name: "overlap_all", pattern: "aa", subject: "aaa", replacement: "X", position: 2, occurrence: 0, want: "aX"},
		{name: "begin_anchor_keeps_original_context", pattern: "^", subject: "abc", replacement: "X", position: 2, occurrence: 0, want: "abc"},
		{name: "anchor_alternative_does_not_steal_overlap", pattern: "aaa|^|aa", subject: "aaa", replacement: "X", position: 2, occurrence: 1, want: "aX"},
		{name: "multiline_anchor_keeps_previous_newline", pattern: "(?m)^b", subject: "a\nb", replacement: "X", position: 3, occurrence: 1, want: "a\nX"},
		{name: "zero_width_match_abutting_discarded_match", pattern: "(?m)a|$", subject: "a\nb", replacement: "X", position: 2, occurrence: 1, want: "aX\nb"},
		{name: "zero_width_after_nonempty_match", pattern: "b*", subject: "ab", replacement: "X", position: 2, occurrence: 0, want: "aXX"},
		{name: "zero_width_all_from_start", pattern: "a*", subject: "abc", replacement: "X", position: 1, occurrence: 0, want: "XXbXcX"},
		{name: "zero_width_all_after_position", pattern: "a*", subject: "abc", replacement: "X", position: 2, occurrence: 0, want: "aXbXcX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularReplace(tc.pattern, tc.subject, tc.replacement, tc.position, tc.occurrence)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	got, err := op.regMap.regularReplace("^$", "", "X", 1, 0)
	require.NoError(t, err)
	require.Empty(t, got, "MySQL leaves an empty REGEXP_REPLACE subject unchanged")
}

func Test_BuiltIn_RegexpPositiveOccurrenceReturnsOnlyRequestedMatch(t *testing.T) {
	op := newOpBuiltInRegexp()
	const subject = "aaaaaaaaaaaaaaaa"

	for _, binary := range []bool{false, true} {
		reg, err := op.regMap.getRegularMatcherWithMode("a", binary)
		require.NoError(t, err)
		match, found, err := op.regMap.regexpNthMatchAtOrAfter(reg, "a", subject, 12, binary, 1)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, [2]int{12, 13}, match,
			"only the requested match is retained, binary=%v", binary)
	}

	got, err := op.regMap.regularReplace("a", subject, "X", 13, 0)
	require.NoError(t, err)
	require.Equal(t, "aaaaaaaaaaaaXXXX", got)
}

func Test_BuiltIn_RegexpStartAwareIteratorMatchesFreshSearchForContextFreePatterns(t *testing.T) {
	op := newOpBuiltInRegexp()
	const subject = "aba中a"
	for _, pattern := range []string{"a", "a+", ".", "中|b", "[ab]+"} {
		reg, err := op.regMap.getRegularMatcher(pattern)
		require.NoError(t, err)
		for _, start := range []int{0, 1, 2, 3, 6} {
			for _, limit := range []int{1, 2, -1} {
				expected := reg.FindAllStringIndex(subject[start:], limit)
				for i := range expected {
					expected[i][0] += start
					expected[i][1] += start
				}
				actual := make([][]int, 0, len(expected))
				visitLimit := int64(limit)
				if limit < 0 {
					visitLimit = 0
				}
				err := op.regMap.regexpVisitAtOrAfter(
					reg, pattern, subject, start, false, visitLimit,
					func(matchStart, matchEnd int) {
						actual = append(actual, []int{matchStart, matchEnd})
					})
				require.NoError(t, err)
				require.Len(t, actual, len(expected),
					"pattern=%q start=%d limit=%d", pattern, start, limit)
				for i := range expected {
					require.Equal(t, expected[i], actual[i],
						"pattern=%q start=%d limit=%d match=%d", pattern, start, limit, i)
				}
			}
		}
	}
}

func Test_BuiltIn_RegexpZeroWidthIterationUsesMySQLSequence(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		pattern    string
		subject    string
		position   int64
		occurrence int64
		want       int64
	}{
		{pattern: "a*", subject: "abc", position: 1, occurrence: 2, want: 2},
		{pattern: "b*", subject: "abc", position: 2, occurrence: 2, want: 3},
	} {
		got, err := op.regMap.regularInstr(tc.pattern, tc.subject, tc.position, tc.occurrence, 0)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "%+v", tc)
	}
}

func Test_BuiltIn_RegexpEmptyMatchMetadataIsBoundedWithMatcherCache(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		pattern string
		want    bool
	}{
		{pattern: "a+", want: false},
		{pattern: "a*", want: true},
		{pattern: "^a", want: false},
		{pattern: `\b`, want: true},
		{pattern: "a|$", want: true},
	} {
		_, got, err := op.regMap.getRegularMatcherInfoWithMode(tc.pattern, false)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, tc.pattern)
	}
	for i := 0; i < mapSizeForRegexp*2; i++ {
		_, _, err := op.regMap.getRegularMatcherInfoWithMode(fmt.Sprintf("x%d", i), false)
		require.NoError(t, err)
	}
	require.LessOrEqual(t, len(op.regMap.mp), mapSizeForRegexp)
	require.Equal(t, len(op.regMap.mp), len(op.regMap.mayMatchEmpty))
}

func Test_BuiltIn_RegexpBinaryPositions(t *testing.T) {
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			subjectType := types.New(oid, 6, 0)
			patternType := types.New(oid, 3, 0)

			instr := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(subjectType, []string{"中中"}, nil),
					NewFunctionTestInput(patternType, []string{"中"}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, []int64{4}, nil),
				newOpBuiltInRegexp().builtInRegexpInstr)
			ok, info := instr.Run()
			require.True(t, ok, info)

			instrEnd := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(subjectType, []string{"中中"}, nil),
					NewFunctionTestInput(patternType, []string{"中"}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
					NewFunctionTestInput(types.T_int8.ToType(), []int8{1}, nil),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, []int64{7}, nil),
				newOpBuiltInRegexp().builtInRegexpInstr)
			ok, info = instrEnd.Run()
			require.True(t, ok, info)

			substr := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(subjectType, []string{"中中"}, nil),
					NewFunctionTestInput(patternType, []string{"中"}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
				},
				NewFunctionTestResult(types.T_varbinary.ToType(), false, []string{"中"}, nil),
				newOpBuiltInRegexp().builtInRegexpSubstr)
			ok, info = substr.Run()
			require.True(t, ok, info)

			replace := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(subjectType, []string{"中中"}, nil),
					NewFunctionTestInput(patternType, []string{"中"}, nil),
					NewFunctionTestInput(types.T_varbinary.ToType(), []string{"X"}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				},
				NewFunctionTestResult(types.T_varbinary.ToType(), false, []string{"中X"}, nil),
				newOpBuiltInRegexp().builtInRegexpReplace)
			ok, info = replace.Run()
			require.True(t, ok, info)
		})
	}
}

func Test_BuiltIn_RegexpBinaryMatcherStartsAtEveryByte(t *testing.T) {
	op := newOpBuiltInRegexp()
	subject := string([]byte{0xe4, 0xb8, 0xad, 0xff})

	for pos, want := range [][]byte{{0xe4}, {0xb8}, {0xad}, {0xff}} {
		start, err := op.regMap.regularInstrWithMode(".", subject, int64(pos+1), 1, 0, true)
		require.NoError(t, err)
		require.Equal(t, int64(pos+1), start, "position %d", pos+1)
		end, err := op.regMap.regularInstrWithMode(".", subject, int64(pos+1), 1, 1, true)
		require.NoError(t, err)
		require.Equal(t, int64(pos+2), end, "position %d", pos+1)

		matched, got, err := op.regMap.regularSubstrWithMode(".", subject, int64(pos+1), 1, true)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, want, []byte(got), "position %d", pos+1)
	}

	for pos := int64(1); pos <= int64(len("中")); pos++ {
		start, err := op.regMap.regularInstrWithMode("^", "中", pos, 1, 0, true)
		require.NoError(t, err)
		require.Equal(t, pos, start, "zero-width position %d", pos)
		end, err := op.regMap.regularInstrWithMode(".*", "中", pos, 1, 1, true)
		require.NoError(t, err)
		require.Equal(t, int64(len("中")+1), end, "match-all position %d", pos)
	}

	got, err := op.regMap.regularReplaceWithMode(".*", "中中", "X", 2, 1, true)
	require.NoError(t, err)
	require.Equal(t, []byte{0xe4, 'X'}, []byte(got))

	got, err = op.regMap.regularReplaceWithMode(".", string([]byte{0xe4}), "中", 1, 0, true)
	require.NoError(t, err)
	require.Equal(t, "中", got)

	for pos := int64(2); pos <= 3; pos++ {
		matched, got, err := op.regMap.regularSubstrWithMode("^|.", "中", pos, 1, true)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, []byte{[]byte("中")[pos-1]}, []byte(got), "position %d", pos)
	}

	invalid := string([]byte{0xff, 0xfe})
	for pos, want := range [][]byte{{0xff}, {0xfe}} {
		start, err := op.regMap.regularInstrWithMode(".", invalid, int64(pos+1), 1, 0, true)
		require.NoError(t, err)
		require.Equal(t, int64(pos+1), start)

		matched, got, err := op.regMap.regularSubstrWithMode(".", invalid, int64(pos+1), 1, true)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, want, []byte(got))
	}
}

func Test_BuiltIn_RegexpBinaryPatternByteEscapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	for value := 0x80; value <= 0xff; value++ {
		got, err := op.regMap.regularInstrWithMode(
			fmt.Sprintf(`\x%02X`, value), string([]byte{byte(value)}), 1, 1, 0, true)
		require.NoError(t, err, "byte 0x%02X", value)
		require.Equal(t, int64(1), got, "byte 0x%02X", value)
	}
	for _, tc := range []struct {
		name    string
		pattern string
		subject string
		want    int64
	}{
		{name: "raw byte", pattern: string([]byte{0xff}), subject: string([]byte{0xff}), want: 1},
		{name: "hex byte", pattern: `\xFF`, subject: string([]byte{0xff}), want: 1},
		{name: "braced hex byte", pattern: `\x{FF}`, subject: string([]byte{0xff}), want: 1},
		{name: "octal byte", pattern: `\377`, subject: string([]byte{0xff}), want: 1},
		{name: "byte range", pattern: `[\x80-\xFF]`, subject: string([]byte{0x7f, 0x80}), want: 2},
		{name: "negated byte range", pattern: `[^\x00-\xFE]`, subject: string([]byte{0xfe, 0xff}), want: 2},
		{name: "ascii hex", pattern: `\x41`, subject: "A", want: 1},
		{name: "quoted escape text", pattern: `\Q\xFF\E`, subject: `\xFF`, want: 1},
		{name: "escaped slash", pattern: `\\xFF`, subject: `\xFF`, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularInstrWithMode(tc.pattern, tc.subject, 1, 1, 0, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	matched, got, err := op.regMap.regularSubstrWithMode(`\xFF`, string([]byte{0xfe, 0xff}), 1, 1, true)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, []byte{0xff}, []byte(got))

	got, err = op.regMap.regularReplaceWithMode(`[\x80-\xFF]`, string([]byte{0x7f, 0x80, 0xff}), "X", 1, 0, true)
	require.NoError(t, err)
	require.Equal(t, []byte{0x7f, 'X', 'X'}, []byte(got))
}

func Test_BuiltIn_RegexpBinaryPatternCannotAddressInternalAlphabet(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, pattern := range []string{`\x{100}`, `\x{E080}`, `[\x{E080}]`, `\p{Co}`, `\P{ASCII}`} {
		t.Run(pattern, func(t *testing.T) {
			_, err := op.regMap.regularInstrWithMode(
				pattern, string([]byte{0x80}), 1, 1, 0, true)
			require.Error(t, err)
		})
	}

	// A raw Unicode code point remains its UTF-8 byte sequence in byte mode; it
	// must never alias the single private-use rune representing byte 0x80.
	position, err := op.regMap.regularInstrWithMode(
		"\ue080", string([]byte{0x80}), 1, 1, 0, true)
	require.NoError(t, err)
	require.Zero(t, position)
	position, err = op.regMap.regularInstrWithMode("\ue080", "\ue080", 1, 1, 0, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), position)

	// Quoting makes the property-looking bytes literal, so it remains valid.
	position, err = op.regMap.regularInstrWithMode(
		`\Q\p{Co}\E`, `\p{Co}`, 1, 1, 0, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), position)

	// Unicode regexp behavior remains available for nonbinary text.
	position, err = op.regMap.regularInstr(`\p{Co}`, "\ue080", 1, 1, 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), position)
}

func Test_BuiltIn_RegexpPositionAnchorPolicies(t *testing.T) {
	op := newOpBuiltInRegexp()

	// SUBSTR and REPLACE retain the complete matcher subject, so a nonzero
	// search position must not create a new beginning-of-subject anchor.
	matched, _, err := op.regMap.regularSubstrWithMode("^", "abc", 2, 1, false)
	require.NoError(t, err)
	require.False(t, matched)

	for _, tc := range []struct {
		pattern string
		value   string
		pos     int64
		want    string
		matched bool
	}{
		{pattern: "$", value: "abc", pos: 2, want: "", matched: true},
		{pattern: "(?m)^b", value: "a\nb", pos: 3, want: "b", matched: true},
		{pattern: `\bb`, value: "ab b", pos: 2, want: "b", matched: true},
	} {
		matched, got, err := op.regMap.regularSubstrWithMode(tc.pattern, tc.value, tc.pos, 1, false)
		require.NoError(t, err)
		require.Equal(t, tc.matched, matched, tc.pattern)
		require.Equal(t, tc.want, got, tc.pattern)
	}

	// INSTR follows MySQL's distinct suffix-subject contract. Anchors and word
	// boundaries at pos > 1 are relative to that suffix, and the reported
	// position is translated back to the original subject.
	for _, tc := range []struct {
		name      string
		pattern   string
		value     string
		pos       int64
		binary    bool
		wantStart int64
		wantEnd   int64
	}{
		{name: "text beginning", pattern: "^", value: "abc", pos: 2, wantStart: 2, wantEnd: 2},
		{name: "multiline beginning", pattern: "(?m)^b", value: "abc", pos: 2, wantStart: 2, wantEnd: 3},
		{name: "word boundary", pattern: `\bb`, value: "ab", pos: 2, wantStart: 2, wantEnd: 3},
		{name: "end", pattern: "$", value: "abc", pos: 2, wantStart: 4, wantEnd: 4},
		{name: "binary byte beginning", pattern: "^.", value: "中", pos: 2, binary: true, wantStart: 2, wantEnd: 3},
	} {
		t.Run("instr_"+tc.name, func(t *testing.T) {
			start, err := op.regMap.regularInstrWithMode(
				tc.pattern, tc.value, tc.pos, 1, 0, tc.binary)
			require.NoError(t, err)
			require.Equal(t, tc.wantStart, start)
			end, err := op.regMap.regularInstrWithMode(
				tc.pattern, tc.value, tc.pos, 1, 1, tc.binary)
			require.NoError(t, err)
			require.Equal(t, tc.wantEnd, end)
		})
	}
}

func Test_BuiltIn_RegexpUsesRowStringDomainAndSurvivesRebind(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInRegexp()
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"中中", "中中"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"中", "中"}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 2}, nil),
	}

	instr := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{4, 2}, nil),
		op.builtInRegexpInstr)
	require.NoError(t, instr.parameters[0].SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, proc.Mp()))
	ok, info := instr.Run()
	require.True(t, ok, info)

	substr := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"中", "中"}, nil),
		op.builtInRegexpSubstr)
	require.NoError(t, substr.parameters[0].SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, proc.Mp()))
	ok, info = substr.Run()
	require.True(t, ok, info)
	require.True(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(0))
	require.False(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(1))

	// Reuse one operator and result wrapper as prepared execution does. Resetting
	// the parameter domain must change semantics and clear the old result domain.
	substr.parameters[0].SetIsBinaryString(false)
	substr.expected.wanted = []string{"中", "中"}
	ok, info = substr.Run()
	require.True(t, ok, info)
	require.False(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(0))
	require.False(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(1))

	substr.parameters[0].SetIsBinaryString(true)
	ok, info = substr.Run()
	require.True(t, ok, info)
	require.True(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(0))

	replace := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"中中", "中中"}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"中", "中"}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"X", "X"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 2}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"中X", "中X"}, nil),
		op.builtInRegexpReplace)
	require.NoError(t, replace.parameters[0].SetRuntimeStringDomainAtWithMP(0, types.RuntimeStringBinary, proc.Mp()))
	ok, info = replace.Run()
	require.True(t, ok, info)
	require.True(t, replace.GetResultVectorDirectly().GetIsBinaryStringAt(0))
	require.False(t, replace.GetResultVectorDirectly().GetIsBinaryStringAt(1))
}

func Test_BuiltIn_RegexpUsesMatchOperandDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	varchar := types.T_varchar.ToType()
	setFirstRowBinary := func(t *testing.T, testCase *FunctionTestCase, parameter int) {
		t.Helper()
		require.NoError(t, testCase.parameters[parameter].SetRuntimeStringDomainAtWithMP(
			0, types.RuntimeStringBinary, proc.Mp()))
	}

	for _, tc := range []struct {
		name string
		fn   fEvalFn
	}{
		{name: "regexp", fn: newOpBuiltInRegexp().builtInRegMatch},
		{name: "not_regexp", fn: newOpBuiltInRegexp().builtInNotRegMatch},
		{name: "regexp_like", fn: newOpBuiltInRegexp().builtInRegexpLike},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []bool{true, false}
			if tc.name == "not_regexp" {
				want = []bool{false, true}
			}
			testCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(varchar, []string{"中", "中"}, nil),
					NewFunctionTestInput(varchar, []string{"..", ".."}, nil),
				},
				NewFunctionTestResult(types.T_bool.ToType(), false, want, nil), tc.fn)
			setFirstRowBinary(t, &testCase, 1)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}

	instr := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(varchar, []string{"中中", "中中"}, nil),
			NewFunctionTestInput(varchar, []string{"中", "中"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 2}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{4, 2}, nil),
		newOpBuiltInRegexp().builtInRegexpInstr)
	setFirstRowBinary(t, &instr, 1)
	ok, info := instr.Run()
	require.True(t, ok, info)

	substr := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(varchar, []string{"中中", "中中"}, nil),
			NewFunctionTestInput(varchar, []string{"中", "中"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 2}, nil),
		},
		NewFunctionTestResult(varchar, false, []string{"中", "中"}, nil),
		newOpBuiltInRegexp().builtInRegexpSubstr)
	setFirstRowBinary(t, &substr, 1)
	ok, info = substr.Run()
	require.True(t, ok, info)
	require.True(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(0))
	require.False(t, substr.GetResultVectorDirectly().GetIsBinaryStringAt(1))

	for _, tc := range []struct {
		name            string
		binaryParameter int
		want            []string
		wantBinary      bool
	}{
		{name: "subject selects binary matching", binaryParameter: 0, want: []string{"\xff\xff\xff", "X"}, wantBinary: true},
		{name: "pattern selects binary matching", binaryParameter: 1, want: []string{"\xff\xff\xff", "X"}, wantBinary: true},
	} {
		t.Run("regexp_replace_"+tc.name, func(t *testing.T) {
			replace := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(varchar, []string{"中", "中"}, nil),
					NewFunctionTestInput(varchar, []string{".", "."}, nil),
					NewFunctionTestInput(varchar, []string{"\xff", "X"}, nil),
				},
				NewFunctionTestResult(varchar, false, tc.want, nil),
				newOpBuiltInRegexp().builtInRegexpReplace)
			setFirstRowBinary(t, &replace, tc.binaryParameter)
			setFirstRowBinary(t, &replace, 2)
			ok, info := replace.Run()
			require.True(t, ok, info)
			require.Equal(t, tc.wantBinary, replace.GetResultVectorDirectly().GetIsBinaryStringAt(0))
			require.False(t, replace.GetResultVectorDirectly().GetIsBinaryStringAt(1))
		})
	}

	for _, tc := range []struct {
		name          string
		replacement   string
		want          string
		controlInputs []FunctionTestInput
	}{
		{name: "three arguments", replacement: "\xff", want: "ÿ"},
		{
			name: "four arguments", replacement: "\x80", want: "€",
			controlInputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			},
		},
		{
			name: "five arguments", replacement: "中", want: "ä¸\u00ad",
			controlInputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
			},
		},
	} {
		t.Run("regexp_replace_binary_replacement_does_not_select_domain_"+tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(varchar, []string{"中"}, nil),
				NewFunctionTestInput(varchar, []string{"."}, nil),
				NewFunctionTestInput(varchar, []string{tc.replacement}, nil),
			}
			inputs = append(inputs, tc.controlInputs...)
			replace := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(varchar, false, []string{tc.want}, nil),
				newOpBuiltInRegexp().builtInRegexpReplace)
			setFirstRowBinary(t, &replace, 2)
			ok, info := replace.Run()
			require.True(t, ok, info)
			require.False(t, replace.GetResultVectorDirectly().GetIsBinaryStringAt(0))
		})
	}

	// A binary replacement must not make the text pattern pass through the
	// binary-regexp validator. This Unicode escape is legal in text mode and
	// intentionally rejected in byte mode.
	replaceWithUnicodePattern := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(varchar, []string{"Ā"}, nil),
			NewFunctionTestInput(varchar, []string{`\x{100}`}, nil),
			NewFunctionTestInput(varchar, []string{"X"}, nil),
		},
		NewFunctionTestResult(varchar, false, []string{"X"}, nil),
		newOpBuiltInRegexp().builtInRegexpReplace)
	setFirstRowBinary(t, &replaceWithUnicodePattern, 2)
	ok, info = replaceWithUnicodePattern.Run()
	require.True(t, ok, info)
	require.False(t, replaceWithUnicodePattern.GetResultVectorDirectly().GetIsBinaryStringAt(0))
}

func TestRegexpBinaryBytesToText(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replacement string
		want        string
	}{
		{name: "ASCII is unchanged", replacement: "AZ", want: "AZ"},
		{name: "latin1 upper byte", replacement: "\xff", want: "ÿ"},
		{name: "windows 1252 printable byte", replacement: "\x80", want: "€"},
		{name: "windows 1252 undefined byte", replacement: "\x81", want: "\u0081"},
		{name: "ASCII suffix after high byte", replacement: "\xffA", want: "ÿA"},
		{name: "utf8 looking bytes are decoded independently", replacement: "中", want: "ä¸\u00ad"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, regexpBinaryBytesToText(tc.replacement))
		})
	}

	windows1252Bytes := make([]byte, 0x20)
	for i := range windows1252Bytes {
		windows1252Bytes[i] = byte(0x80 + i)
	}
	require.Equal(t,
		"€\u0081‚ƒ„…†‡ˆ‰Š‹Œ\u008dŽ\u008f\u0090‘’“”•–—˜™š›œ\u009džŸ",
		regexpBinaryBytesToText(string(windows1252Bytes)))

	latin1Bytes := make([]byte, 0x60)
	latin1Runes := make([]rune, 0, len(latin1Bytes))
	for i := range latin1Bytes {
		latin1Bytes[i] = byte(0xa0 + i)
		latin1Runes = append(latin1Runes, rune(0xa0+i))
	}
	require.Equal(t, string(latin1Runes),
		regexpBinaryBytesToText(string(latin1Bytes)))

	proc := testutil.NewProcess(t)
	constantBinary, err := vector.NewConstBytes(types.T_blob.ToType(), []byte{0xff}, 2, proc.Mp())
	require.NoError(t, err)
	defer constantBinary.Free(proc.Mp())
	converter := newRegexpReplacementDomainConverter(constantBinary)
	require.True(t, converter.mayBeBinary)
	require.True(t, converter.constant)
	require.Equal(t, "\xff", converter.forMatchDomain("\xff", 0, true))
	require.False(t, converter.constantTextConverted,
		"a binary match must not pay for text conversion")
	require.Equal(t, "ÿ", converter.forMatchDomain("\xff", 0, false))
	require.True(t, converter.constantTextConverted)
	require.Equal(t, "ÿ", converter.forMatchDomain("\xff", 1, false),
		"a constant replacement must reuse its converted value across rows")

	constantText, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("中"), 1, proc.Mp())
	require.NoError(t, err)
	defer constantText.Free(proc.Mp())
	textConverter := newRegexpReplacementDomainConverter(constantText)
	require.False(t, textConverter.mayBeBinary)
	require.Equal(t, "中", textConverter.forMatchDomain("中", 0, false))
	require.False(t, textConverter.constantTextConverted)
}

func Test_BuiltIn_RegexpHonorsSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	maskedSecond := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	for _, tc := range []struct {
		name     string
		inputs   []FunctionTestInput
		expected FunctionTestResult
		fn       fEvalFn
	}{
		{
			name: "instr",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "["}, nil),
			},
			expected: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0}, []bool{false, true}),
			fn:       newOpBuiltInRegexp().builtInRegexpInstr,
		},
		{
			name: "substr",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "["}, nil),
			},
			expected: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a", ""}, []bool{false, true}),
			fn:       newOpBuiltInRegexp().builtInRegexpSubstr,
		},
		{
			name: "replace",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "["}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"X", "X"}, nil),
			},
			expected: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"X", ""}, []bool{false, true}),
			fn:       newOpBuiltInRegexp().builtInRegexpReplace,
		},
		{
			name: "regexp_operator",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "["}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false}, []bool{false, true}),
			fn:       newOpBuiltInRegexp().builtInRegMatch,
		},
		{
			name: "regexp_like_match_type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a", "a"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "invalid"}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false}, []bool{false, true}),
			fn:       newOpBuiltInRegexp().builtInRegexpLike,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ft := NewFunctionTestCase(proc, tc.inputs, tc.expected, tc.fn).WithSelectList(maskedSecond)
			ok, info := ft.Run()
			require.True(t, ok, info)
		})
	}
}

func TestRegexpFunctionsPreserveBinaryOverloadDomain(t *testing.T) {
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		subject := types.New(oid, 32, 0)
		for _, tc := range []struct {
			name string
			args []types.Type
		}{
			{name: "ord", args: []types.Type{subject}},
			{name: "regexp_instr", args: []types.Type{subject, subject}},
			{name: "regexp_substr", args: []types.Type{subject, subject}},
			{name: "regexp_replace", args: []types.Type{subject, subject, subject}},
		} {
			resolved, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			_, needsCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, needsCast, "%s(%s)", tc.name, oid)
			if tc.name == "regexp_substr" || tc.name == "regexp_replace" {
				require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(resolved.GetReturnType()))
			}
		}
	}
}

func TestRegexpFunctionsRejectStaticMixedStringDomains(t *testing.T) {
	ctx := context.Background()
	text := types.T_varchar.ToType()
	binary := types.T_varbinary.ToType()
	fixedBinary := types.T_binary.ToType()
	blob := types.T_blob.ToType()
	any := types.T_any.ToType()
	int64Type := types.T_int64.ToType()
	int8Type := types.T_int8.ToType()

	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{name: "reg_match_subject", args: []types.Type{binary, text}},
		{name: "not_reg_match_pattern", args: []types.Type{text, binary}},
		{name: "regexp_like_subject", args: []types.Type{binary, text, text}},
		{name: "regexp_instr_pattern", args: []types.Type{text, binary, int64Type, int64Type, int8Type}},
		{name: "regexp_substr_subject", args: []types.Type{binary, text, int64Type, int64Type}},
		{name: "regexp_replace_subject", args: []types.Type{binary, text, text, int64Type, int64Type}},
		{name: "regexp_replace_pattern", args: []types.Type{text, binary, text}},
		{name: "regexp_replace_replacement", args: []types.Type{text, text, binary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			functionName := strings.TrimSuffix(tc.name, "_subject")
			functionName = strings.TrimSuffix(functionName, "_pattern")
			functionName = strings.TrimSuffix(functionName, "_replacement")
			_, err := GetFunctionByName(ctx, functionName, tc.args)
			require.Error(t, err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ER_CHARACTER_SET_MISMATCH), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
		})
	}

	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{name: "reg_match", args: []types.Type{binary, binary}},
		{name: "regexp_like", args: []types.Type{any, text}},
		{name: "regexp_instr", args: []types.Type{text, any}},
		{name: "regexp_substr", args: []types.Type{any, binary}},
		{name: "regexp_replace", args: []types.Type{binary, any, binary}},
		{name: "regexp_replace", args: []types.Type{any, text, any}},
		{name: "regexp_instr", args: []types.Type{fixedBinary, text}},
		{name: "regexp_instr", args: []types.Type{blob, text}},
		{name: "regexp_replace", args: []types.Type{text, text, blob}},
	} {
		t.Run("accepted_"+tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(ctx, tc.name, tc.args)
			require.NoError(t, err)
		})
	}
}

func TestRegexpFunctionsHonorStringDomainCheckModes(t *testing.T) {
	ctx := context.Background()
	text := types.T_text.ToType()
	binary := types.T_varbinary.ToType()

	resolved, err := GetFunctionByNameWithStringDomainCheckModes(
		ctx, "regexp_substr", []types.Type{text, binary},
		[]StringDomainCheckMode{StringDomainCheckDeferred, StringDomainCheckKnown})
	require.NoError(t, err)
	_, needsCast := resolved.ShouldDoImplicitTypeCast()
	require.False(t, needsCast)
	require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(resolved.GetReturnType()),
		"a fixed binary operand makes binary the only legal result domain")

	_, err = GetFunctionByNameWithStringDomainCheckModes(
		ctx, "regexp_replace", []types.Type{text, text, binary},
		[]StringDomainCheckMode{StringDomainCheckDeferred, StringDomainCheckKnown, StringDomainCheckKnown})
	require.Error(t, err, "known pattern and replacement domains must remain compatible")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch))

	_, err = GetFunctionByNameWithStringDomainCheckModes(
		ctx, "regexp_replace", []types.Type{text, binary, text},
		[]StringDomainCheckMode{StringDomainCheckDeferred, StringDomainCheckDeferred, StringDomainCheckDeferred})
	require.NoError(t, err)

	_, err = GetFunctionByNameWithStringDomainCheckModes(
		ctx, "regexp_instr", []types.Type{text, binary},
		[]StringDomainCheckMode{StringDomainCheckDeferred})
	require.Error(t, err, "a partial mask would silently assign ownership to the wrong argument")

	_, err = GetFunctionByNameWithStringDomainCheckModes(
		ctx, "regexp_instr", []types.Type{text, binary},
		[]StringDomainCheckMode{StringDomainCheckKnown, StringDomainCheckKnown})
	require.Error(t, err, "an execute-time resolved mask must validate every concrete domain")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch))

	for _, tc := range []struct {
		name      string
		args      []types.Type
		modes     []StringDomainCheckMode
		wantError bool
	}{
		{
			name: "binary marker does not trigger against fixed text",
			args: []types.Type{binary, text},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckKnown,
			},
		},
		{
			name: "text marker remains incompatible with fixed binary",
			args: []types.Type{text, binary},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckKnown,
			},
			wantError: true,
		},
		{
			name: "binary marker remains compatible with fixed binary",
			args: []types.Type{binary, binary},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckKnown,
			},
		},
		{
			name: "mixed marker domains do not create a static binary trigger",
			args: []types.Type{binary, text},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckParamMarker,
			},
		},
		{
			name: "nested runtime binary still triggers against fixed text",
			args: []types.Type{binary, text},
			modes: []StringDomainCheckMode{
				StringDomainCheckKnown, StringDomainCheckKnown,
			},
			wantError: true,
		},
		{
			name: "bare null contributes no domain",
			args: []types.Type{text, binary},
			modes: []StringDomainCheckMode{
				StringDomainCheckDomainless, StringDomainCheckKnown,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetFunctionByNameWithStringDomainCheckModes(
				ctx, "regexp_instr", tc.args, tc.modes)
			if tc.wantError {
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch))
				return
			}
			require.NoError(t, err)
		})
	}

	for _, tc := range []struct {
		name       string
		fn         string
		args       []types.Type
		modes      []StringDomainCheckMode
		wantDomain types.StringDomain
	}{
		{
			name: "substr binary pattern marker owns result domain",
			fn:   "regexp_substr",
			args: []types.Type{types.New(types.T_varchar, 10, 0), binary},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckParamMarker,
			},
			wantDomain: types.StringDomainBinary,
		},
		{
			name: "replace binary subject marker owns result domain",
			fn:   "regexp_replace",
			args: []types.Type{binary, text, text},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckParamMarker, StringDomainCheckParamMarker,
			},
			wantDomain: types.StringDomainBinary,
		},
		{
			name: "replace binary pattern marker owns result domain",
			fn:   "regexp_replace",
			args: []types.Type{text, binary, text},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckParamMarker, StringDomainCheckParamMarker,
			},
			wantDomain: types.StringDomainBinary,
		},
		{
			name: "replace binary replacement marker does not own result domain",
			fn:   "regexp_replace",
			args: []types.Type{text, text, binary},
			modes: []StringDomainCheckMode{
				StringDomainCheckParamMarker, StringDomainCheckParamMarker, StringDomainCheckParamMarker,
			},
			wantDomain: types.StringDomainText,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByNameWithStringDomainCheckModes(
				ctx, tc.fn, tc.args, tc.modes)
			require.NoError(t, err)
			require.Equal(t, tc.wantDomain,
				types.StaticStringDomain(resolved.GetReturnType()))
		})
	}
}

func TestRegexpStringDomainCheckModeMatrix(t *testing.T) {
	type operandState struct {
		name    string
		typ     types.Type
		mode    StringDomainCheckMode
		text    bool
		trigger bool
	}
	text := types.T_varchar.ToType()
	binary := types.T_varbinary.ToType()
	states := []operandState{
		{name: "known_text", typ: text, mode: StringDomainCheckKnown, text: true},
		{name: "known_binary", typ: binary, mode: StringDomainCheckKnown, trigger: true},
		{name: "marker_text", typ: text, mode: StringDomainCheckParamMarker, text: true},
		{name: "marker_binary", typ: binary, mode: StringDomainCheckParamMarker},
		{name: "deferred_text", typ: text, mode: StringDomainCheckDeferred},
		{name: "deferred_binary", typ: binary, mode: StringDomainCheckDeferred},
		{name: "domainless_text_shape", typ: text, mode: StringDomainCheckDomainless},
		{name: "domainless_binary_shape", typ: binary, mode: StringDomainCheckDomainless},
	}

	for _, left := range states {
		for _, middle := range states {
			for _, right := range states {
				operands := []operandState{left, middle, right}
				hasText, hasBinaryTrigger := false, false
				args := make([]types.Type, len(operands))
				modes := make([]StringDomainCheckMode, len(operands))
				for i, operand := range operands {
					args[i], modes[i] = operand.typ, operand.mode
					hasText = hasText || operand.text
					hasBinaryTrigger = hasBinaryTrigger || operand.trigger
				}

				_, err := GetFunctionByNameWithStringDomainCheckModes(
					context.Background(), "regexp_replace", args, modes)
				wantError := hasText && hasBinaryTrigger
				if wantError {
					require.Error(t, err, "%s/%s/%s", left.name, middle.name, right.name)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch), err)
				} else {
					require.NoError(t, err, "%s/%s/%s", left.name, middle.name, right.name)
				}
			}
		}
	}
}

func BenchmarkRegexpReplaceModes(b *testing.B) {
	text := strings.Repeat("abc中", 1024)
	ascii := strings.Repeat("abcx", 1024)
	for _, tc := range []struct {
		name    string
		pattern string
		value   string
		binary  bool
	}{
		{name: "text_utf8", pattern: ".", value: text},
		{name: "binary_ascii", pattern: ".", value: ascii, binary: true},
		{name: "binary_utf8_bytes", pattern: ".", value: text, binary: true},
		{name: "zero_width_text", pattern: "a*", value: strings.Repeat("abc", 1024)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			op := newOpBuiltInRegexp()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := op.regMap.regularReplaceWithMode(tc.pattern, tc.value, "X", 1, 0, tc.binary); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRegexpPositionNearEndDenseMatches(b *testing.B) {
	const size = 1 << 20
	asciiSubject := strings.Repeat("a", size)
	highByteSubject := strings.Repeat("\xff", size)
	position := int64(size - 7)
	for _, tc := range []struct {
		name       string
		pattern    string
		subject    string
		binary     bool
		replaceAll bool
	}{
		{name: "instr_text", pattern: "a", subject: asciiSubject},
		{name: "instr_binary_ascii", pattern: "a", subject: asciiSubject, binary: true},
		{name: "instr_binary_high_byte", pattern: "\xff", subject: highByteSubject, binary: true},
		{name: "replace_all_text", pattern: "a", subject: asciiSubject, replaceAll: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			op := newOpBuiltInRegexp()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if tc.replaceAll {
					if _, err := op.regMap.regularReplaceWithMode(
						tc.pattern, tc.subject, "X", position, 0, tc.binary); err != nil {
						b.Fatal(err)
					}
					continue
				}
				if _, err := op.regMap.regularInstrWithMode(
					tc.pattern, tc.subject, position, 1, 0, tc.binary); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_BuiltIn_RegexpEmptySubject(t *testing.T) {
	proc := testutil.NewProcess(t)
	op := newOpBuiltInRegexp()

	defaultInputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"", "", "", ""}, []bool{false, false, true, false}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"^$", "x", "^$", "^$"}, []bool{false, false, false, true}),
	}
	for _, tc := range []struct {
		name     string
		fn       fEvalFn
		expected FunctionTestResult
	}{
		{
			name:     "regexp_instr_default_position",
			fn:       op.builtInRegexpInstr,
			expected: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, 0, 0}, []bool{false, false, true, true}),
		},
		{
			name:     "regexp_substr_default_position",
			fn:       op.builtInRegexpSubstr,
			expected: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "", "", ""}, []bool{false, true, true, true}),
		},
		{
			name:     "regexp_like_control",
			fn:       op.builtInRegexpLike,
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false, false}, []bool{false, false, true, true}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, defaultInputs, tc.expected, tc.fn)
			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}

	explicitInputs := append(defaultInputs[:2:2],
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1, 1}, []bool{false, false, false, false}))
	for _, tc := range []struct {
		name     string
		fn       fEvalFn
		expected FunctionTestResult
	}{
		{
			name:     "regexp_instr_explicit_position",
			fn:       op.builtInRegexpInstr,
			expected: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, 0, 0}, []bool{false, false, true, true}),
		},
		{
			name:     "regexp_substr_explicit_position",
			fn:       op.builtInRegexpSubstr,
			expected: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "", "", ""}, []bool{false, true, true, true}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, explicitInputs, tc.expected, tc.fn)
			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}

	_, err := op.regMap.regularInstr("^$", "", 0, 1, 0)
	require.Error(t, err)
	for _, pos := range []int64{1, 2, 5, 100} {
		index, err := op.regMap.regularInstr("^$", "", pos, 1, 0)
		require.NoError(t, err)
		require.Equal(t, pos, index)
	}
	index, err := op.regMap.regularInstr(".", "", 5, 1, 0)
	require.NoError(t, err)
	require.Zero(t, index)
	index, err = op.regMap.regularInstr("^$", "", 5, 2, 0)
	require.NoError(t, err)
	require.Zero(t, index)
	index, err = op.regMap.regularInstrWithMode("^$", "", 5, 1, 1, true)
	require.NoError(t, err)
	require.Equal(t, int64(5), index)

	for _, pos := range []int64{0, 2} {
		_, _, err = op.regMap.regularSubstr("^$", "", pos, 1)
		require.Error(t, err)
	}

	_, err = op.regMap.regularInstr("^$", "", 1, 0, 0)
	require.Error(t, err)
	_, err = op.regMap.regularInstr("^$", "", 1, 1, -1)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
	_, err = op.regMap.regularInstr("^$", "", 1, 1, 2)
	require.Error(t, err)
	_, err = op.regMap.regularInstr("*", "", 1, 1, 0)
	require.Error(t, err)

	for _, tc := range []struct {
		name    string
		subject string
		pattern string
	}{
		{name: "empty_subject", subject: "", pattern: "^$"},
		{name: "nonempty_subject", subject: "Cat", pattern: "Cat"},
	} {
		t.Run("regexp_instr_negative_return_option_"+tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.subject}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.pattern}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int8.ToType(), []int8{-1}, nil),
			}
			tcc := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
				op.builtInRegexpInstr)
			_, err := tcc.DebugRun()
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
		})
	}

	_, _, err = op.regMap.regularSubstr("^$", "", 1, 0)
	require.Error(t, err)
	_, _, err = op.regMap.regularSubstr("*", "", 1, 1)
	require.Error(t, err)
}

func Test_BuiltIn_RegularLike(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat       string
		str       string
		matchType string
		expected  bool
	}{
		{pat: ".*", str: "Cat", matchType: "c", expected: true},
		{pat: "b+", str: "Cat", matchType: "c", expected: false},
		{pat: "^Ca", str: "Cat", matchType: "c", expected: true},
		{pat: "^Da", str: "Cat", matchType: "c", expected: false},
		{pat: "cat", str: "Cat", matchType: "", expected: false},
		{pat: "cat", str: "Cat", matchType: "i", expected: true},
		{pat: ".", str: "\n", matchType: "", expected: false},
		{pat: ".", str: "\n", matchType: "n", expected: true},
		{pat: "last$", str: "last\nday", matchType: "", expected: false},
		{pat: "last$", str: "last\nday", matchType: "m", expected: true},
		{pat: "abc", str: "ABC", matchType: "icicc", expected: false},
		{pat: "abc", str: "ABC", matchType: "ccici", expected: true},
	}

	for i, c := range cs {
		match, err := op.regMap.regularLike(c.pat, c.str, c.matchType)
		require.NoError(t, err, i)
		require.Equal(t, c.expected, match, i)
	}

	for _, tc := range []struct {
		name      string
		pattern   string
		subject   string
		matchType string
		want      bool
	}{
		{name: "binary honors case insensitive flag", pattern: "a", subject: "A", matchType: "i", want: true},
		{name: "binary honors rightmost insensitive flag", pattern: "a", subject: "A", matchType: "ci", want: true},
		{name: "binary honors rightmost sensitive flag", pattern: "a", subject: "A", matchType: "ic", want: false},
		{name: "binary CP-1252 folds latin one high bytes", pattern: "\xe9", subject: "\xc9", matchType: "i", want: true},
		{name: "binary CP-1252 folds extension high bytes", pattern: "\x9a", subject: "\x8a", matchType: "i", want: true},
		{name: "binary CP-1252 folds escaped high bytes", pattern: `\xE9`, subject: "\xc9", matchType: "i", want: true},
		{name: "binary CP-1252 keeps unrelated high bytes distinct", pattern: "\xff", subject: "\xfe", matchType: "i", want: false},
		{name: "binary high bytes remain sensitive under c", pattern: "\xe9", subject: "\xc9", matchType: "ic", want: false},
		{name: "binary keeps multiline flag", pattern: "^b", subject: "A\nb", matchType: "im", want: true},
		{name: "binary multiline honors insensitive flag", pattern: "^B", subject: "A\nb", matchType: "im", want: true},
		{name: "binary keeps dotall flag", pattern: ".", subject: "\n", matchType: "in", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			match, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.matchType, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, match)
		})
	}

	_, err := op.regMap.regularLikeWithMode("a", "A", "ix", true)
	require.Error(t, err, "binary mode must not bypass match_type validation")

}

func Test_BuiltIn_RegexpLikeRebindsBinaryCaseSensitivity(t *testing.T) {
	proc := testutil.NewProcess(t)
	varchar := types.T_varchar.ToType()
	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(varchar, []string{"É"}, nil),
			NewFunctionTestInput(varchar, []string{"é"}, nil),
			NewFunctionTestInput(varchar, []string{"i"}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, nil),
		newOpBuiltInRegexp().builtInRegexpLike)

	testCase.parameters[0].SetIsBinaryString(true)
	testCase.parameters[1].SetIsBinaryString(true)
	ok, info := testCase.Run()
	require.True(t, ok, info)

	testCase.parameters[0].SetIsBinaryString(false)
	testCase.parameters[1].SetIsBinaryString(false)
	testCase.expected.wanted = []bool{true}
	ok, info = testCase.Run()
	require.True(t, ok, info)

	testCase.parameters[0].SetIsBinaryString(true)
	testCase.parameters[1].SetIsBinaryString(true)
	testCase.expected.wanted = []bool{false}
	ok, info = testCase.Run()
	require.True(t, ok, info)
}

func Test_BuiltIn_RegexpValueFunctionsRejectEmptyPattern(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.T_varchar.ToType()
	int64Type := types.T_int64.ToType()
	int8Type := types.T_int8.ToType()
	nullText := NewFunctionTestInput(text, []string{""}, []bool{true})
	emptyPattern := NewFunctionTestInput(text, []string{""}, []bool{false})
	nullInt64 := NewFunctionTestInput(int64Type, []int64{0}, []bool{true})
	nullInt8 := NewFunctionTestInput(int8Type, []int8{0}, []bool{true})

	for _, tc := range []struct {
		name       string
		fn         fEvalFn
		inputs     []FunctionTestInput
		resultType types.Type
	}{
		{name: "instr_2", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{nullText, emptyPattern}, resultType: int64Type},
		{name: "instr_3", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{nullText, emptyPattern, nullInt64}, resultType: int64Type},
		{name: "instr_4", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{nullText, emptyPattern, nullInt64, nullInt64}, resultType: int64Type},
		{name: "instr_5", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{nullText, emptyPattern, nullInt64, nullInt64, nullInt8}, resultType: int64Type},
		{name: "substr_2", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{nullText, emptyPattern}, resultType: text},
		{name: "substr_3", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{nullText, emptyPattern, nullInt64}, resultType: text},
		{name: "substr_4", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{nullText, emptyPattern, nullInt64, nullInt64}, resultType: text},
		{name: "replace_3", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{nullText, emptyPattern, nullText}, resultType: text},
		{name: "replace_4", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{nullText, emptyPattern, nullText, nullInt64}, resultType: text},
		{name: "replace_5", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{nullText, emptyPattern, nullText, nullInt64, nullInt64}, resultType: text},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc, tc.inputs, NewFunctionTestResult(tc.resultType, true, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			require.Error(t, err)
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(moerr.ER_REGEXP_ILLEGAL_ARGUMENT), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
		})
	}

	for _, call := range []func() error{
		func() error { _, err := newOpBuiltInRegexp().regMap.regularInstr("", "abc", 0, 1, 0); return err },
		func() error { _, _, err := newOpBuiltInRegexp().regMap.regularSubstr("", "abc", 0, 1); return err },
		func() error { _, err := newOpBuiltInRegexp().regMap.regularReplace("", "abc", "X", 0, 0); return err },
	} {
		err := call()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrRegexpIllegalArgument), err)
	}
}

func Test_BuiltIn_RegexpValidatesPresentArgumentsBeforeNullableResult(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.T_varchar.ToType()
	int64Type := types.T_int64.ToType()
	int8Type := types.T_int8.ToType()
	nullText := NewFunctionTestInput(text, []string{""}, []bool{true})
	invalidPattern := NewFunctionTestInput(text, []string{"["}, nil)
	validPattern := NewFunctionTestInput(text, []string{"a"}, nil)
	validText := NewFunctionTestInput(text, []string{"a"}, nil)
	invalidMatchType := NewFunctionTestInput(text, []string{"z"}, nil)
	validMatchType := NewFunctionTestInput(text, []string{"c"}, nil)
	nullInt64 := NewFunctionTestInput(int64Type, []int64{0}, []bool{true})
	nullInt8 := NewFunctionTestInput(int8Type, []int8{0}, []bool{true})

	for _, tc := range []struct {
		name       string
		fn         fEvalFn
		inputs     []FunctionTestInput
		resultType types.Type
	}{
		{name: "regexp_operator_null_subject", fn: newOpBuiltInRegexp().builtInRegMatch, inputs: []FunctionTestInput{nullText, invalidPattern}, resultType: types.T_bool.ToType()},
		{name: "not_regexp_null_subject", fn: newOpBuiltInRegexp().builtInNotRegMatch, inputs: []FunctionTestInput{nullText, invalidPattern}, resultType: types.T_bool.ToType()},
		{name: "regexp_like_null_subject", fn: newOpBuiltInRegexp().builtInRegexpLike, inputs: []FunctionTestInput{nullText, invalidPattern, validMatchType}, resultType: types.T_bool.ToType()},
		{name: "regexp_like_invalid_match_type_before_null_subject", fn: newOpBuiltInRegexp().builtInRegexpLike, inputs: []FunctionTestInput{nullText, validPattern, invalidMatchType}, resultType: types.T_bool.ToType()},
		{name: "regexp_like_invalid_match_type_before_null_pattern", fn: newOpBuiltInRegexp().builtInRegexpLike, inputs: []FunctionTestInput{validText, nullText, invalidMatchType}, resultType: types.T_bool.ToType()},
		{name: "instr_2_null_subject", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{nullText, invalidPattern}, resultType: int64Type},
		{name: "instr_3_null_position", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{validText, invalidPattern, nullInt64}, resultType: int64Type},
		{name: "instr_4_null_occurrence", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{validText, invalidPattern, NewFunctionTestInput(int64Type, []int64{1}, nil), nullInt64}, resultType: int64Type},
		{name: "instr_5_null_result_option", fn: newOpBuiltInRegexp().builtInRegexpInstr, inputs: []FunctionTestInput{validText, invalidPattern, NewFunctionTestInput(int64Type, []int64{1}, nil), NewFunctionTestInput(int64Type, []int64{1}, nil), nullInt8}, resultType: int64Type},
		{name: "substr_2_null_subject", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{nullText, invalidPattern}, resultType: text},
		{name: "substr_3_null_position", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{validText, invalidPattern, nullInt64}, resultType: text},
		{name: "substr_4_null_occurrence", fn: newOpBuiltInRegexp().builtInRegexpSubstr, inputs: []FunctionTestInput{validText, invalidPattern, NewFunctionTestInput(int64Type, []int64{1}, nil), nullInt64}, resultType: text},
		{name: "replace_3_null_replacement", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{validText, invalidPattern, nullText}, resultType: text},
		{name: "replace_4_null_position", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{validText, invalidPattern, validText, nullInt64}, resultType: text},
		{name: "replace_5_null_occurrence", fn: newOpBuiltInRegexp().builtInRegexpReplace, inputs: []FunctionTestInput{validText, invalidPattern, validText, NewFunctionTestInput(int64Type, []int64{1}, nil), nullInt64}, resultType: text},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc, tc.inputs, NewFunctionTestResult(tc.resultType, true, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			require.Error(t, err)
		})
	}

	nullMatchType := NewFunctionTestInput(text, []string{""}, []bool{true})
	control := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{nullText, invalidPattern, nullMatchType},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true}),
		newOpBuiltInRegexp().builtInRegexpLike,
	)
	ok, info := control.Run()
	require.True(t, ok, info, "a NULL match_type remains the earlier NULL result boundary")
}

func Test_BuiltIn_RegexpLikeRejectsEmptyPattern(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
	}{
		{
			name: "two_arguments",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
			},
		},
		{
			name: "three_arguments",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"i"}, []bool{false}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc,
				tc.inputs,
				NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, []bool{false}),
				newOpBuiltInRegexp().builtInRegexpLike,
			)

			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			require.Error(t, err)

			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(3685), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
			require.Equal(t, "Illegal argument to a regular expression.", moErr.Error())
		})
	}
}

func Test_BuiltIn_RegMatchRejectsEmptyPattern(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name string
		fn   fEvalFn
	}{
		{name: "reg_match", fn: newOpBuiltInRegexp().builtInRegMatch},
		{name: "not_reg_match", fn: newOpBuiltInRegexp().builtInNotRegMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{false}),
				},
				NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, []bool{false}),
				tc.fn,
			)

			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			require.Error(t, err)

			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, uint16(3685), moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
			require.Equal(t, "Illegal argument to a regular expression.", moErr.Error())
		})
	}
}

func Test_BuiltIn_RegMatchPreservesNullPattern(t *testing.T) {
	proc := testutil.NewProcess(t)
	tcc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true}),
		newOpBuiltInRegexp().builtInRegMatch,
	)

	succeed, errInfo := tcc.Run()
	require.True(t, succeed, errInfo)
}

func Test_BuiltIn_RegMatchPreservesValidPatterns(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name     string
		pattern  string
		expected bool
		fn       fEvalFn
	}{
		{name: "reg_match", pattern: "^a", expected: true, fn: newOpBuiltInRegexp().builtInRegMatch},
		{name: "not_reg_match", pattern: "^z", expected: true, fn: newOpBuiltInRegexp().builtInNotRegMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.pattern}, []bool{false}),
				},
				NewFunctionTestResult(types.T_bool.ToType(), false, []bool{tc.expected}, []bool{false}),
				tc.fn,
			)

			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

func Test_BuiltIn_DynamicRegexpPatternCacheOwnsKeys(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name       string
		resultType types.Type
		want       func(string) any
		fn         func(*opBuiltInRegexp) fEvalFn
	}{
		{
			name:       "regexp_instr",
			resultType: types.T_int64.ToType(),
			want:       func(string) any { return []int64{1} },
			fn:         func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpInstr },
		},
		{
			name:       "regexp_substr",
			resultType: types.T_varchar.ToType(),
			want:       func(s string) any { return []string{s} },
			fn:         func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpSubstr },
		},
		{
			name:       "regexp_operator",
			resultType: types.T_bool.ToType(),
			want:       func(string) any { return []bool{true} },
			fn:         func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegMatch },
		},
		{
			name:       "regexp_like_control",
			resultType: types.T_bool.ToType(),
			want:       func(string) any { return []bool{true} },
			fn:         func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpLike },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := newOpBuiltInRegexp()
			tcc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"aaaa"}, nil),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"aaaa"}, nil),
				},
				NewFunctionTestResult(tc.resultType, false, tc.want("aaaa"), nil),
				tc.fn(op),
			)

			pattern, _ := vector.GenerateFunctionStrParameter(tcc.parameters[1]).GetStrValue(0)
			collision := findRegexpCacheHashCollision(t, op.regMap.mp, pattern)
			copy(pattern, "aaaa")

			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)

			subject, _ := vector.GenerateFunctionStrParameter(tcc.parameters[0]).GetStrValue(0)
			copy(subject, collision)
			copy(pattern, collision)
			tcc.expected.wanted = tc.want(collision)

			succeed, errInfo = tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

// findRegexpCacheHashCollision simulates a vector buffer being reused for the
// next data block. It finds another four-byte pattern whose map hash metadata
// collides with "aaaa", making a borrowed, mutated map key deterministically
// retrieve the regexp compiled for the preceding block.
func findRegexpCacheHashCollision(t *testing.T, cache map[regexpCacheKey]*regexp.Regexp, pattern []byte) string {
	t.Helper()
	require.Equal(t, "aaaa", string(pattern))

	// Keep one stable entry in the map: Go may randomize a map's hash seed when
	// its last entry is deleted, which would invalidate the collision found here.
	cache[regexpCacheKey{pattern: "sentinel"}] = regexp.MustCompile("sentinel")
	key := functionUtil.QuickBytesToStr(pattern)
	cache[regexpCacheKey{pattern: key}] = regexp.MustCompile(key)
	for value := 1; value < 26*26*26*26; value++ {
		n := value
		for i := len(pattern) - 1; i >= 0; i-- {
			pattern[i] = byte('a' + n%26)
			n /= 26
		}

		candidate := functionUtil.QuickBytesToStr(pattern)
		candidateKey := regexpCacheKey{pattern: candidate}
		if cached, ok := cache[candidateKey]; ok && !cached.MatchString(candidate) {
			delete(cache, candidateKey)
			require.Len(t, cache, 1)
			return string(pattern)
		}
	}

	t.Fatal("failed to find regexp cache hash collision")
	return ""
}

func Test_BuiltIn_LikeUTF8Underscore(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name     string
		pattern  string
		values   []string
		expected []bool
	}{
		{
			name:     "single underscore",
			pattern:  "_",
			values:   []string{"A", "é", "中", "🙂", "ab", ""},
			expected: []bool{true, true, true, true, false, false},
		},
		{
			name:     "leading underscore",
			pattern:  "_tail",
			values:   []string{"Atail", "étail", "中tail", "🙂tail", "abtail", "tail"},
			expected: []bool{true, true, true, true, false, false},
		},
		{
			name:     "trailing underscore",
			pattern:  "head_",
			values:   []string{"headA", "headé", "head中", "head🙂", "headab", "head"},
			expected: []bool{true, true, true, true, false, false},
		},
		{
			name:     "percent then trailing underscore",
			pattern:  "%tail_",
			values:   []string{"tailA", "prefixtailé", "tail中", "prefixtail🙂", "tail", "tailab"},
			expected: []bool{true, true, true, true, false, false},
		},
		{
			name:     "leading underscore then percent",
			pattern:  "_head%",
			values:   []string{"Ahead", "éheadtail", "中head", "🙂headtail", "head", "abhead"},
			expected: []bool{true, true, true, true, false, false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), tc.values, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.pattern}, nil),
				},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.expected, nil),
				newOpBuiltInRegexp().likeFn,
			)

			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

func Test_BuiltIn_RegularMatchForLikeOp(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat      string
		str      string
		expected bool
	}{
		{pat: "__++%", str: "__++", expected: true},
		{pat: "__\\+", str: "__++__", expected: false},
		{pat: "__+", str: "__++__", expected: false},
		{pat: "a+b", str: "a+b", expected: true},
		{pat: "a+b", str: "ab", expected: false},
		{pat: "__..%", str: "__..x", expected: true},
	}

	for i, c := range cs {
		match, err := op.regMap.regularMatchForLikeOp([]byte(c.pat), []byte(c.str))
		require.NoError(t, err, i)
		require.Equal(t, c.expected, match, i)
	}
}

func Test_BuiltIn_RegularMatchForLikeOpWithEscape(t *testing.T) {
	op := newOpBuiltInRegexp()

	testCases := []struct {
		name            string
		pattern         string
		value           string
		escape          rune
		escapeEnabled   bool
		caseInsensitive bool
		expected        bool
	}{
		{name: "custom escape underscore", pattern: "a!_b", value: "a_b", escape: '!', escapeEnabled: true, expected: true},
		{name: "custom escape percent", pattern: "a!%b", value: "a%b", escape: '!', escapeEnabled: true, expected: true},
		{name: "empty escape", pattern: `a\_b`, value: `a\xb`, escapeEnabled: false, expected: true},
		{name: "unicode escape", pattern: "a界_b", value: "a_b", escape: '界', escapeEnabled: true, expected: true},
		{name: "ilike recognizes escape before folding", pattern: "aX_b", value: "A_B", escape: 'X', escapeEnabled: true, caseInsensitive: true, expected: true},
		{name: "ilike lowercase escape spelling remains literal", pattern: "axb", value: "AXB", escape: 'X', escapeEnabled: true, caseInsensitive: true, expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			match, err := op.regMap.regularMatchForLikeOpWithEscape(
				[]byte(tc.pattern),
				[]byte(tc.value),
				tc.escape,
				tc.escapeEnabled,
				tc.caseInsensitive,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, match)
		})
	}
}

func Test_BuiltIn_LikeWithEscape(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name     string
		inputs   []FunctionTestInput
		expected FunctionTestResult
		fn       fEvalFn
	}{
		{
			name: "constant escape",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a_b", "axb"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a!_b", "a!_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"!", "!"}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "empty escape",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"axb"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "null escape disables escaping",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"axb"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "null escape leaves escape byte literal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a!_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "null escape preserves null value propagation",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true}),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "null escape preserves null pattern propagation",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"axb"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{true}),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "ilike null escape disables escaping",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"AXB"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil),
			fn:       newOpBuiltInRegexp().iLikeFn,
		},
		{
			name: "nonconstant escape",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a!_b"}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"!"}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "multicharacter escape",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a!_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"!!"}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, nil),
			fn:       newOpBuiltInRegexp().likeFn,
		},
		{
			name: "ilike preserves case-sensitive escape recognition",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"A_B", "AXB"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"aX_b", "aX_b"}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"X", "X"}, nil),
			},
			expected: NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false}, nil),
			fn:       newOpBuiltInRegexp().iLikeFn,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expected, tc.fn)
			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

func Test_BuiltIn_LikeWithEscapeSQLMode(t *testing.T) {
	testCases := []struct {
		name      string
		configure func(*process.Process)
	}{
		{
			name: "runtime resolver",
			configure: func(proc *process.Process) {
				proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
					require.Equal(t, "sql_mode", name)
					require.True(t, system)
					require.False(t, global)
					return "NO_BACKSLASH_ESCAPES", nil
				})
			},
		},
		{
			name: "serialized session fallback",
			configure: func(proc *process.Process) {
				proc.GetSessionInfo().SqlMode = "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			tc.configure(proc)
			tcc := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"axb"}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, nil),
				},
				NewFunctionTestResult(types.T_bool.ToType(), true, []bool{false}, nil),
				newOpBuiltInRegexp().likeFn,
			)
			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

func Test_BuiltIn_LikeWithNullEscapeSQLMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().SqlMode = "NO_BACKSLASH_ESCAPES"
	resolverCalled := false
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		resolverCalled = true
		return "NO_BACKSLASH_ESCAPES", nil
	})
	tcc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"axb"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"a_b"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil),
		newOpBuiltInRegexp().likeFn,
	)
	succeed, errInfo := tcc.Run()
	require.True(t, succeed, errInfo)
	require.False(t, resolverCalled, "NULL ESCAPE must bypass explicit-empty SQL-mode validation")
}

func Test_BuiltIn_ILikeRejectsInvalidArity(t *testing.T) {
	for _, args := range [][]types.Type{
		nil,
		{types.T_varchar.ToType()},
		{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()},
	} {
		_, err := GetFunctionByName(context.Background(), "ilike", args)
		require.Error(t, err)
	}
}

func Test_BuiltIn_RegularReplace(t *testing.T) {
	op := newOpBuiltInRegexp()

	cs := []struct {
		pat      string
		str      string
		repl     string
		pos      int64
		ocr      int64
		expected string
	}{
		{pat: "[0-9]", str: "1abc2", repl: "#", pos: 1, ocr: 1, expected: "#abc2"},
		{pat: "[0-9]", str: "12abc", repl: "#", pos: 2, ocr: 1, expected: "1#abc"},
		{pat: "[0-9]", str: "01234abcde56789", repl: "#", pos: 1, ocr: 1, expected: "#1234abcde56789"},
		{pat: "[09]", str: "01234abcde56789", repl: "#", pos: 1, ocr: 1, expected: "#1234abcde56789"},
		{pat: "[0-9]", str: "abcdefg123456ABC", repl: "", pos: 4, ocr: 0, expected: "abcdefgABC"},
		{pat: "[0-9]", str: "abcDEfg123456ABC", repl: "", pos: 4, ocr: 0, expected: "abcDEfgABC"},
		{pat: "[0-9]", str: "abcDEfg123456ABC", repl: "", pos: 7, ocr: 0, expected: "abcDEfgABC"},
		{pat: "[0-9]", str: "abcDefg123456ABC", repl: "", pos: 10, ocr: 0, expected: "abcDefg12ABC"},
	}

	for i, c := range cs {
		val, err := op.regMap.regularReplace(c.pat, c.str, c.repl, c.pos, c.ocr)
		require.NoError(t, err, i)
		require.Equal(t, c.expected, val, i)
	}
}

func Test_BuiltIn_RegularSubstr(t *testing.T) {
	op := newOpBuiltInRegexp()

	cc := []struct {
		pat      string
		str      string
		pos      int64
		ocr      int64
		expected string
	}{
		{pat: "[a-z]+", str: "abc def ghi", pos: 1, ocr: 1, expected: "abc"},
		{pat: "[a-z]+", str: "abc def ghi", pos: 1, ocr: 3, expected: "ghi"},
		{pat: "[a-z]+", str: "java t point", pos: 2, ocr: 3, expected: "point"},
		{pat: "[a-z]+", str: "my sql function", pos: 1, ocr: 3, expected: "function"},
	}

	for i, c := range cc {
		match, val, err := op.regMap.regularSubstr(c.pat, c.str, c.pos, c.ocr)
		require.NoError(t, err, i)
		require.True(t, match, i)
		require.Equal(t, c.expected, val, i)
	}
}
