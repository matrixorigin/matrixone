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
		{name: "zero_width_keeps_non_overlapping_iteration", pattern: "b*", subject: "ab", replacement: "X", position: 2, occurrence: 0, want: "aX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularReplace(tc.pattern, tc.subject, tc.replacement, tc.position, tc.occurrence)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
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
		matched, got, err := op.regMap.regularSubstrWithMode(".", subject, int64(pos+1), 1, true)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, want, []byte(got), "position %d", pos+1)
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
		matched, got, err := op.regMap.regularSubstrWithMode(".", invalid, int64(pos+1), 1, true)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, want, []byte(got))
	}
}

func Test_BuiltIn_RegexpAnchorsRemainRelativeToOriginalSubject(t *testing.T) {
	op := newOpBuiltInRegexp()

	matched, _, err := op.regMap.regularSubstrWithMode("^", "abc", 2, 1, false)
	require.NoError(t, err)
	require.False(t, matched)

	position, err := op.regMap.regularInstrWithMode("^", "abc", 2, 1, 0, false)
	require.NoError(t, err)
	require.Zero(t, position)

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

func BenchmarkRegexpReplaceModes(b *testing.B) {
	text := strings.Repeat("abc中", 1024)
	ascii := strings.Repeat("abcx", 1024)
	for _, tc := range []struct {
		name   string
		value  string
		binary bool
	}{
		{name: "text_utf8", value: text},
		{name: "binary_ascii", value: ascii, binary: true},
		{name: "binary_utf8_bytes", value: text, binary: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			op := newOpBuiltInRegexp()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := op.regMap.regularReplaceWithMode(".", tc.value, "X", 1, 0, tc.binary); err != nil {
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

	for _, pos := range []int64{0, 2} {
		_, err := op.regMap.regularInstr("^$", "", pos, 1, 0)
		require.Error(t, err)

		_, _, err = op.regMap.regularSubstr("^$", "", pos, 1)
		require.Error(t, err)
	}

	_, err := op.regMap.regularInstr("^$", "", 1, 0, 0)
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
