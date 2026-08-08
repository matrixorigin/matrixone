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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		retOption int64
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

func Test_BuiltIn_RegexpOptionalMatchType(t *testing.T) {
	op := newOpBuiltInRegexp()

	index, err := op.regMap.regularInstrWithMatchType("cat", "Cat cat", 1, 1, 0, "i")
	require.NoError(t, err)
	require.Equal(t, int64(1), index)
	index, err = op.regMap.regularInstrWithMatchType("cat", "Cat cat", 1, 1, 0, "ic")
	require.NoError(t, err)
	require.Equal(t, int64(5), index)
	index, err = op.regMap.regularInstrWithMatchType("cat", "Cat cat", 1, 1, 0, "ci")
	require.NoError(t, err)
	require.Equal(t, int64(1), index)
	index, err = op.regMap.regularInstrWithMatchType("^b", "a\nb", 1, 1, 0, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), index)

	matched, substr, err := op.regMap.regularSubstrWithMatchType("a.b", "a\nb", 1, 1, "n")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "a\nb", substr)

	replaced, err := op.regMap.regularReplaceWithMatchType("cat", "Cat cat", "X", 1, 0, "i")
	require.NoError(t, err)
	require.Equal(t, "X X", replaced)
	replaced, err = op.regMap.regularReplaceWithMatchType("cat", "Cat cat", "X", 1, 0, "")
	require.NoError(t, err)
	require.Equal(t, "Cat X", replaced)

	_, err = op.regMap.regularInstrWithMatchType("cat", "Cat", 1, 1, 0, "x")
	require.Error(t, err)
	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_WRONG_ARGUMENTS), moErr.MySQLCode())
	require.Equal(t, "HY000", moErr.SqlState())
	require.Equal(t, "Incorrect arguments to regexp_instr", moErr.Error())
}

func Test_BuiltIn_RegexpMySQLBoundarySemantics(t *testing.T) {
	op := newOpBuiltInRegexp()

	_, err := op.regMap.regularInstrWithMatchType("a", "a", 1, 1, -1, "c")
	require.Error(t, err)
	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_WRONG_ARGUMENTS), moErr.MySQLCode())
	_, err = op.regMap.regularInstrWithMatchType("a", "a", 1, 1, 256, "c")
	require.Error(t, err)
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_WRONG_ARGUMENTS), moErr.MySQLCode())

	replaced, err := op.regMap.regularReplaceWithMatchType("a", "abcabc", "X", 4, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "abcXbc", replaced)

	index, err := op.regMap.regularInstrWithMatchType("a", "你a", 1, 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(2), index)
	matched, substr, err := op.regMap.regularSubstrWithMatchType("a", "你a", 2, 1, "c")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "a", substr)
	replaced, err = op.regMap.regularReplaceWithMatchType("a", "你a", "X", 2, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "你X", replaced)

	for _, newline := range []string{"\r", "\u0085", "\u2028", "\u2029"} {
		index, err = op.regMap.regularInstrWithMatchType("^b", "a"+newline+"b", 1, 1, 0, "m")
		require.NoError(t, err)
		require.Equal(t, int64(3), index)
		index, err = op.regMap.regularInstrWithMatchType("^b", "a"+newline+"b", 1, 1, 0, "mu")
		require.NoError(t, err)
		require.Equal(t, int64(0), index)
	}

	_, err = op.regMap.regularInstrWithMatchType("", "abc", 1, 1, 0, "c")
	require.Error(t, err)
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_REGEXP_ILLEGAL_ARGUMENT), moErr.MySQLCode())
	matched, _, err = op.regMap.regularSubstrWithMatchType("", "abc", 1, 1, "c")
	require.False(t, matched)
	require.Error(t, err)
	_, err = op.regMap.regularReplaceWithMatchType("", "abc", "X", 1, 0, "c")
	require.Error(t, err)

	matched, _, err = op.regMap.regularSubstrWithMatchType("a", "abc", 4, 1, "c")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err = op.regMap.regularReplaceWithMatchType("a", "abc", "X", 4, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "abc", replaced)

	index, err = op.regMap.regularInstrWithMatchType("a", "abc", 1, -1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(1), index)
	matched, substr, err = op.regMap.regularSubstrWithMatchType("a", "abc", 1, 0, "c")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "a", substr)
	replaced, err = op.regMap.regularReplaceWithMatchType("a", "abcabc", "X", 1, -1, "c")
	require.NoError(t, err)
	require.Equal(t, "Xbcabc", replaced)

	replaced, err = op.regMap.regularReplaceWithMatchType(
		"([a-z]+)([0-9]+)", "abc123", "$2-$1", 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "123-abc", replaced)
}

func Test_BuiltIn_RegexpReviewCompatibility(t *testing.T) {
	op := newOpBuiltInRegexp()

	matched, err := op.regMap.regularLike("\\r", "a\r", "m")
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = op.regMap.regularLike(".", "\r", "c")
	require.NoError(t, err)
	require.False(t, matched)

	index, err := op.regMap.regularInstrWithMatchType("^$", "a\r\nb", 1, 1, 0, "m")
	require.NoError(t, err)
	require.Zero(t, index)
	index, err = op.regMap.regularInstrWithMatchType("$", "a\r\nb", 1, 2, 0, "m")
	require.NoError(t, err)
	require.Equal(t, int64(5), index)
	index, err = op.regMap.regularInstrWithMatchType("(?m)^b", "a\rb", 1, 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(3), index)

	index, err = op.regMap.regularInstrWithMatchType("^b", "abc", 2, 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(2), index)
	matched, substr, err := op.regMap.regularSubstrWithMatchType("^b", "abc", 2, 1, "c")
	require.NoError(t, err)
	require.False(t, matched)
	require.Empty(t, substr)
	replaced, err := op.regMap.regularReplaceWithMatchType("^b", "abc", "X", 2, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "abc", replaced)

	for _, tc := range []struct {
		replacement string
		expected    string
		mysqlCode   uint16
	}{
		{replacement: "$2-$1", expected: "123-abc"},
		{replacement: "$10", expected: "abc0"},
		{replacement: "$1x", expected: "abcx"},
		{replacement: "$3", mysqlCode: moerr.ER_REGEXP_INDEX_OUTOFBOUNDS_ERROR},
		{replacement: "${1}", mysqlCode: moerr.ER_REGEXP_INVALID_CAPTURE_GROUP_NAME},
		{replacement: "$$", mysqlCode: moerr.ER_REGEXP_INVALID_CAPTURE_GROUP_NAME},
	} {
		replaced, err = op.regMap.regularReplaceWithMatchType(
			"([a-z]+)([0-9]+)", "abc123", tc.replacement, 1, 0, "c")
		if tc.mysqlCode == 0 {
			require.NoError(t, err, tc.replacement)
			require.Equal(t, tc.expected, replaced, tc.replacement)
			continue
		}
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr, tc.replacement)
		require.Equal(t, tc.mysqlCode, moErr.MySQLCode(), tc.replacement)
	}
}

func Test_BuiltIn_RegexpICUAndBinaryCompatibility(t *testing.T) {
	op := newOpBuiltInRegexp()

	index, err := op.regMap.regularInstrBytes([]byte{'a'}, []byte{0xc3, 0xa9, 'a'}, 1, 1, 0, "c", true)
	require.NoError(t, err)
	require.Equal(t, int64(3), index)
	matched, err := op.regMap.regularLikeBytes([]byte{0xfe}, []byte{0xff}, "c", true)
	require.NoError(t, err)
	require.False(t, matched)
	matched, err = op.regMap.regularLikeBytes([]byte(`\p{Sc}`), []byte{0x80}, "c", true)
	require.NoError(t, err)
	require.True(t, matched)

	for _, tc := range []struct {
		pattern string
		subject string
	}{
		{pattern: `\d`, subject: "١"},
		{pattern: `\w`, subject: "中"},
		{pattern: `\s`, subject: "\u00a0"},
		{pattern: `[[:alpha:]]`, subject: "中"},
		{pattern: `\X`, subject: "👨‍👩‍👧"},
		{pattern: `\R`, subject: "\r\n"},
		{pattern: `a++`, subject: "a"},
		{pattern: `[a-z&&[^b]]`, subject: "a"},
		{pattern: `[]a.]`, subject: "]"},
	} {
		matched, err = op.regMap.regularLike(tc.pattern, tc.subject, "c")
		require.NoError(t, err, tc.pattern)
		require.True(t, matched, tc.pattern)
	}

	matched, err = op.regMap.regularLike(".", "\r", "u")
	require.NoError(t, err)
	require.True(t, matched)
	index, err = op.regMap.regularInstrWithMatchType("$", "a\r", 1, 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(2), index)
	index, err = op.regMap.regularInstrWithMatchType("$", "a\r\n", 1, 1, 0, "u")
	require.NoError(t, err)
	require.Equal(t, int64(3), index)

	replaced, err := op.regMap.regularReplaceWithMatchType(`\bb`, "ab", "X", 2, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "ab", replaced)
	replaced, err = op.regMap.regularReplace("(a)", "a", `\$1`, 1, 0)
	require.NoError(t, err)
	require.Equal(t, "$1", replaced)
	replaced, err = op.regMap.regularReplace("(a)", "a", `\\$1`, 1, 0)
	require.NoError(t, err)
	require.Equal(t, `\a`, replaced)
	replaced, err = op.regMap.regularReplace("(a)", strings.Repeat("a", 100), "$1$1$1", 1, 0)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("a", 300), replaced)
}

func Test_BuiltIn_RegexpZeroWidthBoundarySemantics(t *testing.T) {
	op := newOpBuiltInRegexp()

	index, err := op.regMap.regularInstrWithMatchType("a*", "", 1, 1, 0, "c")
	require.NoError(t, err)
	require.Equal(t, int64(1), index)

	matched, substr, err := op.regMap.regularSubstrWithMatchType("a*", "", 1, 1, "c")
	require.NoError(t, err)
	require.True(t, matched)
	require.Empty(t, substr)

	matched, substr, err = op.regMap.regularSubstrWithMatchType("$", "a", 2, 1, "c")
	require.NoError(t, err)
	require.True(t, matched)
	require.Empty(t, substr)

	replaced, err := op.regMap.regularReplaceWithMatchType("$", "a", "X", 2, 0, "c")
	require.NoError(t, err)
	require.Equal(t, "aX", replaced)
}

func Test_BuiltIn_RegexpReplacePreservesSkippedMatches(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		pos        int64
		occurrence int64
		expected   string
	}{
		{pos: 1, occurrence: 1, expected: "Tiger Dog Cat Dog Cat"},
		{pos: 1, occurrence: 2, expected: "Cat Dog Tiger Dog Cat"},
		{pos: 1, occurrence: 3, expected: "Cat Dog Cat Dog Tiger"},
		{pos: 2, occurrence: 2, expected: "Cat Dog Cat Dog Tiger"},
		{pos: 1, occurrence: 0, expected: "Tiger Dog Tiger Dog Tiger"},
	} {
		actual, err := op.regMap.regularReplaceWithMatchType(
			"Cat", "Cat Dog Cat Dog Cat", "Tiger", tc.pos, tc.occurrence, "c")
		require.NoError(t, err)
		require.Equal(t, tc.expected, actual, "pos=%d occurrence=%d", tc.pos, tc.occurrence)
	}
}

func Test_BuiltIn_RegexpHonorsFunctionSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.T_varchar.ToType()
	maskFirstRow := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}

	for _, tc := range []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		fn         fEvalFn
	}{
		{name: "regexp", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "a"}, nil),
		}, resultType: types.T_bool.ToType(), fn: newOpBuiltInRegexp().builtInRegMatch},
		{name: "not regexp", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "z"}, nil),
		}, resultType: types.T_bool.ToType(), fn: newOpBuiltInRegexp().builtInNotRegMatch},
		{name: "regexp_like", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "a"}, nil),
		}, resultType: types.T_bool.ToType(), fn: newOpBuiltInRegexp().builtInRegexpLike},
		{name: "regexp_instr", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "a"}, nil),
		}, resultType: types.T_int64.ToType(), fn: newOpBuiltInRegexp().builtInRegexpInstr},
		{name: "regexp_substr", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "a"}, nil),
		}, resultType: stringType, fn: newOpBuiltInRegexp().builtInRegexpSubstr},
		{name: "regexp_replace", inputs: []FunctionTestInput{
			NewFunctionTestInput(stringType, []string{"a", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"*", "a"}, nil),
			NewFunctionTestInput(stringType, []string{"X", "X"}, nil),
		}, resultType: stringType, fn: newOpBuiltInRegexp().builtInRegexpReplace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc, tc.inputs, NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(2))
			require.NoError(t, tcc.fn(tcc.parameters, tcc.result, proc, 2, maskFirstRow))
			result := tcc.GetResultVectorDirectly()
			require.True(t, result.IsNull(0))
			require.False(t, result.IsNull(1))
		})
	}
}

func Test_BuiltIn_RegexpICUErrorMapping(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		pattern   string
		mysqlCode uint16
	}{
		{pattern: "*", mysqlCode: moerr.ER_REGEXP_RULE_SYNTAX},
		{pattern: "(", mysqlCode: moerr.ER_REGEXP_MISMATCHED_PAREN},
		{pattern: "[z-a]", mysqlCode: moerr.ER_REGEXP_INVALID_RANGE},
	} {
		_, err := op.regMap.regularLike(tc.pattern, "a", "c")
		var moErr *moerr.Error
		require.ErrorAs(t, err, &moErr, tc.pattern)
		require.Equal(t, tc.mysqlCode, moErr.MySQLCode(), tc.pattern)
		require.Equal(t, "HY000", moErr.SqlState(), tc.pattern)
	}
}

func Test_BuiltIn_RegexpUsesMySQLTimeLimit(t *testing.T) {
	op := newOpBuiltInRegexp()
	_, err := op.regMap.regularLike("(a|aa)+$", strings.Repeat("a", 30)+"b", "c")
	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, uint16(moerr.ER_REGEXP_TIME_OUT), moErr.MySQLCode())
	require.Equal(t, "HY000", moErr.SqlState())
}

func TestRegexpTypeMatchPreservesBinaryOperands(t *testing.T) {
	binary := types.T_varbinary.ToType()
	integer := types.T_int32.ToType()
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{name: "reg_match", args: []types.Type{binary, binary}},
		{name: "regexp_instr", args: []types.Type{binary, binary, integer}},
		{name: "regexp_replace", args: []types.Type{binary, binary, binary, integer}},
		{name: "regexp_substr", args: []types.Type{binary, binary, integer}},
	} {
		result, err := GetFunctionByName(context.Background(), tc.name, tc.args)
		require.NoError(t, err, tc.name)
		targets, cast := result.ShouldDoImplicitTypeCast()
		require.True(t, cast, tc.name)
		require.Equal(t, types.T_varbinary, targets[0].Oid, tc.name)
		require.Equal(t, types.T_varbinary, targets[1].Oid, tc.name)
		if tc.name == "regexp_replace" || tc.name == "regexp_substr" {
			require.Equal(t, types.T_varbinary, result.GetReturnType().Oid, tc.name)
		}
	}

	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{name: "regexp_substr", args: []types.Type{types.T_int64.ToType(), binary}},
		{name: "regexp_replace", args: []types.Type{types.T_int64.ToType(), binary, binary}},
	} {
		result, err := GetFunctionByName(context.Background(), tc.name, tc.args)
		require.NoError(t, err, tc.name)
		require.Equal(t, types.T_varbinary, result.GetReturnType().Oid, tc.name)
	}
}

func Test_BuiltIn_RegexpRuntimeBinaryMetadataSelectsBytePath(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.T_varchar.ToType()
	subject := string([]byte{0xc3, 0xa9, 'a'})
	inputs := []FunctionTestInput{
		NewFunctionTestInput(stringType, []string{subject}, nil),
		NewFunctionTestInput(stringType, []string{"."}, nil),
	}
	tcc := NewFunctionTestCase(
		proc, inputs, NewFunctionTestResult(stringType, false, nil, nil),
		newOpBuiltInRegexp().builtInRegexpSubstr)
	tcc.parameters[0].SetIsBin(true)
	require.NoError(t, tcc.result.PreExtendAndReset(1))
	require.NoError(t, tcc.fn(tcc.parameters, tcc.result, proc, 1, nil))
	result := tcc.GetResultVectorDirectly()
	require.Equal(t, types.T_varchar, result.GetType().Oid)
	require.False(t, result.GetIsBin())
	require.Equal(t, "C383", fmt.Sprintf("%X", result.GetBytesAt(0)))
}

func Test_BuiltIn_RegexpBinaryVectorsUseByteSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	binaryType := types.T_varbinary.ToType()
	subject := string([]byte{0xc3, 0xa9, 'a'})

	for _, tc := range []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		result     any
		fn         fEvalFn
	}{
		{
			name: "operator",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(binaryType, []string{subject}, nil),
				NewFunctionTestInput(binaryType, []string{"a"}, nil),
			},
			resultType: types.T_bool.ToType(), result: []bool{true}, fn: newOpBuiltInRegexp().builtInRegMatch,
		},
		{
			name: "instr",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(binaryType, []string{subject}, nil),
				NewFunctionTestInput(binaryType, []string{"a"}, nil),
			},
			resultType: types.T_int64.ToType(), result: []int64{3}, fn: newOpBuiltInRegexp().builtInRegexpInstr,
		},
		{
			name: "substr",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(binaryType, []string{subject}, nil),
				NewFunctionTestInput(binaryType, []string{"a"}, nil),
			},
			resultType: binaryType, result: []string{"a"}, fn: newOpBuiltInRegexp().builtInRegexpSubstr,
		},
		{
			name: "replace",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(binaryType, []string{subject}, nil),
				NewFunctionTestInput(binaryType, []string{"a"}, nil),
				NewFunctionTestInput(binaryType, []string{"X"}, nil),
			},
			resultType: binaryType, result: []string{string([]byte{0xc3, 0xa9, 'X'})}, fn: newOpBuiltInRegexp().builtInRegexpReplace,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var expected FunctionTestResult
			switch value := tc.result.(type) {
			case []bool:
				expected = NewFunctionTestResult(tc.resultType, false, value, nil)
			case []int64:
				expected = NewFunctionTestResult(tc.resultType, false, value, nil)
			case []string:
				expected = NewFunctionTestResult(tc.resultType, false, value, nil)
			}
			tcc := NewFunctionTestCase(proc, tc.inputs, expected, tc.fn)
			succeed, errInfo := tcc.Run()
			require.True(t, succeed, errInfo)
		})
	}
}

func Test_BuiltIn_RegexpOperatorAndLikeShareICUSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"\r"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"."}, nil),
	}
	for _, fn := range []fEvalFn{
		newOpBuiltInRegexp().builtInRegMatch,
		newOpBuiltInRegexp().builtInRegexpLike,
	} {
		tcc := NewFunctionTestCase(
			proc, inputs, NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, nil), fn)
		succeed, errInfo := tcc.Run()
		require.True(t, succeed, errInfo)
	}
}

func BenchmarkRegexpReplaceAllLinear(b *testing.B) {
	op := newOpBuiltInRegexp()
	for _, size := range []int{1024, 2048, 4096, 8192, 16384} {
		input := strings.Repeat("a", size)
		b.Run(fmt.Sprintf("bytes_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := op.regMap.regularReplace("a", input, "X", 1, 0)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_BuiltIn_RegexpValidatesPatternBeforeNullAndPositionShortcuts(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.T_varchar.ToType()
	intType := types.T_int64.ToType()

	for _, tc := range []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		fn         fEvalFn
		mysqlCode  uint16
	}{
		{
			name: "substr empty subject and pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, nil),
				NewFunctionTestInput(stringType, []string{""}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpSubstr,
			mysqlCode:  moerr.ER_REGEXP_ILLEGAL_ARGUMENT,
		},
		{
			name: "substr position after end and empty pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{"a"}, nil),
				NewFunctionTestInput(stringType, []string{""}, nil),
				NewFunctionTestInput(intType, []int64{2}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpSubstr,
			mysqlCode:  moerr.ER_REGEXP_ILLEGAL_ARGUMENT,
		},
		{
			name: "replace empty subject and pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, nil),
				NewFunctionTestInput(stringType, []string{""}, nil),
				NewFunctionTestInput(stringType, []string{"X"}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpReplace,
			mysqlCode:  moerr.ER_REGEXP_ILLEGAL_ARGUMENT,
		},
		{
			name: "instr null subject and empty pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{""}, nil),
			},
			resultType: intType,
			fn:         newOpBuiltInRegexp().builtInRegexpInstr,
			mysqlCode:  moerr.ER_REGEXP_ILLEGAL_ARGUMENT,
		},
		{
			name: "replace null subject and invalid match type",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{"a"}, nil),
				NewFunctionTestInput(stringType, []string{"X"}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(intType, []int64{0}, nil),
				NewFunctionTestInput(stringType, []string{"x"}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpReplace,
			mysqlCode:  moerr.ER_WRONG_ARGUMENTS,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc, tc.inputs, NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, tc.mysqlCode, moErr.MySQLCode())
			require.Equal(t, "HY000", moErr.SqlState())
		})
	}
}

func Test_BuiltIn_RegexpValidatesNumericArgumentsBeforeNullResult(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.T_varchar.ToType()
	intType := types.T_int64.ToType()
	returnOptionType := types.T_int64.ToType()

	for _, tc := range []struct {
		name       string
		inputs     []FunctionTestInput
		resultType types.Type
		fn         fEvalFn
		mysqlCode  uint16
		sqlState   string
	}{
		{
			name: "instr invalid return option",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{"a"}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(returnOptionType, []int64{-1}, nil),
				NewFunctionTestInput(stringType, []string{"c"}, nil),
			},
			resultType: intType,
			fn:         newOpBuiltInRegexp().builtInRegexpInstr,
			mysqlCode:  moerr.ER_WRONG_ARGUMENTS,
			sqlState:   "HY000",
		},
		{
			name: "instr invalid return option precedes invalid pattern",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{"*"}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(returnOptionType, []int64{-1}, nil),
				NewFunctionTestInput(stringType, []string{"c"}, nil),
			},
			resultType: intType,
			fn:         newOpBuiltInRegexp().builtInRegexpInstr,
			mysqlCode:  moerr.ER_WRONG_ARGUMENTS,
			sqlState:   "HY000",
		},
		{
			name: "substr invalid position",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{"a"}, nil),
				NewFunctionTestInput(intType, []int64{0}, nil),
				NewFunctionTestInput(intType, []int64{1}, nil),
				NewFunctionTestInput(stringType, []string{"c"}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpSubstr,
			mysqlCode:  moerr.ER_WRONG_PARAMETERS_TO_NATIVE_FCT,
			sqlState:   "42000",
		},
		{
			name: "replace invalid position",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{"a"}, nil),
				NewFunctionTestInput(stringType, []string{"X"}, nil),
				NewFunctionTestInput(intType, []int64{0}, nil),
				NewFunctionTestInput(intType, []int64{0}, nil),
				NewFunctionTestInput(stringType, []string{"c"}, nil),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpReplace,
			mysqlCode:  moerr.ER_WRONG_PARAMETERS_TO_NATIVE_FCT,
			sqlState:   "42000",
		},
		{
			name: "replace invalid pattern with null replacement",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
				NewFunctionTestInput(stringType, []string{""}, nil),
				NewFunctionTestInput(stringType, []string{""}, []bool{true}),
			},
			resultType: stringType,
			fn:         newOpBuiltInRegexp().builtInRegexpReplace,
			mysqlCode:  moerr.ER_REGEXP_ILLEGAL_ARGUMENT,
			sqlState:   "HY000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tcc := NewFunctionTestCase(
				proc, tc.inputs, NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(tcc.fnLength))
			_, err := tcc.DebugRun()
			var moErr *moerr.Error
			require.ErrorAs(t, err, &moErr)
			require.Equal(t, tc.mysqlCode, moErr.MySQLCode())
			require.Equal(t, tc.sqlState, moErr.SqlState())
		})
	}
}

func TestRegexpOptionalMatchTypeOverloads(t *testing.T) {
	utf8 := types.T_varchar.ToType()
	integer := types.T_int64.ToType()
	returnOption := types.T_int64.ToType()

	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{name: "regexp_instr", args: []types.Type{utf8, utf8, integer, integer, returnOption, utf8}},
		{name: "regexp_substr", args: []types.Type{utf8, utf8, integer, integer, utf8}},
		{name: "regexp_replace", args: []types.Type{utf8, utf8, utf8, integer, integer, utf8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
		})
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

func TestRegexpFunctionsPreserveNonBinaryInputs(t *testing.T) {
	utf8 := types.T_varchar.ToType()
	binary := types.T_binary.ToType()
	integer := types.T_int64.ToType()
	nullType := types.T_any.ToType()

	for _, tc := range []struct {
		name         string
		functionName string
		args         []types.Type
	}{
		{name: "regexp_operator_varchar", functionName: "reg_match", args: []types.Type{utf8, utf8}},
		{name: "regexp_like_null_subject", functionName: "regexp_like", args: []types.Type{nullType, utf8}},
		{name: "regexp_instr_numeric_subject", functionName: "regexp_instr", args: []types.Type{integer, utf8}},
		{name: "regexp_replace_varchar_replacement", functionName: "regexp_replace", args: []types.Type{utf8, utf8, utf8}},
		{name: "regexp_like_binary_match_type", functionName: "regexp_like", args: []types.Type{utf8, utf8, binary}},
		{name: "non_regexp_binary_arguments", functionName: "replace", args: []types.Type{binary, utf8, utf8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(context.Background(), tc.functionName, tc.args)
			require.NoError(t, err)
		})
	}
}

func TestRegexpCharacterSetValidationPreservesArityError(t *testing.T) {
	_, err := GetFunctionByName(context.Background(), "regexp_like", []types.Type{types.T_binary.ToType()})
	require.Error(t, err)

	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, moerr.ErrInvalidArg, moErr.ErrorCode())
	require.NotEqual(t, uint16(3995), moErr.MySQLCode())
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
