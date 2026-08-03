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
		{pat: "[0-9]", str: "abcDefg123456ABC", repl: "", pos: 10, ocr: 0, expected: "abcDefgABC"},
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
