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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestRegexpInvalidBinaryOperandArities(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.T_varchar.ToType()
	for _, source := range []types.StringSource{types.StringSourceUserVariable, types.StringSourceSQLPrepare, types.StringSourceCOMStmt} {
		for _, tc := range []struct {
			name     string
			min, max int
			fn       func(*opBuiltInRegexp) fEvalFn
		}{
			{"reg_match", 2, 2, func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegMatch }},
			{"not_reg_match", 2, 2, func(op *opBuiltInRegexp) fEvalFn { return op.builtInNotRegMatch }},
			{"regexp_like", 2, 3, func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpLike }},
			{"regexp_instr", 2, 5, func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpInstr }},
			{"regexp_substr", 2, 4, func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpSubstr }},
			{"regexp_replace", 3, 5, func(op *opBuiltInRegexp) fEvalFn { return op.builtInRegexpReplace }},
		} {
			for arity := tc.min; arity <= tc.max; arity++ {
				t.Run(fmt.Sprintf("%d/%s/%d", source, tc.name, arity), func(t *testing.T) {
					inputs := []FunctionTestInput{
						NewFunctionTestInput(text, []string{"\xffa"}, nil),
						NewFunctionTestInput(text, []string{"."}, nil),
					}
					expected := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil)
					switch tc.name {
					case "not_reg_match":
						expected = NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, nil)
					case "regexp_instr":
						expected = NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil)
					case "regexp_substr":
						value := "ÿ"
						if source == types.StringSourceUserVariable {
							value = "\xff"
						}
						expected = NewFunctionTestResult(text, false, []string{value}, nil)
					case "regexp_replace":
						inputs = append(inputs, NewFunctionTestInput(text, []string{"X"}, nil))
						expected = NewFunctionTestResult(text, false, []string{"XX"}, nil)
					}
					for len(inputs) < arity {
						if tc.name == "regexp_like" {
							inputs = append(inputs, NewFunctionTestInput(text, []string{"c"}, nil))
						} else if tc.name == "regexp_instr" && len(inputs) == 4 {
							inputs = append(inputs, NewFunctionTestInput(types.T_int8.ToType(), []int8{0}, nil))
						} else {
							value := int64(1)
							if tc.name == "regexp_replace" && len(inputs) == 4 {
								value = 0
							}
							inputs = append(inputs, NewFunctionTestInput(types.T_int64.ToType(), []int64{value}, nil))
						}
					}
					test := NewFunctionTestCase(proc, inputs, expected, tc.fn(newOpBuiltInRegexp()))
					require.NoError(t, test.parameters[0].SetStringSource(source))
					require.NoError(t, test.parameters[0].SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, proc.Mp()))
					ok, info := test.Run()
					require.True(t, ok, info)
				})
			}
		}
	}
}

func TestRegexpTerminalBoundaryAndOccurrence(t *testing.T) {
	for _, tc := range []struct {
		subject  string
		binary   bool
		terminal int64
	}{
		{"éa", false, 3}, {"éa", true, 4}, {"\xffa", true, 3},
	} {
		op := newOpBuiltInRegexp()
		matched, value, err := op.regMap.regularSubstrWithMode(".", tc.subject, tc.terminal, 1, tc.binary)
		require.NoError(t, err)
		require.False(t, matched)
		require.Empty(t, value)
		matched, value, err = op.regMap.regularSubstrWithMode("$", tc.subject, tc.terminal, 1, tc.binary)
		require.NoError(t, err)
		require.True(t, matched)
		require.Empty(t, value)
		value, err = op.regMap.regularReplaceWithMode(".", tc.subject, "X", tc.terminal, 0, tc.binary)
		require.NoError(t, err)
		require.Equal(t, tc.subject, value)
		value, err = op.regMap.regularReplaceWithMode("$", tc.subject, "X", tc.terminal, 0, tc.binary)
		require.NoError(t, err)
		require.Equal(t, tc.subject+"X", value)
		_, err = op.regMap.regularInstrWithMode("$", tc.subject, tc.terminal, 1, 0, tc.binary)
		require.Error(t, err)
		_, _, err = op.regMap.regularSubstrWithMode(".", tc.subject, tc.terminal+1, 1, tc.binary)
		require.Error(t, err)
		_, err = op.regMap.regularReplaceWithMode(".", tc.subject, "X", tc.terminal+1, 0, tc.binary)
		require.Error(t, err)
	}
	for _, occurrence := range []int64{-1, 0, 1} {
		op := newOpBuiltInRegexp()
		matched, value, err := op.regMap.regularSubstr("a", "aba", 1, occurrence)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, "a", value)
		position, err := op.regMap.regularInstr("a", "aba", 1, occurrence, 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), position)
		value, err = op.regMap.regularReplace("a", "aba", "X", 1, occurrence)
		require.NoError(t, err)
		if occurrence == 0 {
			require.Equal(t, "XbX", value)
		} else {
			require.Equal(t, "Xba", value)
		}
	}
}
