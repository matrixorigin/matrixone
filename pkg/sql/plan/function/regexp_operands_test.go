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

// Expectations come from MySQL 8.4.8, not from the execution-domain helpers.
// Static acceptance has its own matrix in TestRegexpStringDomainCheckModeMatrix:
// these vectors exercise only the runtime contract after successful binding.
func TestRegexpIndependentOperandRoles(t *testing.T) {
	proc := testutil.NewProcess(t)
	text := types.T_varchar.ToType()
	integer := types.T_int64.ToType()
	for _, source := range []types.StringSource{
		types.StringSourceExpression, types.StringSourceLiteral,
		types.StringSourceUserVariable, types.StringSourceSQLPrepare, types.StringSourceCOMStmt,
	} {
		for mask := 0; mask < 4; mask++ {
			subjectBinary, patternBinary := mask&1 != 0, mask&2 != 0
			resultBinary := mask != 0 && source != types.StringSourceSQLPrepare && source != types.StringSourceCOMStmt
			for _, name := range []string{"reg_match", "not_reg_match", "regexp_like", "regexp_instr", "regexp_substr", "regexp_replace"} {
				minArity, maxArity := 2, 2
				switch name {
				case "regexp_like":
					maxArity = 3
				case "regexp_instr":
					maxArity = 5
				case "regexp_substr":
					maxArity = 4
				case "regexp_replace":
					minArity, maxArity = 3, 5
				}
				for arity := minArity; arity <= maxArity; arity++ {
					t.Run(fmt.Sprintf("source_%d/mask_%d/%s_%d", source, mask, name, arity), func(t *testing.T) {
						op := newOpBuiltInRegexp()
						pattern := "é"
						var fn fEvalFn
						var expected FunctionTestResult
						equal := subjectBinary == patternBinary
						switch name {
						case "reg_match":
							fn = op.builtInRegMatch
							expected = NewFunctionTestResult(types.T_bool.ToType(), false, []bool{equal, false}, []bool{false, true})
						case "not_reg_match":
							fn = op.builtInNotRegMatch
							expected = NewFunctionTestResult(types.T_bool.ToType(), false, []bool{!equal, false}, []bool{false, true})
						case "regexp_like":
							fn = op.builtInRegexpLike
							expected = NewFunctionTestResult(types.T_bool.ToType(), false, []bool{equal, false}, []bool{false, true})
						case "regexp_instr":
							fn, pattern = op.builtInRegexpInstr, "a"
							position := int64(2)
							if subjectBinary {
								position = 3
							}
							expected = NewFunctionTestResult(integer, false, []int64{position, 0}, []bool{false, true})
						case "regexp_substr":
							fn, pattern = op.builtInRegexpSubstr, "."
							value := "é"
							if resultBinary {
								value = "\xe9"
							}
							if subjectBinary {
								value = "Ã"
								if resultBinary {
									value = "\xc3"
								}
							}
							if arity >= 3 {
								value = "a"
								if subjectBinary {
									value = "©"
									if resultBinary {
										value = "\xa9"
									}
								}
							}
							expected = NewFunctionTestResult(text, false, []string{value, ""}, []bool{false, true})
						case "regexp_replace":
							fn, pattern = op.builtInRegexpReplace, "."
							value := "XX"
							if subjectBinary {
								value = "XXX"
							}
							expected = NewFunctionTestResult(text, false, []string{value, ""}, []bool{false, true})
						}
						inputs := []FunctionTestInput{
							NewFunctionTestInput(text, []string{"éa", ""}, []bool{false, true}),
							NewFunctionTestInput(text, []string{pattern, pattern}, nil),
						}
						if name == "regexp_replace" {
							inputs = append(inputs, NewFunctionTestInput(text, []string{"X", "X"}, nil))
						}
						for len(inputs) < arity {
							if name == "regexp_like" {
								inputs = append(inputs, NewFunctionTestInput(text, []string{"c", "c"}, nil))
							} else if name == "regexp_instr" && len(inputs) == 4 {
								inputs = append(inputs, NewFunctionTestInput(types.T_int8.ToType(), []int8{0, 0}, nil))
							} else {
								value := int64(1)
								if len(inputs) == 2 {
									value = 2
								}
								if name == "regexp_replace" && len(inputs) == 4 {
									value = 0
								}
								inputs = append(inputs, NewFunctionTestInput(integer, []int64{value, value}, nil))
							}
						}
						test := NewFunctionTestCase(proc, inputs, expected, fn)
						for position, binary := range []bool{subjectBinary, patternBinary} {
							require.NoError(t, test.parameters[position].SetStringSource(source))
							if binary {
								require.NoError(t, test.parameters[position].SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, proc.Mp()))
							}
						}
						ok, info := test.Run()
						require.True(t, ok, info)
						if name == "regexp_substr" || name == "regexp_replace" {
							require.Equal(t, resultBinary, test.GetResultVectorDirectly().GetIsBinaryStringAt(0))
						}
					})
				}
			}
		}
	}
}

func TestRegexpOutputEncoding(t *testing.T) {
	for _, tc := range []struct{ input, prefix string }{
		{"ASCII", "ASCII"}, {"éa", "éa"}, {"\xffa", ""}, {"a\xffb", "a"}, {"é\xc3", "é"},
	} {
		require.Equal(t, tc.prefix, regexpValidTextPrefix(tc.input))
	}
	for _, tc := range []struct{ text, binary string }{
		{"ASCII", "ASCII"}, {"é", "\xe9"}, {"€", "\x80"}, {"\u0081", "\x81"}, {"中", "?"},
	} {
		require.Equal(t, tc.binary, regexpTextToBinaryBytes(tc.text))
	}
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	require.Equal(t, string(allBytes), regexpTextToBinaryBytes(regexpBinaryBytesToText(string(allBytes))))
}
