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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// information_schema.COLUMNS.COLUMN_DEFAULT must expose the default value
// (MySQL behavior), not the SQL text of the literal.
func Test_BuiltIn_MoShowVisibleBinColumnDefault(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		origin string
		want   string
		null   bool
	}{
		{origin: "", null: true},
		{origin: "null", null: true},
		{origin: "NuLl", null: true},
		{origin: "'NULL'", want: "NULL"},
		{origin: "NULL", null: true},
		{origin: "'n'", want: "n"},
		{origin: "''", want: ""},
		{origin: "'it''s'", want: "it's"},
		{origin: `'a\\b'`, want: `a\b`},
		{origin: `'a\%b'`, want: `a\%b`},
		{origin: "'2024-01-01'", want: "2024-01-01"},
		{origin: `'a\nb\tc\r\0\b\Z'`, want: "a\nb\tc\r\x00\b\x1a"},
		{origin: `'a\_b'`, want: `a\_b`},
		{origin: `'a\qb'`, want: "aqb"},
		{origin: `'tail\\'`, want: `tail\`},
		{origin: `'tail\'`, want: `'tail\'`},
		{origin: `'unterminated`, want: `'unterminated`},
		{origin: "'a' + 'b'", want: "'a' + 'b'"},
		{origin: "9", want: "9"},
		{origin: "current_timestamp()", want: "current_timestamp()"},
		{origin: "(json_array())", want: "(json_array())"},
		{origin: "('x')", want: "('x')"},
		{origin: "'a' 'b'", want: "'a' 'b'"},
	} {
		t.Run(tc.origin, func(t *testing.T) {
			defBytes, err := types.Encode(&plan.Default{OriginString: tc.origin, NullAbility: true})
			require.NoError(t, err)
			input := tcTemp{
				info: "show visible bin column default",
				inputs: []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{string(defBytes)}, nil),
					NewFunctionTestInput(types.T_uint8.ToType(), []uint8{defaultExpr}, nil),
				},
				expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}),
			}
			tcc := NewFunctionTestCase(proc, input.inputs, input.expect, builtInMoShowVisibleBin)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, input.info, info)
		})
	}
}

func TestMoShowVisibleBinDefaultModesRemainSeparate(t *testing.T) {
	proc := testutil.NewProcess(t)
	defBytes, err := types.Encode(&plan.Default{OriginString: "'n'"})
	require.NoError(t, err)
	updateBytes, err := types.Encode(&plan.OnUpdate{OriginString: "'n'"})
	require.NoError(t, err)
	for _, tc := range []struct {
		mode    uint8
		payload []byte
		want    string
	}{
		{onUpdateExpr, updateBytes, "'n'"},
		{defaultExpr, defBytes, "n"},
		{defExprInShowColumns, defBytes, "n"},
	} {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{string(tc.payload)}, nil),
			NewFunctionTestInput(types.T_uint8.ToType(), []uint8{tc.mode}, nil),
		}
		expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil)
		testCase := NewFunctionTestCase(proc, inputs, expected, builtInMoShowVisibleBin)
		succeed, info := testCase.RunAndFree()
		require.True(t, succeed, info)
	}
}

// SHOW COLUMNS / DESC Default must expose the default value like MySQL
// (decoded literal), not a re-escaped SQL fragment. OriginString is already a
// SQL literal, so mode 4 must not apply formatStr (which doubled quotes).
func Test_BuiltIn_MoShowVisibleBinShowColumnsDefault(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		origin string
		want   string
		null   bool
	}{
		{origin: "", null: true},
		{origin: "null", null: true},
		{origin: "NULL", null: true},
		{origin: "'n'", want: "n"},
		{origin: "''", want: ""},
		{origin: "'it''s'", want: "it's"},
		{origin: "'x`y'", want: "x`y"},
		{origin: `'a\\b'`, want: `a\b`},
		{origin: "'NULL'", want: "NULL"},
		{origin: "9", want: "9"},
		{origin: "current_timestamp()", want: "current_timestamp()"},
		{origin: "(json_array())", want: "(json_array())"},
		{origin: "('x')", want: "('x')"},
	} {
		t.Run(tc.origin, func(t *testing.T) {
			defBytes, err := types.Encode(&plan.Default{OriginString: tc.origin, NullAbility: true})
			require.NoError(t, err)
			input := tcTemp{
				info: "show visible bin show columns default",
				inputs: []FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{string(defBytes)}, nil),
					NewFunctionTestInput(types.T_uint8.ToType(), []uint8{defExprInShowColumns}, nil),
				},
				expect: NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.null}),
			}
			tcc := NewFunctionTestCase(proc, input.inputs, input.expect, builtInMoShowVisibleBin)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, input.info, info)
		})
	}
}
