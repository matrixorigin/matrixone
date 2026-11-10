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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestCastUnboundedCharTargetPreservesLargeStrings is the executor half of
// the omitted-length CAST(x AS CHAR) contract: with the target bound as the
// unbounded TEXT type, string sources of any size pass through the cast
// untouched, while an explicit bounded VARCHAR target keeps truncating.
func TestCastUnboundedCharTargetPreservesLargeStrings(t *testing.T) {
	proc := testutil.NewProcess(t)
	varcharMax := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	textTarget := types.T_text.ToType()

	cases := []struct {
		name string
		src  string
	}{
		{"above_64k_100000", strings.Repeat("ab", 50000)},
		{"exactly_at_boundary_65535", strings.Repeat("a", 65535)},
		{"one_over_boundary_65536", strings.Repeat("a", 65536)},
		{"multibyte_300000_bytes", strings.Repeat("你好", 50000)},
		{"empty", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Unbounded TEXT target: the whole value survives.
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(varcharMax, []string{c.src}, nil),
					NewFunctionTestInput(textTarget, []string{}, nil),
				},
				NewFunctionTestResult(textTarget, false, []string{c.src}, nil), NewCast)
			succeed, info := tc.Run()
			require.True(t, succeed, info)

			// NULL stays NULL through the same cast.
			tcNull := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(varcharMax, []string{c.src}, []bool{true}),
					NewFunctionTestInput(textTarget, []string{}, nil),
				},
				NewFunctionTestResult(textTarget, false, []string{""}, []bool{true}), NewCast)
			succeed, info = tcNull.Run()
			require.True(t, succeed, info)
		})
	}

	// Control: the explicit bounded VARCHAR(65535) target still truncates in
	// the normal (non-strict) cast mode; this fix must not loosen it.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(varcharMax, []string{strings.Repeat("a", 100000)}, nil),
			NewFunctionTestInput(varcharMax, []string{}, nil),
		},
		NewFunctionTestResult(varcharMax, false, []string{strings.Repeat("a", types.MaxVarcharLen)}, nil), NewCast)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
}

// TestCastUnboundedCharTargetPreservesLargeJson verifies the same contract
// for JSON sources: serialized documents larger than the VARCHAR default
// survive an omitted-length CHAR cast intact, and scalar JSON strings keep
// their unquoted content.
func TestCastUnboundedCharTargetPreservesLargeJson(t *testing.T) {
	proc := testutil.NewProcess(t)
	textTarget := types.T_text.ToType()
	varcharMax := types.New(types.T_varchar, types.MaxVarcharLen, 0)

	bigPayload := strings.Repeat("a", 100000)
	// The serialized form keeps every payload byte: {"s": "<payload>"}
	// The text feed is already in the canonical single-space marshal form
	// (see TestCastJsonToVarchar: {"k":"v"} marshals as {"k": "v"}), so the
	// expected result of the unbounded cast is the same text.
	marshaled := `{"s": "` + bigPayload + `"}`
	encoded := makeJSONEncodedFromText(t, []string{marshaled}, []bool{false})

	// Document JSON survives fully with an unbounded target.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encoded, nil),
			NewFunctionTestInput(textTarget, []string{}, nil),
		},
		NewFunctionTestResult(textTarget, false, []string{marshaled}, nil), NewCast)
	succeed, info := tc.Run()
	require.True(t, succeed, info)
	require.Len(t, marshaled, len(`{"s": "`)+len(bigPayload)+len(`"}`))

	// Scalar JSON string survives fully through explicit CAST, in its
	// serialized JSON form: MySQL's JSON conversion rules serialize the whole
	// JSON value, so the enclosing quotes are part of the output (#32374).
	scalar := `"` + bigPayload + `"`
	encodedScalar := makeJSONEncodedFromText(t, []string{scalar}, []bool{false})
	tcScalar := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encodedScalar, nil),
			NewFunctionTestInput(textTarget, []string{}, nil),
		},
		NewFunctionTestResult(textTarget, false, []string{scalar}, nil), NewExplicitCast)
	succeed, info = tcScalar.Run()
	require.True(t, succeed, info)

	// Escaped scalar content serializes with JSON escaping intact and stays
	// faithful to the original representation.
	escaped := "\"a\\\"b\""
	encodedEscaped := makeJSONEncodedFromText(t, []string{escaped}, []bool{false})
	tcEscaped := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encodedEscaped, nil),
			NewFunctionTestInput(textTarget, []string{}, nil),
		},
		NewFunctionTestResult(textTarget, false, []string{"\"a\\\"b\""}, nil), NewExplicitCast)
	succeed, info = tcEscaped.Run()
	require.True(t, succeed, info)

	// Control: implicit conversion keeps exposing the scalar string's
	// unquoted payload, so expression coercions are unchanged.
	tcImplicit := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encodedScalar, nil),
			NewFunctionTestInput(textTarget, []string{}, nil),
		},
		NewFunctionTestResult(textTarget, false, []string{bigPayload}, nil), NewCast)
	succeed, info = tcImplicit.Run()
	require.True(t, succeed, info)

	// Explicit CAST of the scalar string against the bounded VARCHAR(65535)
	// target truncates the serialized text by runes.
	tcScalarTrunc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encodedScalar, nil),
			NewFunctionTestInput(varcharMax, []string{}, nil),
		},
		NewFunctionTestResult(varcharMax, false,
			[]string{truncateStringByRunes(scalar, types.MaxVarcharLen)}, nil), NewExplicitCast)
	succeed, info = tcScalarTrunc.Run()
	require.True(t, succeed, info)

	// Control: with the explicit bounded VARCHAR(65535) target the same
	// document still truncates and JSON fidelity is lost.
	tcTrunc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encoded, nil),
			NewFunctionTestInput(varcharMax, []string{}, nil),
		},
		NewFunctionTestResult(varcharMax, false,
			[]string{truncateStringByRunes(marshaled, types.MaxVarcharLen)}, nil), NewCast)
	succeed, info = tcTrunc.Run()
	require.True(t, succeed, info)
}

// TestCastTsToTextTarget verifies the TS source keeps its cast capability set
// aligned with VARCHAR: casting a commit timestamp to the unbounded string
// target must remain supported and keep the same textual value as the
// VARCHAR cast.
func TestCastTsToTextTarget(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.True(t, IfTypeCastSupported(types.T_TS, types.T_text))
	require.True(t, IfTypeCastSupported(types.T_TS, types.T_varchar))

	ts := types.BuildTS(int64(1718000000)*int64(1e9), 0)

	fromText := types.T_text.ToType()
	tcText := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_TS.ToType(), []types.TS{ts}, nil),
			NewFunctionTestInput(fromText, []string{}, nil),
		},
		NewFunctionTestResult(fromText, false, []string{ts.ToString()}, nil), NewCast)
	succeed, info := tcText.Run()
	require.True(t, succeed, info)

	varcharMax := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	tcVarchar := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_TS.ToType(), []types.TS{ts}, nil),
			NewFunctionTestInput(varcharMax, []string{}, nil),
		},
		NewFunctionTestResult(varcharMax, false, []string{ts.ToString()}, nil), NewCast)
	succeed, info = tcVarchar.Run()
	require.True(t, succeed, info)
}
