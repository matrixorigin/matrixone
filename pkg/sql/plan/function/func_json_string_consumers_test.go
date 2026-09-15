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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONStringConsumerJSONOverloads(t *testing.T) {
	wantType := types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin)
	cases := []struct {
		name string
		args []types.Type
	}{
		{
			name: "concat",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "concat_ws",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_json.ToType()},
		},
		{
			name: "elt",
			args: []types.Type{types.T_int64.ToType(), types.T_json.ToType(), types.T_varchar.ToType()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(1), overloadIndex)
			_, shouldCast := got.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Equal(t, wantType, got.GetReturnType())
		})
	}
}

func TestJSONStringConsumerRegisteredExecutors(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	tests := []struct {
		name   string
		args   []types.Type
		inputs []FunctionTestInput
		want   string
	}{
		{
			name: "concat",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `{"a":1}`)}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"!"}, nil),
			},
			want: `{"a": 1}!`,
		},
		{
			name: "concat_ws",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_json.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `"|"`)}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `1`)}, nil),
			},
			want: `x"|"1`,
		},
		{
			name: "elt",
			args: []types.Type{types.T_int64.ToType(), types.T_json.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `[1]`)}, nil),
			},
			want: `[1]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(proc.Ctx, tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(1), overloadIndex)

			inputs := make([]*vector.Vector, len(tc.inputs))
			for i, input := range tc.inputs {
				inputs[i] = newVectorByType(proc.Mp(), input.typ, input.values, nil)
			}
			defer func() {
				for _, input := range inputs {
					input.Free(proc.Mp())
				}
			}()

			result, err := RunFunctionDirectly(proc, got.GetEncodedOverloadID(), inputs, 1)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, tc.want, string(result.GetBytesAt(0)))
		})
	}
}

func TestJSONStringConsumerBinaryReturnType(t *testing.T) {
	binaryType := types.T_blob.ToType()
	for _, name := range []string{"concat", "concat_ws", "elt"} {
		t.Run(name, func(t *testing.T) {
			args := map[string][]types.Type{
				"concat":    []types.Type{types.T_json.ToType(), binaryType},
				"concat_ws": []types.Type{types.T_json.ToType(), binaryType},
				"elt":       []types.Type{types.T_int64.ToType(), types.T_json.ToType(), binaryType},
			}[name]
			got, err := GetFunctionByName(context.Background(), name, args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(1), overloadIndex)
			require.Equal(t, binaryType, got.GetReturnType())
		})
	}
}

func TestJSONStringConsumerChecksRejectMissingOverload(t *testing.T) {
	missingJSONOverload := []overload{{overloadId: 0}}
	for _, tc := range []struct {
		name  string
		check func([]overload, []types.Type) checkResult
		args  []types.Type
	}{
		{name: "concat", check: builtInConcatCheck, args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()}},
		{name: "concat_ws", check: concatWsCheck, args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()}},
		{name: "elt", check: eltCheck, args: []types.Type{types.T_int64.ToType(), types.T_json.ToType()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.check(missingJSONOverload, tc.args)
			require.Equal(t, failedFunctionParametersWrong, got.status)
		})
	}
}

func TestJSONStringConsumersRejectMalformedStoredJSON(t *testing.T) {
	validJSON := mustJsonBinaryString(t, `1`)
	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		jsonAt int
		fn     fEvalFn
	}{
		{
			name: "concat",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
			},
			jsonAt: 0,
			fn:     builtInConcat,
		},
		{
			name: "concat_ws separator",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
			},
			jsonAt: 0,
			fn:     ConcatWs,
		},
		{
			name: "concat_ws value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"|"}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
			},
			jsonAt: 1,
			fn:     ConcatWs,
		},
		{
			name: "elt selected value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
			},
			jsonAt: 1,
			fn:     Elt,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			caseUnderTest := NewFunctionTestCase(
				proc, tc.inputs,
				NewFunctionTestResult(types.T_text.ToType(), true, nil, nil),
				tc.fn,
			)
			caseUnderTest.parameters[tc.jsonAt].GetBytesAt(0)[0] = 0xff
			ok, info := caseUnderTest.Run()
			require.True(t, ok, info)
		})
	}
}

func TestJSONStringConsumerCastsOnlyNonJSONOperands(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	tests := []struct {
		name       string
		args       []types.Type
		jsonAt     int
		castAt     int
		castType   types.T
		castWidth  int32
		returnType types.Type
	}{
		{
			name:       "concat",
			args:       []types.Type{types.T_json.ToType(), types.T_int64.ToType()},
			jsonAt:     0,
			castAt:     1,
			castType:   types.T_varchar,
			castWidth:  20,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin),
		},
		{
			name:       "concat_ws",
			args:       []types.Type{types.T_json.ToType(), types.T_int64.ToType(), types.T_json.ToType()},
			jsonAt:     0,
			castAt:     1,
			castType:   types.T_varchar,
			castWidth:  20,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin),
		},
		{
			name:       "elt",
			args:       []types.Type{types.T_int32.ToType(), types.T_json.ToType()},
			jsonAt:     1,
			castAt:     0,
			castType:   types.T_int64,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(proc.Ctx, tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			require.Equal(t, int32(1), overloadIndex)
			casts, shouldCast := got.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, types.T_json, casts[tc.jsonAt].Oid)
			require.Equal(t, tc.castType, casts[tc.castAt].Oid)
			if tc.castWidth != 0 {
				require.Equal(t, tc.castWidth, casts[tc.castAt].Width)
			}
			require.Equal(t, tc.returnType, got.GetReturnType())
		})
	}
}

func TestJSONStringConsumersUseVisibleJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	jsonValues := []string{
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `[1,true,"x"]`),
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `12.5`),
		mustJsonBinaryString(t, `false`),
		mustJsonBinaryString(t, `null`),
	}
	jsonNulls := []bool{false, false, false, false, false, true}
	prefix := []string{"p", "p", "p", "p", "p", "p"}
	suffix := []string{"s", "s", "s", "s", "s", "s"}
	concat := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), prefix, nil),
			NewFunctionTestInput(types.T_json.ToType(), jsonValues, jsonNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), suffix, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{
				`p{"a": 1}s`,
				`p[1, true, "x"]s`,
				`p"x"s`,
				`p12.5s`,
				`pfalses`,
				"",
			}, []bool{false, false, false, false, false, true}),
		builtInConcat,
	)
	ok, info := concat.Run()
	require.True(t, ok, info)

	textControl := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{`{"a":1}x`}, nil),
		builtInConcat,
	)
	ok, info = textControl.Run()
	require.True(t, ok, info)

	binaryValue := string([]byte{'a', 0, 0xff})
	binaryControl := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{binaryValue}, nil),
			NewFunctionTestInput(types.T_blob.ToType(), []string{"!"}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{binaryValue + "!"}, nil),
		builtInConcat,
	)
	ok, info = binaryControl.Run()
	require.True(t, ok, info)
}

func TestConcatWsJSONStringOperands(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	separator := []string{
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
	}
	values := []string{
		mustJsonBinaryString(t, `null`),
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `{"a":1}`),
		"",
		mustJsonBinaryString(t, `1`),
	}
	valueNulls := []bool{false, false, false, true, false}
	other := []string{"z", "z", "", "z", "z"}
	otherNulls := []bool{false, false, true, false, false}
	separatorNulls := []bool{false, false, false, false, true}

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), separator, separatorNulls),
			NewFunctionTestInput(types.T_json.ToType(), values, valueNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), other, otherNulls),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{
				`null"|"z`,
				`"x""|"z`,
				`{"a": 1}`,
				"z",
				"",
			}, []bool{false, false, false, false, true}),
		ConcatWs,
	)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestEltJSONStringOperandsAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	jsonValues := []string{
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `null`),
	}
	jsonNulls := []bool{false, false, false, false, false, true}
	thirdJSON := []string{
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
	}
	indices := []int64{1, 2, 0, 3, 4, 1}
	textValues := []string{"text", "text", "text", "text", "text", "text"}

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), indices, nil),
			NewFunctionTestInput(types.T_json.ToType(), jsonValues, jsonNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), textValues, nil),
			NewFunctionTestInput(types.T_json.ToType(), thirdJSON, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{`{"a": 1}`, "text", "", `[1, true]`, "", ""},
			[]bool{false, false, true, false, true, true}),
		Elt,
	)
	ok, info := tc.Run()
	require.True(t, ok, info)

	// The JSON vector is deliberately malformed. Every row selects the text
	// operand or is out of range, so ELT must never marshal the unselected JSON.
	unselected := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 0, 2}, nil),
			NewFunctionTestInput(types.T_json.ToType(), []string{
				mustJsonBinaryString(t, `1`),
				mustJsonBinaryString(t, `1`),
				mustJsonBinaryString(t, `1`),
			}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"chosen", "ignored", "chosen"}, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{"chosen", "", "chosen"}, []bool{false, true, false}),
		Elt,
	)
	// The vector admission path has already accepted these values. Mutating the
	// stored type byte after admission gives the executor an invalid, unselected
	// payload without teaching the test framework to admit malformed JSON.
	for i := 0; i < 3; i++ {
		unselected.parameters[1].GetBytesAt(i)[0] = 0xff
	}
	ok, info = unselected.Run()
	require.True(t, ok, info)
}
