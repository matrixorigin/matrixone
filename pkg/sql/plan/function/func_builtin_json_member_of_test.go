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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func runJSONMemberOfCase(t *testing.T, inputs []FunctionTestInput, expected FunctionTestResult) {
	t.Helper()
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(proc, inputs, expected, jsonMemberOf)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfScalarAndNullSemantics(t *testing.T) {
	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{17, 7, 17}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[23,"abc",17,"ab",10]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, 1}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"17", "ab", "null"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[17,"ab",null]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 1, 0}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 17, 0}, []bool{true, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`[17]`, `null`, `null`}, []bool{false, true, false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0, 0}, []bool{true, true, false}))
}

func TestJSONMemberOfPreparedScalarKinds(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{"17", "17", "true", "true"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[17,true,"true"]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, 1, 1}, nil),
		jsonMemberOf,
	)
	testCase.parameters[0].SetPrepareParamKinds([]vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
		vector.PrepareParamBoolean,
		vector.PrepareParamNone,
	})
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfPreparedConcreteFloat32PreservesWireValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{"0.1"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"[0.10000000149011612]"}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
	testCase.parameters[0].SetPrepareParamKind(vector.PrepareParamFloat)
	testCase.parameters[0].SetPrepareParamType(types.T_float32)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfPreparedBinaryStringKeepsOpaqueDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	raw := string([]byte{0, 1, 2})
	array, err := bytejson.CreateByteJSON([]any{newTypedByteJson(bytejson.TpCodeOpaque, raw)})
	require.NoError(t, err)
	encoded, err := array.Marshal()
	require.NoError(t, err)

	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{raw}, nil),
			NewFunctionTestConstInput(types.T_json.ToType(), []string{string(encoded)}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
	testCase.parameters[0].SetIsBinaryString(true)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfJSONValuesAndNonArrayRHS(t *testing.T) {
	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{
				mustJsonBinaryString(t, `null`),
				mustJsonBinaryString(t, `1`),
			}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[null,1]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 1}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{
				mustJsonBinaryString(t, `[4,5]`),
				mustJsonBinaryString(t, `{"a":1}`),
				mustJsonBinaryString(t, `{"a":1}`),
			}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[[3,4],[4,5]]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, 0}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), []string{
				mustJsonBinaryString(t, `{"a":1}`),
				mustJsonBinaryString(t, `{"a":2}`),
			}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`{"a":1}`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0}, nil))
}

func TestJSONMemberOfSelectListAndInvalidJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2, 3}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`[1]`, `invalid`, `[3]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonMemberOf)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
	require.NoError(t, testCase.result.PreExtendAndReset(3))
	require.NoError(t, testCase.fn(testCase.parameters, testCase.result, proc, 3, selectList))
	result := testCase.result.GetResultVector()
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](result)[0])
	require.True(t, result.IsNull(1))
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](result)[2])

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[1`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil))
}

func TestJSONMemberOfLargeConstantArrayUsesExactComparator(t *testing.T) {
	values := make([]string, 0, 512)
	for i := 0; i < 512; i++ {
		values = append(values, fmt.Sprintf("%d", i))
	}
	array := "[" + strings.Join(values, ",") + "]"
	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 511, 512, 17}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{array}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 1, 0, 1}, nil))
}

func TestJSONMemberOfFunctionRegistration(t *testing.T) {
	ctx := context.Background()
	result, err := GetFunctionByName(ctx, "member of", []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetReturnType().Oid)

	_, err = GetFunctionByName(ctx, "member of", []types.Type{types.T_int64.ToType(), types.T_bool.ToType()})
	require.EqualError(t, err, "Invalid data type for JSON data in argument 2 to function member of; a JSON string or JSON type is required.")

	_, err = GetFunctionByName(ctx, "member of", []types.Type{types.T_array_float32.ToType(), types.T_varchar.ToType()})
	require.Error(t, err, "native SQL vectors must not be accepted as MEMBER OF left operands")
}
