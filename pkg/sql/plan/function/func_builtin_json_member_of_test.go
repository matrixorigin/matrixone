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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func TestJSONMemberOfYearUsesNumericJSONDomain(t *testing.T) {
	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2024}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[2024]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2024}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`["2024"]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, nil))
}

func TestJSONMemberOfPreparedYearUsesNumericJSONDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{"2024"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[2024]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
	testCase.parameters[0].SetPrepareParamKind(vector.PrepareParamInteger)
	testCase.parameters[0].SetPrepareParamType(types.T_year)
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

func TestJSONMemberOfStaticBinaryLeftKeepsOpaqueDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	raw := string([]byte{0, 1, 2})
	array, err := bytejson.CreateByteJSON([]any{newTypedByteJson(bytejson.TpCodeOpaque, raw)})
	require.NoError(t, err)
	encoded, err := array.Marshal()
	require.NoError(t, err)

	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_binary.ToType(), []string{raw}, nil),
			NewFunctionTestConstInput(types.T_json.ToType(), []string{string(encoded)}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
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

func TestJSONMemberOfNullLeftSkipsRightValidation(t *testing.T) {
	binaryCharset := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetBinary)
	tests := []struct {
		name  string
		right FunctionTestInput
	}{
		{
			name:  "numeric",
			right: NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
		},
		{
			name:  "binary",
			right: NewFunctionTestInput(types.T_binary.ToType(), []string{"[1]", "[1]", "[1]"}, nil),
		},
		{
			name:  "malformed-json",
			right: NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-json", "not-json", "not-json"}, nil),
		},
		{
			name:  "binary-charset",
			right: NewFunctionTestInput(binaryCharset, []string{"[1]", "[1]", "[1]"}, nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 0, 0}, []bool{true, true, true}),
					test.right,
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0, 0, 0}, []bool{true, true, true}),
				jsonMemberOf,
			)
			succeed, message := testCase.Run()
			require.True(t, succeed, message)
		})
	}

	proc := testutil.NewProcess(t)
	prepared := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
			NewFunctionTestInput(types.T_text.ToType(), []string{"1"}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		jsonMemberOf,
	)
	prepared.parameters[1].SetPrepareParamKind(vector.PrepareParamInteger)
	prepared.parameters[1].SetPrepareParamType(types.T_int64)
	succeed, message := prepared.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfNullRightSkipsPreparedLeftValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{"10"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
		jsonMemberOf,
	)
	testCase.parameters[0].SetPrepareParamType(types.T_bit)
	succeed, message := testCase.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfFunctionRegistration(t *testing.T) {
	ctx := context.Background()
	result, err := GetFunctionByName(ctx, "member of", []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetReturnType().Oid)

	result, err = GetFunctionByName(ctx, "member of", []types.Type{types.T_int64.ToType(), types.T_bool.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetReturnType().Oid)

	_, err = GetFunctionByName(ctx, "member of", []types.Type{types.T_array_float32.ToType(), types.T_varchar.ToType()})
	require.Error(t, err, "native SQL vectors must not be accepted as MEMBER OF left operands")
}

func TestJSONMemberOfRejectsInvalidRightDomainsAtExecution(t *testing.T) {
	want := "Cannot create a JSON value from a string with CHARACTER SET 'binary'."
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		testCase := NewFunctionTestCase(
			testutil.NewProcess(t),
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(oid.ToType(), []string{"[1]"}, nil),
			},
			NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
			jsonMemberOf,
		)
		require.NoError(t, testCase.result.PreExtendAndReset(1), oid.String())
		err := testCase.fn(testCase.parameters, testCase.result, testCase.proc, 1, nil)
		require.EqualError(t, err, want, oid.String())
		require.Equal(t, uint16(moerr.ER_INVALID_JSON_CHARSET), err.(*moerr.Error).MySQLCode(), oid.String())
	}

	binaryCharset := types.NewWithCharset(types.T_varchar, 32, 0, types.CharsetBinary)
	testCase := NewFunctionTestCase(
		testutil.NewProcess(t),
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			NewFunctionTestInput(binaryCharset, []string{"[1]"}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonMemberOf,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := testCase.fn(testCase.parameters, testCase.result, testCase.proc, 1, nil)
	require.EqualError(t, err, want)
	require.Equal(t, uint16(moerr.ER_INVALID_JSON_CHARSET), err.(*moerr.Error).MySQLCode())
}

func TestJSONMemberOfRejectsUnsupportedRightDomainAtExecution(t *testing.T) {
	testCase := NewFunctionTestCase(
		testutil.NewProcess(t),
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonMemberOf,
	)
	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := testCase.fn(testCase.parameters, testCase.result, testCase.proc, 1, nil)
	require.EqualError(t, err,
		"Invalid data type for JSON data in argument 2 to function member of; a JSON string or JSON type is required.")
}

func TestJSONMemberOfCountEvaluableRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 0, 2, 0}, []bool{false, true, false, true}),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"[1]"}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonMemberOf,
	)
	require.Equal(t, 2, jsonMemberOfCountEvaluableRows(testCase.parameters[0], 4, nil))
	require.False(t, jsonOverlapShouldPrepareScalar(512, 2))
	require.True(t, jsonOverlapShouldPrepareScalar(512, 20))
}

func TestJSONMemberOfRejectsBinaryRightRuntimeProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`[1]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonMemberOf,
	)
	testCase.parameters[1].SetIsBinaryString(true)
	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := testCase.fn(testCase.parameters, testCase.result, proc, 1, nil)
	require.EqualError(t, err, "Cannot create a JSON value from a string with CHARACTER SET 'binary'.")
	require.Equal(t, uint16(moerr.ER_INVALID_JSON_CHARSET), err.(*moerr.Error).MySQLCode())
}

func TestJSONMemberOfPreservesSpecialScalarDomains(t *testing.T) {
	bitValue, err := bitToJSON(0x10a, 9, context.Background())
	require.NoError(t, err)
	bitArray, err := bytejson.CreateByteJSON([]any{bitValue})
	require.NoError(t, err)
	bitJSON, err := bitArray.Marshal()
	require.NoError(t, err)

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_bit, 9, 0), []uint64{0x10a}, nil),
			NewFunctionTestInput(types.T_json.ToType(), []string{string(bitJSON)}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil))

	const geoJSON = `[{"type":"Point","coordinates":[1,2]}]`
	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{geoJSON}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil))

	runJSONMemberOfCase(t,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_geometry32.ToType(), []string{geom32WKB(t, "POINT(1 2)")}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{geoJSON}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil))
}

func TestJSONMemberOfPreparedSpecialDomains(t *testing.T) {
	const geoJSON = `[{"type":"Point","coordinates":[1,2]}]`
	preparedGeometry := NewFunctionTestCase(
		testutil.NewProcess(t),
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{string(encodeGeometryPayload("POINT(1 2)", 0, false))}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{geoJSON}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
	preparedGeometry.parameters[0].SetPrepareParamType(types.T_geometry)
	preparedGeometry.parameters[0].SetPrepareParamKind(vector.PrepareParamNone)
	succeed, message := preparedGeometry.Run()
	require.True(t, succeed, message)

	preparedEnum := NewFunctionTestCase(
		testutil.NewProcess(t),
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_text.ToType(), []string{"red"}, nil),
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{`["red"]`}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil),
		jsonMemberOf,
	)
	preparedEnum.parameters[0].SetPrepareParamType(types.T_enum)
	preparedEnum.parameters[0].SetPrepareParamKind(vector.PrepareParamNone)
	succeed, message = preparedEnum.Run()
	require.True(t, succeed, message)
}

func TestJSONMemberOfRejectsLossyPreparedDomains(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  types.T
		data any
	}{
		{name: "bit", typ: types.T_bit, data: "266"},
		{name: "vecf32", typ: types.T_array_float32, data: "[1 2]"},
		{name: "vecf64", typ: types.T_array_float64, data: "[1 2]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_text.ToType(), []string{test.data.(string)}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"[1]"}, nil),
				},
				NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
				jsonMemberOf,
			)
			testCase.parameters[0].SetPrepareParamType(test.typ)
			testCase.parameters[0].SetPrepareParamKind(vector.PrepareParamNone)
			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := testCase.fn(testCase.parameters, testCase.result, proc, 1, nil)
			require.Error(t, err)
		})
	}
}
