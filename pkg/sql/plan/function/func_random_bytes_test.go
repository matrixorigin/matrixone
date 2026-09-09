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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRandomBytesTypeCheckAcceptsMysqlCoercibleArguments(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		typ      types.Type
		overload int32
	}{
		{typ: types.T_any.ToType(), overload: 2},
		{typ: types.T_bool.ToType(), overload: 2},
		{typ: types.T_int8.ToType(), overload: 2},
		{typ: types.T_int64.ToType(), overload: 0},
		{typ: types.T_uint64.ToType(), overload: 1},
		{typ: types.T_float64.ToType(), overload: 2},
		{typ: types.T_decimal64.ToType(), overload: 2},
		{typ: types.T_varchar.ToType(), overload: 2},
		{typ: types.T_binary.ToType(), overload: 2},
		{typ: types.T_year.ToType(), overload: 2},
	} {
		resolved, err := GetFunctionByName(ctx, "random_bytes", []types.Type{tc.typ})
		require.NoError(t, err, tc.typ)
		require.Equal(t, tc.overload, resolved.overloadId, tc.typ)
		require.False(t, resolved.needCast, tc.typ)
		require.Equal(t, types.T_blob, resolved.retType.Oid, tc.typ)
	}

	_, err := GetFunctionByName(ctx, "random_bytes", []types.Type{types.T_date.ToType()})
	require.Error(t, err)
}

func TestRandomBytesCoercesBoolTextFloatAndDecimalLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	decimalValue, err := types.Decimal64FromFloat64(1.5, 10, 1)
	require.NoError(t, err)

	tests := []struct {
		name   string
		typ    types.Type
		values any
		nulls  []bool
		want   []int
	}{
		{name: "bool", typ: types.T_bool.ToType(), values: []bool{true, false}, nulls: []bool{false, true}, want: []int{1, -1}},
		{name: "numeric_text", typ: types.T_varchar.ToType(), values: []string{"2tail", "1.5"}, want: []int{2, 1}},
		{name: "binary_integer", typ: types.T_binary.ToType(), values: []string{"\x02"}, want: []int{2}},
		{name: "float", typ: types.T_float64.ToType(), values: []float64{1.5, 2.5}, want: []int{2, 2}},
		{name: "decimal", typ: types.New(types.T_decimal64, 10, 1), values: []types.Decimal64{decimalValue}, want: []int{2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputs := []FunctionTestInput{NewFunctionTestInput(tt.typ, tt.values, tt.nulls)}
			caseTest := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
			require.NoError(t, caseTest.result.PreExtendAndReset(caseTest.fnLength))
			require.NoError(t, RandomBytes(caseTest.parameters, caseTest.result, proc, caseTest.fnLength, nil))

			result := caseTest.GetResultVectorDirectly()
			require.Equal(t, len(tt.want), result.Length())
			for i, want := range tt.want {
				if want < 0 {
					require.True(t, result.IsNull(uint64(i)))
					continue
				}
				require.False(t, result.IsNull(uint64(i)))
				require.Len(t, result.GetBytesAt(i), want)
			}
		})
	}
}

func TestRandomBytesUsesPreparedSourceKindForTextTransport(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name  string
		kind  vector.PrepareParamKind
		value string
		want  int
	}{
		{name: "float", kind: vector.PrepareParamFloat, value: "1.5", want: 2},
		{name: "decimal", kind: vector.PrepareParamDecimal, value: "2.5", want: 2},
		{name: "boolean", kind: vector.PrepareParamBoolean, value: "true", want: 1},
		{name: "long_integer_prefix", kind: vector.PrepareParamNone, value: "0000000000000000000000000000001024tail", want: 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_text.ToType(), []string{tc.value}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
			caseTest.parameters[0].SetPrepareParamKind(tc.kind)

			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			require.NoError(t, RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil))
			require.Len(t, caseTest.GetResultVectorDirectly().GetBytesAt(0), tc.want)
		})
	}
}

func TestRandomBytesTreatsInvalidNumericTextAsRangeError(t *testing.T) {
	proc := testutil.NewProcess(t)
	caseTest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)

	require.NoError(t, caseTest.result.PreExtendAndReset(1))
	err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
}

func TestRandomBytesRejectsInvalidFloatLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.5, 1024.6, -1.5} {
		t.Run(fmt.Sprintf("%v", value), func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_float64.ToType(), []float64{value}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)

			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
		})
	}
}

func TestRandomBytesPreservesUntypedNullAndSkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := vector.NewConstNull(types.T_any.ToType(), 2, proc.Mp())
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, RandomBytes([]*vector.Vector{input}, result, proc, 2, nil))
	require.True(t, result.GetResultVector().IsNull(0))
	require.True(t, result.GetResultVector().IsNull(1))

	caseTest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"2", "not-a-number"}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	require.NoError(t, caseTest.result.PreExtendAndReset(2))
	require.NoError(t, RandomBytes(caseTest.parameters, caseTest.result, proc, 2, selectList))
	require.Len(t, caseTest.GetResultVectorDirectly().GetBytesAt(0), 2)
	require.True(t, caseTest.GetResultVectorDirectly().IsNull(1))
}

func TestRandomBytesAcceptsBoundsAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(
				types.T_int64.ToType(),
				[]int64{1, randomBytesMaxLength, 0},
				[]bool{false, false, true},
			),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil),
		RandomBytes,
	)

	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, RandomBytes(testCase.parameters, testCase.result, proc, testCase.fnLength, nil))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, testCase.fnLength, result.Length())
	require.False(t, result.IsNull(0))
	require.Len(t, result.GetBytesAt(0), 1)
	require.False(t, result.IsNull(1))
	require.Len(t, result.GetBytesAt(1), randomBytesMaxLength)
	require.True(t, result.IsNull(2))
}

func TestRandomBytesRejectsOutOfRangeSignedLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, length := range []int64{-1, 0, randomBytesMaxLength + 1, math.MaxInt64} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), []int64{length}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
				RandomBytes,
			)

			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := RandomBytes(testCase.parameters, testCase.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
			require.EqualError(t, err, "length value is out of range in 'random_bytes'")
		})
	}
}

func TestRandomBytesRejectsOutOfRangeUnsignedLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, length := range []uint64{randomBytesMaxLength + 1, math.MaxUint64} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_uint64.ToType(), []uint64{length}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
				RandomBytes,
			)

			require.NoError(t, testCase.result.PreExtendAndReset(1))
			err := RandomBytes(testCase.parameters, testCase.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
			require.EqualError(t, err, "length value is out of range in 'random_bytes'")
		})
	}
}

func TestRandomBytesSkipsMaskedOutOfRangeRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 0}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil),
		RandomBytes,
	)
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}

	require.NoError(t, testCase.result.PreExtendAndReset(testCase.fnLength))
	require.NoError(t, RandomBytes(testCase.parameters, testCase.result, proc, testCase.fnLength, selectList))

	result := testCase.GetResultVectorDirectly()
	require.Equal(t, 2, result.Length())
	require.False(t, result.IsNull(0))
	require.Len(t, result.GetBytesAt(0), 1)
	require.True(t, result.IsNull(1))
}

func TestRandomBytesReportsEntropySourceFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{16}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil),
		RandomBytes,
	)

	require.NoError(t, testCase.result.PreExtendAndReset(1))
	err := randomBytesWithReader(
		testCase.parameters,
		testCase.result,
		proc,
		1,
		nil,
		func([]byte) (int, error) { return 0, errors.New("entropy source unavailable") },
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
	require.EqualError(t, err, "internal error: random_bytes failed to generate 16 bytes: entropy source unavailable")
}
