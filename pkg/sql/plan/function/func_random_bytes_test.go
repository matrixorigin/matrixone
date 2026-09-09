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
		{name: "numeric_text", typ: types.T_varchar.ToType(), values: []string{"2tail", "1.5", "\t+2tail"}, want: []int{2, 1, 2}},
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
		{name: "decimal", kind: vector.PrepareParamDecimal, value: "2.5", want: 3},
		{name: "decimal_exact_precision", kind: vector.PrepareParamDecimal, value: "2.5000000000000001", want: 3},
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

func TestRandomBytesDecimalUsesExactSQLRounding(t *testing.T) {
	proc := testutil.NewProcess(t)
	run := func(t *testing.T, typ types.Type, values any, want []int) {
		t.Helper()
		caseTest := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(typ, values, nil)},
			NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
		require.NoError(t, caseTest.result.PreExtendAndReset(caseTest.fnLength))
		require.NoError(t, RandomBytes(caseTest.parameters, caseTest.result, proc, caseTest.fnLength, nil))
		result := caseTest.GetResultVectorDirectly()
		for row, length := range want {
			require.False(t, result.IsNull(uint64(row)))
			require.Len(t, result.GetBytesAt(row), length)
		}
	}
	runOutOfRange := func(t *testing.T, typ types.Type, value any) {
		t.Helper()
		caseTest := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(typ, value, nil)},
			NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)
		require.NoError(t, caseTest.result.PreExtendAndReset(1))
		err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
	}

	for _, tc := range []struct {
		name string
		typ  types.Type
		make func(string) any
	}{
		{
			name: "decimal64",
			typ:  types.New(types.T_decimal64, 18, 1),
			make: func(value string) any {
				parsed, err := types.ParseDecimal64(value, 18, 1)
				require.NoError(t, err)
				return []types.Decimal64{parsed}
			},
		},
		{
			name: "decimal128",
			typ:  types.New(types.T_decimal128, 38, 1),
			make: func(value string) any {
				parsed, err := types.ParseDecimal128(value, 38, 1)
				require.NoError(t, err)
				return []types.Decimal128{parsed}
			},
		},
		{
			name: "decimal256",
			typ:  types.New(types.T_decimal256, 76, 1),
			make: func(value string) any {
				parsed, err := types.ParseDecimal256(value, 76, 1)
				require.NoError(t, err)
				return []types.Decimal256{parsed}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc.typ, tc.make("0.5"), []int{1})
			run(t, tc.typ, tc.make("2.5"), []int{3})
			runOutOfRange(t, tc.typ, tc.make("1024.5"))
		})
	}

	// This value is exactly representable as a DECIMAL but not as a float64.
	// Keep the scale high enough to ensure a float conversion cannot silently
	// change the rounded integer.
	for _, tc := range []struct {
		name string
		typ  types.Type
		make func(string) any
	}{
		{
			name: "decimal64_precision",
			typ:  types.New(types.T_decimal64, 18, 16),
			make: func(value string) any {
				parsed, err := types.ParseDecimal64(value, 18, 16)
				require.NoError(t, err)
				return []types.Decimal64{parsed}
			},
		},
		{
			name: "decimal128_precision",
			typ:  types.New(types.T_decimal128, 38, 16),
			make: func(value string) any {
				parsed, err := types.ParseDecimal128(value, 38, 16)
				require.NoError(t, err)
				return []types.Decimal128{parsed}
			},
		},
		{
			name: "decimal256_precision",
			typ:  types.New(types.T_decimal256, 76, 16),
			make: func(value string) any {
				parsed, err := types.ParseDecimal256(value, 76, 16)
				require.NoError(t, err)
				return []types.Decimal256{parsed}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc.typ, tc.make("2.5000000000000001"), []int{3})
		})
	}

	// Decimal128 and Decimal256 use multiple 19-digit division chunks in
	// Decimal.Scale. RANDOM_BYTES must round the original decimal once rather
	// than round an intermediate chunk (for example, 0.49 must not become 1).
	for _, tc := range []struct {
		name string
		typ  types.Type
		make func(string) any
	}{
		{
			name: "decimal128_high_scale",
			typ:  types.New(types.T_decimal128, 38, 20),
			make: func(value string) any {
				parsed, err := types.ParseDecimal128(value, 38, 20)
				require.NoError(t, err)
				return []types.Decimal128{parsed}
			},
		},
		{
			name: "decimal256_high_scale",
			typ:  types.New(types.T_decimal256, 76, 40),
			make: func(value string) any {
				parsed, err := types.ParseDecimal256(value, 76, 40)
				require.NoError(t, err)
				return []types.Decimal256{parsed}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc.typ, tc.make("2.49"), []int{2})
			run(t, tc.typ, tc.make("1024.49"), []int{1024})
			runOutOfRange(t, tc.typ, tc.make("0.49"))
			runOutOfRange(t, tc.typ, tc.make("1024.5"))
		})
	}
}

func TestRandomBytesDecimalHighScalePathsAgree(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal128, 38, 20)
	value, err := types.ParseDecimal128("2.49", 38, 20)
	require.NoError(t, err)

	run := func(t *testing.T, input FunctionTestInput) int {
		t.Helper()
		caseTest := NewFunctionTestCase(proc, []FunctionTestInput{input},
			NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
		require.NoError(t, caseTest.result.PreExtendAndReset(caseTest.fnLength))
		require.NoError(t, RandomBytes(caseTest.parameters, caseTest.result, proc, caseTest.fnLength, nil))
		return len(caseTest.GetResultVectorDirectly().GetBytesAt(0))
	}

	require.Equal(t, 2, run(t, NewFunctionTestInput(typ, []types.Decimal128{value}, nil)))
	require.Equal(t, 2, run(t, NewFunctionTestConstInput(typ, []types.Decimal128{value}, nil)))

	prepared := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_text.ToType(), []string{"2.49"}, nil)},
		NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
	prepared.parameters[0].SetPrepareParamKind(vector.PrepareParamDecimal)
	require.NoError(t, prepared.result.PreExtendAndReset(1))
	require.NoError(t, RandomBytes(prepared.parameters, prepared.result, proc, 1, nil))
	require.Len(t, prepared.GetResultVectorDirectly().GetBytesAt(0), 2)
}

func TestRandomBytesHonorsRuntimeTextOverrideForBinaryCommonType(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("constant text override", func(t *testing.T) {
		input, err := vector.NewConstBytes(types.T_varbinary.ToType(), []byte("2"), 1, proc.Mp())
		require.NoError(t, err)
		defer input.Free(proc.Mp())
		require.NoError(t, input.SetRuntimeStringDomainWithMP(types.RuntimeStringText, proc.Mp()))

		result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(1))
		require.NoError(t, RandomBytes([]*vector.Vector{input}, result, proc, 1, nil))
		require.Len(t, result.GetResultVector().GetBytesAt(0), 2)
	})

	t.Run("mixed prepared text and masked binary", func(t *testing.T) {
		input := vector.NewVec(types.T_varbinary.ToType())
		defer input.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(input, []byte("2.5"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(input, []byte{0}, false, proc.Mp()))
		require.NoError(t, input.SetSelectedValueBinaryStringRowsWithMP([]bool{false, true}, proc.Mp()))
		input.SetPrepareParamKinds([]vector.PrepareParamKind{
			vector.PrepareParamDecimal,
			vector.PrepareParamNone,
		})

		result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(2))
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		require.NoError(t, RandomBytes([]*vector.Vector{input}, result, proc, 2, selectList))
		require.Len(t, result.GetResultVector().GetBytesAt(0), 3)
		require.True(t, result.GetResultVector().IsNull(1))
	})
}

func TestRandomBytesCoversAllAcceptedScalarGetterTypes(t *testing.T) {
	proc := testutil.NewProcess(t)
	year, err := types.ParseMoYearFromInt(1)
	require.NoError(t, err)

	tests := []struct {
		name    string
		typ     types.Type
		values  any
		want    int
		wantErr bool
	}{
		{name: "bit", typ: types.T_bit.ToType(), values: []uint64{1}, want: 1},
		{name: "int8", typ: types.T_int8.ToType(), values: []int8{1}, want: 1},
		{name: "int16", typ: types.T_int16.ToType(), values: []int16{1}, want: 1},
		{name: "int32", typ: types.T_int32.ToType(), values: []int32{1}, want: 1},
		{name: "int64", typ: types.T_int64.ToType(), values: []int64{1}, want: 1},
		{name: "uint8", typ: types.T_uint8.ToType(), values: []uint8{1}, want: 1},
		{name: "uint16", typ: types.T_uint16.ToType(), values: []uint16{1}, want: 1},
		{name: "uint32", typ: types.T_uint32.ToType(), values: []uint32{1}, want: 1},
		{name: "uint64", typ: types.T_uint64.ToType(), values: []uint64{1}, want: 1},
		{name: "float32", typ: types.T_float32.ToType(), values: []float32{1.5}, want: 2},
		{name: "float64", typ: types.T_float64.ToType(), values: []float64{1.5}, want: 2},
		{name: "year", typ: types.T_year.ToType(), values: []types.MoYear{year}, wantErr: true},
		{name: "char", typ: types.T_char.ToType(), values: []string{"2"}, want: 2},
		{name: "text", typ: types.T_text.ToType(), values: []string{"2"}, want: 2},
		{name: "blob", typ: types.T_blob.ToType(), values: []string{"\x02"}, want: 2},
		{name: "binary", typ: types.T_binary.ToType(), values: []string{"\x02"}, want: 2},
		{name: "varbinary", typ: types.T_varbinary.ToType(), values: []string{"\x02"}, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(tt.typ, tt.values, nil)},
				NewFunctionTestResult(types.T_blob.ToType(), false, nil, nil), RandomBytes)
			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, caseTest.GetResultVectorDirectly().GetBytesAt(0), tt.want)
		})
	}
}

func TestRandomBytesRejectsInvalidBinaryLengths(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "too_wide", value: "123456789"},
		{name: "signed_overflow", value: "\x80\x00\x00\x00\x00\x00\x00\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(types.T_binary.ToType(), []string{tc.value}, nil)},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)
			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
			require.Error(t, err)
		})
	}
}

func TestRandomBytesRejectsInvalidParameterShapeAndType(t *testing.T) {
	proc := testutil.NewProcess(t)
	result := vector.NewFunctionResultWrapper(types.T_blob.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.Error(t, randomBytesWithReader(nil, result, proc, 1, nil, func([]byte) (int, error) {
		return 0, nil
	}))
	require.Error(t, randomBytesWithReader([]*vector.Vector{nil}, result, proc, 1, nil, func([]byte) (int, error) {
		return 0, nil
	}))

	unsupported := vector.NewVec(types.T_date.ToType())
	defer unsupported.Free(proc.Mp())
	_, err := makeRandomBytesLengthGetter(unsupported, proc)
	require.Error(t, err)
}

func TestRandomBytesTreatsInvalidNumericTextAsRangeError(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, value := range []string{"abc", "\u00a02"} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{value}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)

			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
		})
	}
}

func TestRandomBytesTreatsInvalidPreparedDecimalAsRangeError(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, value := range []string{"1/2", "1e999", "1.2.3"} {
		t.Run(value, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_text.ToType(), []string{value}, nil),
				},
				NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), RandomBytes)
			caseTest.parameters[0].SetPrepareParamKind(vector.PrepareParamDecimal)

			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := RandomBytes(caseTest.parameters, caseTest.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrPreparedParamOutOfRange), err)
		})
	}
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
