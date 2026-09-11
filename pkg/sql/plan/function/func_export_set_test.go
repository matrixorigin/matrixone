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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExportSetNumericBits(t *testing.T) {
	decimal64Scale1 := types.New(types.T_decimal64, 18, 1)
	decimal64Scale2 := types.New(types.T_decimal64, 18, 2)
	decimal128Scale20 := types.New(types.T_decimal128, 38, 20)
	decimal256Scale1 := types.New(types.T_decimal256, 65, 1)

	parseDecimal64 := func(typ types.Type, values ...string) []types.Decimal64 {
		result := make([]types.Decimal64, len(values))
		for i, value := range values {
			parsed, err := types.ParseDecimal64(value, typ.Width, typ.Scale)
			require.NoError(t, err)
			result[i] = parsed
		}
		return result
	}
	parseDecimal128 := func(typ types.Type, values ...string) []types.Decimal128 {
		result := make([]types.Decimal128, len(values))
		for i, value := range values {
			parsed, err := types.ParseDecimal128(value, typ.Width, typ.Scale)
			require.NoError(t, err)
			result[i] = parsed
		}
		return result
	}

	tests := []struct {
		name   string
		typ    types.Type
		values any
		nulls  []bool
		want   []string
	}{
		{
			name: "float32 ties to even",
			typ:  types.T_float32.ToType(),
			values: []float32{
				0.4, 0.5, 1.4, 1.5, 1.9, 2.5, 3.5,
				-0.5, -1.5, -2.5, -3.5, 0,
			},
			nulls: []bool{false, false, false, false, false, false, false, false, false, false, false, true},
			want: []string{
				"NNNN", "NNNN", "YNNN", "NYNN", "NYNN", "NYNN", "NNYN",
				"NNNN", "NYYY", "NYYY", "NNYY", "",
			},
		},
		{
			name: "float64 ties to even",
			typ:  types.T_float64.ToType(),
			values: []float64{
				0.4, 0.5, 1.4, 1.5, 1.9, 2.5, 3.5,
				-0.5, -1.5, -2.5, -3.5, 0,
			},
			nulls: []bool{false, false, false, false, false, false, false, false, false, false, false, true},
			want: []string{
				"NNNN", "NNNN", "YNNN", "NYNN", "NYNN", "NYNN", "NNYN",
				"NNNN", "NYYY", "NYYY", "NNYY", "",
			},
		},
		{
			name:   "decimal64 scale 1",
			typ:    decimal64Scale1,
			values: parseDecimal64(decimal64Scale1, "0.4", "0.5", "1.4", "1.5", "2.5", "-0.5", "-1.5", "-2.5", "0"),
			nulls:  []bool{false, false, false, false, false, false, false, false, true},
			want:   []string{"NNNN", "YNNN", "YNNN", "NYNN", "YYNN", "YYYY", "NYYY", "YNYY", ""},
		},
		{
			name:   "decimal64 scale 2",
			typ:    decimal64Scale2,
			values: parseDecimal64(decimal64Scale2, "0.40", "0.50", "1.40", "1.50", "2.50", "-0.50", "-1.50", "-2.50", "0"),
			nulls:  []bool{false, false, false, false, false, false, false, false, true},
			want:   []string{"NNNN", "YNNN", "YNNN", "NYNN", "YYNN", "YYYY", "NYYY", "YNYY", ""},
		},
		{
			name: "decimal128 precision",
			typ:  decimal128Scale20,
			values: parseDecimal128(decimal128Scale20,
				"1.49999999999999999999", "1.50000000000000000000",
				"-1.49999999999999999999", "-1.50000000000000000000", "0"),
			nulls: []bool{false, false, false, false, true},
			want:  []string{"YNNN", "NYNN", "YYYY", "NYYY", ""},
		},
		{
			name: "decimal256 range",
			typ:  decimal256Scale1,
			values: []types.Decimal256{
				mustParseDecimal256(t, "2.5", 1),
				mustParseDecimal256(t, "9223372036854775808.5", 1),
				mustParseDecimal256(t, "-9223372036854775809.5", 1),
				{},
			},
			nulls: []bool{false, false, false, true},
			want:  []string{"YYNN", "YYYY", "NNNN", ""},
		},
		{
			name:   "boolean",
			typ:    types.T_bool.ToType(),
			values: []bool{true, false, false},
			nulls:  []bool{false, false, true},
			want:   []string{"YNNN", "NNNN", ""},
		},
		{
			name:   "signed integer control",
			typ:    types.T_int64.ToType(),
			values: []int64{0, 1, 2, 3, -1, 0},
			nulls:  []bool{false, false, false, false, false, true},
			want:   []string{"NNNN", "YNNN", "NYNN", "YYNN", "YYYY", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			tc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(test.typ, test.values, test.nulls),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{"Y"}, nil),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{"N"}, nil),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{""}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{4}, nil),
			}, NewFunctionTestResult(types.T_blob.ToType(), false, test.want, test.nulls), fEvalFn(ExportSet))
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestExportSetConstantAndSelection(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 4, 1)
	decimal, err := types.ParseDecimal64("1.5", decimalType.Width, decimalType.Scale)
	require.NoError(t, err)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	tc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestConstInput(decimalType, []types.Decimal64{decimal, decimal, decimal}, nil),
		NewFunctionTestConstInput(types.T_blob.ToType(), []string{"Y"}, nil),
		NewFunctionTestConstInput(types.T_blob.ToType(), []string{"N"}, nil),
		NewFunctionTestConstInput(types.T_blob.ToType(), []string{""}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{4}, nil),
	}, NewFunctionTestResult(types.T_blob.ToType(), false,
		[]string{"", "NYNN", "NYNN"}, []bool{true, false, false}), fEvalFn(ExportSet)).WithSelectList(
		&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}},
	)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestExportSetAcceptsBooleanAndDecimal256Bits(t *testing.T) {
	for _, bitsType := range []types.Type{
		types.T_bool.ToType(), types.New(types.T_decimal256, 65, 1),
	} {
		resolved, err := GetFunctionByName(context.Background(), "export_set", []types.Type{
			bitsType, types.T_varchar.ToType(), types.T_varchar.ToType(),
		})
		require.NoError(t, err)
		_, shouldCast := resolved.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast)
	}
}

func TestExportSetRejectsOutOfRangeFloats(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, test := range []struct {
		name  string
		typ   types.Type
		value any
	}{
		{name: "float32 NaN", typ: types.T_float32.ToType(), value: []float32{float32(math.NaN())}},
		{name: "float32 positive infinity", typ: types.T_float32.ToType(), value: []float32{float32(math.Inf(1))}},
		{name: "float32 out of range", typ: types.T_float32.ToType(), value: []float32{1e20}},
		{name: "float64 NaN", typ: types.T_float64.ToType(), value: []float64{math.NaN()}},
		{name: "float64 positive infinity", typ: types.T_float64.ToType(), value: []float64{math.Inf(1)}},
		{name: "float64 negative infinity", typ: types.T_float64.ToType(), value: []float64{math.Inf(-1)}},
		{name: "float64 positive out of range", typ: types.T_float64.ToType(), value: []float64{1e20}},
		{name: "float64 negative out of range", typ: types.T_float64.ToType(), value: []float64{-1e20}},
		{name: "float64 exclusive upper bound", typ: types.T_float64.ToType(), value: []float64{math.Exp2(63)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseTest := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(test.typ, test.value, nil),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{"Y"}, nil),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{"N"}, nil),
				NewFunctionTestConstInput(types.T_blob.ToType(), []string{""}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{4}, nil),
			}, NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), fEvalFn(ExportSet))
			require.NoError(t, caseTest.result.PreExtendAndReset(1))
			err := ExportSet(caseTest.parameters, caseTest.result, proc, 1, nil)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		})
	}
}
