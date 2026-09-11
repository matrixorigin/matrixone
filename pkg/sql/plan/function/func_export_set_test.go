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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExportSetNumericBits(t *testing.T) {
	decimal64Scale1 := types.New(types.T_decimal64, 18, 1)
	decimal64Scale2 := types.New(types.T_decimal64, 18, 2)
	decimal128Scale20 := types.New(types.T_decimal128, 38, 20)

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

func TestExportSetAcceptsBooleanBits(t *testing.T) {
	resolved, err := GetFunctionByName(context.Background(), "export_set", []types.Type{
		types.T_bool.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	})
	require.NoError(t, err)
	_, shouldCast := resolved.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast)
}
