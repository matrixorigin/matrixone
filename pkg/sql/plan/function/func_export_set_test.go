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

func runExportSetTestCase(t *testing.T, inputs []FunctionTestInput, wanted []string, nullList []bool) {
	t.Helper()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(types.T_varchar.ToType(), false, wanted, nullList),
		ExportSet,
	)
	succeeded, errInfo := testCase.Run()
	require.True(t, succeeded, errInfo)
}

func exportSetStringInput(values []string, nullList []bool) FunctionTestInput {
	return NewFunctionTestInput(types.T_varchar.ToType(), values, nullList)
}

func exportSetCountInput(typ types.Type, values any, constant bool) FunctionTestInput {
	if constant {
		return NewFunctionTestConstInput(typ, values, nil)
	}
	return NewFunctionTestInput(typ, values, nil)
}

func TestExportSetNumberOfBitsIntegerTypes(t *testing.T) {
	cases := []struct {
		name string
		typ  types.Type
		val  any
	}{
		{"int8", types.T_int8.ToType(), []int8{4}},
		{"int16", types.T_int16.ToType(), []int16{4}},
		{"int32", types.T_int32.ToType(), []int32{4}},
		{"int64", types.T_int64.ToType(), []int64{4}},
		{"uint8", types.T_uint8.ToType(), []uint8{4}},
		{"uint16", types.T_uint16.ToType(), []uint16{4}},
		{"uint32", types.T_uint32.ToType(), []uint32{4}},
		{"uint64", types.T_uint64.ToType(), []uint64{4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, constant := range []bool{false, true} {
				inputs := []FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), []int64{5}, nil),
					exportSetStringInput([]string{"Y"}, nil),
					exportSetStringInput([]string{"N"}, nil),
					exportSetStringInput([]string{","}, nil),
					exportSetCountInput(tc.typ, tc.val, constant),
				}
				runExportSetTestCase(t, inputs, []string{"Y,N,Y,N"}, nil)
			}
		})
	}
}

func TestExportSetNumberOfBitsBoundaries(t *testing.T) {
	allOnes := strings.Repeat("Y", 64)
	runExportSetTestCase(t, []FunctionTestInput{
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0, -1, 5}, nil),
		exportSetStringInput([]string{"Y", "Y", "Y"}, nil),
		exportSetStringInput([]string{"N", "N", "N"}, nil),
		exportSetStringInput([]string{"", "", "-"}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{0, -1, 65}, nil),
	}, []string{"", allOnes, "Y-N-Y-N" + strings.Repeat("-N", 60)}, nil)

	runExportSetTestCase(t, []FunctionTestInput{
		NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
		exportSetStringInput([]string{"Y"}, nil),
		exportSetStringInput([]string{"N"}, nil),
		exportSetStringInput([]string{""}, nil),
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{^uint64(0)}, nil),
	}, []string{allOnes}, nil)
}

func TestExportSetExplicitNullOptionalArguments(t *testing.T) {
	runExportSetTestCase(t, []FunctionTestInput{
		NewFunctionTestInput(types.T_int64.ToType(), []int64{5, 5}, nil),
		exportSetStringInput([]string{"Y", "Y"}, nil),
		exportSetStringInput([]string{"N", "N"}, nil),
		exportSetStringInput([]string{",", "-"}, []bool{false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{4, 4}, nil),
	}, []string{"Y,N,Y,N", ""}, []bool{false, true})

	runExportSetTestCase(t, []FunctionTestInput{
		NewFunctionTestInput(types.T_int64.ToType(), []int64{5, 5}, nil),
		exportSetStringInput([]string{"Y", "Y"}, nil),
		exportSetStringInput([]string{"N", "N"}, nil),
		exportSetStringInput([]string{",", ","}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{4, 4}, []bool{false, true}),
	}, []string{"Y,N,Y,N", ""}, []bool{false, true})
}

func TestExportSetBitsBitAndDecimalTypes(t *testing.T) {
	decimal64Five, err := types.ParseDecimal64("5.0", 18, 1)
	require.NoError(t, err)
	decimal64NegativeOne, err := types.ParseDecimal64("-1.0", 18, 1)
	require.NoError(t, err)
	decimal128Five, err := types.ParseDecimal128("5.0", 38, 1)
	require.NoError(t, err)
	decimal128NegativeOne, err := types.ParseDecimal128("-1.0", 38, 1)
	require.NoError(t, err)

	cases := []struct {
		name  string
		input FunctionTestInput
		want  []string
	}{
		{
			name:  "bit",
			input: NewFunctionTestInput(types.New(types.T_bit, 8, 0), []uint64{0x85}, nil),
			want:  []string{"Y,N,Y,N,N,N,N,Y"},
		},
		{
			name:  "decimal64",
			input: NewFunctionTestInput(types.New(types.T_decimal64, 18, 1), []types.Decimal64{decimal64Five, decimal64NegativeOne}, nil),
			want:  []string{"Y,N,Y,N", "Y,Y,Y,Y"},
		},
		{
			name:  "decimal128",
			input: NewFunctionTestInput(types.New(types.T_decimal128, 38, 1), []types.Decimal128{decimal128Five, decimal128NegativeOne}, nil),
			want:  []string{"Y,N,Y,N", "Y,Y,Y,Y"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bits := tc.input
			countValue := int64(4)
			if tc.name == "bit" {
				countValue = 8
			}
			countValues := make([]int64, len(tc.want))
			for i := range countValues {
				countValues[i] = countValue
			}
			inputs := []FunctionTestInput{
				bits,
				exportSetStringInput(make([]string, len(tc.want)), nil),
				exportSetStringInput(make([]string, len(tc.want)), nil),
				exportSetStringInput(make([]string, len(tc.want)), nil),
				NewFunctionTestInput(types.T_int64.ToType(), countValues, nil),
			}
			for i := range tc.want {
				inputs[1].values.([]string)[i] = "Y"
				inputs[2].values.([]string)[i] = "N"
				inputs[3].values.([]string)[i] = ","
			}
			runExportSetTestCase(t, inputs, tc.want, nil)

			if tc.name == "bit" {
				runExportSetTestCase(t, []FunctionTestInput{
					NewFunctionTestConstInput(bits.typ, []uint64{0x85}, nil),
					exportSetStringInput([]string{"Y"}, nil),
					exportSetStringInput([]string{"N"}, nil),
					exportSetStringInput([]string{","}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{8}, nil),
				}, tc.want[:1], nil)
			}
		})
	}
}

func TestExportSetDecimal128PreservesIntegerPrecision(t *testing.T) {
	value, err := types.ParseDecimal128("9007199254740993", 38, 0)
	require.NoError(t, err)
	wanted := []string{"Y"}
	parts := make([]string, 54)
	for bit := range parts {
		parts[bit] = "N"
	}
	parts[0] = "Y"
	parts[53] = "Y"
	wanted[0] = strings.Join(parts, ",")

	runExportSetTestCase(t, []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal128, 38, 0), []types.Decimal128{value}, nil),
		exportSetStringInput([]string{"Y"}, nil),
		exportSetStringInput([]string{"N"}, nil),
		exportSetStringInput([]string{","}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{54}, nil),
	}, wanted, nil)
}

func TestExportSetResultByteLengthZeroAndInvalid(t *testing.T) {
	size, ok := exportSetResultByteLength(0, []byte("Y"), []byte("N"), []byte(","), 0, 1024)
	require.True(t, ok)
	require.Zero(t, size)

	_, ok = exportSetResultByteLength(0, []byte("Y"), []byte("N"), []byte(","), -1, 1024)
	require.False(t, ok)
	_, ok = exportSetResultByteLength(0, []byte("Y"), []byte("N"), []byte(","), 65, 1024)
	require.False(t, ok)
}
