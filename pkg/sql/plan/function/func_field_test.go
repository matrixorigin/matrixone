// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFieldExactTypeResolution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inputs []types.Type
		idx    int
		target types.T
	}{
		{"decimal64", []types.Type{types.New(types.T_decimal64, 4, 2), types.New(types.T_decimal64, 3, 1)}, 11, types.T_decimal128},
		{"wide scale span", []types.Type{types.New(types.T_decimal128, 38, 0), types.New(types.T_decimal128, 38, 38)}, 11, types.T_decimal128},
		{"decimal256", []types.Type{types.New(types.T_decimal256, 65, 30), types.New(types.T_decimal128, 38, 0)}, 12, types.T_decimal256},
		{"integer decimal", []types.Type{types.T_uint64.ToType(), types.New(types.T_decimal64, 10, 3)}, 11, types.T_decimal128},
		{"mixed integer", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()}, 13, types.T_any},
		{"float control", []types.Type{types.T_float64.ToType(), types.New(types.T_decimal128, 38, 0)}, 10, types.T_float64},
		{"string control", []types.Type{types.T_varchar.ToType(), types.New(types.T_decimal128, 38, 0)}, 10, types.T_float64},
		{"bit", []types.Type{types.T_bit.ToType(), types.T_bit.ToType()}, 8, types.T_any},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fieldCheck(nil, tc.inputs)
			require.Equal(t, tc.idx, got.idx)
			if tc.target == types.T_any {
				require.Equal(t, succeedMatched, got.status)
			} else {
				require.Equal(t, succeedWithCast, got.status)
				for i, target := range got.finalType {
					require.Equal(t, tc.target, target.Oid)
					if target.IsDecimal() {
						require.Equal(t, tc.inputs[i].Scale, target.Scale)
					}
				}
			}
		})
	}
}

func TestFieldIntegerRepresentations(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_uint64.ToType(), []uint64{^uint64(0), 1, 7}, nil),
		NewFunctionTestInput(types.T_int8.ToType(), []int8{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int16.ToType(), []int16{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int32.ToType(), []int32{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint8.ToType(), []uint8{255, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint16.ToType(), []uint16{65535, 2, 8}, nil),
		NewFunctionTestInput(types.T_uint32.ToType(), []uint32{^uint32(0), 1, 8}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
	}
	fc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 7, 0}, nil), FieldInteger)
	ok, info := fc.Run()
	require.True(t, ok, info)
}

func TestFieldDecimalScalesAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal128, 38, 1), []types.Decimal128{{B0_63: 12}, {B0_63: 12}, {}}, []bool{false, false, true}),
		NewFunctionTestInput(types.New(types.T_decimal128, 38, 2), []types.Decimal128{{B0_63: 120}, {B0_63: 121}, {}}, []bool{false, true, false}),
		NewFunctionTestConstInput(types.New(types.T_decimal128, 38, 2), []types.Decimal128{{B0_63: 120}}, nil),
	}
	fc := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 2, 0}, nil), FieldDecimal128)
	ok, info := fc.Run()
	require.True(t, ok, info)

	d256Inputs := []FunctionTestInput{
		NewFunctionTestInput(types.New(types.T_decimal256, 65, 1), []types.Decimal256{{B0_63: 12}, {B0_63: 13}, {}}, []bool{false, false, true}),
		NewFunctionTestInput(types.New(types.T_decimal256, 65, 2), []types.Decimal256{{B0_63: 120}, {B0_63: 121}, {}}, nil),
	}
	fc = NewFunctionTestCase(proc, d256Inputs,
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 0, 0}, nil), FieldDecimal256)
	ok, info = fc.Run()
	require.True(t, ok, info)
	require.True(t, decimal256Equal(types.Decimal256{B0_63: 120}, types.Decimal256{B0_63: 12}, 2, 1))
	require.True(t, decimal256Equal(types.Decimal256{B0_63: 12}, types.Decimal256{B0_63: 12}, 1, 1))
	maximum, err := types.ParseDecimal256("99999999999999999999999999999999999999999999999999999999999999999", 65, 0)
	require.NoError(t, err)
	require.False(t, decimal256Equal(maximum, types.Decimal256{B0_63: 1}, 0, 30))
	require.False(t, decimal256Equal(types.Decimal256{B0_63: 1}, maximum, 30, 0))
}
