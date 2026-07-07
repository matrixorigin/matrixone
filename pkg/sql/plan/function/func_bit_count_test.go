// Copyright 2021 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitCountInteger(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 1, 64, -1, -2}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 1, 1, 64, 63}, nil),
		BitCountInteger[int64])
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitCountString(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{
				"", "   ", "64", "+1", "-1", "-2", "255",
				"9223372036854775808",
				"18446744073709551615",
				"18446744073709551616",
				"-9223372036854775808",
				"-18446744073709551616",
			}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{
			0, 0, 1, 1, 64, 63, 8,
			1,
			64,
			64,
			1,
			1,
		}, nil),
		BitCountNonBinaryString)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitCountStringInvalidFormat(t *testing.T) {
	_, err := bitCountFromMysqlIntegerString("123abc")
	require.Error(t, err)

	_, err = bitCountFromMysqlIntegerString("1.9")
	require.Error(t, err)
}

func TestBitCountFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{
				1.4, 1.5, 2.5, 3.5,
				-1.4, -1.5, -2.5, -3.5,
				-9223372036854774784.0, -9223372036854775808.0,
				math.Inf(1), math.Inf(-1),
			}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{
			1, 1, 1, 1,
			64, 63, 63, 62,
			2, 1,
			64, 1,
		}, nil),
		BitCountFloat[float64])
	ok, info := tc.Run()
	require.True(t, ok, info)

	_, err := bitCountFromFloat(math.NaN(), proc)
	require.Error(t, err)
}

func TestBitCountDecimal64(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal64, 18, 1)
	inputs := []string{"1.4", "1.5", "2.5", "-1.4", "-1.5", "-2.5"}
	values := make([]types.Decimal64, len(inputs))
	for i, input := range inputs {
		v, err := types.ParseDecimal64(input, typ.Width, typ.Scale)
		require.NoError(t, err)
		values[i] = v
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 1, 2, 64, 63, 63}, nil),
		BitCountDecimal64)
	ok, info := tc.Run()
	require.True(t, ok, info)

	got, err := bitCountFromDecimal64(types.Decimal64Max, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(63), got)

	got, err = bitCountFromDecimal64(types.Decimal64Min, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)
}

func TestBitCountDecimal128(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal128, 38, 0)
	inputs := []string{
		"9223372036854775807",
		"9223372036854775808",
		"18446744073709551615",
		"-1",
		"-9223372036854775808",
		"-18446744073709551616",
	}
	values := make([]types.Decimal128, len(inputs))
	for i, input := range inputs {
		v, err := types.ParseDecimal128(input, typ.Width, typ.Scale)
		require.NoError(t, err)
		values[i] = v
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{63, 1, 64, 64, 1, 1}, nil),
		BitCountDecimal128)
	ok, info := tc.Run()
	require.True(t, ok, info)

	overflow, err := types.ParseDecimal128("18446744073709551616", typ.Width, typ.Scale)
	require.NoError(t, err)
	_, err = bitCountFromDecimal128(overflow, typ.Scale)
	require.Error(t, err)
}

func TestBitCountDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal256, 65, 1)
	values := []types.Decimal256{
		mustParseDecimal256(t, "1.5", 1),
		mustParseDecimal256(t, "2.5", 1),
		mustParseDecimal256(t, "-1.5", 1),
		mustParseDecimal256(t, "9223372036854775807.0", 1),
		mustParseDecimal256(t, "9223372036854775808.0", 1),
		mustParseDecimal256(t, "18446744073709551615.0", 1),
		mustParseDecimal256(t, "-1.0", 1),
		mustParseDecimal256(t, "-18446744073709551616.0", 1),
	}

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(typ, values, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 2, 63, 63, 1, 64, 64, 1}, nil),
		BitCountDecimal256)
	ok, info := tc.Run()
	require.True(t, ok, info)

	overflow := mustParseDecimal256(t, "18446744073709551616.0", typ.Scale)
	_, err := bitCountFromDecimal256(overflow, typ.Scale)
	require.Error(t, err)
}

func TestBitCountBinaryString(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varbinary.ToType(), []string{"64", "@", "\xff"}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{7, 1, 8}, nil),
		BitCountBinaryString)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitCountVarcharWithIsBin(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	input := testutil.MakeVarlenaVector(
		[][]byte{[]byte("64"), []byte("@"), {0x00}, {0xff}},
		nil,
		types.T_varchar.ToType(),
		mp,
	)
	defer input.Free(mp)
	input.SetIsBin(true)

	result := vector.NewFunctionResultWrapper(types.T_uint64.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(input.Length()))

	require.NoError(t, BitCountNonBinaryString([]*vector.Vector{input}, result, proc, input.Length(), nil))
	require.Equal(t, []uint64{7, 1, 0, 8}, vector.MustFixedColNoTypeCheck[uint64](result.GetResultVector()))
}

func TestBitCountTypeCheck(t *testing.T) {
	ctx := context.Background()

	get, err := GetFunctionByName(ctx, "bit_count", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(BIT_COUNT), get.fid)
	require.Equal(t, int32(13), get.overloadId)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_binary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_varbinary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)

	get, err = GetFunctionByName(ctx, "bit_count", []types.Type{types.T_blob.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(14), get.overloadId)
}
