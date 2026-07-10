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
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitwiseUint64UsesUnsignedResult(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{math.MaxUint64, math.MaxUint64}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{8, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxUint64, math.MaxUint64}, nil),
		operatorOpBitOrUint64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseUint64RightShiftIsLogical(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{math.MaxUint64, math.MaxUint64, 1}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 64, math.MaxUint64}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxInt64, 0, 0}, nil),
		operatorOpBitShiftRightUint64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseUint64PreservesHighBit(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{uint64(math.MaxInt64) + 1}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{8}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{uint64(math.MaxInt64) + 1 + 8}, nil),
		operatorOpBitOrUint64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseNumericArgumentsCastToUint64(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"int8", []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}},
		{"int16", []types.Type{types.T_int16.ToType(), types.T_int64.ToType()}},
		{"int32", []types.Type{types.T_int32.ToType(), types.T_int64.ToType()}},
		{"int64", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}},
		{"uint8", []types.Type{types.T_uint8.ToType(), types.T_int64.ToType()}},
		{"uint16", []types.Type{types.T_uint16.ToType(), types.T_int64.ToType()}},
		{"uint32", []types.Type{types.T_uint32.ToType(), types.T_int64.ToType()}},
		{"mixed integers", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()}},
		{"decimal64", []types.Type{types.New(types.T_decimal64, 10, 0), types.T_int64.ToType()}},
		{"decimal128", []types.Type{types.New(types.T_decimal128, 20, 0), types.T_int64.ToType()}},
		{"float32", []types.Type{types.T_float32.ToType(), types.T_int64.ToType()}},
		{"float64", []types.Type{types.T_float64.ToType(), types.T_int64.ToType()}},
		{"char", []types.Type{types.T_char.ToType(), types.T_int64.ToType()}},
		{"varchar", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}},
		{"text", []types.Type{types.T_text.ToType(), types.T_int64.ToType()}},
	} {
		for _, op := range []string{"&", "|", "^", "<<", ">>"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				get, err := GetFunctionByName(ctx, op, tc.args)
				require.NoError(t, err)
				targets, cast := get.ShouldDoImplicitTypeCast()
				require.True(t, cast)
				require.Equal(t, []types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, targets)
				require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
			})
		}
	}

	get, err := GetFunctionByName(ctx, "unary_tilde", []types.Type{types.New(types.T_decimal64, 10, 0)})
	require.NoError(t, err)
	targets, cast := get.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, []types.Type{types.T_uint64.ToType()}, targets)
	require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
}

func TestBitwiseBinaryArgumentsKeepBytewiseOverloads(t *testing.T) {
	ctx := context.Background()
	get, err := GetFunctionByName(ctx, "|", []types.Type{types.T_binary.ToType(), types.T_binary.ToType()})
	require.NoError(t, err)
	targets, cast := get.ShouldDoImplicitTypeCast()
	require.False(t, cast)
	require.Nil(t, targets)
	require.Equal(t, int32(1), get.overloadId)
	require.Equal(t, types.T_binary, get.GetReturnType().Oid)

	get, err = GetFunctionByName(ctx, "|", []types.Type{types.T_varbinary.ToType(), types.T_varbinary.ToType()})
	require.NoError(t, err)
	targets, cast = get.ShouldDoImplicitTypeCast()
	require.False(t, cast)
	require.Nil(t, targets)
	require.Equal(t, int32(2), get.overloadId)
	require.Equal(t, types.T_varbinary, get.GetReturnType().Oid)

	get, err = GetFunctionByName(ctx, "|", []types.Type{types.T_varbinary.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	targets, cast = get.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, []types.Type{types.T_varbinary.ToType(), types.T_varbinary.ToType()}, targets)
	require.Equal(t, int32(2), get.overloadId)
	require.Equal(t, types.T_varbinary, get.GetReturnType().Oid)
}
