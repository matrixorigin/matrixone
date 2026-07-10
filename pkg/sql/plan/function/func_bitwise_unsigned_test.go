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

func TestBitwiseInt64UsesUnsignedResult(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{8, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxUint64, math.MaxUint64}, nil),
		operatorOpBitOrInt64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseInt64RightShiftIsLogical(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -1, 1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 64, -1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxInt64, 0, 0}, nil),
		operatorOpBitShiftRightInt64Fn)
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

func TestUnaryTildeVarcharUsesInt64NumericCoercion(t *testing.T) {
	get, err := GetFunctionByName(context.Background(), "unary_tilde", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(3), get.overloadId)
	require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
}

func TestBitwiseMixedSignedUnsignedUsesUint64Result(t *testing.T) {
	get, err := GetFunctionByName(context.Background(), "|", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, get.GetReturnType().Oid)

	get, err = GetFunctionByName(context.Background(), "|", []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, get.GetReturnType().Oid)

	get, err = GetFunctionByName(context.Background(), "|", []types.Type{types.T_binary.ToType(), types.T_binary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(1), get.overloadId)
	require.Equal(t, types.T_binary, get.GetReturnType().Oid)

	get, err = GetFunctionByName(context.Background(), "|", []types.Type{types.T_varbinary.ToType(), types.T_varbinary.ToType()})
	require.NoError(t, err)
	require.Equal(t, int32(2), get.overloadId)
	require.Equal(t, types.T_varbinary, get.GetReturnType().Oid)
}

func TestBitwiseMixedSignedUnsignedUsesUnsignedBitPatterns(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{8, 8}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxUint64, 9}, nil),
		operatorOpBitOrUint64Int64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)

	tc = NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{math.MaxInt64}, nil),
		operatorOpBitShiftRightInt64Uint64Fn)
	ok, info = tc.Run()
	require.True(t, ok, info)
}
