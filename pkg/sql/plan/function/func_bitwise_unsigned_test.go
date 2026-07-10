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

func TestBitwiseInt64UsesUnsignedBitPattern(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{8}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{math.MaxUint64}, nil),
		operatorOpBitOrInt64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseInt64RightShiftUsesUnsignedBitPattern(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 64}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{math.MaxInt64, 0}, nil),
		operatorOpBitShiftRightInt64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseMixedIntegerInputsUseUnsignedBitPatterns(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{8}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{math.MaxUint64}, nil),
		operatorOpBitOrUint64Int64Fn)
	ok, info := tc.Run()
	require.True(t, ok, info)

	tc = NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{math.MaxInt64}, nil),
		operatorOpBitShiftRightInt64Uint64Fn)
	ok, info = tc.Run()
	require.True(t, ok, info)
}

func TestBitwiseNumericArgumentsKeepSignedAndUnsignedPaths(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		args        []types.Type
		shouldCast  bool
		targetTypes []types.Type
		overloadID  int32
	}{
		{"int64", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, false, nil, 0},
		{"varchar", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, true, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, 0},
		{"uint64", []types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, false, nil, 3},
		{"uint64 int64", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()}, false, nil, 4},
		{"int64 uint64", []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()}, false, nil, 5},
	} {
		for _, op := range []string{"&", "|", "^"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				get, err := GetFunctionByName(ctx, op, tc.args)
				require.NoError(t, err)
				targets, cast := get.ShouldDoImplicitTypeCast()
				require.Equal(t, tc.shouldCast, cast)
				require.Equal(t, tc.targetTypes, targets)
				require.Equal(t, tc.overloadID, get.overloadId)
				require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
			})
		}
	}

	for _, tc := range []struct {
		name       string
		args       []types.Type
		overloadID int32
	}{
		{"int64", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, 0},
		{"varchar", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, 0},
		{"uint64", []types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, 1},
		{"uint64 int64", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()}, 2},
		{"int64 uint64", []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()}, 3},
	} {
		for _, op := range []string{"<<", ">>"} {
			t.Run(tc.name+"/"+op, func(t *testing.T) {
				get, err := GetFunctionByName(ctx, op, tc.args)
				require.NoError(t, err)
				require.Equal(t, tc.overloadID, get.overloadId)
				require.Equal(t, types.T_uint64, get.GetReturnType().Oid)
			})
		}
	}

	get, err := GetFunctionByName(ctx, "unary_tilde", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	targets, cast := get.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, []types.Type{types.T_int64.ToType()}, targets)
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
