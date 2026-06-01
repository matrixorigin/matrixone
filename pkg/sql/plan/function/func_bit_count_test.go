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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"64", "-1", "255"}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{1, 64, 8}, nil),
		BitCountNonBinaryString)
	ok, info := tc.Run()
	require.True(t, ok, info)
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
