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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestNumericValueCastRegistrationAndKinds(t *testing.T) {
	ctx := t.Context()
	argTypes := []types.Type{
		types.T_text.ToType(),
		types.T_float64.ToType(),
		types.T_bool.ToType(),
		types.T_int32.ToType(),
	}
	got, err := GetFunctionByNameWithOverload(ctx, "cast", argTypes, NumericValueCastOverload)
	require.NoError(t, err)
	require.Equal(t, int32(CAST), got.fid)
	require.Equal(t, NumericValueCastOverload, got.overloadId)
	require.Equal(t, types.T_float64.ToType(), got.retType)

	bad := append([]types.Type(nil), argTypes...)
	bad[2] = types.T_int8.ToType()
	_, err = GetFunctionByNameWithOverload(ctx, "cast", bad, NumericValueCastOverload)
	require.Error(t, err)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	sourceType := types.T_text.ToType()
	inputs := []FunctionTestInput{
		NewFunctionTestInput(sourceType, []string{"1", "1", "ignored", "1"}, []bool{false, false, true, false}),
		NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0}, nil),
		NewFunctionTestInput(types.T_bool.ToType(), []bool{false, true, false, false}, nil),
		NewFunctionTestConstInput(types.T_int32.ToType(), []int32{NumericValueCastPolicyComparison}, nil),
	}
	test := NewFunctionTestCase(proc, inputs,
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{1, 49, 0, 1}, []bool{false, false, true, false}),
		NewNumericValueCast)
	defer test.Free()

	source := test.parameters[0]
	require.False(t, source.GetIsBin())
	success, failure := test.Run()
	require.True(t, success, failure)
	require.False(t, source.GetIsBin(), "CAST9 must not mutate source string provenance")
}

func TestIntegerArgumentCastAcceptsExplicitSourceKind(t *testing.T) {
	ctx := t.Context()
	args := []types.Type{types.T_text.ToType(), types.T_int64.ToType(), types.T_bool.ToType()}
	for _, overload := range []int32{
		IntegerArgumentCastOverload,
		TruncatedIntegerArgumentCastOverload,
		TextIntegerBitsCastOverload,
		TemporalIntegerArgumentCastOverload,
	} {
		_, err := GetFunctionByNameWithOverload(ctx, "cast", args, overload)
		if overload == TextIntegerBitsCastOverload {
			// This owner has a uint64 target; its source-kind shape is checked
			// below with the matching physical target.
			continue
		}
		require.NoError(t, err, "overload %d should accept the explicit source kind", overload)
	}
	bitArgs := []types.Type{types.T_text.ToType(), types.T_uint64.ToType(), types.T_bool.ToType()}
	_, err := GetFunctionByNameWithOverload(ctx, "cast", bitArgs, TextIntegerBitsCastOverload)
	require.NoError(t, err)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	test := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_text.ToType(), []string{"1", "1"}, nil),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil),
		NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
	}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{49, 1}, nil), NewIntegerArgumentCast)
	defer test.Free()
	ok, info := test.Run()
	require.True(t, ok, info)
}
