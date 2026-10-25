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

func TestLegacyIntegerDecimalBindingIsVerificationOnly(t *testing.T) {
	ctx := t.Context()
	legacy := WithLegacyIntegerDecimalCoercions(ctx)
	require.False(t, legacyIntegerDecimalCoercions(nil))
	require.False(t, legacyIntegerDecimalCoercions(ctx))
	require.True(t, legacyIntegerDecimalCoercions(legacy))
	inputs := []types.Type{types.T_int64.ToType(), types.New(types.T_time, 0, 0)}
	for _, name := range []string{">", "=", "<=>", "+", "-", "*", "mod"} {
		t.Run(name, func(t *testing.T) {
			current, err := GetFunctionByName(ctx, name, inputs)
			require.NoError(t, err)
			old, err := GetFunctionByName(legacy, name, inputs)
			require.NoError(t, err)
			oldTargets, cast := old.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			require.Equal(t, types.New(types.T_decimal64, 18, 0), oldTargets[0])
			require.NotEqual(t, current.targetTypes, oldTargets)
			again, err := GetFunctionByName(ctx, name, inputs)
			require.NoError(t, err)
			require.Equal(t, current, again, "no shared binding policy can leak to another statement")
		})
	}
	// Even executing with the verification context must not relax a declared
	// DECIMAL target. Only synthesized binding targets are reconstructed.
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	proc.Ctx = legacy
	target := types.New(types.T_decimal64, 18, 0)
	castArgs := []types.Type{inputs[0], target}
	current, err := GetFunctionByName(ctx, "cast", castArgs)
	require.NoError(t, err)
	old, err := GetFunctionByName(legacy, "cast", castArgs)
	require.NoError(t, err)
	require.Equal(t, current, old, "explicit CAST binding must retain its declared target")
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(inputs[0], []int64{1000000000000000000}, nil),
		NewFunctionTestInput(target, []types.Decimal64{}, nil),
	}, NewFunctionTestResult(target, true, nil, nil), NewCast)
	t.Cleanup(fc.Free)
	_, err = fc.DebugRun()
	require.ErrorContains(t, err, "Decimal64(18,0)")
}

func TestLegacyDecimalRangeIndependentOfCurrentChecker(t *testing.T) {
	ctx := t.Context()
	for _, narrow := range []types.T{types.T_int8, types.T_int16, types.T_int32} {
		for _, bounds := range [][2]types.T{{types.T_int64, narrow}, {narrow, types.T_int64}} {
			t.Run(bounds[0].String()+"_"+bounds[1].String(), func(t *testing.T) {
				inputs := []types.Type{types.New(types.T_time, 0, 0), bounds[0].ToType(), bounds[1].ToType()}
				_, err := GetFunctionByName(ctx, "between", inputs)
				require.Error(t, err, "ordinary binding needs its binary-comparison fallback")
				legacy, err := GetFunctionByName(WithLegacyIntegerDecimalCoercions(ctx), "between", inputs)
				require.NoError(t, err, "historical BETWEEN must resolve independently of current checker success")
				targets, cast := legacy.ShouldDoImplicitTypeCast()
				require.True(t, cast)
				require.Equal(t, []types.Type{types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 18, 0)}, targets)
				require.Equal(t, encodeOverloadID(BETWEEN, 0), legacy.GetEncodedOverloadID())
				_, err = GetFunctionByName(ctx, "between", inputs)
				require.Error(t, err, "historical verification must not change normal binding")
			})
		}
	}
}

func TestLegacyDecimalRangeRetainsSignatureValidation(t *testing.T) {
	ctx := WithLegacyIntegerDecimalCoercions(t.Context())
	inputs := []types.Type{types.New(types.T_time, 0, 0), types.T_int64.ToType(), types.T_int16.ToType()}
	for _, invalid := range [][]types.Type{inputs[:2], append(append([]types.Type{}, inputs...), types.T_uint8.ToType()),
		{inputs[0], inputs[1], types.T_bool.ToType()}} {
		_, err := GetFunctionByName(ctx, "between", invalid)
		require.Error(t, err)
	}
	for _, flag := range []types.T{types.T_int8, types.T_uint16, types.T_bool} {
		_, err := GetFunctionByName(ctx, "in_range", append(append([]types.Type{}, inputs...), flag.ToType()))
		require.Error(t, err, "the boundary flag is not a numeric operand")
	}
	legacy, err := GetFunctionByName(ctx, "in_range", append(inputs, types.T_uint8.ToType()))
	require.NoError(t, err)
	targets, cast := legacy.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, types.T_uint8.ToType(), targets[3])
	require.Equal(t, encodeOverloadID(IN_RANGE, 0), legacy.GetEncodedOverloadID())
}
