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
