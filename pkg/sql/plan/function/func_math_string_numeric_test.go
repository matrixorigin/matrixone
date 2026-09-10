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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExactMathStringNumericPrefixExecutors(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := NewFunctionTestInput(types.T_varchar.ToType(), []string{
		"1.5", "-1.5", " 1.5", "+1.5", "1.5tail", "abc", "",
	}, nil)

	run := func(name string, expect FunctionTestResult, op fEvalFn) {
		t.Helper()
		testCase := NewFunctionTestCase(proc, []FunctionTestInput{inputs}, expect, op)
		ok, info := testCase.Run()
		require.True(t, ok, "%s: %s", name, info)
	}
	run("abs", NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{1.5, 1.5, 1.5, 1.5, 1.5, 0, 0}, nil), AbsStr)
	run("sign", NewFunctionTestResult(types.T_int64.ToType(), false,
		[]int64{1, -1, 1, 1, 1, 0, 0}, nil), SignStr)
	run("ceil", NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{2, -1, 2, 2, 2, 0, 0}, nil), CeilStr)
	run("floor", NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{1, -2, 1, 1, 1, 0, 0}, nil), FloorStr)
	run("round", NewFunctionTestResult(types.T_float64.ToType(), false,
		[]float64{2, -2, 2, 2, 2, 0, 0}, nil), RoundStr)
	truncateInputs := []FunctionTestInput{
		inputs,
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, nil),
	}
	testCase := NewFunctionTestCase(proc, truncateInputs,
		NewFunctionTestResult(types.T_float64.ToType(), false,
			[]float64{1, -1, 1, 1, 1, 0, 0}, nil), TruncateStr)
	ok, info := testCase.Run()
	require.True(t, ok, "truncate: %s", info)

	nullInput := NewFunctionTestInput(types.T_varchar.ToType(), []string{"1.5", "ignored"}, []bool{false, true})
	testCase = NewFunctionTestCase(proc, []FunctionTestInput{nullInput},
		NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1.5, 0}, []bool{false, true}), AbsStr)
	ok, info = testCase.Run()
	require.True(t, ok, "NULL propagation: %s", info)
}

func TestExactMathStringNumericPrefixTypeMatching(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		args        []types.Type
		returnType  types.T
		shouldCast  bool
		targetTypes []types.T
	}{
		{"abs string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"abs char", []types.Type{types.T_char.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"round text", []types.Type{types.T_text.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"sign string", []types.Type{types.T_varchar.ToType()}, types.T_int64, true, []types.T{types.T_float64}},
		{"ceil string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"floor string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"round string", []types.Type{types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64}},
		{"truncate string", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"round string digits string", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"truncate string digits string", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_int64}},
		{"mod string", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"mod right string", []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"mod char", []types.Type{types.T_char.ToType(), types.T_int64.ToType()}, types.T_float64, true, []types.T{types.T_float64, types.T_float64}},
		{"abs int control", []types.Type{types.T_int64.ToType()}, types.T_int64, false, nil},
		{"mod int control", []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, types.T_int64, false, nil},
		{"round decimal control", []types.Type{types.New(types.T_decimal64, 10, 2)}, types.T_decimal64, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := GetFunctionByName(ctx, test.name[:indexOfSpace(test.name)], test.args)
			require.NoError(t, err)
			require.Equal(t, test.returnType, got.GetReturnType().Oid)
			targets, shouldCast := got.ShouldDoImplicitTypeCast()
			require.Equal(t, test.shouldCast, shouldCast)
			if test.targetTypes != nil {
				require.Len(t, targets, len(test.targetTypes))
				for i := range targets {
					require.Equal(t, test.targetTypes[i], targets[i].Oid)
				}
			}
		})
	}
}

func indexOfSpace(s string) int {
	for i := range s {
		if s[i] == ' ' {
			return i
		}
	}
	return len(s)
}
