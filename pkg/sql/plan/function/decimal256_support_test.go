// Copyright 2021 - 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func mustParseDecimal256(t *testing.T, value string, scale int32) types.Decimal256 {
	t.Helper()
	dec, err := types.ParseDecimal256(value, 65, scale)
	require.NoError(t, err)
	return dec
}

func hasOverload(functionID int, args ...types.T) bool {
	fn := allSupportedFunctions[functionID]
	for _, ov := range fn.Overloads {
		if len(ov.args) != len(args) {
			continue
		}
		matched := true
		for i := range args {
			if ov.args[i] != args[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func TestDecimal256CompareFns(t *testing.T) {
	proc := testutil.NewProcess(t)
	decType := types.New(types.T_decimal256, 65, 2)
	v1 := mustParseDecimal256(t, "1.23", 2)
	v2 := mustParseDecimal256(t, "2.34", 2)

	tests := []struct {
		name   string
		fn     executeLogicOfOverload
		expect []bool
		nulls  []bool
	}{
		{
			name:   "=",
			fn:     equalFn,
			expect: []bool{true, false, false, true, false},
			nulls:  []bool{false, false, false, false, true},
		},
		{
			name:   ">",
			fn:     greatThanFn,
			expect: []bool{false, true, false, false, false},
			nulls:  []bool{false, false, false, false, true},
		},
		{
			name:   ">=",
			fn:     greatEqualFn,
			expect: []bool{true, true, false, true, false},
			nulls:  []bool{false, false, false, false, true},
		},
		{
			name:   "!=",
			fn:     notEqualFn,
			expect: []bool{false, true, true, false, false},
			nulls:  []bool{false, false, false, false, true},
		},
		{
			name:   "<",
			fn:     lessThanFn,
			expect: []bool{false, false, true, false, false},
			nulls:  []bool{false, false, false, false, true},
		},
		{
			name:   "<=",
			fn:     lessEqualFn,
			expect: []bool{true, false, true, true, false},
			nulls:  []bool{false, false, false, false, true},
		},
	}

	inputs := []FunctionTestInput{
		NewFunctionTestInput(decType, []types.Decimal256{v1, v2, v1, v2, {}}, []bool{false, false, false, false, true}),
		NewFunctionTestInput(decType, []types.Decimal256{v1, v1, v2, v2, {}}, []bool{false, false, false, false, true}),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(types.T_bool.ToType(), false, tc.expect, tc.nulls), fEvalFn(tc.fn))
			ok, info := ftc.Run()
			require.True(t, ok, info)
		})
	}

	nullSafe := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{v1, v1, {}}, []bool{false, true, true}),
			NewFunctionTestInput(decType, []types.Decimal256{v1, v1, {}}, []bool{false, false, true}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, true}, []bool{false, false, false}),
		nullSafeEqualFn,
	)
	ok, info := nullSafe.Run()
	require.True(t, ok, info)
}

func TestDecimal256RangeAndInOperators(t *testing.T) {
	proc := testutil.NewProcess(t)
	decType := types.New(types.T_decimal256, 65, 2)
	v1 := mustParseDecimal256(t, "1.23", 2)
	v2 := mustParseDecimal256(t, "2.34", 2)
	v3 := mustParseDecimal256(t, "3.45", 2)
	left := mustParseDecimal256(t, "1.50", 2)
	right := mustParseDecimal256(t, "3.00", 2)

	require.True(t, hasOverload(IN, types.T_decimal256, types.T_decimal256))
	require.True(t, hasOverload(NOT_IN, types.T_decimal256, types.T_decimal256))

	betweenTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{v1, v2, v3, {}}, []bool{false, false, false, true}),
			NewFunctionTestConstInput(decType, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestConstInput(decType, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true, false, false}, []bool{false, false, false, true}),
		betweenImpl,
	)
	ok, info := betweenTC.Run()
	require.True(t, ok, info)

	inRangeTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{v1, v2, v3, {}}, []bool{false, false, false, true}),
			NewFunctionTestConstInput(decType, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestConstInput(decType, []types.Decimal256{right}, []bool{false}),
			NewFunctionTestConstInput(types.T_uint8.ToType(), []uint8{0}, []bool{false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true, false, false}, []bool{false, false, false, true}),
		inRangeImpl,
	)
	ok, info = inRangeTC.Run()
	require.True(t, ok, info)

	inTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{v1, v2, v3, {}}, []bool{false, false, false, true}),
			NewFunctionTestInput(decType, []types.Decimal256{v2, v3}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true, true, false}, []bool{false, false, false, true}),
		newOpOperatorFixedIn[types.Decimal256]().operatorIn,
	)
	ok, info = inTC.Run()
	require.True(t, ok, info)

	notInTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{v1, v2, v3, {}}, []bool{false, false, false, true}),
			NewFunctionTestInput(decType, []types.Decimal256{v2, v3}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false, false}, []bool{false, false, false, true}),
		newOpOperatorFixedIn[types.Decimal256]().operatorNotIn,
	)
	ok, info = notInTC.Run()
	require.True(t, ok, info)
}

func TestDecimal256ConditionalOperators(t *testing.T) {
	proc := testutil.NewProcess(t)
	decType := types.New(types.T_decimal256, 65, 2)
	v1 := mustParseDecimal256(t, "1.23", 2)
	v2 := mustParseDecimal256(t, "2.34", 2)

	require.NotEqual(t, failedFunctionParametersWrong, caseCheck(nil, []types.Type{types.T_bool.ToType(), decType, decType}).status)
	require.NotEqual(t, failedFunctionParametersWrong, iffCheck(nil, []types.Type{types.T_bool.ToType(), decType, decType}).status)
	require.True(t, hasOverload(COALESCE, types.T_decimal256))

	caseTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, true}, []bool{false, false, false}),
			NewFunctionTestInput(decType, []types.Decimal256{v1, v1, v1}, []bool{false, false, false}),
			NewFunctionTestInput(decType, []types.Decimal256{v2, v2, v2}, []bool{false, false, false}),
		},
		NewFunctionTestResult(decType, false, []types.Decimal256{v1, v2, v1}, []bool{false, false, false}),
		caseFn,
	)
	ok, info := caseTC.Run()
	require.True(t, ok, info)

	iffTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, true}, []bool{false, false, false}),
			NewFunctionTestInput(decType, []types.Decimal256{v1, v1, v1}, []bool{false, false, false}),
			NewFunctionTestInput(decType, []types.Decimal256{v2, v2, v2}, []bool{false, false, false}),
		},
		NewFunctionTestResult(decType, false, []types.Decimal256{v1, v2, v1}, []bool{false, false, false}),
		iffFn,
	)
	ok, info = iffTC.Run()
	require.True(t, ok, info)

	coalesceTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decType, []types.Decimal256{{}, v1, {}}, []bool{true, false, true}),
			NewFunctionTestInput(decType, []types.Decimal256{v2, v2, {}}, []bool{false, false, true}),
			NewFunctionTestInput(decType, []types.Decimal256{v1, v1, {}}, []bool{false, false, true}),
		},
		NewFunctionTestResult(decType, false, []types.Decimal256{v2, v1, {}}, []bool{false, false, true}),
		CoalesceGeneral[types.Decimal256],
	)
	ok, info = coalesceTC.Run()
	require.True(t, ok, info)
}
