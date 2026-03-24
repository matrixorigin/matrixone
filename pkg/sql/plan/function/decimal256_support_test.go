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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func resolvedReturnType(t *testing.T, functionID int, inputs []types.Type) types.Type {
	t.Helper()
	fn := allSupportedFunctions[functionID]
	result := fn.checkFn(fn.Overloads, inputs)
	require.NotEqual(t, failedFunctionParametersWrong, result.status)

	finalTypes := inputs
	if result.status == succeedWithCast {
		finalTypes = result.finalType
	}
	return fn.Overloads[result.idx].retType(finalTypes)
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

func TestDecimal256CompareScaleOverflowReturnsError(t *testing.T) {
	proc := testutil.NewProcess(t)
	dec0 := types.New(types.T_decimal256, 65, 0)
	dec2 := types.New(types.T_decimal256, 65, 2)

	left := types.Decimal256{
		B0_63:    ^uint64(0),
		B64_127:  ^uint64(0),
		B128_191: ^uint64(0),
		B192_255: ^uint64(0) >> 1,
	}
	right := mustParseDecimal256(t, "0.00", 2)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec0, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), true, []bool{}, []bool{}),
		equalFn,
	)
	ok, info := tc.Run()
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

func TestDecimal256ArithmeticTypeResolution(t *testing.T) {
	dec2 := types.New(types.T_decimal256, 65, 2)
	dec4 := types.New(types.T_decimal256, 65, 4)

	plusType := resolvedReturnType(t, PLUS, []types.Type{dec2, dec4})
	require.Equal(t, types.T_decimal256, plusType.Oid)
	require.Equal(t, int32(65), plusType.Width)
	require.Equal(t, int32(4), plusType.Scale)

	minusType := resolvedReturnType(t, MINUS, []types.Type{dec2, dec4})
	require.Equal(t, types.T_decimal256, minusType.Oid)
	require.Equal(t, int32(65), minusType.Width)
	require.Equal(t, int32(4), minusType.Scale)

	multiType := resolvedReturnType(t, MULTI, []types.Type{dec2, dec4})
	require.Equal(t, types.T_decimal256, multiType.Oid)
	require.Equal(t, int32(65), multiType.Width)
	require.Equal(t, int32(6), multiType.Scale)

	divType := resolvedReturnType(t, DIV, []types.Type{dec4, dec2})
	require.Equal(t, types.T_decimal256, divType.Oid)
	require.Equal(t, int32(65), divType.Width)
	require.Equal(t, int32(10), divType.Scale)

	require.True(t, hasOverload(UNARY_PLUS, types.T_decimal256))
	require.True(t, hasOverload(UNARY_MINUS, types.T_decimal256))
}

func TestDecimal256ArithmeticFns(t *testing.T) {
	proc := testutil.NewProcess(t)
	dec2 := types.New(types.T_decimal256, 65, 2)
	dec4 := types.New(types.T_decimal256, 65, 4)
	dec6 := types.New(types.T_decimal256, 65, 6)
	dec10 := types.New(types.T_decimal256, 65, 10)

	left := mustParseDecimal256(t, "5.2500", 4)
	right := mustParseDecimal256(t, "2.00", 2)

	plusTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{mustParseDecimal256(t, "7.2500", 4)}, []bool{false}),
		plusFn,
	)
	ok, info := plusTC.Run()
	require.True(t, ok, info)

	minusTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{mustParseDecimal256(t, "3.2500", 4)}, []bool{false}),
		minusFn,
	)
	ok, info = minusTC.Run()
	require.True(t, ok, info)

	multiTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(dec6, false, []types.Decimal256{mustParseDecimal256(t, "10.500000", 6)}, []bool{false}),
		multiFn,
	)
	ok, info = multiTC.Run()
	require.True(t, ok, info)

	divTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(dec10, false, []types.Decimal256{mustParseDecimal256(t, "2.6250000000", 10)}, []bool{false}),
		divFn,
	)
	ok, info = divTC.Run()
	require.True(t, ok, info)

	modTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{mustParseDecimal256(t, "1.2500", 4)}, []bool{false}),
		modFn,
	)
	ok, info = modTC.Run()
	require.True(t, ok, info)

	intDivTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{left}, []bool{false}),
			NewFunctionTestInput(dec2, []types.Decimal256{right}, []bool{false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{3}, []bool{false}),
		integerDivFn,
	)
	ok, info = intDivTC.Run()
	require.True(t, ok, info)
}

func TestDecimal256UnaryOperators(t *testing.T) {
	proc := testutil.NewProcess(t)
	dec4 := types.New(types.T_decimal256, 65, 4)
	positive := mustParseDecimal256(t, "5.2500", 4)
	negative := mustParseDecimal256(t, "-5.2500", 4)

	unaryPlusTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{negative}, []bool{false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{negative}, []bool{false}),
		operatorUnaryPlus[types.Decimal256],
	)
	ok, info := unaryPlusTC.Run()
	require.True(t, ok, info)

	unaryMinusTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{positive}, []bool{false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{negative}, []bool{false}),
		operatorUnaryMinusDecimal256,
	)
	ok, info = unaryMinusTC.Run()
	require.True(t, ok, info)
}

func TestDecimal256PlusFnRespectsSelectListNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	dec4 := types.New(types.T_decimal256, 65, 4)
	left := mustParseDecimal256(t, "5.2500", 4)
	right := mustParseDecimal256(t, "2.0000", 4)

	ivecs := []*vector.Vector{
		newVectorByType(proc.Mp(), dec4, []types.Decimal256{left, left}, nil),
		newVectorByType(proc.Mp(), dec4, []types.Decimal256{right, right}, nil),
	}
	defer ivecs[0].Free(proc.Mp())
	defer ivecs[1].Free(proc.Mp())

	result := vector.NewFunctionResultWrapper(dec4, proc.Mp())
	require.NoError(t, result.PreExtendAndReset(2))

	selectList := &FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{true, false},
	}
	err := plusFn(ivecs, result, proc, 2, selectList)
	require.NoError(t, err)

	resultVec := result.GetResultVector()
	values := vector.MustFixedColNoTypeCheck[types.Decimal256](resultVec)
	require.Equal(t, mustParseDecimal256(t, "7.2500", 4), values[0])
	require.True(t, resultVec.GetNulls().Contains(1))
}

func TestDecimal256MathFunctions(t *testing.T) {
	proc := testutil.NewProcess(t)
	dec4 := types.New(types.T_decimal256, 65, 4)
	digitsType := types.T_int64.ToType()
	positive := mustParseDecimal256(t, "12.3412", 4)
	negative := mustParseDecimal256(t, "-12.3412", 4)
	zero := mustParseDecimal256(t, "0.0000", 4)

	require.True(t, hasOverload(ABS, types.T_decimal256))
	require.True(t, hasOverload(SIGN, types.T_decimal256))
	require.True(t, hasOverload(CEIL, types.T_decimal256))
	require.True(t, hasOverload(CEIL, types.T_decimal256, types.T_int64))
	require.True(t, hasOverload(FLOOR, types.T_decimal256))
	require.True(t, hasOverload(FLOOR, types.T_decimal256, types.T_int64))
	require.True(t, hasOverload(ROUND, types.T_decimal256))
	require.True(t, hasOverload(ROUND, types.T_decimal256, types.T_int64))
	require.True(t, hasOverload(TRUNCATE, types.T_decimal256))
	require.True(t, hasOverload(TRUNCATE, types.T_decimal256, types.T_int64))

	absTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{positive, negative, zero}, []bool{false, false, false}),
		},
		NewFunctionTestResult(dec4, false, []types.Decimal256{positive, positive, zero}, []bool{false, false, false}),
		AbsDecimal256,
	)
	ok, info := absTC.Run()
	require.True(t, ok, info)

	signTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(dec4, []types.Decimal256{positive, zero, negative}, []bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 0, -1}, []bool{false, false, false}),
		SignDecimal256,
	)
	ok, info = signTC.Run()
	require.True(t, ok, info)

	digitsInput := []FunctionTestInput{
		NewFunctionTestInput(dec4, []types.Decimal256{positive, negative}, []bool{false, false}),
		NewFunctionTestConstInput(digitsType, []int64{2}, []bool{false}),
	}

	ceilTC := NewFunctionTestCase(proc,
		digitsInput,
		NewFunctionTestResult(dec4, false,
			[]types.Decimal256{
				mustParseDecimal256(t, "12.3500", 4),
				mustParseDecimal256(t, "-12.3400", 4),
			},
			[]bool{false, false}),
		CeilDecimal256,
	)
	ok, info = ceilTC.Run()
	require.True(t, ok, info)

	floorTC := NewFunctionTestCase(proc,
		digitsInput,
		NewFunctionTestResult(dec4, false,
			[]types.Decimal256{
				mustParseDecimal256(t, "12.3400", 4),
				mustParseDecimal256(t, "-12.3500", 4),
			},
			[]bool{false, false}),
		FloorDecimal256,
	)
	ok, info = floorTC.Run()
	require.True(t, ok, info)

	roundTC := NewFunctionTestCase(proc,
		digitsInput,
		NewFunctionTestResult(dec4, false,
			[]types.Decimal256{
				mustParseDecimal256(t, "12.3400", 4),
				mustParseDecimal256(t, "-12.3400", 4),
			},
			[]bool{false, false}),
		RoundDecimal256,
	)
	ok, info = roundTC.Run()
	require.True(t, ok, info)

	truncateTC := NewFunctionTestCase(proc,
		digitsInput,
		NewFunctionTestResult(dec4, false,
			[]types.Decimal256{
				mustParseDecimal256(t, "12.3400", 4),
				mustParseDecimal256(t, "-12.3400", 4),
			},
			[]bool{false, false}),
		TruncateDecimal256,
	)
	ok, info = truncateTC.Run()
	require.True(t, ok, info)
}

func TestDecimal256StringTypeCoercion(t *testing.T) {
	dec4 := types.New(types.T_decimal256, 65, 4)
	varchar := types.T_varchar.ToType()

	ok, left, right := fixedTypeCastRule1(dec4, varchar)
	require.True(t, ok)
	require.Equal(t, types.T_decimal256, left.Oid)
	require.Equal(t, types.T_decimal256, right.Oid)
	require.GreaterOrEqual(t, left.Width, int32(65))
	require.GreaterOrEqual(t, right.Width, int32(65))
	require.Equal(t, int32(4), left.Scale)
	require.Equal(t, int32(4), right.Scale)

	ok, left, right = fixedTypeCastRule1(varchar, dec4)
	require.True(t, ok)
	require.Equal(t, types.T_decimal256, left.Oid)
	require.Equal(t, types.T_decimal256, right.Oid)
	require.GreaterOrEqual(t, left.Width, int32(65))
	require.GreaterOrEqual(t, right.Width, int32(65))
	require.Equal(t, int32(4), left.Scale)
	require.Equal(t, int32(4), right.Scale)

	ok, left, right = fixedTypeCastRule2(dec4, varchar)
	require.True(t, ok)
	require.Equal(t, types.T_decimal256, left.Oid)
	require.Equal(t, types.T_decimal256, right.Oid)
	require.GreaterOrEqual(t, left.Width, int32(65))
	require.GreaterOrEqual(t, right.Width, int32(65))
	require.Equal(t, int32(4), left.Scale)
	require.Equal(t, int32(4), right.Scale)
}
