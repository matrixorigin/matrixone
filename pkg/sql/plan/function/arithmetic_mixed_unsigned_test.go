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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMixedInt64Uint64ResolversUseUnsignedDomainOverloads(t *testing.T) {
	for _, operator := range []string{"+", "-", "*"} {
		for _, operands := range [][2]types.Type{
			{types.T_uint64.ToType(), types.T_int64.ToType()},
			{types.T_int64.ToType(), types.T_uint64.ToType()},
		} {
			resolved, err := GetFunctionByName(
				context.Background(), operator, operands[:])
			require.NoError(t, err)
			require.Equal(t, int32(2), resolved.overloadId)
			require.Equal(t, types.New(types.T_decimal128, 38, 0), resolved.GetReturnType())
			targets, needCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, needCast)
			require.Equal(t, []types.Type{
				types.New(types.T_decimal128, 38, 0),
				types.New(types.T_decimal128, 38, 0),
			}, targets)
		}
	}

	resolved, err := GetFunctionByName(context.Background(), "+", []types.Type{
		types.T_uint32.ToType(), types.T_int64.ToType(),
	})
	require.NoError(t, err)
	require.NotEqual(t, int32(2), resolved.overloadId,
		"smaller integer combinations keep the existing resolver")
}

func TestMixedUnsignedArithmeticDomainChecks(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 38, 0)
	maxUnsigned := types.Decimal128{B0_63: math.MaxUint64}
	negativeOne := types.Decimal128{B0_63: math.MaxUint64, B64_127: math.MaxUint64}

	for _, test := range []struct {
		name     string
		fn       fEvalFn
		left     types.Decimal128
		right    types.Decimal128
		mode     string
		want     types.Decimal128
		wantCode uint16
	}{
		{name: "plus maximum", fn: mixedUnsignedPlusFn, left: maxUnsigned, right: types.Decimal128{}, want: maxUnsigned},
		{name: "plus overflow", fn: mixedUnsignedPlusFn, left: maxUnsigned, right: types.Decimal128{B0_63: 1}, wantCode: moerr.ER_DATA_OUT_OF_RANGE},
		{name: "minus negative", fn: mixedUnsignedMinusFn, left: types.Decimal128{}, right: types.Decimal128{B0_63: 1}, wantCode: moerr.ER_DATA_OUT_OF_RANGE},
		{name: "minus negative allowed", fn: mixedUnsignedMinusFn, left: types.Decimal128{}, right: types.Decimal128{B0_63: 1}, mode: "NO_UNSIGNED_SUBTRACTION", want: negativeOne},
		{name: "multiply maximum", fn: mixedUnsignedMultiFn, left: maxUnsigned, right: types.Decimal128{B0_63: 1}, want: maxUnsigned},
		{name: "multiply overflow", fn: mixedUnsignedMultiFn, left: maxUnsigned, right: types.Decimal128{B0_63: 2}, wantCode: moerr.ER_DATA_OUT_OF_RANGE},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.SessionInfo.SqlMode = test.mode
			caseUnderTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(decimalType, []types.Decimal128{test.left}, nil),
					NewFunctionTestInput(decimalType, []types.Decimal128{test.right}, nil),
				},
				NewFunctionTestResult(decimalType, false, []types.Decimal128{test.want}, nil),
				test.fn,
			)
			require.NoError(t, caseUnderTest.result.PreExtendAndReset(1))
			err := caseUnderTest.fn(
				caseUnderTest.parameters, caseUnderTest.result, proc, 1, nil)
			if test.wantCode != 0 {
				require.Error(t, err)
				moError, ok := err.(*moerr.Error)
				require.True(t, ok)
				require.Equal(t, test.wantCode, moError.MySQLCode())
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want,
				vector.GetFixedAtNoTypeCheck[types.Decimal128](
					caseUnderTest.GetResultVectorDirectly(), 0))
		})
	}
}

func TestMixedUnsignedArithmeticSkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	decimalType := types.New(types.T_decimal128, 38, 0)
	maxUnsigned := types.Decimal128{B0_63: math.MaxUint64}
	caseUnderTest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(decimalType, []types.Decimal128{maxUnsigned, maxUnsigned}, nil),
			NewFunctionTestInput(decimalType, []types.Decimal128{{}, {B0_63: 1}}, nil),
		},
		NewFunctionTestResult(decimalType, false, nil, nil),
		mixedUnsignedPlusFn,
	)
	require.NoError(t, caseUnderTest.result.PreExtendAndReset(2))
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	require.NoError(t, caseUnderTest.fn(
		caseUnderTest.parameters, caseUnderTest.result, proc, 2, selectList))
	require.True(t, caseUnderTest.GetResultVectorDirectly().GetNulls().Contains(1))
}

func TestNoUnsignedSubtractionModeRequiresExactToken(t *testing.T) {
	require.True(t, sqlModeContainsToken(
		"STRICT_TRANS_TABLES, NO_UNSIGNED_SUBTRACTION", "NO_UNSIGNED_SUBTRACTION"))
	require.False(t, sqlModeContainsToken(
		"NOT_NO_UNSIGNED_SUBTRACTION", "NO_UNSIGNED_SUBTRACTION"))
}
