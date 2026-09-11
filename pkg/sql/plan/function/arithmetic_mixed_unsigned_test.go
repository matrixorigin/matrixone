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
			require.Equal(t, types.New(types.T_uint64, 64, -1), resolved.GetReturnType())
			targets, needCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, needCast)
			require.Len(t, targets, 2)
			for i, target := range targets {
				require.Equal(t, operands[i].Oid, target.Oid)
				require.Equal(t, int32(64), target.Width)
			}
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
		{name: "minus negative allowed", fn: signedUnsignedMinusFn, left: types.Decimal128{}, right: types.Decimal128{B0_63: 1}, mode: "NO_UNSIGNED_SUBTRACTION", want: negativeOne},
		{name: "signed positive overflow", fn: signedUnsignedMinusFn, left: maxUnsigned, mode: "NO_UNSIGNED_SUBTRACTION", wantCode: moerr.ER_DATA_OUT_OF_RANGE},
		{name: "signed negative overflow", fn: signedUnsignedMinusFn, left: negativeOne, right: maxUnsigned, mode: "NO_UNSIGNED_SUBTRACTION", wantCode: moerr.ER_DATA_OUT_OF_RANGE},
		{name: "plus negative operand", fn: mixedUnsignedPlusFn, left: maxUnsigned, right: negativeOne, want: types.Decimal128{B0_63: math.MaxUint64 - 1}},
		{name: "multiply maximum", fn: mixedUnsignedMultiFn, left: maxUnsigned, right: types.Decimal128{B0_63: 1}, want: maxUnsigned},
		{name: "multiply overflow", fn: mixedUnsignedMultiFn, left: maxUnsigned, right: types.Decimal128{B0_63: 2}, wantCode: moerr.ER_DATA_OUT_OF_RANGE},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			resultType := types.T_uint64.ToType()
			if test.mode != "" {
				resultType = types.T_int64.ToType()
			}
			caseUnderTest := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					integerDomainTestInput(test.left),
					integerDomainTestInput(test.right),
				},
				NewFunctionTestResult(resultType, false, nil, nil),
				test.fn,
			)
			defer caseUnderTest.result.Free()
			for _, parameter := range caseUnderTest.parameters {
				defer parameter.Free(proc.Mp())
			}
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
			if resultType.Oid == types.T_int64 {
				require.Equal(t, int64(test.want.B0_63), vector.GetFixedAtNoTypeCheck[int64](caseUnderTest.GetResultVectorDirectly(), 0))
			} else {
				require.Equal(t, test.want.B0_63, vector.GetFixedAtNoTypeCheck[uint64](caseUnderTest.GetResultVectorDirectly(), 0))
			}
		})
	}
}

func TestMixedUnsignedArithmeticSkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	maxUnsigned := uint64(math.MaxUint64)
	caseUnderTest := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{maxUnsigned, maxUnsigned}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil),
		mixedUnsignedPlusFn,
	)
	defer caseUnderTest.result.Free()
	for _, parameter := range caseUnderTest.parameters {
		defer parameter.Free(proc.Mp())
	}
	require.NoError(t, caseUnderTest.result.PreExtendAndReset(2))
	selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	require.NoError(t, caseUnderTest.fn(
		caseUnderTest.parameters, caseUnderTest.result, proc, 2, selectList))
	require.True(t, caseUnderTest.GetResultVectorDirectly().GetNulls().Contains(1))
}

func TestUnsignedSubtractionBindsResultDomain(t *testing.T) {
	for _, pair := range [][2]types.Type{
		{types.T_uint64.ToType(), types.T_int64.ToType()},
		{types.T_int64.ToType(), types.T_uint64.ToType()},
		{types.T_uint64.ToType(), types.T_uint64.ToType()},
		{types.T_uint8.ToType(), types.T_int8.ToType()},
	} {
		ctx := WithNoUnsignedSubtraction(context.Background(), true)
		resolved, err := GetFunctionByName(ctx, "-", pair[:])
		require.NoError(t, err)
		require.Equal(t, int32(3), resolved.overloadId)
		require.Equal(t, types.T_int64, resolved.GetReturnType().Oid)
	}
	inner, err := GetFunctionByName(context.Background(), "+", []types.Type{types.T_uint64.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)
	outer, err := GetFunctionByName(context.Background(), "-", []types.Type{inner.GetReturnType(), types.T_int64.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, outer.GetReturnType().Oid)
	require.Equal(t, int32(2), outer.overloadId)
}

func integerDomainTestInput(value types.Decimal128) FunctionTestInput {
	if value.Sign() {
		return NewFunctionTestInput(types.T_int64.ToType(), []int64{int64(value.B0_63)}, nil)
	}
	return NewFunctionTestInput(types.T_uint64.ToType(), []uint64{value.B0_63}, nil)
}
