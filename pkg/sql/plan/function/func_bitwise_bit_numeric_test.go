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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBitUnaryTildeUsesUnsigned64Domain(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		typ    types.Type
		input  []uint64
		result []uint64
	}{
		{
			name:   "bit1",
			typ:    types.New(types.T_bit, 1, 0),
			input:  []uint64{0, 1},
			result: []uint64{math.MaxUint64, math.MaxUint64 - 1},
		},
		{
			name:   "bit8",
			typ:    types.New(types.T_bit, 8, 0),
			input:  []uint64{0, 0x80, 0xff},
			result: []uint64{math.MaxUint64, math.MaxUint64 ^ 0x80, math.MaxUint64 ^ 0xff},
		},
		{
			name:   "bit64",
			typ:    types.New(types.T_bit, 64, 0),
			input:  []uint64{0, uint64(1) << 63, math.MaxUint64},
			result: []uint64{math.MaxUint64, math.MaxInt64, 0},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bound, err := GetFunctionByName(ctx, "unary_tilde", []types.Type{test.typ})
			require.NoError(t, err)
			require.Equal(t, int32(11), bound.overloadId)
			targets, cast := bound.ShouldDoImplicitTypeCast()
			require.False(t, cast)
			require.Nil(t, targets)
			require.Equal(t, types.T_uint64, bound.GetReturnType().Oid)
			assertBitwiseExecFactory(t, bound)

			proc := testutil.NewProcess(t)
			tc := NewFunctionTestCase(proc,
				[]FunctionTestInput{NewFunctionTestInput(test.typ, test.input, nil)},
				NewFunctionTestResult(types.T_uint64.ToType(), false, test.result, nil),
				operatorUnaryTilde[uint64])
			cleanupBitwiseTestCase(t, &tc)
			ok, info := tc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestResolveNumericBitIntegerDivDomains(t *testing.T) {
	bit8 := types.New(types.T_bit, 8, 0)
	bit64 := types.New(types.T_bit, 64, 0)
	decimalContext := types.New(types.T_decimal128, 38, 0)
	tests := []struct {
		name      string
		left      types.Type
		right     types.Type
		wantRight types.T
		wantCast  bool
		outer     *types.Type
	}{
		{name: "signed small divisor", left: bit8, right: types.T_int16.ToType(), wantRight: types.T_int64, wantCast: true},
		{name: "signed bigint divisor", left: bit64, right: types.T_int64.ToType(), wantRight: types.T_int64},
		{name: "unsigned divisor", left: bit8, right: types.T_uint32.ToType(), wantRight: types.T_uint64, wantCast: true},
		{name: "bit divisor", left: bit8, right: types.New(types.T_bit, 1, 0), wantRight: types.T_uint64, wantCast: true},
		{name: "untyped divisor", left: bit64, right: types.T_any.ToType(), wantRight: types.T_uint64, wantCast: true, outer: &decimalContext},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left, right, ok := integerDivBitTypes(test.left, test.right)
			require.True(t, ok)
			require.Equal(t, test.left, left, "preserve the BIT width")
			require.Equal(t, test.wantRight, right.Oid)
			require.True(t, integerDivBitResolvedTypes(left, right))

			resolved, ok := resolveNumericBinaryTypes(numericOpIntegerDiv, test.left, test.right, test.outer)
			require.True(t, ok)
			require.Equal(t, test.left, resolved.left)
			require.Equal(t, test.wantRight, resolved.right.Oid)
			require.Equal(t, types.T_uint64, resolved.result.Oid)

			bound, err := GetFunctionByName(context.Background(), "div", []types.Type{test.left, test.right})
			require.NoError(t, err)
			targets, cast := bound.ShouldDoImplicitTypeCast()
			require.Equal(t, test.wantCast, cast)
			if test.wantCast {
				require.Equal(t, test.left, targets[0])
				require.Equal(t, test.wantRight, targets[1].Oid)
			}
			require.Equal(t, types.T_uint64, bound.GetReturnType().Oid)
		})
	}
}

func TestIntegerDivBitPreservesFullUnsignedRange(t *testing.T) {
	proc := testutil.NewProcess(t)
	bit64 := types.New(types.T_bit, 64, 0)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(bit64, []uint64{0, uint64(1) << 63, math.MaxUint64}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 1, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{0, uint64(1) << 63, math.MaxUint64}, nil),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc)
	ok, info := tc.Run()
	require.True(t, ok, info)

	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(bit64, []uint64{uint64(1) << 63, uint64(1) << 63}, nil),
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 2}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{uint64(1) << 63, uint64(1) << 62}, nil),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc2)
	ok, info = tc2.Run()
	require.True(t, ok, "constant BIT dividend broadcast: %s", info)
}

func TestIntegerDivBitSignedDivisorAndMinInt64(t *testing.T) {
	proc := testutil.NewProcess(t)
	bit64 := types.New(types.T_bit, 64, 0)
	tc1 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(bit64, []uint64{0, 1, 1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -2, math.MinInt64}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 0, 0}, nil),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc1)
	ok, info := tc1.Run()
	require.True(t, ok, info)

	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(bit64, []uint64{math.MaxInt64, uint64(1) << 63}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{math.MinInt64, math.MinInt64}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc2)
	require.NoError(t, tc2.result.PreExtendAndReset(2))
	err := tc2.fn(tc2.parameters, tc2.result, proc, 2, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "expected unsigned overflow, got %v", err)
	require.Equal(t, uint64(0), vector.MustFixedColNoTypeCheck[uint64](tc2.GetResultVectorDirectly())[0])
}

func TestIntegerDivBitNullConstantAndMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	bit64 := types.New(types.T_bit, 64, 0)
	t.Run("constant divisor and null propagation", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{uint64(1) << 63, 0}, []bool{false, true}),
				NewFunctionTestConstInput(types.T_uint64.ToType(), []uint64{1}, []bool{false}),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{uint64(1) << 63, 0}, []bool{false, true}),
			integerDivFn)
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("masked negative quotient is not evaluated", func(t *testing.T) {
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{0, uint64(1) << 63}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-1, -1}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil),
			integerDivFn).WithSelectList(selectList)
		cleanupBitwiseTestCase(t, &tc)
		require.NoError(t, tc.result.PreExtendAndReset(2))
		require.NoError(t, tc.fn(tc.parameters, tc.result, proc, 2, selectList))
		result := tc.GetResultVectorDirectly()
		require.Equal(t, uint64(0), vector.MustFixedColNoTypeCheck[uint64](result)[0])
		require.False(t, result.GetNulls().Contains(0))
		require.True(t, result.GetNulls().Contains(1))
	})
}

func TestIntegerDivBitZeroDivisorRespectsNullAndStrictMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	bit64 := types.New(types.T_bit, 64, 0)
	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 1)
	defer atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, -1)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(bit64, []uint64{0, 1}, []bool{true, false}),
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), true, nil, nil),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc)
	ok, info := tc.Run()
	require.True(t, ok, "nonnull zero divisor should raise despite a NULL sibling: %s", info)

	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(bit64, []uint64{0, 1}, []bool{true, false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 1}, nil),
		},
		NewFunctionTestResult(types.T_uint64.ToType(), false,
			[]uint64{0, 1}, []bool{true, false}),
		integerDivFn)
	cleanupBitwiseTestCase(t, &tc2)
	ok, info = tc2.Run()
	require.True(t, ok, info)
}

func TestIntegerDivBitUnsignedZeroAndMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	bit64 := types.New(types.T_bit, 64, 0)
	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 1)
	defer atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, -1)

	t.Run("strict nonnull zero divisor errors", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{1}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil),
			integerDivFn)
		cleanupBitwiseTestCase(t, &tc)
		require.NoError(t, tc.result.PreExtendAndReset(1))
		err := tc.fn(tc.parameters, tc.result, proc, 1, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrDivByZero), "expected division-by-zero error, got %v", err)
	})

	t.Run("null suppresses zero divisor", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{0, 4}, []bool{true, false}),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0, 2}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{0, 2}, []bool{true, false}),
			integerDivFn)
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("masked zero divisor is not evaluated", func(t *testing.T) {
		selectList := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(bit64, []uint64{8, 9}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{2, 0}, nil),
			},
			NewFunctionTestResult(types.T_uint64.ToType(), false,
				[]uint64{4, 0}, []bool{false, true}),
			integerDivFn).WithSelectList(selectList)
		cleanupBitwiseTestCase(t, &tc)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})
}
