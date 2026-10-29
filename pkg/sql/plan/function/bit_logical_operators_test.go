// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

// MySQL evaluates BIT as a number in a boolean context (non-zero is TRUE,
// zero is FALSE, NULL stays NULL), so the logical operators AND/OR/XOR/NOT
// must accept a BIT operand and bind it through an implicit BIT -> BOOL cast.
// The acceptance lives in the boolean argument positions of these operators
// (logicalOperatorTypeCheck / logicalFixedTypeMatch), not in the global
// implicit-cast table.
func TestLogicalOperatorsAcceptBitOperand(t *testing.T) {
	ctx := context.Background()
	bit1 := types.New(types.T_bit, 1, 0)
	bit8 := types.New(types.T_bit, 8, 0)
	bit64 := types.New(types.T_bit, 64, 0)
	boolType := types.T_bool.ToType()
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"and", []types.Type{bit8, boolType}},
		{"and", []types.Type{boolType, bit8}},
		{"and", []types.Type{bit1, bit64}},
		{"or", []types.Type{bit8, boolType}},
		{"or", []types.Type{boolType, bit64}},
		{"xor", []types.Type{bit8, boolType}},
		{"xor", []types.Type{bit1, bit1}},
		{"not", []types.Type{bit1}},
		{"not", []types.Type{bit64}},
	} {
		resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
		require.NoError(t, err, tc.name)
		require.True(t, resolved.needCast, tc.name)
		for i := range tc.args {
			require.Equal(t, types.T_bool, resolved.targetTypes[i].Oid, tc.name)
		}
		require.Equal(t, types.T_bool, resolved.retType.Oid, tc.name)
	}
}

// Swapping NOT and XOR to logicalFixedTypeMatch must not change resolution
// for any non-BIT input: the same domains still cast to BOOL, untyped NULL
// (T_any) still binds, and arity is still enforced by the overloads.
func TestLogicalOperatorsNonBitOperandsUnchanged(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	int8Type := types.T_int8.ToType()
	int64Type := types.T_int64.ToType()
	varcharType := types.T_varchar.ToType()
	decimalType := types.T_decimal64.ToType()
	anyType := types.T_any.ToType()
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"and", []types.Type{int8Type, boolType}},
		{"and", []types.Type{boolType, int64Type, varcharType}},
		{"or", []types.Type{decimalType, int8Type}},
		{"xor", []types.Type{int8Type, int64Type}},
		{"xor", []types.Type{boolType, boolType}},
		{"not", []types.Type{int8Type}},
		{"not", []types.Type{varcharType}},
		{"not", []types.Type{anyType}},
		{"and", []types.Type{anyType, boolType}},
		{"xor", []types.Type{anyType, decimalType}},
	} {
		resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
		require.NoError(t, err, tc.name)
		require.Equal(t, types.T_bool, resolved.retType.Oid, tc.name)
		if resolved.needCast {
			for i := range tc.args {
				require.Equal(t, types.T_bool, resolved.targetTypes[i].Oid, tc.name)
			}
		}
	}

	// Arity stays fixed for NOT and XOR after the checkFn swap.
	_, err := GetFunctionByName(ctx, "not", []types.Type{boolType, boolType})
	require.Error(t, err, "not() with two arguments must stay a plan-time error")
	_, err = GetFunctionByName(ctx, "xor", []types.Type{boolType, boolType, boolType})
	require.Error(t, err, "xor() with three arguments must stay a plan-time error")
	// Types outside the boolean domain still fail.
	_, err = GetFunctionByName(ctx, "and", []types.Type{types.T_date.ToType(), boolType})
	require.Error(t, err, "and(date, bool) must stay a plan-time error")
}

// BIT's implicit-cast rows must keep their documented numeric preferences,
// and BIT -> BOOL must stay OUT of the global implicit-cast table: in the
// conditional resolvers (CASE/IF/COALESCE), BOOL is the only type reachable
// from both BIT and BOOL branches, so a global rule would silently collapse
// non-zero BIT values to TRUE. The operator-local matchers accept BIT in the
// boolean positions instead.
func TestBitImplicitCastRulesRemainScoped(t *testing.T) {
	bitType := types.New(types.T_bit, 16, 0)
	can, _ := fixedImplicitTypeCast(bitType, types.T_bool)
	require.False(t, can, "BIT -> BOOL must not live in the global implicit-cast table")
	require.True(t, isLogicalBoolOperand(bitType), "BIT is accepted as a boolean-context operand")

	can, cost := fixedImplicitTypeCast(bitType, types.T_int64)
	require.True(t, can)
	require.Equal(t, 2, cost)
	can, cost = fixedImplicitTypeCast(bitType, types.T_uint64)
	require.True(t, can)
	require.Equal(t, 1, cost)
	for _, target := range []types.T{types.T_int8, types.T_float64, types.T_decimal64, types.T_varchar, types.T_date} {
		can, _ := fixedImplicitTypeCast(bitType, target)
		require.False(t, can, target.String())
	}
}

// The conditional resolvers must never pick BOOL as the common result type of
// a BIT value branch: there is no implicit BOOL -> numeric path, so BOOL
// would be the only candidate and a non-zero BIT value (128, the BIT(64)
// high bit, ...) would collapse to TRUE. With the operator-local BIT
// acceptance, mixing BIT and BOOL value branches stays a plan-time error
// (unchanged base behavior), while BIT-only branches still resolve to a BIT
// result so values survive.
func TestConditionalResolversDoNotCollapseBitToBool(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	bit8 := types.New(types.T_bit, 8, 0)
	bit64 := types.New(types.T_bit, 64, 0)

	// BIT mixed with BOOL value branches: both orders, both widths, all three
	// resolvers. None of these may resolve to BOOL.
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"case", []types.Type{boolType, bit8, boolType}},
		{"case", []types.Type{boolType, boolType, bit8}},
		{"case", []types.Type{boolType, bit64, boolType}},
		{"if", []types.Type{boolType, bit8, boolType}},
		{"if", []types.Type{boolType, boolType, bit8}},
		{"if", []types.Type{boolType, bit64, boolType}},
		{"coalesce", []types.Type{bit8, boolType}},
		{"coalesce", []types.Type{boolType, bit8}},
		{"coalesce", []types.Type{bit64, boolType}},
	} {
		_, err := GetFunctionByName(ctx, tc.name, tc.args)
		require.Error(t, err, "%s%v must not resolve mixed BIT/BOOL value branches", tc.name, tc.args)
	}

	// BIT-only value branches keep the full BIT domain instead of reducing to
	// a truth value. IF/CASE select the native T_bit candidate at cost 0, the
	// pre-existing base resolution. COALESCE has no BIT overload (also base
	// behavior), so its cheapest cast overload is UINT64 via the bit->uint64
	// implicit cast; both preserve 128 and the BIT(64) high bit.
	resolved, err := GetFunctionByName(ctx, "coalesce", []types.Type{bit8, bit8})
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, resolved.retType.Oid)
	resolved, err = GetFunctionByName(ctx, "coalesce", []types.Type{bit64, bit64})
	require.NoError(t, err)
	require.Equal(t, types.T_uint64, resolved.retType.Oid)
	resolved, err = GetFunctionByName(ctx, "if", []types.Type{boolType, bit8, bit64})
	require.NoError(t, err)
	require.Equal(t, types.T_bit, resolved.retType.Oid)
	resolved, err = GetFunctionByName(ctx, "case", []types.Type{boolType, bit8, bit8})
	require.NoError(t, err)
	require.Equal(t, types.T_bit, resolved.retType.Oid)
}

// The BIT -> BOOL cast keeps MySQL truthiness: any non-zero value is TRUE,
// zero is FALSE, NULL stays NULL. 128 in BIT(8) and the BIT(64) high bit must
// not be read as negative or as FALSE.
func TestBitToBoolCastTruthiness(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	run := func(t *testing.T, width int32, values []uint64, nulls []bool, expected []bool) {
		t.Helper()
		from := types.New(types.T_bit, width, 0)
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(from, values, nulls),
				NewFunctionTestInput(types.T_bool.ToType(), []bool{}, nil),
			},
			NewFunctionTestResult(types.T_bool.ToType(), false, expected, nulls),
			NewCast)
		ok, info := fc.RunAndFree()
		require.True(t, ok, info)
	}

	t.Run("bit8", func(t *testing.T) {
		run(t, 8,
			[]uint64{0, 1, 2, 128, 255, 0},
			[]bool{false, false, false, false, false, true},
			[]bool{false, true, true, true, true, false})
	})
	t.Run("bit64_high_bit", func(t *testing.T) {
		run(t, 64,
			[]uint64{0, 1 << 63, 1<<63 | 1, 1},
			[]bool{false, false, false, true},
			[]bool{false, true, true, false})
	})
	t.Run("bit1", func(t *testing.T) {
		run(t, 1,
			[]uint64{0, 1, 0},
			[]bool{false, false, true},
			[]bool{false, true, false})
	})
}
