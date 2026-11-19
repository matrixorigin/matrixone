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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// MySQL treats TRUE/FALSE as the integers 1/0, so a CASE/IF/COALESCE that
// mixes a BOOL branch with a numeric branch returns the numeric type
// (COALESCE(2, TRUE) = 2). Resolving the common type to BOOL turns every
// numeric value into 0/1.
func TestBoolNumericValueBranchesResolveToNumeric(t *testing.T) {
	ctx := context.Background()
	b := types.T_bool.ToType()
	i32 := types.T_int32.ToType()
	i64 := types.T_int64.ToType()
	f64 := types.T_float64.ToType()
	d64 := types.New(types.T_decimal64, 10, 2)
	d128 := types.New(types.T_decimal128, 20, 3)

	for _, tc := range []struct {
		name string
		fn   string
		args []types.Type
		ret  types.T
	}{
		{"coalesce(int, bool)", "coalesce", []types.Type{i64, b}, types.T_int64},
		{"coalesce(bool, int)", "coalesce", []types.Type{b, i64}, types.T_int64},
		{"coalesce(double, bool)", "coalesce", []types.Type{f64, b}, types.T_float64},
		{"coalesce(null, int32, bool)", "coalesce", []types.Type{types.T_any.ToType(), i32, b}, types.T_int64},
		// A BOOL value branch contributes its real 0/1 domain (one integral
		// digit) when the common decimal precision is derived, so
		// decimal64(10,2) keeps its own family instead of being widened by
		// the 19 digits a BIGINT stand-in would claim.
		{"coalesce(decimal64, bool)", "coalesce", []types.Type{d64, b}, types.T_decimal64},
		{"coalesce(bool, decimal128)", "coalesce", []types.Type{b, d128}, types.T_decimal128},
		{"coalesce(int, bool, decimal64)", "coalesce", []types.Type{i64, b, d64}, types.T_decimal128},
		{"case when c then int else bool", "case", []types.Type{b, i64, b}, types.T_int64},
		{"case when c then int else decimal", "case", []types.Type{b, i32, d64}, types.T_decimal64},
		{"case when c then bool when c then double", "case", []types.Type{b, b, b, f64}, types.T_float64},
		{"if(c, double, bool)", "iff", []types.Type{b, f64, b}, types.T_float64},
		{"if(c, bool, int)", "iff", []types.Type{b, b, i64}, types.T_int64},
		{"if(c, decimal64, bool)", "iff", []types.Type{b, d64, b}, types.T_decimal64},
		// BOOL-only branches keep BOOL.
		{"coalesce(bool, bool)", "coalesce", []types.Type{b, b}, types.T_bool},
		{"case when c then bool else bool", "case", []types.Type{b, b, b, b}, types.T_bool},
		{"if(c, bool, bool)", "iff", []types.Type{b, b, b}, types.T_bool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, tc.fn, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.ret, get.GetReturnType().Oid)
			if targets, shouldCast := get.ShouldDoImplicitTypeCast(); shouldCast {
				for i, arg := range tc.args {
					// A BOOL value branch must be cast to the numeric result type,
					// not the other way round; CASE/IF conditions stay BOOL.
					isCondition := (tc.fn == "case" && i%2 == 0 && !(len(tc.args)%2 == 1 && i == len(tc.args)-1)) ||
						(tc.fn == "iff" && i == 0)
					if isCondition {
						require.Equal(t, types.T_bool, targets[i].Oid, "arg %d", i)
					} else if arg.Oid != types.T_any {
						require.Equal(t, tc.ret, targets[i].Oid, "arg %d", i)
					}
				}
			}
		})
	}
}

// High-scale DECIMAL value branches stay DECIMAL across all four constructs:
// the BOOL branch's one-digit 0/1 domain leaves room for scales up to 75,
// while a 19-digit stand-in would exceed DECIMAL256's 76 digits from scale 58
// on and silently fold the result back to BOOL (issue #31200 review).
func TestBoolDecimalHighScaleValueBranches(t *testing.T) {
	ctx := context.Background()
	b := types.T_bool.ToType()
	d57 := types.New(types.T_decimal256, 65, 57)
	d58 := types.New(types.T_decimal256, 65, 58)
	d75 := types.New(types.T_decimal256, 76, 75)

	for _, tc := range []struct {
		name string
		fn   string
		args []types.Type
		want types.Type
	}{
		{"coalesce(decimal(65,57), bool)", "coalesce", []types.Type{d57, b}, d57},
		{"coalesce(bool, decimal(65,58))", "coalesce", []types.Type{b, d58}, d58},
		{"coalesce(decimal(76,75), bool)", "coalesce", []types.Type{d75, b}, d75},
		{"case when c then decimal(65,58) else bool", "case", []types.Type{b, d58, b}, d58},
		{"case when c then bool else decimal(65,57)", "case", []types.Type{b, b, d57}, d57},
		{"if(c, decimal(65,58), bool)", "iff", []types.Type{b, d58, b}, d58},
		{"if(c, bool, decimal(65,57))", "iff", []types.Type{b, b, d57}, d57},
	} {
		t.Run(tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, tc.fn, tc.args)
			require.NoError(t, err)
			require.Equal(t, types.T_decimal256, get.GetReturnType().Oid)
			require.Equal(t, tc.want.Width, get.GetReturnType().Width, "width")
			require.Equal(t, tc.want.Scale, get.GetReturnType().Scale, "scale")
			targets, shouldCast := get.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			for i, arg := range tc.args {
				if arg.Oid != types.T_bool {
					continue
				}
				isCondition := (tc.fn == "case" && i%2 == 0) || (tc.fn == "iff" && i == 0)
				if !isCondition {
					require.Equal(t, types.T_decimal256, targets[i].Oid, "arg %d", i)
					require.Equal(t, tc.want.Width, targets[i].Width, "arg %d width", i)
					require.Equal(t, tc.want.Scale, targets[i].Scale, "arg %d scale", i)
				}
			}
		})
	}
}

// A mixture whose common decimal domain cannot hold even the BOOL branch's
// 0/1 values (DECIMAL(76,76) has no integral digit) must report a resolution
// error instead of silently folding every branch back to BOOL.
func TestBoolDecimalUnresolvableDoesNotFoldToBool(t *testing.T) {
	ctx := context.Background()
	b := types.T_bool.ToType()
	d76 := types.New(types.T_decimal256, 76, 76)

	for _, tc := range []struct {
		name string
		fn   string
		args []types.Type
	}{
		{"coalesce(decimal(76,76), bool)", "coalesce", []types.Type{d76, b}},
		{"case when c then decimal(76,76) else bool", "case", []types.Type{b, d76, b}},
		{"if(c, decimal(76,76), bool)", "iff", []types.Type{b, d76, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(ctx, tc.fn, tc.args)
			require.Error(t, err)
		})
	}
}

// BOOL mixes whose other value domain is not numeric keep the legacy
// resolution, verified identical on the base commit and after this change:
// BIT/BOOL conditionals stay invalid, and string domains still win over BOOL
// (the plain checkers are kept as the fallback for these inputs).
func TestBoolNonNumericValueBranchesUnchanged(t *testing.T) {
	ctx := context.Background()
	b := types.T_bool.ToType()
	bit := types.New(types.T_bit, 1, 0)
	varchar := types.T_varchar.ToType()
	date := types.T_date.ToType()

	cases := []struct {
		name      string
		fn        string
		args      []types.Type
		ret       types.T // meaningful only when expectErr is empty
		expectErr string
	}{
		{"if(c, bit, bool)", "iff", []types.Type{b, bit, b}, 0, "bad value"},
		{"coalesce(bit, bool)", "coalesce", []types.Type{bit, b}, 0, "bad value"},
		{"coalesce(varchar, bool)", "coalesce", []types.Type{varchar, b}, types.T_varchar, ""},
		{"if(c, date, bool)", "iff", []types.Type{b, date, b}, types.T_varchar, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			get, err := GetFunctionByName(ctx, tc.fn, tc.args)
			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.ret, get.GetReturnType().Oid)
		})
	}
}

// MySQL treats TRUE/FALSE as 1/0 in a DECIMAL context, so BOOL can be cast to
// every DECIMAL width. This is the cast the BOOL/numeric common types need.
func TestBoolToDecimalCastAndClamp(t *testing.T) {
	proc := testutil.NewProcess(t)

	one64, err := types.Decimal64(1).Scale(2)
	require.NoError(t, err)
	one128, err := types.Decimal128{B0_63: 1}.Scale(3)
	require.NoError(t, err)
	one256, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(2)
	require.NoError(t, err)

	d64 := types.New(types.T_decimal64, 10, 2)
	d128 := types.New(types.T_decimal128, 20, 3)
	d256 := types.New(types.T_decimal256, 40, 2)

	typeParam := func(target types.Type) FunctionTestInput {
		switch target.Oid {
		case types.T_decimal64:
			return NewFunctionTestInput(target, []types.Decimal64{}, []bool{})
		case types.T_decimal128:
			return NewFunctionTestInput(target, []types.Decimal128{}, []bool{})
		default:
			return NewFunctionTestInput(target, []types.Decimal256{}, []bool{})
		}
	}
	for _, tc := range []struct {
		name   string
		target types.Type
		want   any
	}{
		{"decimal64", d64, []types.Decimal64{one64, 0, 0}},
		{"decimal128", d128, []types.Decimal128{one128, {}, {}}},
		{"decimal256", d256, []types.Decimal256{one256, {}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false, false}, []bool{false, false, true}),
				typeParam(tc.target),
			}
			expect := NewFunctionTestResult(tc.target, false, tc.want, []bool{false, false, true})
			tcc := NewFunctionTestCase(proc, inputs, expect, NewCast)
			succeed, info := tcc.RunAndFree()
			require.True(t, succeed, tc.name, info)
		})
	}

	// The explicit cast reuses the integer clamp: CAST(TRUE AS DECIMAL(1,1))
	// gives 0.9 like CAST(1 AS DECIMAL(1,1)).
	nine, err := types.ParseDecimal64("0.9", 1, 1)
	require.NoError(t, err)
	t.Run("explicit clamp", func(t *testing.T) {
		target := types.New(types.T_decimal64, 1, 1)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, []bool{false}),
			NewFunctionTestInput(target, []types.Decimal64{}, []bool{}),
		}
		expect := NewFunctionTestResult(target, false, []types.Decimal64{nine}, []bool{false})
		tcc := NewFunctionTestCase(proc, inputs, expect, NewExplicitCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, "explicit clamp", info)
	})
}

// The implicit/assignment BOOL-to-DECIMAL conversion (the "cast" overload DML
// uses) must enforce the declared precision: TRUE needs one integral digit,
// so a DECIMAL(W,S) with S >= W rejects it exactly like the string assignment
// path rejects '1', instead of storing the out-of-domain coefficient. FALSE
// and NULL convert into the same narrow domains, and targets with room for 1
// keep working.
func TestBoolToDecimalAssignmentRangeCheck(t *testing.T) {
	proc := testutil.NewProcess(t)

	cast := func(target types.Type, value bool) error {
		src := vector.NewVec(types.T_bool.ToType())
		defer src.Free(proc.Mp())
		require.NoError(t, vector.AppendFixed(src, value, false, proc.Mp()))
		targetVec := vector.NewConstNull(target, 1, proc.Mp())
		defer targetVec.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(target, proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(1))
		return NewCast([]*vector.Vector{src, targetVec}, result, proc, 1, nil)
	}

	require.ErrorContains(t, cast(types.New(types.T_decimal64, 1, 1), true),
		"1 beyond the range, can't be converted to Decimal64(1,1).")
	require.ErrorContains(t, cast(types.New(types.T_decimal128, 2, 2), true),
		"1 beyond the range, can't be converted to Decimal128(2,2).")
	require.ErrorContains(t, cast(types.New(types.T_decimal256, 1, 1), true),
		"1 beyond the range, can't be converted to Decimal256(1,1).")

	// One integral digit of room: TRUE converts at the target scale.
	require.NoError(t, cast(types.New(types.T_decimal64, 2, 1), true))
	require.NoError(t, cast(types.New(types.T_decimal64, 1, 0), true))
	require.NoError(t, cast(types.New(types.T_decimal128, 2, 1), true))
	require.NoError(t, cast(types.New(types.T_decimal256, 65, 58), true))

	// FALSE is 0 and fits the same narrow domains that reject TRUE.
	require.NoError(t, cast(types.New(types.T_decimal64, 1, 1), false))
	require.NoError(t, cast(types.New(types.T_decimal128, 2, 2), false))
	require.NoError(t, cast(types.New(types.T_decimal256, 1, 1), false))
}

// Value-level proof for the conversions the range check admits: TRUE yields 1
// at the target scale (including a 58-scale coefficient), FALSE yields 0, and
// NULL stays NULL.
func TestBoolToDecimalAssignmentValues(t *testing.T) {
	proc := testutil.NewProcess(t)

	one21, err := types.Decimal64(1).Scale(1)
	require.NoError(t, err)
	one256, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(58)
	require.NoError(t, err)

	d21 := types.New(types.T_decimal64, 2, 1)
	d11 := types.New(types.T_decimal64, 1, 1)
	d58 := types.New(types.T_decimal256, 65, 58)

	t.Run("true converts to 1 at target scale", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, []bool{false}),
			NewFunctionTestInput(d21, []types.Decimal64{}, []bool{}),
		}
		expect := NewFunctionTestResult(d21, false, []types.Decimal64{one21}, []bool{false})
		tcc := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("false and null fit the narrow domain", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{false, false}, []bool{false, true}),
			NewFunctionTestInput(d11, []types.Decimal64{}, []bool{}),
		}
		expect := NewFunctionTestResult(d11, false, []types.Decimal64{0, 0}, []bool{false, true})
		tcc := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("high scale keeps the exact coefficient", func(t *testing.T) {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, []bool{false}),
			NewFunctionTestInput(d58, []types.Decimal256{}, []bool{}),
		}
		expect := NewFunctionTestResult(d58, false, []types.Decimal256{one256}, []bool{false})
		tcc := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := tcc.RunAndFree()
		require.True(t, succeed, info)
	})
}
