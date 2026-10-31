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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestDecimalBoolBinaryOperatorSupportLikeInt64 pins issue 31181: every
// operator that binds an INT64 operand next to a DECIMAL also binds BOOL
// (MySQL treats TRUE/FALSE as 1/0 next to a DECIMAL), and every operator
// that rejects INT64/DECIMAL also rejects BOOL. Before the fix, all of
// these failed at plan time with "invalid argument operator <op>, bad value
// [DECIMAL* BOOL]".
func TestDecimalBoolBinaryOperatorSupportLikeInt64(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	int64Type := types.T_int64.ToType()
	operators := []string{"+", "-", "*", "/", "%", "div", "=", "<>", ">", "<", ">=", "<="}
	for _, dec := range []types.Type{
		types.New(types.T_decimal64, 10, 2),
		types.New(types.T_decimal128, 30, 4),
		types.New(types.T_decimal256, 60, 4),
	} {
		for _, name := range operators {
			for _, swap := range []bool{false, true} {
				boolArgs := []types.Type{dec, boolType}
				intArgs := []types.Type{dec, int64Type}
				if swap {
					boolArgs = []types.Type{boolType, dec}
					intArgs = []types.Type{int64Type, dec}
				}
				_, intErr := GetFunctionByName(ctx, name, intArgs)
				_, boolErr := GetFunctionByName(ctx, name, boolArgs)
				if intErr != nil {
					// Pre-existing parity hole (INT64 / DECIMAL256 and
					// INT64 div DECIMAL256 are not bound on main): BOOL keeps
					// the same contract instead of binding wider than an int.
					require.Error(t, boolErr, "%s %s %s", boolArgs[0].Oid, name, boolArgs[1].Oid)
					continue
				}
				require.NoError(t, boolErr, "%s %s %s", boolArgs[0].Oid, name, boolArgs[1].Oid)
			}
		}
	}
}

// TestDecimalBoolBinaryTargets pins the implicit cast envelope for DECIMAL op
// BOOL. The BOOL operand is a 0/1 value with exactly one integral digit, so
// the decimal side keeps its own precision/scale and the bool side joins the
// same decimal storage domain with scale 0. Equality comparison aligns both
// operands to the tightest common decimal type (the same alignment used for
// integer operands), which keeps `d = true` inside the tightest domain that
// can hold both value ranges.
func TestDecimalBoolBinaryTargets(t *testing.T) {
	ctx := context.Background()
	boolType := types.T_bool.ToType()
	aligned := func(oid types.T, width, scale int32) types.Type {
		return types.New(oid, width, scale)
	}
	cases := []struct {
		name string
		dec  types.Type
		// Expected coercion targets for DEC <op> BOOL, left operand first.
		// addName/subName coerce BOOL to its exact one-digit domain in the
		// decimal operand's storage class (#31034 keeps the decimal side's
		// source precision); mulName does the same. The envelope group
		// (/, %, div, relational) pads both sides to the fixed envelope,
		// identical to the INT64 operand.
		addSub   [2]types.Type
		mul      [2]types.Type
		envelope [2]types.Type
		eq       types.Type // aligned common type for `=`
	}{
		{"decimal64", types.New(types.T_decimal64, 10, 2),
			[2]types.Type{aligned(types.T_decimal128, 10, 2), aligned(types.T_decimal128, 1, 0)},
			[2]types.Type{aligned(types.T_decimal64, 10, 2), aligned(types.T_decimal64, 1, 0)},
			[2]types.Type{aligned(types.T_decimal128, 38, 2), aligned(types.T_decimal128, 38, 0)},
			aligned(types.T_decimal128, 10, 2)},
		{"decimal128", types.New(types.T_decimal128, 30, 4),
			[2]types.Type{aligned(types.T_decimal128, 30, 4), aligned(types.T_decimal128, 1, 0)},
			[2]types.Type{aligned(types.T_decimal128, 30, 4), aligned(types.T_decimal128, 1, 0)},
			[2]types.Type{aligned(types.T_decimal128, 30, 4), aligned(types.T_decimal128, 38, 0)},
			aligned(types.T_decimal128, 30, 4)},
		{"decimal256", types.New(types.T_decimal256, 60, 4),
			[2]types.Type{aligned(types.T_decimal256, 60, 4), aligned(types.T_decimal256, 1, 0)},
			[2]types.Type{aligned(types.T_decimal256, 60, 4), aligned(types.T_decimal256, 1, 0)},
			[2]types.Type{aligned(types.T_decimal256, 60, 4), aligned(types.T_decimal256, 76, 0)},
			aligned(types.T_decimal256, 60, 4)},
	}
	check := func(msg string, actual types.Type, oid types.T, width, scale int32) {
		t.Helper()
		require.Equal(t, oid, actual.Oid, msg)
		require.Equal(t, width, actual.Width, msg)
		require.Equal(t, scale, actual.Scale, msg)
	}
	for _, tc := range cases {
		for _, name := range []string{"+", "-", "*", "/", "%", "div", "<>", ">", "<", ">=", "<="} {
			var leftPlain, rightPlain types.Type
			switch name {
			case "+", "-":
				leftPlain, rightPlain = tc.addSub[0], tc.addSub[1]
			case "*":
				leftPlain, rightPlain = tc.mul[0], tc.mul[1]
			default:
				leftPlain, rightPlain = tc.envelope[0], tc.envelope[1]
			}
			resolved, err := GetFunctionByName(ctx, name, []types.Type{tc.dec, boolType})
			require.NoError(t, err, "%s %s bool", tc.name, name)
			require.True(t, resolved.needCast, "%s %s bool", tc.name, name)
			require.Len(t, resolved.targetTypes, 2, "%s %s bool", tc.name, name)
			check(tc.name+" "+name+" decimal side", resolved.targetTypes[0],
				leftPlain.Oid, leftPlain.Width, leftPlain.Scale)
			check(tc.name+" "+name+" bool side", resolved.targetTypes[1],
				rightPlain.Oid, rightPlain.Width, rightPlain.Scale)

			if (name == "/" || name == "div") && tc.dec.Oid == types.T_decimal256 {
				// BOOL before a DECIMAL256 dividend does not bind for the
				// division operators; an INT64 operand in the same position
				// does not bind either, so BOOL carries no extra limitation.
				_, err = GetFunctionByName(ctx, name,
					[]types.Type{types.T_int64.ToType(), tc.dec})
				require.Error(t, err, "int64 %s %s", name, tc.name)
				continue
			}
			resolved, err = GetFunctionByName(ctx, name, []types.Type{boolType, tc.dec})
			require.NoError(t, err, "bool %s %s", name, tc.name)
			require.True(t, resolved.needCast, "bool %s %s", name, tc.name)
			check("bool "+name+" "+tc.name+" bool side", resolved.targetTypes[0],
				rightPlain.Oid, rightPlain.Width, rightPlain.Scale)
			check("bool "+name+" "+tc.name+" decimal side", resolved.targetTypes[1],
				leftPlain.Oid, leftPlain.Width, leftPlain.Scale)
		}

		// `=` aligns both sides into the tightest common DECIMAL domain.
		resolved, err := GetFunctionByName(ctx, "=", []types.Type{tc.dec, boolType})
		require.NoError(t, err, "%s = bool", tc.name)
		require.True(t, resolved.needCast, "%s = bool", tc.name)
		require.Len(t, resolved.targetTypes, 2, "%s = bool", tc.name)
		check(tc.name+" = bool decimal side", resolved.targetTypes[0],
			tc.eq.Oid, tc.eq.Width, tc.eq.Scale)
		check(tc.name+" = bool bool side", resolved.targetTypes[1],
			tc.eq.Oid, tc.eq.Width, tc.eq.Scale)
		_, err = GetFunctionByName(ctx, "<=>", []types.Type{tc.dec, boolType})
		require.NoError(t, err, "%s <=> bool", tc.name)
	}
}

func TestBoolToDecimalCastSupported(t *testing.T) {
	ctx := context.Background()
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		require.True(t, IfTypeCastSupported(types.T_bool, oid), "bool -> %s", oid)
		_, err := GetFunctionByName(ctx, "cast", []types.Type{types.T_bool.ToType(), oid.ToType()})
		require.NoError(t, err, "cast(bool as %s)", oid)
	}
}

// TRUE casts to 1 and FALSE to 0, scaled to the target DECIMAL type, with
// NULL propagated, for each decimal storage width.
func TestBoolToDecimalCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := context.Background()
	d64 := types.New(types.T_decimal64, 10, 2)
	one64, err := types.Decimal64(1).Scale(2)
	require.NoError(t, err)
	d128 := types.New(types.T_decimal128, 30, 4)
	one128, err := types.Decimal128{B0_63: 1}.Scale(4)
	require.NoError(t, err)
	d256 := types.New(types.T_decimal256, 60, 4)
	one256, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(4)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		to     types.Type
		target any
		want   any
	}{
		{"decimal64", d64, []types.Decimal64{}, []types.Decimal64{one64, 0, 0}},
		{"decimal128", d128, []types.Decimal128{}, []types.Decimal128{one128, {}, {}}},
		{"decimal256", d256, []types.Decimal256{}, []types.Decimal256{one256, {}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(ctx, "cast", []types.Type{types.T_bool.ToType(), tc.to})
			require.NoError(t, err)
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(),
						[]bool{true, false, true}, []bool{false, false, true}),
					NewFunctionTestInput(tc.to, tc.target, nil),
				},
				NewFunctionTestResult(tc.to, false, tc.want, []bool{false, false, true}),
				NewCast)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}

// Assigning TRUE into a DECIMAL column obeys the declared precision exactly
// like the integer 1: the conversion reuses the #30639 width validators, so a
// DECIMAL(p,p) column (maximum 0.99...) rejects TRUE with the identical
// integer diagnostic while FALSE and NULL always fit. Widths that admit 1
// accept it at the declared scale, and a synthesized zero-width expression
// cast stays unbounded so ordinary SELECT-context coercions are unaffected.
func TestBoolToDecimalAssignmentWidth(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	one128wide, err := types.Decimal128{B0_63: 1}.Scale(19)
	require.NoError(t, err)
	one256wide, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(40)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		to     types.Type
		target any
		want   any
		values []bool
		nulls  []bool
	}{
		{"decimal64_fits_1", types.New(types.T_decimal64, 1, 0), []types.Decimal64{},
			[]types.Decimal64{1, 0}, []bool{true, false}, nil},
		{"decimal64_fits_1_scale", types.New(types.T_decimal64, 2, 1), []types.Decimal64{},
			[]types.Decimal64{10, 0}, []bool{true, false}, nil},
		// FALSE and NULL fit any declared width; the null row even carries a
		// masked TRUE, mirroring the integer check skipping NULL rows.
		{"decimal64_false_null_accept", types.New(types.T_decimal64, 1, 1), []types.Decimal64{},
			[]types.Decimal64{0, 0}, []bool{false, true}, []bool{false, true}},
		{"decimal128_fits", types.New(types.T_decimal128, 20, 19), []types.Decimal128{},
			[]types.Decimal128{one128wide, {}}, []bool{true, false}, nil},
		{"decimal256_fits", types.New(types.T_decimal256, 41, 40), []types.Decimal256{},
			[]types.Decimal256{one256wide, {}}, []bool{true, false}, nil},
		{"synthesized_zero_width_accept", types.New(types.T_decimal64, 0, 0), []types.Decimal64{},
			[]types.Decimal64{1, 0}, []bool{true, false}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(), tc.values, tc.nulls),
					NewFunctionTestInput(tc.to, tc.target, nil),
				},
				NewFunctionTestResult(tc.to, false, tc.want, tc.nulls),
				NewCast)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}

	for _, tc := range []struct {
		name    string
		to      types.Type
		target  any
		values  []bool
		nulls   []bool
		wantErr string
	}{
		{"decimal64_1_1_reject", types.New(types.T_decimal64, 1, 1), []types.Decimal64{},
			[]bool{true}, nil, "Decimal64(1,1)"},
		{"decimal64_2_2_reject", types.New(types.T_decimal64, 2, 2), []types.Decimal64{},
			[]bool{true}, nil, "Decimal64(2,2)"},
		{"decimal64_3_3_reject", types.New(types.T_decimal64, 3, 3), []types.Decimal64{},
			[]bool{true}, nil, "Decimal64(3,3)"},
		// Only the TRUE row triggers the bound, not the declared width alone.
		{"decimal64_1_1_mixed_reject", types.New(types.T_decimal64, 1, 1), []types.Decimal64{},
			[]bool{false, true, false}, nil, "Decimal64(1,1)"},
		{"decimal128_19_19_reject", types.New(types.T_decimal128, 19, 19), []types.Decimal128{},
			[]bool{true}, nil, "Decimal128(19,19)"},
		{"decimal128_38_38_reject", types.New(types.T_decimal128, 38, 38), []types.Decimal128{},
			[]bool{true}, nil, "Decimal128(38,38)"},
		{"decimal256_40_40_reject", types.New(types.T_decimal256, 40, 40), []types.Decimal256{},
			[]bool{true}, nil, "Decimal256(40,40)"},
		{"decimal256_76_76_reject", types.New(types.T_decimal256, 76, 76), []types.Decimal256{},
			[]bool{true}, nil, "Decimal256(76,76)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_bool.ToType(), tc.values, tc.nulls),
					NewFunctionTestInput(tc.to, tc.target, nil),
				},
				NewFunctionTestResult(tc.to, true, nil, tc.nulls),
				NewCast)
			defer fc.Free()
			_, err := fc.DebugRun()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "error %v", err)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	// BOOL shares the integer validators, so the out-of-range diagnostic for
	// TRUE is byte-identical to the integer 1 rejection.
	t.Run("int_true_same_diagnostic", func(t *testing.T) {
		narrow := types.New(types.T_decimal64, 1, 1)
		boolCase := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), []bool{true}, nil),
				NewFunctionTestInput(narrow, []types.Decimal64{}, nil),
			},
			NewFunctionTestResult(narrow, true, nil, nil),
			NewCast)
		defer boolCase.Free()
		_, boolErr := boolCase.DebugRun()
		require.Error(t, boolErr)
		intCase := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(narrow, []types.Decimal64{}, nil),
			},
			NewFunctionTestResult(narrow, true, nil, nil),
			NewCast)
		defer intCase.Free()
		_, intErr := intCase.DebugRun()
		require.Error(t, intErr)
		require.Equal(t, intErr.Error(), boolErr.Error())
	})
}

// CAST(TRUE AS DECIMAL(1,1)) clamps to the column maximum like CAST(1 AS
// DECIMAL(1,1)): the explicit cast shares the integer parse-and-clamp path.
func TestBoolToDecimalExplicitCastClamps(t *testing.T) {
	proc := testutil.NewProcess(t)
	d := types.New(types.T_decimal64, 1, 1)
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(), []bool{true, false}, nil),
			NewFunctionTestInput(d, []types.Decimal64{}, nil),
		},
		NewFunctionTestResult(d, false, []types.Decimal64{9, 0}, nil),
		NewExplicitCast)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}

// A full-scale DECIMAL128 next to a BOOL must promote its arithmetic operands
// to DECIMAL256 exactly like an integer operand does: 0.99e38+1e38 overflows
// the signed Decimal128 coefficient range when the widening is missed.
func TestDecimalBoolArithmeticWideningBoundary(t *testing.T) {
	boolT := types.T_bool.ToType()
	intT := types.T_int64.ToType()
	full := types.New(types.T_decimal128, 38, 38)
	boolDomain := types.New(types.T_decimal256, 1, 0)

	for _, op := range []string{"+", "-", "*"} {
		// Only multiplication pads the DECIMAL intermediate by the one-digit
		// BOOL coefficient (38 + 1 = 39); addition and subtraction keep the
		// source precision. INT64 coerces the decimal operand to the
		// identical intermediate and differs only in its own operand width
		// (19 digits vs the exact one digit of 0/1).
		decSide := types.New(types.T_decimal256, 38, full.Scale)
		if op == "*" {
			decSide = types.New(types.T_decimal256, 39, full.Scale)
		}
		decimalFirst, err := GetFunctionByName(context.Background(), op, []types.Type{full, boolT})
		require.NoError(t, err, op)
		casts, needCast := decimalFirst.ShouldDoImplicitTypeCast()
		require.True(t, needCast, op)
		require.Equal(t, []types.Type{decSide, boolDomain}, casts, op)

		boolFirst, err := GetFunctionByName(context.Background(), op, []types.Type{boolT, full})
		require.NoError(t, err, op)
		casts, needCast = boolFirst.ShouldDoImplicitTypeCast()
		require.True(t, needCast, op)
		require.Equal(t, []types.Type{boolDomain, decSide}, casts, op)

		// INT64 promotes the decimal operand into the same physical domain
		// with the identical intermediate width.
		intRef, err := GetFunctionByName(context.Background(), op, []types.Type{full, intT})
		require.NoError(t, err, op)
		intCasts, needCast := intRef.ShouldDoImplicitTypeCast()
		require.True(t, needCast, op)
		require.Equal(t, []types.Type{decSide, types.New(types.T_decimal256, 19, 0)}, intCasts, op)
	}
}

// BOOL derives division precision as an exact one-digit, zero-scale operand:
// wide dividends promote to DECIMAL256 like with an integer, and a nondefault
// div_precision_increment applies to both operand orders.
func TestDecimalBoolDivisionPrecisionDerivation(t *testing.T) {
	boolT := types.T_bool.ToType()
	intT := types.T_int64.ToType()
	d10s2 := types.New(types.T_decimal64, 10, 2)

	for _, tc := range []struct {
		name      string
		increment int32
		inputs    []types.Type
		want      types.Type
	}{
		{"bool dividend increment 0", 0, []types.Type{boolT, d10s2}, types.New(types.T_decimal128, 3, 0)},
		{"bool dividend default", 4, []types.Type{boolT, d10s2}, types.New(types.T_decimal128, 7, 4)},
		{"bool dividend increment 10", 10, []types.Type{boolT, d10s2}, types.New(types.T_decimal128, 13, 10)},
		{"bool divisor default", 4, []types.Type{d10s2, boolT}, types.New(types.T_decimal128, 14, 6)},
		{"bool divisor increment 10", 10, []types.Type{d10s2, boolT}, types.New(types.T_decimal128, 20, 12)},
		{"wide quotient promotes", 4, []types.Type{types.New(types.T_decimal128, 38, 0), boolT}, types.New(types.T_decimal256, 42, 4)},
		{"wide divisor scale promotes", 4, []types.Type{boolT, types.New(types.T_decimal128, 38, 38)}, types.New(types.T_decimal256, 43, 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(
				WithDivPrecisionIncrement(context.Background(), tc.increment), "/", tc.inputs)
			require.NoError(t, err)
			require.Equal(t, tc.want, resolved.GetReturnType())
		})
	}

	// A DECIMAL dividend divides by BOOL with the same result type as by INT64.
	for _, side := range []types.Type{d10s2, types.New(types.T_decimal128, 38, 0)} {
		boolDiv, err := GetFunctionByName(context.Background(), "/", []types.Type{side, boolT})
		require.NoError(t, err)
		intDiv, err := GetFunctionByName(context.Background(), "/", []types.Type{side, intT})
		require.NoError(t, err)
		require.Equal(t, intDiv.GetReturnType(), boolDiv.GetReturnType(), "dividend %v", side)
	}
}

// MOD display metadata and integer-division return derivation count BOOL as
// one exact integral digit with zero scale. That is the integer contract:
// every operand contributes its own width, so a DECIMAL % BOOL stays as narrow
// as the decimal operand while a DECIMAL % INT64 keeps the 19 digit domain.
func TestDecimalBoolModAndIntegerDivMetadata(t *testing.T) {
	boolT := types.T_bool.ToType()
	intT := types.T_int64.ToType()
	d10s2 := types.New(types.T_decimal64, 10, 2)
	d30s4 := types.New(types.T_decimal128, 30, 4)

	// max(decimal width, exact bool width), decimal scale preserved.
	require.Equal(t,
		types.New(types.T_decimal128, 10, 2),
		DecimalModReturnType([]types.Type{d10s2, boolT}, types.New(types.T_decimal128, 38, 2)))
	require.Equal(t,
		types.New(types.T_decimal128, 30, 4),
		DecimalModReturnType([]types.Type{boolT, d30s4}, types.New(types.T_decimal128, 38, 4)))
	// INT64 keeps its own 19 digit domain; BOOL narrows to the exact 0/1 span.
	require.Equal(t,
		types.New(types.T_decimal128, 19, 2),
		DecimalModReturnType([]types.Type{d10s2, intT}, types.New(types.T_decimal128, 38, 2)))

	// Integer-division display derives from the dividend's precision and the
	// divisor's scale, so BOOL and INT64 divisors agree exactly.
	boolDiv, ok := DecimalIntegerDivReturnType([]types.Type{d10s2, boolT})
	require.True(t, ok)
	intDiv, ok := DecimalIntegerDivReturnType([]types.Type{d10s2, intT})
	require.True(t, ok)
	require.Equal(t, intDiv, boolDiv)
	require.Equal(t, types.New(types.T_int64, 9, 0), boolDiv)
}
