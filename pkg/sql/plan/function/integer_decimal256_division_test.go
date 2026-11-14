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
	"math/big"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The "/" admission table (fixedBinaryCastRule2) binds DECIMAL256 dividends
// with integer divisors, but its integer-dividend mirror was missing: every
// integer LEFT x DECIMAL256 RIGHT pair errored with an invalid-argument
// operator message while d / u executed. These cases pin the admitted rows,
// the implicit-cast targets, MySQL division return typing derived from the
// ORIGINAL integer operand, real column producer/consumer values through the
// public binding, and the unchanged neighbor cells.

func decimal256AtScaleTestHelper(t *testing.T, digits string, scale int32) types.Decimal256 {
	t.Helper()
	z, ok := new(big.Int).SetString(digits, 10)
	require.True(t, ok, "bad decimal literal %q", digits)
	if scale > 0 {
		z.Mul(z, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
	}
	return decimal256FromBigWideTest(z)
}

func decimal256FromUint64TestHelper(v uint64) types.Decimal256 {
	return decimal256FromBigWideTest(new(big.Int).SetUint64(v))
}

func parseDecimal256LiteralTestHelper(t *testing.T, literal string) types.Decimal256 {
	t.Helper()
	value, _, err := types.Parse256(literal)
	require.NoError(t, err)
	return value
}

// boundDecimal256SlashExecution binds "/" publicly and returns the overload
// execution fabric the colexec expression executor obtains for the same
// overload id, so the consumer path is the production one.
func boundDecimal256SlashExecution(
	t *testing.T, name string, args []types.Type,
) (types.Type, executeLogicOfOverload) {
	t.Helper()
	bound, err := GetFunctionByName(context.Background(), name, args)
	require.NoError(t, err)
	overload, err := GetFunctionById(context.Background(), bound.GetEncodedOverloadID())
	require.NoError(t, err)
	if overload.newOpWithFree != nil {
		exec, reset, free, retained := overload.newOpWithFree()
		t.Cleanup(func() { _ = free() })
		t.Cleanup(func() { _ = reset() })
		_ = retained
		return bound.GetReturnType(), exec
	}
	return bound.GetReturnType(), overload.newOp()
}

func TestIntegerOverDecimal256DivisionBinding(t *testing.T) {
	divisor := types.New(types.T_decimal256, 65, 30)
	for _, tc := range []struct {
		name      string
		left      types.T
		precision int32
	}{
		{"int8", types.T_int8, 37},
		{"int16", types.T_int16, 39},
		{"int32", types.T_int32, 44},
		{"int64", types.T_int64, 53},
		{"uint8", types.T_uint8, 37},
		{"uint16", types.T_uint16, 39},
		{"uint32", types.T_uint32, 44},
		{"uint64", types.T_uint64, 54},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := GetFunctionByName(
				context.Background(), "/", []types.Type{tc.left.ToType(), divisor})
			require.NoError(t, err)
			require.Equal(t,
				types.New(types.T_decimal256, tc.precision, 4), bound.GetReturnType())
			targets, cast := bound.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			// The integer operand casts through the DECIMAL256 default physical
			// type at scale zero, mirroring the legacy reverse rows; the
			// DECIMAL256 divisor keeps its declared width and scale.
			require.Equal(t, types.T_decimal256.ToType(), targets[0])
			require.Equal(t, types.New(types.T_decimal256, 65, 30), targets[1])
		})
	}
}

func TestIntegerOverDecimal256DivisionBindingControls(t *testing.T) {
	divisor := types.New(types.T_decimal256, 65, 30)

	t.Run("decimal256 dividend over integers keeps the reverse contract", func(t *testing.T) {
		for _, right := range []types.Type{
			types.T_int64.ToType(), types.T_uint8.ToType(),
		} {
			bound, err := GetFunctionByName(
				context.Background(), "/", []types.Type{divisor, right})
			require.NoError(t, err)
			require.Equal(t, types.T_decimal256, bound.GetReturnType().Oid)
			targets, cast := bound.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			require.Equal(t, types.T_decimal256, targets[0].Oid)
			require.Equal(t, types.T_decimal256, targets[1].Oid)
		}
	})

	t.Run("integer over decimal128 stays on the decimal128 cast", func(t *testing.T) {
		bound, err := GetFunctionByName(context.Background(), "/", []types.Type{
			types.T_int8.ToType(), types.New(types.T_decimal128, 38, 10)})
		require.NoError(t, err)
		require.Equal(t, types.T_decimal128, bound.GetReturnType().Oid)
		targets, cast := bound.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, types.T_decimal128, targets[0].Oid)
		require.Equal(t, types.T_decimal128, targets[1].Oid)
	})

	t.Run("integer over integer still binds the double path", func(t *testing.T) {
		bound, err := GetFunctionByName(context.Background(), "/", []types.Type{
			types.T_int64.ToType(), types.T_int64.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_float64, bound.GetReturnType().Oid)
		_, err = GetFunctionByName(context.Background(), "/", []types.Type{
			types.T_uint32.ToType(), types.T_int64.ToType()})
		require.NoError(t, err)
	})

	t.Run("float over decimal256 still falls back to double", func(t *testing.T) {
		bound, err := GetFunctionByName(context.Background(), "/", []types.Type{
			types.T_float64.ToType(), divisor})
		require.NoError(t, err)
		require.Equal(t, types.T_float64, bound.GetReturnType().Oid)
	})

	t.Run("decimal256 over decimal256 keeps the same-type cell", func(t *testing.T) {
		_, err := GetFunctionByName(context.Background(), "/", []types.Type{
			divisor, types.T_decimal256.ToType()})
		require.NoError(t, err)
	})

	t.Run("bool dividend stays unbound for a decimal256 divisor", func(t *testing.T) {
		_, err := GetFunctionByName(context.Background(), "/", []types.Type{
			types.T_bool.ToType(), divisor})
		require.Error(t, err)
	})

	t.Run("existing DIV cells keep their contracts", func(t *testing.T) {
		bound, err := GetFunctionByName(context.Background(), "div", []types.Type{
			types.T_uint64.ToType(), types.New(types.T_decimal128, 20, 3)})
		require.NoError(t, err)
		require.Equal(t, types.T_uint64, bound.GetReturnType().Oid)

		_, err = GetFunctionByName(context.Background(), "div", []types.Type{
			divisor, types.T_uint32.ToType()})
		require.NoError(t, err)
	})
}

// TestIntegerOverDecimal256DivisionColumns exercises the producer (implicit
// cast kernel the new binding selects) and the consumer (the bound "/"
// overload execution on cast column operands), including INT64_MIN/UINT64_MAX
// exactness and NULL propagation in both operand positions.
func TestIntegerOverDecimal256DivisionColumns(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	divisor := types.New(types.T_decimal256, 65, 30)
	castTarget := types.New(types.T_decimal256, 65, 0)

	t.Run("implicit cast target keeps signed extremes exact", func(t *testing.T) {
		signed := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64, 7, 0}, []bool{false, false, true}),
				NewFunctionTestInput(castTarget, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(castTarget, false, []types.Decimal256{
				decimal256FromBigWideTest(big.NewInt(math.MinInt64)),
				decimal256FromBigWideTest(big.NewInt(7)),
				{},
			}, []bool{false, false, true}),
			NewCast)
		succeed, info := signed.RunAndFree()
		require.True(t, succeed, info)

		unsigned := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{math.MaxUint64, 7, 0}, []bool{false, false, true}),
				NewFunctionTestInput(castTarget, []types.Decimal256{}, nil),
			},
			NewFunctionTestResult(castTarget, false, []types.Decimal256{
				decimal256FromUint64TestHelper(math.MaxUint64),
				decimal256FromUint64TestHelper(7),
				{},
			}, []bool{false, false, true}),
			NewCast)
		succeed, info = unsigned.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("uint64 column fixture", func(t *testing.T) {
		retType, exec := boundDecimal256SlashExecution(
			t, "/", []types.Type{types.T_uint64.ToType(), divisor})
		columnCase := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(castTarget, []types.Decimal256{
					decimal256FromUint64TestHelper(10),
					decimal256FromUint64TestHelper(math.MaxUint64),
					{},
					decimal256FromUint64TestHelper(1),
				}, []bool{false, false, true, false}),
				NewFunctionTestInput(divisor, []types.Decimal256{
					decimal256AtScaleTestHelper(t, "2", 30),
					decimal256AtScaleTestHelper(t, "1", 30),
					decimal256AtScaleTestHelper(t, "2", 30),
					{},
				}, []bool{false, false, false, true}),
			},
			NewFunctionTestResult(retType, false, []types.Decimal256{
				parseDecimal256LiteralTestHelper(t, "5.0000"),
				parseDecimal256LiteralTestHelper(t, "18446744073709551615.0000"),
				{},
				{},
			}, []bool{false, false, true, true}),
			exec)
		succeed, info := columnCase.RunAndFree()
		require.True(t, succeed, info)
	})

	t.Run("int64 column fixture", func(t *testing.T) {
		retType, exec := boundDecimal256SlashExecution(
			t, "/", []types.Type{types.T_int64.ToType(), divisor})
		columnCase := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(castTarget, []types.Decimal256{
					decimal256FromBigWideTest(big.NewInt(10)),
					decimal256FromBigWideTest(big.NewInt(math.MinInt64)),
					{},
					decimal256FromBigWideTest(big.NewInt(1)),
				}, []bool{false, false, true, false}),
				NewFunctionTestInput(divisor, []types.Decimal256{
					decimal256AtScaleTestHelper(t, "2", 30),
					decimal256AtScaleTestHelper(t, "1", 30),
					decimal256AtScaleTestHelper(t, "2", 30),
					{},
				}, []bool{false, false, false, true}),
			},
			NewFunctionTestResult(retType, false, []types.Decimal256{
				parseDecimal256LiteralTestHelper(t, "5.0000"),
				parseDecimal256LiteralTestHelper(t, "-9223372036854775808.0000"),
				{},
				{},
			}, []bool{false, false, true, true}),
			exec)
		succeed, info := columnCase.RunAndFree()
		require.True(t, succeed, info)
	})

	widths := []struct {
		name   string
		typ    types.Type
		digits string
		want   string
	}{
		{"int8", types.T_int8.ToType(), "127", "63.5000"},
		{"int16", types.T_int16.ToType(), "32767", "16383.5000"},
		{"int32", types.T_int32.ToType(), "2147483647", "1073741823.5000"},
		{"uint8", types.T_uint8.ToType(), "255", "127.5000"},
		{"uint16", types.T_uint16.ToType(), "65535", "32767.5000"},
		{"uint32", types.T_uint32.ToType(), "4294967295", "2147483647.5000"},
	}
	for _, tc := range widths {
		t.Run(tc.name+" column value", func(t *testing.T) {
			retType, exec := boundDecimal256SlashExecution(
				t, "/", []types.Type{tc.typ, divisor})
			z, ok := new(big.Int).SetString(tc.digits, 10)
			require.True(t, ok)
			columnCase := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(castTarget,
						[]types.Decimal256{decimal256FromBigWideTest(z)}, nil),
					NewFunctionTestInput(divisor,
						[]types.Decimal256{decimal256AtScaleTestHelper(t, "2", 30)}, nil),
				},
				NewFunctionTestResult(retType, false,
					[]types.Decimal256{parseDecimal256LiteralTestHelper(t, tc.want)}, nil),
				exec)
			succeed, info := columnCase.RunAndFree()
			require.True(t, succeed, info)
		})
	}
}

func TestIntegerOverDecimal256DivisionResolver(t *testing.T) {
	divisor := types.New(types.T_decimal256, 65, 30)
	for _, tc := range []struct {
		name      string
		left      types.Type
		precision int32
	}{
		{"int64", types.T_int64.ToType(), 53},
		{"uint64", types.T_uint64.ToType(), 54},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := GetFunctionByName(
				context.Background(), "/", []types.Type{tc.left, divisor})
			require.NoError(t, err)

			// The prepared-statement numeric resolver delegates operand casting
			// to the same owner as the binder: identical implicit-cast targets,
			// and a DECIMAL256 division result typing at scale 4.
			castLeft, castRight, numeric, ok := ResolveNumericBinaryTypes(
				"/", tc.left, divisor, nil)
			require.True(t, ok)
			targets, cast := bound.ShouldDoImplicitTypeCast()
			require.True(t, cast)
			require.Equal(t, targets[0], castLeft)
			require.Equal(t, targets[1], castRight)
			require.Equal(t, types.T_decimal256, numeric.Oid)
			require.Equal(t, int32(4), numeric.Scale)

			// "/" has no historical-era branch in the resolver: legacyDecimalDiv
			// gates integer DIV only, so both modes resolve the pair the same way.
			for _, legacy := range []bool{false, true} {
				resolved, ok := resolveNumericBinaryTypesMode(
					numericOpDiv, tc.left, divisor, nil, legacy)
				require.True(t, ok)
				require.Equal(t, castLeft, resolved.left)
				require.Equal(t, castRight, resolved.right)
				require.Equal(t, numeric.Oid, resolved.result.Oid)
				require.Equal(t, numeric.Scale, resolved.result.Scale)
			}
		})
	}

	t.Run("div side of the shared table", func(t *testing.T) {
		// The unsigned-mixed and decimal256-dividend DIV cells predate this
		// change and must keep resolving untouched.
		for _, mode := range []bool{false, true} {
			_, ok := resolveNumericBinaryTypesMode(
				numericOpIntegerDiv, types.T_uint64.ToType(),
				types.New(types.T_decimal128, 20, 3), nil, mode)
			require.True(t, ok)
			_, ok = resolveNumericBinaryTypesMode(
				numericOpIntegerDiv, divisor, types.T_uint32.ToType(), nil, mode)
			require.True(t, ok)
		}

		// The shared cast-table admission also reaches the otherwise
		// unsupported signed-integer DIV fallback in both eras.
		for _, mode := range []bool{false, true} {
			_, ok := resolveNumericBinaryTypesMode(
				numericOpIntegerDiv, types.T_int64.ToType(), divisor, nil, mode)
			require.True(t, ok) // still an unsupported pair before the rows land
		}
	})
}

// TestIntegerOverDecimal256DivisionSignedDIV closes the DIV consumer of the
// shared cast admission end to end: the added rows newly route a signed
// integer dividend with a DECIMAL256 divisor through the public "div" binder
// (overload 0, signed BIGINT quotient with MySQL's sign-inclusive display
// width) onto fixedTypeCastRule2's physical targets — the decimal256 default
// scale-0 dividend cast and the preserved (65,30) divisor — identical in both
// resolver eras; the production cast kernel keeps INT64_MIN sign-extended; the
// real overload executor truncates toward zero, keeps the existing NULL and
// zero-divisor policy, and surfaces the kernel's typed overflow identity for
// INT64_MIN DIV -1 instead of wrapping it.
func TestIntegerOverDecimal256DivisionSignedDIV(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	signedCol := types.T_int64.ToType()
	divisor := types.New(types.T_decimal256, 65, 30)

	// Public binding: signed BIGINT dividend over DECIMAL256 divisor lands on
	// the signed decimal256 DIV overload; DecimalIntegerDivReturnType counts
	// 19 dividend digits capped by 19 plus the sign.
	bound, err := GetFunctionByName(context.Background(), "div",
		[]types.Type{signedCol, divisor})
	require.NoError(t, err)
	require.Equal(t, types.New(types.T_int64, 20, 0), bound.GetReturnType())
	targets, cast := bound.ShouldDoImplicitTypeCast()
	require.True(t, cast)
	require.Equal(t, types.T_decimal256.ToType(), targets[0])
	require.Equal(t, types.New(types.T_decimal256, 65, 30), targets[1])

	// Both resolver eras delegate to the same physical casts and quotient
	// domain; only the shared cast-table rows changed.
	for _, legacy := range []bool{false, true} {
		resolved, ok := resolveNumericBinaryTypesMode(
			numericOpIntegerDiv, signedCol, divisor, nil, legacy)
		require.True(t, ok)
		require.Equal(t, targets[0], resolved.left)
		require.Equal(t, targets[1], resolved.right)
		require.Equal(t, types.T_int64, resolved.result.Oid)
	}

	// Producer: the production cast kernel at the binding-chosen target keeps
	// INT64_MIN sign-extended and NULL rows null.
	casted := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(signedCol,
				[]int64{math.MinInt64, 10, -7, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(targets[0], []types.Decimal256{}, nil),
		},
		NewFunctionTestResult(targets[0], false, []types.Decimal256{
			decimal256FromBigWideTest(big.NewInt(math.MinInt64)),
			decimal256FromBigWideTest(big.NewInt(10)),
			decimal256FromBigWideTest(big.NewInt(-7)),
			{},
		}, []bool{false, false, false, true}),
		NewCast)
	succeed, info := casted.RunAndFree()
	require.True(t, succeed, info)

	// Consumer: the bound overload executor truncates toward zero, propagates
	// NULL, and keeps the existing zero-divisor NULL policy.
	retType, exec := boundDecimal256SlashExecution(t, "div",
		[]types.Type{signedCol, divisor})
	require.Equal(t, bound.GetReturnType(), retType)
	quotients := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(targets[0], []types.Decimal256{
				decimal256FromBigWideTest(big.NewInt(10)),
				decimal256FromBigWideTest(big.NewInt(-7)),
				decimal256FromBigWideTest(big.NewInt(math.MinInt64)),
				{},
				decimal256FromBigWideTest(big.NewInt(1)),
			}, []bool{false, false, false, true, false}),
			NewFunctionTestInput(divisor, []types.Decimal256{
				decimal256AtScaleTestHelper(t, "2", 30),
				decimal256AtScaleTestHelper(t, "2", 30),
				decimal256AtScaleTestHelper(t, "1", 30),
				decimal256AtScaleTestHelper(t, "2", 30),
				{}, // zero divisor keeps the existing NULL policy
			}, nil),
		},
		NewFunctionTestResult(retType, false,
			[]int64{5, -3, math.MinInt64, 0, 0},
			[]bool{false, false, false, true, true}),
		exec)
	succeed, info = quotients.RunAndFree()
	require.True(t, succeed, info)

	// Error closure: INT64_MIN DIV -1 does not fit BIGINT. The executor
	// surfaces the existing kernel's typed out-of-range identity: the batch
	// wrapper wraps the decimal quotient-narrowing overflow detail into a
	// typed out-of-range error at the public executor boundary. The real
	// moerr code is asserted, never a bare error-string match.
	// The owned vectors and result wrapper are released by defers registered
	// immediately after each allocation, so a failed assertion (require's
	// Goexit) can never leak them; the failure path must not depend on the
	// tail of the test function to clean up.
	left := newVectorByType(proc.Mp(), targets[0],
		[]types.Decimal256{decimal256FromBigWideTest(big.NewInt(math.MinInt64))}, nil)
	defer left.Free(proc.Mp())
	right := newVectorByType(proc.Mp(), divisor,
		[]types.Decimal256{decimal256AtScaleTestHelper(t, "-1", 30)}, nil)
	defer right.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(retType, proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	err = exec([]*vector.Vector{left, right}, result, proc, 1, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange),
		"actual division overflow identity: %v", err)
}
