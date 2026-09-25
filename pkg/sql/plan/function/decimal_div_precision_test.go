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
	"math/big"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalDivisionTypeUsesPrecisionIncrement(t *testing.T) {
	decimal10Scale2 := types.New(types.T_decimal64, 10, 2)
	decimal50Scale10 := types.New(types.T_decimal256, 50, 10)
	integer := types.T_int64.ToType()

	tests := []struct {
		name      string
		increment int32
		inputs    []types.Type
		want      types.Type
		wantCasts []types.Type
	}{
		{
			name:      "zero increment",
			increment: 0,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 12, 2),
		},
		{
			name:      "default increment",
			increment: 4,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 16, 6),
		},
		{
			name:      "larger increment",
			increment: 10,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal128, 22, 12),
		},
		{
			name:      "precision promotes physical decimal",
			increment: 30,
			inputs:    []types.Type{decimal10Scale2, decimal10Scale2},
			want:      types.New(types.T_decimal256, 42, 30),
			wantCasts: []types.Type{
				types.New(types.T_decimal256, 10, 2),
				types.New(types.T_decimal256, 10, 2),
			},
		},
		{
			name:      "decimal256 precision and scale caps",
			increment: 30,
			inputs:    []types.Type{decimal50Scale10, integer},
			want:      types.New(types.T_decimal256, 65, 30),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := WithDivPrecisionIncrement(context.Background(), test.increment)
			resolved, err := GetFunctionByName(ctx, "/", test.inputs)
			require.NoError(t, err)
			require.Equal(t, test.want, resolved.GetReturnType())
			casts, shouldCast := resolved.ShouldDoImplicitTypeCast()
			if test.wantCasts != nil {
				require.True(t, shouldCast)
				require.Equal(t, test.wantCasts, casts)
			}
		})
	}

	resolved, err := GetFunctionByName(
		WithDivPrecisionIncrement(context.Background(), 30),
		"/",
		[]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
	)
	require.NoError(t, err)
	require.Equal(t, types.T_float64.ToType(), resolved.GetReturnType())
}

func TestDecimalDivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal64, 10, 2)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(inputType, []types.Decimal64{100}, nil),
		NewFunctionTestInput(inputType, []types.Decimal64{300}, nil),
	}

	for _, test := range []struct {
		name string
		typ  types.Type
		want types.Decimal128
	}{
		{
			name: "scale 2",
			typ:  types.New(types.T_decimal128, 12, 2),
			want: types.Decimal128{B0_63: 33},
		},
		{
			name: "scale 6",
			typ:  types.New(types.T_decimal128, 16, 6),
			want: types.Decimal128{B0_63: 333333},
		},
		{
			name: "scale 12",
			typ:  types.New(types.T_decimal128, 22, 12),
			want: types.Decimal128{B0_63: 333333333333},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			caseUnderTest := NewFunctionTestCase(
				proc,
				inputs,
				NewFunctionTestResult(test.typ, false, []types.Decimal128{test.want}, nil),
				divFn,
			)
			passed, info := caseUnderTest.Run()
			require.True(t, passed, info)
			require.Equal(t, test.typ, *caseUnderTest.GetResultVectorDirectly().GetType())
		})
	}
}

func TestDecimal256DivisionExecutionUsesBoundResultScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 10, 2)
	one, err := types.ParseDecimal256("1.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	three, err := types.ParseDecimal256("3.00", inputType.Width, inputType.Scale)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 42, 30)
	want, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{one}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{three}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
		divFn,
	)
	passed, info := caseUnderTest.Run()
	require.True(t, passed, info)
}

func TestDecimal256DivisionAvoidsScaledNumeratorOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal256, 65, 0)
	value, err := types.ParseDecimal256(
		"99999999999999999999999999999999999999999999999999999999999999999",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	third, err := types.ParseDecimal256(
		"33333333333333333333333333333333333333333333333333333333333333333",
		inputType.Width,
		inputType.Scale,
	)
	require.NoError(t, err)
	resultType := types.New(types.T_decimal256, 65, 30)
	wantOne, err := types.ParseDecimal256("1.000000000000000000000000000000", resultType.Width, resultType.Scale)
	require.NoError(t, err)
	wantThird, err := types.ParseDecimal256("0.333333333333333333333333333333", resultType.Width, resultType.Scale)
	require.NoError(t, err)

	caseUnderTest := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(inputType, []types.Decimal256{value, third}, nil),
			NewFunctionTestInput(inputType, []types.Decimal256{value, value}, nil),
		},
		NewFunctionTestResult(resultType, false, []types.Decimal256{wantOne, wantThird}, nil),
		divFn,
	)
	passed, info := caseUnderTest.Run()
	require.True(t, passed, info)
}

func TestDecimalDivisionScaleCapBelowInputScale(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("decimal128", func(t *testing.T) {
		leftType := types.New(types.T_decimal128, 38, 37)
		rightType := types.New(types.T_decimal128, 1, 0)
		resultType := types.New(types.T_decimal128, 38, 30)
		left, err := types.ParseDecimal128("0.1000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal128("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal128("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal128{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal128{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal128{want}, nil),
			divFn,
		)
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
	})

	t.Run("decimal256", func(t *testing.T) {
		leftType := types.New(types.T_decimal256, 65, 40)
		rightType := types.New(types.T_decimal256, 1, 0)
		resultType := types.New(types.T_decimal256, 65, 30)
		left, err := types.ParseDecimal256("0.1000000000000000000000000000000000000000", leftType.Width, leftType.Scale)
		require.NoError(t, err)
		right, err := types.ParseDecimal256("2", rightType.Width, rightType.Scale)
		require.NoError(t, err)
		want, err := types.ParseDecimal256("0.050000000000000000000000000000", resultType.Width, resultType.Scale)
		require.NoError(t, err)
		caseUnderTest := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(leftType, []types.Decimal256{left}, nil),
				NewFunctionTestInput(rightType, []types.Decimal256{right}, nil),
			},
			NewFunctionTestResult(resultType, false, []types.Decimal256{want}, nil),
			divFn,
		)
		passed, info := caseUnderTest.Run()
		require.True(t, passed, info)
	})
}

func TestDecimal256DivisionShiftBoundaries(t *testing.T) {
	denominator, err := types.ParseDecimal256("1"+strings.Repeat("0", 46), 47, 0)
	require.NoError(t, err)
	for _, test := range []struct {
		name, numerator, want string
	}{
		{"ordinary", "1" + strings.Repeat("0", 46), "1"},
		{"shifted numerator changes sign", "3" + strings.Repeat("0", 46), "3"},
		{"divisor alignment wraps", "28" + strings.Repeat("0", 45), "2.8"},
		{"scale multiplication overflows", "6" + strings.Repeat("0", 46), "6"},
	} {
		t.Run(test.name, func(t *testing.T) {
			numerator, err := types.ParseDecimal256(test.numerator, 47, 0)
			require.NoError(t, err)
			for _, signs := range []struct {
				negativeNumerator, negativeDenominator bool
			}{
				{false, false}, {true, false}, {false, true}, {true, true},
			} {
				x, y := numerator, denominator
				if signs.negativeNumerator {
					x = x.Minus()
				}
				if signs.negativeDenominator {
					y = y.Minus()
				}
				got := make([]types.Decimal256, 1)
				require.NoError(t, d256DivAtScale([]types.Decimal256{x}, []types.Decimal256{y},
					got, 0, 0, 30, nulls.NewWithSize(1), true))
				want := test.want
				if !strings.Contains(want, ".") {
					want += "."
				}
				want += strings.Repeat("0", 30-len(strings.SplitN(want, ".", 2)[1]))
				if signs.negativeNumerator != signs.negativeDenominator {
					want = "-" + want
				}
				require.Equal(t, want, got[0].Format(30), "signs=%+v", signs)
			}
		})
	}

	// A NULL row with a zero divisor must never enter the arithmetic loop.
	got := make([]types.Decimal256, 2)
	resultNulls := nulls.NewWithSize(2)
	resultNulls.Add(1)
	x, err := types.ParseDecimal256("28"+strings.Repeat("0", 45), 47, 0)
	require.NoError(t, err)
	require.NoError(t, d256DivAtScale([]types.Decimal256{x, x},
		[]types.Decimal256{denominator, {}}, got, 0, 0, 30, resultNulls, true))
	require.Equal(t, "2.8"+strings.Repeat("0", 29), got[0].Format(30))
	require.True(t, resultNulls.Contains(1))
}

func TestDecimal256DivisionInternalHeadroom(t *testing.T) {
	// These physical coefficients straddle the conservative 2^253 boundary.
	// The reference computes the quotient and remainder with independent big.Int
	// operations rather than calling the production fallback.
	maxLow := ^uint64(0)
	denominator := types.Decimal256{B0_63: 3, B128_191: 1}
	denominatorBig, ok := new(big.Int).SetString(denominator.Format(0), 10)
	require.True(t, ok)
	for _, numerator := range []types.Decimal256{
		{B0_63: maxLow, B64_127: maxLow, B128_191: maxLow, B192_255: (1 << 61) - 1},
		{B192_255: 1 << 61},
		{B0_63: 1, B192_255: 1 << 61},
		{B192_255: 1 << 62},
	} {
		numeratorBig, ok := new(big.Int).SetString(numerator.Format(0), 10)
		require.True(t, ok)
		want, remainder := new(big.Int), new(big.Int)
		want.QuoRem(numeratorBig, denominatorBig, remainder)
		if new(big.Int).Lsh(remainder, 1).Cmp(denominatorBig) >= 0 {
			want.Add(want, big.NewInt(1))
		}
		var got types.Decimal256
		require.NoError(t, d256DivAdjusted(numerator, denominator, 0, false, &got))
		require.Equal(t, want.String(), got.Format(0), "numerator=%s", numerator.Format(0))
	}
}

func TestDecimalDivisionNegativeScaleRoundsOnce(t *testing.T) {
	for _, test := range []struct {
		coefficient int64
		want        string
	}{
		{9, "0"}, {10, "1"}, {11, "1"}, {-9, "0"}, {-10, "-1"}, {-11, "-1"},
	} {
		coefficient := test.coefficient
		if coefficient < 0 {
			coefficient = -coefficient
		}
		x := types.Decimal128{B0_63: uint64(coefficient)}
		if test.coefficient < 0 {
			x = x.Minus()
		}
		y := types.Decimal128{B0_63: 2}
		got128 := make([]types.Decimal128, 1)
		require.NoError(t, d128DivAtScale([]types.Decimal128{x}, []types.Decimal128{y},
			got128, 1, 0, 0, nulls.NewWithSize(1), true))
		require.Equal(t, test.want, got128[0].Format(0))
		x256 := types.Decimal256{B0_63: uint64(coefficient)}
		if test.coefficient < 0 {
			x256 = x256.Minus()
		}
		got256 := make([]types.Decimal256, 1)
		require.NoError(t, d256DivAtScale([]types.Decimal256{x256},
			[]types.Decimal256{{B0_63: 2}}, got256, 1, 0, 0, nulls.NewWithSize(1), true))
		require.Equal(t, test.want, got256[0].Format(0))
	}
}

func TestDecimalDivisionNegativeScaleWideFallback(t *testing.T) {
	// Multiplying this divisor by ten exceeds signed D128, while the rounded
	// quotient remains representable. The D256 fixed-width path must take over.
	x, err := types.ParseDecimal128("9"+strings.Repeat("0", 36), 38, 1)
	require.NoError(t, err)
	y, err := types.ParseDecimal128("18"+strings.Repeat("0", 36), 38, 0)
	require.NoError(t, err)
	got128 := make([]types.Decimal128, 1)
	require.NoError(t, d128DivAtScale([]types.Decimal128{x}, []types.Decimal128{y},
		got128, 1, 0, 0, nulls.NewWithSize(1), true))
	require.Equal(t, "1", got128[0].Format(0))

	// A genuine D256 numerator takes the generic branch, and a divisor
	// multiplication that exceeds D256 takes the bounded wide fallback.
	wideX, err := types.ParseDecimal256("3000000000", 47, 37)
	require.NoError(t, err)
	wideY, err := types.ParseDecimal256("1"+strings.Repeat("0", 20), 21, 0)
	require.NoError(t, err)
	got256 := make([]types.Decimal256, 1)
	require.NoError(t, d256DivAtScale([]types.Decimal256{wideX}, []types.Decimal256{wideY},
		got256, 37, 0, 30, nulls.NewWithSize(1), true))
	require.Equal(t, "0.00000000003"+strings.Repeat("0", 19), got256[0].Format(30))

	maxCoefficient := "1" + strings.Repeat("0", 64)
	hugeX, err := types.ParseDecimal256("1"+strings.Repeat("0", 14), 65, 50)
	require.NoError(t, err)
	hugeY, err := types.ParseDecimal256(maxCoefficient, 65, 0)
	require.NoError(t, err)
	require.NoError(t, d256DivAtScale([]types.Decimal256{hugeX}, []types.Decimal256{hugeY},
		got256, 50, 0, 30, nulls.NewWithSize(1), true))
	require.Equal(t, "0."+strings.Repeat("0", 30), got256[0].Format(30))
}

func TestDecimal256DivisionDeclaredPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	leftType := types.New(types.T_decimal256, 38, 0)
	rightType := types.New(types.T_decimal256, 38, 30)
	resultType := types.New(types.T_decimal256, 65, 0)
	left, err := types.ParseDecimal256("1"+strings.Repeat("0", 37), 38, 0)
	require.NoError(t, err)
	right, err := types.ParseDecimal256("0."+strings.Repeat("0", 29)+"1", 38, 30)
	require.NoError(t, err)
	test := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(leftType, []types.Decimal256{left}, nil),
			NewFunctionTestInput(rightType, []types.Decimal256{right}, nil)},
		NewFunctionTestResult(resultType, true, nil, nil), divFn)
	passed, info := test.Run()
	require.True(t, passed, info)

	// A true half-up carry across the bound can be made at this typed physical
	// boundary even though the corresponding numerator exceeds a legal SQL type.
	physicalLeft := types.New(types.T_decimal256, 76, 0)
	carry, err := types.ParseDecimal256("2"+strings.Repeat("0", 65), 76, 0)
	require.NoError(t, err)
	carry, err = carry.Sub256(types.Decimal256{B0_63: 1})
	require.NoError(t, err)
	test = NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(physicalLeft, []types.Decimal256{carry}, nil),
			NewFunctionTestInput(physicalLeft, []types.Decimal256{{B0_63: 2}}, nil)},
		NewFunctionTestResult(resultType, true, nil, nil), divFn)
	passed, info = test.Run()
	require.True(t, passed, info)
}

func BenchmarkDecimalDivisionScaleCap(b *testing.B) {
	const rows = 1024
	x, err := types.ParseDecimal128("0.1", 38, 37)
	if err != nil {
		b.Fatal(err)
	}
	xs, ys, results := make([]types.Decimal128, rows), make([]types.Decimal128, rows), make([]types.Decimal128, rows)
	for i := range xs {
		xs[i], ys[i] = x, types.Decimal128{B0_63: 2}
	}
	resultNulls := nulls.NewWithSize(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := d128DivAtScale(xs, ys, results, 37, 0, 30, resultNulls, true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecimal256DivisionScaleCap(b *testing.B) {
	const rows = 1024
	x, err := types.ParseDecimal256("0.1", 65, 40)
	if err != nil {
		b.Fatal(err)
	}
	xs, ys, results := make([]types.Decimal256, rows), make([]types.Decimal256, rows), make([]types.Decimal256, rows)
	for i := range xs {
		xs[i], ys[i] = x, types.Decimal256{B0_63: 2}
	}
	resultNulls := nulls.NewWithSize(rows)
	for _, test := range []struct {
		name     string
		divisors []types.Decimal256
	}{
		{"vector", ys},
		{"constant", ys[:1]},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := d256DivAtScale(xs, test.divisors, results, 40, 0, 30, resultNulls, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
